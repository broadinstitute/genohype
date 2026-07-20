//! Benchmark metrics collection for export operations
//!
//! Collects CPU, memory, and I/O metrics during export to help
//! identify bottlenecks and guide scaling decisions.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "benchmark")]
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
#[cfg(feature = "benchmark")]
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, RefreshKind, System};

/// Input table metadata for the benchmark report
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct InputMetadata {
    /// Path to the input table
    pub path: String,
    /// Total number of partitions
    pub num_partitions: usize,
    /// Total size of input data in bytes (if known)
    pub total_size_bytes: Option<u64>,
    /// Key fields
    pub key_fields: Vec<String>,
    /// Number of fields in schema
    pub num_fields: usize,
}

// Re-export progress types from core
pub use genohype_core::progress::RowSizeStats;

/// A single snapshot of system metrics
#[allow(dead_code)]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MetricsSample {
    /// Timestamp (seconds since start)
    pub elapsed_secs: f64,
    /// CPU usage percentage (0-100, averaged across all cores)
    pub cpu_percent: f32,
    /// Per-core CPU usage
    pub cpu_per_core: Vec<f32>,
    /// Memory used in bytes
    pub memory_used: u64,
    /// Memory total in bytes
    pub memory_total: u64,
    /// Disk read bytes since last sample
    pub disk_read_bytes: u64,
    /// Disk write bytes since last sample
    pub disk_write_bytes: u64,
    /// Disk read bytes/sec (computed from delta)
    pub disk_read_bytes_sec: u64,
    /// Disk write bytes/sec (computed from delta)
    pub disk_write_bytes_sec: u64,
    /// Rows processed so far
    pub rows_processed: usize,
    /// Partitions completed so far
    pub partitions_completed: usize,
}

/// Summary statistics from a benchmark run
#[derive(Debug, serde::Deserialize)]
pub struct BenchmarkReport {
    /// Total duration
    pub duration: Duration,
    /// All collected samples
    pub samples: Vec<MetricsSample>,
    /// Total rows processed
    pub total_rows: usize,
    /// Total partitions
    pub total_partitions: usize,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Input table metadata
    pub input_metadata: Option<InputMetadata>,
    /// Output path
    pub output_path: Option<String>,
    /// Output size in bytes (measured at end)
    pub output_size_bytes: Option<u64>,
    /// Available disk space at start (bytes)
    pub disk_space_available: Option<u64>,
    /// Total disk space (bytes)
    pub disk_space_total: Option<u64>,
    /// Row size statistics from sampling
    pub row_size_stats: Option<RowSizeStats>,
}

impl BenchmarkReport {
    /// Calculate average CPU utilization
    pub fn avg_cpu_percent(&self) -> f32 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples.iter().map(|s| s.cpu_percent).sum::<f32>() / self.samples.len() as f32
    }

    /// Calculate max CPU utilization
    pub fn max_cpu_percent(&self) -> f32 {
        self.samples
            .iter()
            .map(|s| s.cpu_percent)
            .fold(0.0, f32::max)
    }

    /// Calculate average memory usage in GB
    pub fn avg_memory_gb(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let avg_bytes =
            self.samples.iter().map(|s| s.memory_used).sum::<u64>() / self.samples.len() as u64;
        avg_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Calculate max memory usage in GB
    pub fn max_memory_gb(&self) -> f64 {
        let max_bytes = self
            .samples
            .iter()
            .map(|s| s.memory_used)
            .max()
            .unwrap_or(0);
        max_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Calculate total memory in GB
    pub fn total_memory_gb(&self) -> f64 {
        let total = self.samples.first().map(|s| s.memory_total).unwrap_or(0);
        total as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Calculate rows per second throughput
    pub fn rows_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.total_rows as f64 / secs
        } else {
            0.0
        }
    }

    /// Calculate partitions per second throughput
    pub fn partitions_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.total_partitions as f64 / secs
        } else {
            0.0
        }
    }

    /// Calculate average disk read MB/s
    pub fn avg_disk_read_mb_sec(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let avg = self
            .samples
            .iter()
            .map(|s| s.disk_read_bytes_sec)
            .sum::<u64>()
            / self.samples.len() as u64;
        avg as f64 / (1024.0 * 1024.0)
    }

    /// Calculate average disk write MB/s
    pub fn avg_disk_write_mb_sec(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let avg = self
            .samples
            .iter()
            .map(|s| s.disk_write_bytes_sec)
            .sum::<u64>()
            / self.samples.len() as u64;
        avg as f64 / (1024.0 * 1024.0)
    }

    /// Calculate max disk write MB/s
    pub fn max_disk_write_mb_sec(&self) -> f64 {
        let max = self
            .samples
            .iter()
            .map(|s| s.disk_write_bytes_sec)
            .max()
            .unwrap_or(0);
        max as f64 / (1024.0 * 1024.0)
    }

    /// Calculate max disk read MB/s
    pub fn max_disk_read_mb_sec(&self) -> f64 {
        let max = self
            .samples
            .iter()
            .map(|s| s.disk_read_bytes_sec)
            .max()
            .unwrap_or(0);
        max as f64 / (1024.0 * 1024.0)
    }

    /// Determine the likely bottleneck
    pub fn identify_bottleneck(&self) -> Bottleneck {
        let avg_cpu = self.avg_cpu_percent();
        let max_mem = self.max_memory_gb();
        let total_mem = self.total_memory_gb();
        let mem_usage_pct = if total_mem > 0.0 {
            max_mem / total_mem * 100.0
        } else {
            0.0
        };

        // Check if we're CPU-bound (high CPU utilization)
        if avg_cpu > 80.0 {
            return Bottleneck::Cpu {
                utilization: avg_cpu,
            };
        }

        // Check if we're memory-bound
        if mem_usage_pct > 85.0 {
            return Bottleneck::Memory {
                usage_percent: mem_usage_pct as f32,
            };
        }

        // Check disk I/O
        let avg_disk_write = if self.samples.is_empty() {
            0
        } else {
            self.samples
                .iter()
                .map(|s| s.disk_write_bytes_sec)
                .sum::<u64>()
                / self.samples.len() as u64
        };

        // If CPU is low but we're writing data, might be I/O bound
        if avg_cpu < 50.0 && avg_disk_write > 0 {
            return Bottleneck::Io {
                write_mb_sec: avg_disk_write as f64 / (1024.0 * 1024.0),
            };
        }

        Bottleneck::Unknown { cpu: avg_cpu }
    }

    /// Generate scaling recommendations
    pub fn scaling_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        let bottleneck = self.identify_bottleneck();

        match bottleneck {
            Bottleneck::Cpu { utilization } => {
                recommendations.push(format!(
                    "CPU-bound ({:.0}% avg utilization) - more CPUs should improve performance",
                    utilization
                ));

                // Estimate scaling
                let current_time = self.duration.as_secs_f64();
                let efficiency = utilization / 100.0;
                if efficiency > 0.5 {
                    recommendations.push(format!(
                        "Estimated with 2x CPUs: {:.1}h (assuming linear scaling)",
                        current_time / 2.0 / 3600.0
                    ));
                }
            }
            Bottleneck::Memory { usage_percent } => {
                recommendations.push(format!(
                    "Memory-bound ({:.0}% usage) - more RAM may help, or reduce --shard-count",
                    usage_percent
                ));
            }
            Bottleneck::Io { write_mb_sec } => {
                recommendations.push(format!(
                    "I/O-bound ({:.0} MB/s writes) - faster storage may help, more CPUs won't",
                    write_mb_sec
                ));
            }
            Bottleneck::Unknown { cpu } => {
                if cpu < 30.0 {
                    recommendations.push(format!(
                        "Low CPU utilization ({:.0}%) - may be I/O or network bound",
                        cpu
                    ));
                } else {
                    recommendations.push(format!(
                        "Moderate CPU utilization ({:.0}%) - mixed workload",
                        cpu
                    ));
                }
            }
        }

        recommendations
    }

    /// Print a formatted report
    pub fn print(&self) {
        use owo_colors::OwoColorize;

        println!();
        println!("{}", "Benchmark Report".bold().underline());
        println!();

        // Input table info
        if let Some(ref meta) = self.input_metadata {
            println!("{}", "Input:".green());
            println!("  {} {}", "Path:".cyan(), meta.path);
            println!("  {} {}", "Partitions:".cyan(), meta.num_partitions);
            if let Some(size) = meta.total_size_bytes {
                println!(
                    "  {} {:.2} GB",
                    "Size:".cyan(),
                    size as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            }
            println!("  {} {:?}", "Key fields:".cyan(), meta.key_fields);
            println!("  {} {}", "Schema fields:".cyan(), meta.num_fields);
            println!();
        }

        // Row size statistics
        if let Some(ref stats) = self.row_size_stats {
            println!("{}", "Decoded Row Size (in-memory, sampled):".green());
            println!("  {} {} rows", "Samples:".cyan(), stats.sample_count);
            println!(
                "  {} {:.1} KB",
                "Average:".cyan(),
                stats.avg_bytes() / 1024.0
            );
            println!(
                "  {} {:.1} KB",
                "Min:".cyan(),
                stats.min_bytes as f64 / 1024.0
            );
            println!(
                "  {} {:.1} KB",
                "Max:".cyan(),
                stats.max_bytes as f64 / 1024.0
            );
            if let Some((total_fields, max_depth, array_count)) = stats.schema_stats {
                println!(
                    "  {} {} fields, depth {}, {} arrays",
                    "Schema:".cyan(),
                    total_fields,
                    max_depth,
                    array_count
                );
            }
            // Estimate total decoded data size
            let estimated_total = stats.avg_bytes() * self.total_rows as f64;
            println!(
                "  {} {:.2} GB (uncompressed)",
                "Est. memory footprint:".cyan(),
                estimated_total / (1024.0 * 1024.0 * 1024.0)
            );

            // Show per-field breakdown (top 10 by size)
            let sorted_fields = stats.sorted_field_stats();
            if !sorted_fields.is_empty() {
                println!();
                println!("{}", "  Field Contribution (% of decoded row):".green());
                let total_avg = stats.avg_bytes();
                for (i, field) in sorted_fields.iter().take(10).enumerate() {
                    let pct = if total_avg > 0.0 {
                        field.avg_bytes() / total_avg * 100.0
                    } else {
                        0.0
                    };
                    println!(
                        "  {:2}. {:30} {:>8.1} B ({:>5.1}%)  {}",
                        i + 1,
                        field.name.cyan(),
                        field.avg_bytes(),
                        pct,
                        field.type_desc.dimmed()
                    );
                }
                if sorted_fields.len() > 10 {
                    println!("      ... and {} more fields", sorted_fields.len() - 10);
                }
            }
            println!();
        }

        // Output info
        if let Some(ref path) = self.output_path {
            println!("{}", "Output:".green());
            println!("  {} {}", "Path:".cyan(), path);
            if let Some(size) = self.output_size_bytes {
                println!(
                    "  {} {:.2} GB",
                    "Size:".cyan(),
                    size as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            }
            println!();
        }

        // Duration and throughput
        println!("{}", "Performance:".green());
        println!(
            "  {} {:.1}s ({:.1}m)",
            "Duration:".cyan(),
            self.duration.as_secs_f64(),
            self.duration.as_secs_f64() / 60.0
        );
        println!(
            "  {} {:.0} rows/sec",
            "Throughput:".cyan(),
            self.rows_per_sec()
        );
        println!(
            "  {} {:.2} partitions/sec",
            "Partition rate:".cyan(),
            self.partitions_per_sec()
        );
        if let (Some(in_meta), Some(out_size)) = (&self.input_metadata, self.output_size_bytes) {
            if let Some(in_size) = in_meta.total_size_bytes {
                let compression_ratio = in_size as f64 / out_size as f64;
                println!("  {} {:.2}x", "Compression:".cyan(), compression_ratio);
            }
        }
        println!();

        // CPU metrics
        println!("{}", "CPU:".green());
        println!("  {} {}", "Cores:".cyan(), self.num_cpus);
        println!(
            "  {} {:.1}%",
            "Avg utilization:".cyan(),
            self.avg_cpu_percent()
        );
        println!(
            "  {} {:.1}%",
            "Max utilization:".cyan(),
            self.max_cpu_percent()
        );
        println!();

        // Memory metrics
        println!("{}", "Memory:".green());
        println!("  {} {:.1} GB", "Total:".cyan(), self.total_memory_gb());
        println!("  {} {:.1} GB", "Avg used:".cyan(), self.avg_memory_gb());
        let mem_pct = if self.total_memory_gb() > 0.0 {
            self.max_memory_gb() / self.total_memory_gb() * 100.0
        } else {
            0.0
        };
        println!(
            "  {} {:.1} GB ({:.0}%)",
            "Max used:".cyan(),
            self.max_memory_gb(),
            mem_pct
        );
        println!();

        // Disk I/O metrics
        println!("{}", "Disk I/O:".green());
        println!(
            "  {} {:.1} MB/s (avg), {:.1} MB/s (max)",
            "Read:".cyan(),
            self.avg_disk_read_mb_sec(),
            self.max_disk_read_mb_sec()
        );
        println!(
            "  {} {:.1} MB/s (avg), {:.1} MB/s (max)",
            "Write:".cyan(),
            self.avg_disk_write_mb_sec(),
            self.max_disk_write_mb_sec()
        );
        if let (Some(avail), Some(total)) = (self.disk_space_available, self.disk_space_total) {
            println!(
                "  {} {:.1} GB / {:.1} GB ({:.0}% free)",
                "Space:".cyan(),
                avail as f64 / (1024.0 * 1024.0 * 1024.0),
                total as f64 / (1024.0 * 1024.0 * 1024.0),
                avail as f64 / total as f64 * 100.0
            );
        }
        println!();

        // Bottleneck analysis
        println!("{}", "Analysis:".green());
        let bottleneck = self.identify_bottleneck();
        println!("  {} {:?}", "Bottleneck:".cyan(), bottleneck);
        println!();

        // Recommendations
        println!("{}", "Recommendations:".green());
        for rec in self.scaling_recommendations() {
            println!("  {} {}", "-".cyan(), rec);
        }
    }

    /// Merge another report into this one (aggregating cluster totals).
    ///
    /// Used by the pool coordinator to combine results from multiple workers.
    pub fn merge(&mut self, other: BenchmarkReport) {
        // Sum totals
        self.total_rows += other.total_rows;
        self.total_partitions += other.total_partitions;

        // Sum output sizes if available
        match (self.output_size_bytes, other.output_size_bytes) {
            (Some(s1), Some(s2)) => self.output_size_bytes = Some(s1 + s2),
            (None, Some(s2)) => self.output_size_bytes = Some(s2),
            _ => {}
        }

        // Append samples for aggregate CPU/memory stats
        self.samples.extend(other.samples);

        // Duration is max (wall clock time for cluster job)
        self.duration = std::cmp::max(self.duration, other.duration);

        // Sum CPU cores (cluster total)
        self.num_cpus += other.num_cpus;
    }

    /// Create an empty report for aggregation.
    pub fn empty() -> Self {
        Self {
            duration: std::time::Duration::default(),
            samples: vec![],
            total_rows: 0,
            total_partitions: 0,
            num_cpus: 0,
            input_metadata: None,
            output_path: None,
            output_size_bytes: None,
            disk_space_available: None,
            disk_space_total: None,
            row_size_stats: None,
        }
    }
}

/// Identified bottleneck type
#[derive(Debug, Clone)]
pub enum Bottleneck {
    Cpu { utilization: f32 },
    Memory { usage_percent: f32 },
    Io { write_mb_sec: f64 },
    Unknown { cpu: f32 },
}

/// Background metrics collector
pub struct MetricsCollector {
    /// Flag to signal the collector to stop
    stop_flag: Arc<AtomicBool>,
    /// Counter for rows processed (updated by caller)
    pub rows_counter: Arc<AtomicUsize>,
    /// Counter for partitions completed (updated by caller)
    pub partitions_counter: Arc<AtomicUsize>,
    /// Background thread handle
    handle: Option<JoinHandle<(Vec<MetricsSample>, Option<u64>, Option<u64>)>>,
    /// Start time
    start_time: Instant,
    /// Number of CPUs
    num_cpus: usize,
    /// Total partitions (for report)
    total_partitions: usize,
    /// Input table metadata
    input_metadata: Option<InputMetadata>,
    /// Output path
    output_path: Option<String>,
    /// Row size statistics (thread-safe for updates from parallel workers)
    row_size_stats: Arc<std::sync::Mutex<RowSizeStats>>,
}

impl MetricsCollector {
    /// Start collecting metrics in the background
    #[cfg(feature = "benchmark")]
    pub fn start(sample_interval: Duration, total_partitions: usize) -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let rows_counter = Arc::new(AtomicUsize::new(0));
        let partitions_counter = Arc::new(AtomicUsize::new(0));

        let stop_flag_clone = stop_flag.clone();
        let rows_counter_clone = rows_counter.clone();
        let partitions_counter_clone = partitions_counter.clone();

        let start_time = Instant::now();
        let start_time_clone = start_time;

        // Get CPU count before spawning thread
        let sys =
            System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));
        let num_cpus = sys.cpus().len();

        let handle = thread::spawn(move || {
            let mut samples = Vec::new();
            let mut sys = System::new_with_specifics(
                RefreshKind::new()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            );
            let disks = Disks::new_with_refreshed_list();

            // Track disk space (from first disk with significant space)
            let (disk_space_available, disk_space_total) = disks
                .list()
                .iter()
                .filter(|d| d.total_space() > 1024 * 1024 * 1024) // > 1GB
                .map(|d| (Some(d.available_space()), Some(d.total_space())))
                .next()
                .unwrap_or((None, None));

            // Initial refresh to get baseline
            sys.refresh_cpu_all();
            thread::sleep(Duration::from_millis(200));

            // Track previous disk stats for rate calculation
            let mut prev_read_bytes: u64 = 0;
            let mut prev_write_bytes: u64 = 0;
            let mut prev_time = start_time_clone;

            while !stop_flag_clone.load(Ordering::Relaxed) {
                sys.refresh_cpu_all();
                sys.refresh_memory();

                let now = std::time::Instant::now();
                let elapsed = start_time_clone.elapsed().as_secs_f64();
                let delta_secs = prev_time.elapsed().as_secs_f64().max(0.001);

                // CPU metrics
                let cpu_per_core: Vec<f32> = sys.cpus().iter().map(|c| c.cpu_usage()).collect();
                let cpu_percent = if cpu_per_core.is_empty() {
                    0.0
                } else {
                    cpu_per_core.iter().sum::<f32>() / cpu_per_core.len() as f32
                };

                // Memory metrics
                let memory_used = sys.used_memory();
                let memory_total = sys.total_memory();

                // Disk I/O - use /proc/diskstats on Linux for accurate rates
                let (curr_read_bytes, curr_write_bytes) = get_disk_io_bytes();

                let read_delta = curr_read_bytes.saturating_sub(prev_read_bytes);
                let write_delta = curr_write_bytes.saturating_sub(prev_write_bytes);

                let disk_read_bytes_sec = (read_delta as f64 / delta_secs) as u64;
                let disk_write_bytes_sec = (write_delta as f64 / delta_secs) as u64;

                prev_read_bytes = curr_read_bytes;
                prev_write_bytes = curr_write_bytes;
                prev_time = now;

                let sample = MetricsSample {
                    elapsed_secs: elapsed,
                    cpu_percent,
                    cpu_per_core,
                    memory_used,
                    memory_total,
                    disk_read_bytes: read_delta,
                    disk_write_bytes: write_delta,
                    disk_read_bytes_sec,
                    disk_write_bytes_sec,
                    rows_processed: rows_counter_clone.load(Ordering::Relaxed),
                    partitions_completed: partitions_counter_clone.load(Ordering::Relaxed),
                };

                samples.push(sample);

                thread::sleep(sample_interval);
            }

            (samples, disk_space_available, disk_space_total)
        });

        Self {
            stop_flag,
            rows_counter,
            partitions_counter,
            handle: Some(handle),
            start_time,
            num_cpus,
            total_partitions,
            input_metadata: None,
            output_path: None,
            row_size_stats: Arc::new(std::sync::Mutex::new(RowSizeStats::new())),
        }
    }

    /// Start a no-op collector when benchmark feature is disabled
    #[cfg(not(feature = "benchmark"))]
    pub fn start(_sample_interval: Duration, total_partitions: usize) -> Self {
        Self {
            stop_flag: Arc::new(AtomicBool::new(false)),
            rows_counter: Arc::new(AtomicUsize::new(0)),
            partitions_counter: Arc::new(AtomicUsize::new(0)),
            handle: None,
            start_time: Instant::now(),
            num_cpus: 0,
            total_partitions,
            input_metadata: None,
            output_path: None,
            row_size_stats: Arc::new(std::sync::Mutex::new(RowSizeStats::new())),
        }
    }

    /// Set input table metadata
    pub fn set_input_metadata(&mut self, meta: InputMetadata) {
        self.input_metadata = Some(meta);
    }

    /// Set output path
    pub fn set_output_path(&mut self, path: String) {
        self.output_path = Some(path);
    }

    /// Get a clone of the row size stats Arc for use in parallel workers
    pub fn row_size_stats_handle(&self) -> Arc<std::sync::Mutex<RowSizeStats>> {
        self.row_size_stats.clone()
    }

    /// Stop collecting and return the report
    pub fn finish(mut self, total_rows: usize) -> BenchmarkReport {
        self.stop_flag.store(true, Ordering::Relaxed);

        let (samples, disk_space_available, disk_space_total) =
            if let Some(handle) = self.handle.take() {
                handle.join().unwrap_or_default()
            } else {
                (Vec::new(), None, None)
            };

        // Calculate output size if path is set
        let output_size_bytes = self.output_path.as_ref().and_then(|path| {
            if std::path::Path::new(path).is_dir() {
                std::fs::read_dir(path).ok().map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter_map(|e| e.metadata().ok())
                        .map(|m| m.len())
                        .sum()
                })
            } else {
                std::fs::metadata(path).ok().map(|m| m.len())
            }
        });

        // Extract row size stats
        let row_size_stats = Arc::try_unwrap(self.row_size_stats)
            .ok()
            .and_then(|mutex| mutex.into_inner().ok())
            .filter(|stats| stats.sample_count > 0);

        BenchmarkReport {
            duration: self.start_time.elapsed(),
            samples,
            total_rows,
            total_partitions: self.total_partitions,
            num_cpus: self.num_cpus,
            input_metadata: self.input_metadata,
            output_path: self.output_path,
            output_size_bytes,
            disk_space_available,
            disk_space_total,
            row_size_stats,
        }
    }
}

/// Get total disk I/O bytes (read, write) from /proc/diskstats on Linux.
#[cfg(all(feature = "benchmark", target_os = "linux"))]
fn get_disk_io_bytes() -> (u64, u64) {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let mut total_read = 0u64;
    let mut total_write = 0u64;

    if let Ok(file) = File::open("/proc/diskstats") {
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 10 {
                continue;
            }

            // Count whole disks, not partitions or virtual loop/RAM devices.
            let name = parts[2];
            if (name.chars().last().is_some_and(|c| c.is_ascii_digit())
                && !name.starts_with("nvme"))
                || name.contains("loop")
                || name.contains("ram")
            {
                continue;
            }

            if let (Ok(read_sectors), Ok(write_sectors)) =
                (parts[5].parse::<u64>(), parts[9].parse::<u64>())
            {
                // Linux reports sectors in 512-byte units.
                total_read += read_sectors * 512;
                total_write += write_sectors * 512;
            }
        }
    }

    (total_read, total_write)
}

/// Disk I/O counters are unavailable on systems without /proc/diskstats.
#[cfg(all(feature = "benchmark", not(target_os = "linux")))]
fn get_disk_io_bytes() -> (u64, u64) {
    (0, 0)
}
