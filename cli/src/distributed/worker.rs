//! Worker client for distributed processing.
//!
//! The worker connects to a coordinator, requests work, processes partitions,
//! and reports completion. It loops until receiving an Exit response.
//!
//! Includes a background telemetry loop that sends heartbeats with system
//! metrics to the coordinator for the dashboard UI.

use crate::distributed::message::{
    CompleteRequest, CoreTaskInfo, HeartbeatRequest, JobSpec, ManhattanAggregateSpec,
    ManhattanScanSpec, ManhattanSource, TaskType,
    TelemetrySnapshot, WorkRequest, WorkResponse,
};
use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
use genohype_core::parquet::{build_record_batch, ParquetWriter};
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use crate::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Configuration for a worker.
pub struct WorkerConfig {
    /// URL of the coordinator (e.g., "http://10.0.0.5:3000")
    pub coordinator_url: String,
    /// Unique identifier for this worker
    pub worker_id: String,
    /// Retry delay when waiting for work (milliseconds)
    pub poll_interval_ms: u64,
    /// Connection timeout (seconds)
    pub connect_timeout_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            coordinator_url: String::new(),
            worker_id: String::new(),
            poll_interval_ms: 2000,
            connect_timeout_secs: 30,
        }
    }
}

/// Shared state between the main worker loop and the telemetry background task.
pub struct TelemetryState {
    /// Total rows processed so far
    total_rows: AtomicUsize,
    /// Currently active partition (usize::MAX = none)
    active_partition: AtomicUsize,
    /// Total partitions completed
    partitions_completed: AtomicUsize,
    /// Signal to stop the telemetry loop
    stop: AtomicBool,
    /// Map of Rayon thread ID to currently executing task info
    core_tasks: std::sync::Mutex<std::collections::HashMap<usize, CoreTaskInfo>>,
}

const NO_ACTIVE_PARTITION: usize = usize::MAX;

/// RAII guard to safely register and unregister a task for a Rayon thread.
///
/// When created, this guard registers the current Rayon thread's ID with the task
/// it is executing. When dropped (either normally or due to panic/error), it automatically
/// removes the registration, ensuring the core_tasks map stays accurate.
struct CoreTaskGuard<'a> {
    ts: &'a Arc<TelemetryState>,
    thread_id: Option<usize>,
}

impl<'a> CoreTaskGuard<'a> {
    /// Create a new guard that registers the current Rayon thread as executing the given task.
    fn new(ts: &'a Arc<TelemetryState>, task_info: CoreTaskInfo) -> Self {
        let thread_id = rayon::current_thread_index();
        if let Some(tid) = thread_id {
            if let Ok(mut map) = ts.core_tasks.lock() {
                map.insert(tid, task_info);
            }
        }
        Self { ts, thread_id }
    }

    /// Create a guard for a partition-based task.
    fn partition(ts: &'a Arc<TelemetryState>, partition_id: usize) -> Self {
        Self::new(ts, CoreTaskInfo::partition(partition_id))
    }

    /// Create a guard for a phenotype-based task.
    fn phenotype(ts: &'a Arc<TelemetryState>, phenotype_id: impl Into<String>, label: Option<String>) -> Self {
        Self::new(ts, CoreTaskInfo::phenotype(phenotype_id, label))
    }

    /// Create a guard for a custom task type.
    fn custom(ts: &'a Arc<TelemetryState>, task_type: impl Into<String>, task_id: impl Into<String>) -> Self {
        Self::new(ts, CoreTaskInfo::custom(task_type, task_id))
    }
}

impl<'a> Drop for CoreTaskGuard<'a> {
    fn drop(&mut self) {
        if let Some(tid) = self.thread_id {
            if let Ok(mut map) = self.ts.core_tasks.lock() {
                map.remove(&tid);
            }
        }
    }
}

/// Run the worker loop.
///
/// This function blocks until the coordinator signals job completion.
pub async fn run_worker(config: WorkerConfig) -> Result<()> {
    println!(
        "Worker {} starting, connecting to {}",
        config.worker_id, config.coordinator_url
    );

    let hardware = {
        use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::new())
                .with_memory(MemoryRefreshKind::new().with_ram()),
        );
        sys.refresh_cpu_list(CpuRefreshKind::new());
        sys.refresh_memory();
        crate::distributed::message::HardwareSpec {
            num_cores: sys.cpus().len(),
            total_memory_mb: sys.total_memory() / (1024 * 1024),
        }
    };

    println!(
        "Hardware detected: {} cores, {} MB RAM",
        hardware.num_cores, hardware.total_memory_mb
    );

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .timeout(Duration::from_secs(300)) // 5 minute request timeout
        .build()
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create HTTP client: {}", e),
            ))
        })?;

    let work_url = format!("{}/work", config.coordinator_url);
    let complete_url = format!("{}/complete", config.coordinator_url);

    // Shared telemetry state between main loop and background heartbeat task
    let telemetry_state = Arc::new(TelemetryState {
        total_rows: AtomicUsize::new(0),
        active_partition: AtomicUsize::new(NO_ACTIVE_PARTITION),
        partitions_completed: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
        core_tasks: std::sync::Mutex::new(std::collections::HashMap::new()),
    });

    // Spawn background telemetry heartbeat loop
    let heartbeat_handle = spawn_telemetry_loop(
        client.clone(),
        config.coordinator_url.clone(),
        config.worker_id.clone(),
        telemetry_state.clone(),
    );

    // Cache the QueryEngine between work requests to avoid re-reading metadata
    let mut cached_engine: Option<(String, QueryEngine)> = None;

    loop {
        // Request work from coordinator
        let work_response = match request_work(&client, &work_url, &config.worker_id, &hardware).await {
            Ok(resp) => resp,
            Err(e) => {
                eprintln!(
                    "Failed to connect to coordinator: {}. Retrying in {}ms...",
                    e, config.poll_interval_ms
                );
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms * 2)).await;
                continue;
            }
        };

        match work_response {
            WorkResponse::Exit => {
                println!("Received Exit signal. Worker shutting down.");
                break;
            }
            WorkResponse::Wait => {
                telemetry_state
                    .active_partition
                    .store(NO_ACTIVE_PARTITION, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
            WorkResponse::Task {
                tasks,
                input_path,
                payload,
                total_tasks: _,
                filters,
                intervals,
            } => {
                let job_spec: JobSpec = match serde_json::from_value(payload) {
                    Ok(spec) => spec,
                    Err(e) => {
                        eprintln!("Failed to deserialize job spec from payload: {}", e);
                        continue;
                    }
                };

                // Extract partition indices from task descriptors for backwards compatibility
                let partitions: Vec<usize> = tasks
                    .iter()
                    .filter_map(|t| {
                        if let Ok(task_type) = serde_json::from_value::<TaskType>(t.payload.clone()) {
                            match task_type {
                                TaskType::Partition { partition_index, .. } => return Some(partition_index),
                                TaskType::Stress { iteration } => return Some(iteration),
                                _ => {}
                            }
                        }
                        // Fallback: try to parse the task ID as a partition index
                        t.id.parse::<usize>().ok().or_else(|| {
                            // Secondary fallback for IDs like "stress_0"
                            t.id.rsplit('_').next().and_then(|s| s.parse::<usize>().ok())
                        })
                    })
                    .collect();

                // Extract task IDs for completion reporting
                let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();

                // Build description including batch info for aggregate batch jobs
                let desc = if let JobSpec::ManhattanAggregateBatch { specs } = &job_spec {
                    format!("batch of {} aggregation tasks", specs.len())
                } else {
                    job_spec.description().to_string()
                };

                println!(
                    "Received {} task(s): {:?} ({})",
                    tasks.len(),
                    task_ids,
                    desc
                );

                // Update telemetry: mark first partition as active
                if let Some(&first) = partitions.first() {
                    telemetry_state
                        .active_partition
                        .store(first, Ordering::Relaxed);
                }

                // Process the assigned partitions on a blocking thread
                // (QueryEngine uses blocking I/O internally)
                let partitions_clone = partitions.clone();
                let input_clone = input_path.clone();
                let job_spec_clone = job_spec.clone();
                let filters_clone = filters.clone();
                let intervals_clone = intervals.clone();
                let ts = telemetry_state.clone();

                let result = tokio::task::spawn_blocking(move || {
                    dispatch_job(
                        cached_engine,
                        &partitions_clone,
                        &input_clone,
                        &job_spec_clone,
                        &filters_clone,
                        &intervals_clone,
                        Some(ts),
                    )
                })
                .await;

                let (rows_processed, result_json) = match result {
                    Ok(Ok((rows, result, engine_back))) => {
                        cached_engine = engine_back;
                        (rows, result)
                    }
                    Ok(Err(e)) => {
                        let error_msg = format!("{}", e);
                        eprintln!("Error processing tasks {:?}: {}", task_ids, error_msg);
                        cached_engine = None;

                        // Report failure to coordinator so it can track and display the error
                        let fail_req = CompleteRequest {
                            worker_id: config.worker_id.clone(),
                            tasks: task_ids.clone(),
                            items_processed: 0,
                            result_json: None,
                            error: Some(error_msg),
                        };
                        if let Err(post_err) = client.post(&complete_url).json(&fail_req).send().await {
                            eprintln!("Failed to report error to coordinator: {}", post_err);
                        }
                        continue;
                    }
                    Err(e) => {
                        eprintln!("Task panicked processing tasks {:?}: {}", task_ids, e);
                        cached_engine = None;
                        continue;
                    }
                };

                // Update telemetry counters
                telemetry_state
                    .active_partition
                    .store(NO_ACTIVE_PARTITION, Ordering::Relaxed);
                telemetry_state
                    .partitions_completed
                    .fetch_add(task_ids.len(), Ordering::Relaxed);

                // Report completion (with optional result_json for aggregation)
                if let Err(e) = report_completion(
                    &client,
                    &complete_url,
                    &config.worker_id,
                    &task_ids,
                    rows_processed,
                    result_json,
                )
                .await
                {
                    eprintln!("Failed to report completion: {}", e);
                    // Continue anyway - coordinator will handle duplicates
                }

                println!(
                    "Completed tasks {:?} ({} rows)",
                    task_ids, rows_processed
                );
            }
        }
    }

    // Stop telemetry background task
    telemetry_state.stop.store(true, Ordering::Relaxed);
    let _ = heartbeat_handle.await;

    Ok(())
}

/// Request work from the coordinator.
async fn request_work(
    client: &reqwest::Client,
    url: &str,
    worker_id: &str,
    hardware: &crate::distributed::message::HardwareSpec,
) -> Result<WorkResponse> {
    let request = WorkRequest {
        worker_id: worker_id.to_string(),
        hardware: Some(hardware.clone()),
    };

    let response = client
        .post(url)
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed: {}", e),
            ))
        })?;

    let work_response: WorkResponse = response.json().await.map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to parse response: {}", e),
        ))
    })?;

    Ok(work_response)
}

/// Report completion to the coordinator.
async fn report_completion(
    client: &reqwest::Client,
    url: &str,
    worker_id: &str,
    tasks: &[String],
    items_processed: usize,
    result_json: Option<serde_json::Value>,
) -> Result<()> {
    let request = CompleteRequest {
        worker_id: worker_id.to_string(),
        tasks: tasks.to_vec(),
        items_processed,
        result_json,
        error: None,
    };

    client
        .post(url)
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed: {}", e),
            ))
        })?;

    Ok(())
}

/// Spawn a background task that periodically sends heartbeats with telemetry to the coordinator.
fn spawn_telemetry_loop(
    client: reqwest::Client,
    coordinator_url: String,
    worker_id: String,
    state: Arc<TelemetryState>,
) -> tokio::task::JoinHandle<()> {
    let heartbeat_url = format!("{}/heartbeat", coordinator_url);
    let start_time = Instant::now();

    // Initialize sysinfo for system metrics (CPU, memory)
    let sys = std::sync::Mutex::new({
        use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                .with_memory(MemoryRefreshKind::new().with_ram()),
        );
        // Initial refresh to establish baseline
        sys.refresh_all();
        sys
    });

    // Initialize disk and network monitoring
    let disks = std::sync::Mutex::new(sysinfo::Disks::new_with_refreshed_list());
    let networks = std::sync::Mutex::new(sysinfo::Networks::new_with_refreshed_list());

    let mut prev_rows: usize = 0;
    let mut prev_time = start_time;

    // Track previous network counters for rate calculation
    let mut prev_net_rx: u64 = 0;
    let mut prev_net_tx: u64 = 0;

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            if state.stop.load(Ordering::Relaxed) {
                break;
            }

            let total_rows = state.total_rows.load(Ordering::Relaxed);
            let active_part = state.active_partition.load(Ordering::Relaxed);
            let parts_done = state.partitions_completed.load(Ordering::Relaxed);

            let now = Instant::now();
            let dt = now.duration_since(prev_time).as_secs_f64();
            let rows_per_sec = if dt > 0.0 {
                (total_rows.saturating_sub(prev_rows)) as f64 / dt
            } else {
                0.0
            };
            prev_rows = total_rows;
            prev_time = now;

            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Collect system metrics (CPU and memory)
            let (cpu, cpu_per_core, mem_used, mem_total) = {
                use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};
                let mut s = sys.lock().unwrap();
                s.refresh_specifics(
                    RefreshKind::new()
                        .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                        .with_memory(MemoryRefreshKind::new().with_ram()),
                );
                let per_core: Vec<f32> = s.cpus().iter().map(|c| c.cpu_usage()).collect();
                let cpu_avg = per_core.iter().sum::<f32>() / per_core.len().max(1) as f32;
                (
                    Some(cpu_avg),
                    Some(per_core),
                    Some(s.used_memory()),
                    Some(s.total_memory()),
                )
            };

            // Collect disk metrics
            let (disk_used, disk_total) = {
                let mut d = disks.lock().unwrap();
                d.refresh_list();
                let mut used = 0u64;
                let mut total = 0u64;
                for disk in d.iter() {
                    total += disk.total_space();
                    used += disk.total_space().saturating_sub(disk.available_space());
                }
                (Some(used), Some(total))
            };

            // Collect network metrics
            let (net_rx_sec, net_tx_sec, _net_rx_total, _net_tx_total) = {
                let mut n = networks.lock().unwrap();
                n.refresh();
                let (current_rx, current_tx) = n
                    .iter()
                    .fold((0u64, 0u64), |(rx, tx), (_, iface)| {
                        (rx + iface.total_received(), tx + iface.total_transmitted())
                    });

                // Calculate rates (bytes/sec)
                let rx_sec = if dt > 0.0 {
                    current_rx.saturating_sub(prev_net_rx) as f64 / dt
                } else {
                    0.0
                };
                let tx_sec = if dt > 0.0 {
                    current_tx.saturating_sub(prev_net_tx) as f64 / dt
                } else {
                    0.0
                };
                prev_net_rx = current_rx;
                prev_net_tx = current_tx;

                (Some(rx_sec), Some(tx_sec), Some(current_rx), Some(current_tx))
            };

            // Collect the core tasks map (Rayon thread ID -> partition ID)
            let core_tasks = {
                let map = state.core_tasks.lock().unwrap();
                if map.is_empty() {
                    None
                } else {
                    Some(map.clone())
                }
            };

            let snapshot = TelemetrySnapshot {
                timestamp_ms,
                cpu_percent: cpu,
                memory_used_bytes: mem_used,
                memory_total_bytes: mem_total,
                items_per_sec: rows_per_sec,
                total_items: total_rows,
                active_partition: if active_part == NO_ACTIVE_PARTITION {
                    None
                } else {
                    Some(active_part)
                },
                partitions_completed: parts_done,
                // Extended metrics
                cpu_per_core,
                disk_read_bytes_sec: None, // sysinfo doesn't provide disk I/O rates directly
                disk_write_bytes_sec: None,
                disk_used_bytes: disk_used,
                disk_total_bytes: disk_total,
                network_rx_bytes_sec: net_rx_sec,
                network_tx_bytes_sec: net_tx_sec,
                core_tasks,
                current_batch_size: None, // Set by coordinator, not known to worker
            };

            let req = HeartbeatRequest {
                worker_id: worker_id.clone(),
                telemetry: snapshot,
            };

            // Best-effort: don't let heartbeat failures block the worker
            let _ = client.post(&heartbeat_url).json(&req).send().await;
        }
    })
}

/// Dispatch job based on JobSpec to the appropriate processor.
///
/// Returns (rows_processed, result_json, cached_engine).
pub fn dispatch_job(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    job_spec: &JobSpec,
    filters: &[String],
    intervals: &[String],
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<serde_json::Value>, Option<(String, QueryEngine)>)> {
    // Parse filters from strings
    let key_ranges = parse_filter_strings(filters);
    let interval_list = parse_interval_strings(intervals);

    match job_spec {
        JobSpec::ExportParquet { output_path } => {
            let (rows, engine) = process_parquet_export(
                cached_engine,
                partitions,
                input_path,
                output_path,
                &key_ranges,
                interval_list.as_ref(),
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::ExportJson { output_path, .. } => {
            let (rows, engine) = process_json_export(
                cached_engine,
                partitions,
                input_path,
                output_path,
                &key_ranges,
                interval_list.as_ref(),
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::Summary => {
            let (rows, stats, engine) = process_summary(
                cached_engine,
                partitions,
                input_path,
                telemetry,
            )?;
            // Convert stats to JSON for aggregation on coordinator
            let result_json = serde_json::to_value(&stats).ok();
            Ok((rows, result_json, engine))
        }
        JobSpec::Validate { .. } => {
            // TODO: Implement distributed validation
            eprintln!("Validate job not yet implemented for distributed mode");
            Ok((0, None, cached_engine))
        }
        JobSpec::Manhattan { .. } => {
            // Manhattan is a coordinator-level job spec for submission
            Err(crate::HailError::InvalidFormat(
                "Manhattan should not be dispatched to worker - it's for coordinator submission".to_string()
            ))
        }
        JobSpec::ManhattanBatch { .. } => {
            // ManhattanBatch is a coordinator-level job spec for submission
            // It should never be dispatched directly to workers
            Err(crate::HailError::InvalidFormat(
                "ManhattanBatch should not be dispatched to worker - it's for coordinator batch submission".to_string()
            ))
        }
        JobSpec::ManhattanScan(spec) => {
            let (rows, engine) = process_manhattan_scan_v2(
                cached_engine,
                partitions,
                spec,
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::ManhattanAggregate(spec) => {
            let (rows, summary) = process_manhattan_aggregate(spec)?;
            Ok((rows, Some(summary), None))
        }
        JobSpec::ManhattanAggregateBatch { specs } => {
            use rayon::prelude::*;

            println!("Processing batch of {} aggregation tasks...", specs.len());

            // Execute all aggregations in parallel using the worker's thread pool
            // This allows nested parallelism:
            // - Top level: parallel phenotypes
            // - Inner level: parallel locus plots (within process_manhattan_aggregate)
            let results: Vec<Result<(usize, serde_json::Value)>> = specs.par_iter()
                .map(|spec| {
                    // Track the phenotype being processed on this Rayon thread
                    let phenotype_id = spec.phenotype_id.clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    let label = spec.ancestry.clone();
                    let _core_guard = telemetry.as_ref()
                        .map(|ts| CoreTaskGuard::phenotype(ts, &phenotype_id, label));

                    process_manhattan_aggregate(spec)
                })
                .collect();

            // Sum rows and collect summaries
            let mut total_rows = 0;
            let mut summaries = Vec::new();

            for res in results {
                let (rows, summary) = res?;
                total_rows += rows;
                summaries.push(summary);
            }

            // Combine summaries into a wrapper
            let combined_summary = serde_json::json!({
                "batch_results": summaries
            });

            Ok((total_rows, Some(combined_summary), None))
        }
        JobSpec::Loci(spec) => {
            let rows = process_loci(spec)?;
            Ok((rows, None, None))
        }
        JobSpec::ExportClickhouse { clickhouse_url, table_name } => {
            #[cfg(feature = "clickhouse")]
            {
                let (rows, engine) = process_clickhouse_export(
                    cached_engine,
                    partitions,
                    input_path,
                    clickhouse_url,
                    table_name,
                    telemetry,
                )?;
                Ok((rows, None, engine))
            }
            #[cfg(not(feature = "clickhouse"))]
            {
                let _ = (clickhouse_url, table_name); // suppress unused warning
                Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Worker binary not built with 'clickhouse' feature. Rebuild with --features clickhouse"
                )))
            }
        }
        JobSpec::IngestManhattan { .. } => {
            // This is a coordinator-level job spec, should never be sent to workers
            Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "IngestManhattan is a coordinator job spec, not a worker task"
            )))
        }
        JobSpec::IngestManhattanTask { phenotype_id, ancestry, base_path, clickhouse_url, database } => {
            #[cfg(feature = "clickhouse")]
            {
                let rows = process_ingest_manhattan(
                    phenotype_id,
                    ancestry,
                    base_path,
                    clickhouse_url,
                    database,
                )?;
                Ok((rows, None, None))
            }
            #[cfg(not(feature = "clickhouse"))]
            {
                let _ = (phenotype_id, ancestry, base_path, clickhouse_url, database);
                Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Worker binary not built with 'clickhouse' feature. Rebuild with --features clickhouse"
                )))
            }
        }
        JobSpec::Stress(spec) => {
            let rows = process_stress(partitions, spec, telemetry)?;
            Ok((rows, None, cached_engine))
        }
    }
}

/// Process a synthetic stress test workload.
///
/// Simulates CPU, Memory, and Network I/O loads concurrently based on the `StressSpec` parameters.
fn process_stress(
    partitions: &[usize],
    spec: &crate::distributed::message::StressSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<usize> {
    use rayon::prelude::*;
    use std::time::Instant;
    use std::io::{Read, Write};

    println!("Processing {} stress partitions...", partitions.len());

    let results: Vec<Result<usize>> = partitions.par_iter().map(|&partition_id| {
        // Tag this thread for the dashboard's per-core task view (stress task type)
        let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::custom(ts, "stress", partition_id.to_string()));

        // 1. Memory Load: allocate and hold a large vector.
        let mut mem_hog: Vec<u64> = Vec::new();
        if spec.memory_mb > 0 {
            let elements = (spec.memory_mb * 1024 * 1024) / 8;
            mem_hog.resize(elements, 0u64);
            // Force the OS to actually allocate the physical pages by writing to them.
            for i in 0..elements {
                mem_hog[i] = i as u64;
            }
        }

        // 2. CPU Load: spin doing heavy math operations.
        if spec.cpu_secs > 0.0 {
            let start = Instant::now();
            let mut dummy: f64 = 1.0;
            while start.elapsed().as_secs_f64() < spec.cpu_secs {
                // Inner tight loop to avoid excessive clock reads
                for _ in 0..10_000 {
                    dummy = dummy.sin().cos().exp();
                }
            }
            // Prevent the compiler from optimizing away the math loop
            std::hint::black_box(dummy);
        }

        // 3. Generate read data if requested (write temp file, then read it back)
        let generated_read_path = if spec.generate_read_data {
            if let Some(write_dir) = &spec.write_dir {
                let temp_path = format!("{}/stress_read_{}.bin", write_dir.trim_end_matches('/'), partition_id);

                // Write the temporary file
                if let Ok(mut writer) = StreamingCloudWriter::new(&temp_path) {
                    let chunk_size = 8 * 1024 * 1024; // 8MB chunks
                    let total_bytes = spec.read_data_size_mb * 1024 * 1024;
                    let buf = vec![0xBB; chunk_size];
                    let mut written = 0;

                    while written < total_bytes {
                        let to_write = std::cmp::min(chunk_size, total_bytes - written);
                        let _ = writer.write(&buf[..to_write]);
                        written += to_write;
                    }
                    let _ = writer.finish();
                }

                Some(temp_path)
            } else {
                None
            }
        } else {
            None
        };

        // 4. Network RX Load: stream data from a remote path and discard it.
        // Use generated path if available, otherwise use explicit read_path
        let read_source = generated_read_path.as_ref().or(spec.read_path.as_ref());
        if let Some(read_path) = read_source {
            if let Ok(mut reader) = genohype_core::io::get_reader(read_path) {
                let mut buf = vec![0u8; 8 * 1024 * 1024]; // 8MB read buffer
                while let Ok(n) = reader.read(&mut buf) {
                    if n == 0 { break; }
                }
            }
        }

        // 5. Network TX Load: stream random bytes to a remote path (separate from generated read data).
        if let Some(write_dir) = &spec.write_dir {
            // Only write stress output files if not in generate-read-data-only mode
            // or if we want both TX and generated read data
            if !spec.generate_read_data {
                let out_path = format!("{}/stress_{}.bin", write_dir.trim_end_matches('/'), partition_id);
                if let Ok(mut writer) = StreamingCloudWriter::new(&out_path) {
                    let buf = vec![0xAA; 8 * 1024 * 1024]; // 8MB block
                    // Write 4 chunks to simulate a 32MB file per partition
                    for _ in 0..4 {
                        let _ = writer.write(&buf);
                    }
                    let _ = writer.finish();
                }
            }
        }

        // Keep the memory allocated until the end of the task
        std::hint::black_box(&mem_hog);

        // Bump the simulated rows counter to give the dashboard some throughput data
        let rows_simulated = 10_000;
        if let Some(ref t) = telemetry {
            t.total_rows.fetch_add(rows_simulated, Ordering::Relaxed);
        }

        Ok(rows_simulated)
    }).collect();

    let mut total_rows = 0;
    for res in results {
        total_rows += res?;
    }

    Ok(total_rows)
}

/// Process partitions and export to ClickHouse.
///
/// Uses a producer-consumer pattern with bounded concurrency to prevent OOM:
/// - A semaphore limits how many partitions are processed concurrently (default: 8)
/// - Each partition spawns a reader thread that sends row chunks via bounded channel
/// - The consumer thread uploads chunks to ClickHouse
/// - Backpressure: if uploads are slow, the channel fills and reading pauses
///
/// Environment variables for tuning:
/// - HAIL_DECODER_MAX_CONCURRENT_UPLOADS: max concurrent partitions (default: 8)
/// - HAIL_DECODER_CLICKHOUSE_CHUNK_SIZE: rows per upload chunk (default: 25000)
#[cfg(feature = "clickhouse")]
fn process_clickhouse_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    url: &str,
    table: &str,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::export::ClickHouseClient;
    use crossbeam_channel::bounded;
    use rayon::prelude::*;

    // Configuration from environment (with defaults)
    let max_concurrent: usize = std::env::var("HAIL_DECODER_MAX_CONCURRENT_UPLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let chunk_size: usize = std::env::var("HAIL_DECODER_CLICKHOUSE_CHUNK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25_000);

    println!(
        "Processing {} partitions to ClickHouse table '{}' (concurrency: {}, chunk_size: {})...",
        partitions.len(),
        table,
        max_concurrent,
        chunk_size
    );

    // Semaphore to limit concurrent partition processing
    let semaphore = Semaphore::new(max_concurrent);

    // Clone refs for the parallel closure
    let input_path = input_path.to_string();
    let url = url.to_string();
    let table = table.to_string();

    // Process partitions in parallel (but bounded by semaphore)
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            // Acquire semaphore permit - blocks if too many partitions in flight
            let _permit = semaphore.acquire();

            // Bounded channel with capacity 2: provides backpressure
            // If ClickHouse uploads are slow, reader thread will block
            let (tx, rx) = bounded::<Result<Vec<genohype_core::codec::EncodedValue>>>(2);

            let input_path_clone = input_path.clone();
            let chunk_size_clone = chunk_size;

            // Spawn reader thread: reads rows and sends chunks through channel
            std::thread::spawn(move || {
                let engine_res = QueryEngine::open_path(&input_path_clone);
                match engine_res {
                    Ok(engine) => {
                        match engine.scan_partition_iter(partition_id, &[]) {
                            Ok(iter) => {
                                let mut batch = Vec::with_capacity(chunk_size_clone);
                                for row_res in iter {
                                    match row_res {
                                        Ok(row) => {
                                            batch.push(row);
                                            if batch.len() >= chunk_size_clone {
                                                // Send chunk - blocks if channel is full (backpressure)
                                                if tx.send(Ok(std::mem::replace(
                                                    &mut batch,
                                                    Vec::with_capacity(chunk_size_clone),
                                                ))).is_err() {
                                                    // Receiver dropped, stop reading
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx.send(Err(e));
                                            return;
                                        }
                                    }
                                }
                                // Send remaining rows
                                if !batch.is_empty() {
                                    let _ = tx.send(Ok(batch));
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
                // tx is dropped here, closing the channel
            });

            // Consumer: receive chunks and upload to ClickHouse
            // Open engine to get schema (metadata read is cheap)
            let engine = QueryEngine::open_path(&input_path)?;
            let row_type = engine.row_type().clone();
            let arrow_schema = Arc::new(genohype_core::parquet::schema::create_schema(&row_type)?);

            let client = ClickHouseClient::new(&url);
            let ts = telemetry.clone();

            let mut partition_rows = 0;
            let mut chunks_uploaded = 0;
            const BATCH_SIZE: usize = 4096; // Internal batching for Parquet writing

            // Receive and upload chunks until channel closes
            for batch_res in rx {
                let batch = batch_res?;

                let uploaded = upload_chunk_to_clickhouse(
                    &client,
                    &table,
                    &batch,
                    &row_type,
                    arrow_schema.clone(),
                    BATCH_SIZE,
                )?;

                partition_rows += uploaded;
                chunks_uploaded += 1;

                if let Some(ref t) = ts {
                    t.total_rows.fetch_add(uploaded, Ordering::Relaxed);
                }

                // Log progress every 10 chunks
                if chunks_uploaded % 10 == 0 {
                    println!(
                        "    Partition {} progress: {} rows in {} chunks",
                        partition_id, partition_rows, chunks_uploaded
                    );
                }
            }

            println!(
                "  Partition {} complete: {} rows in {} chunk(s) uploaded to ClickHouse",
                partition_id, partition_rows, chunks_uploaded
            );

            Ok(partition_rows)
        })
        .collect();

    // Check errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();
    Ok((total, None))
}

/// Upload a chunk of rows to ClickHouse as an in-memory Parquet buffer.
///
/// This avoids writing to disk entirely - the Parquet data is built in memory
/// and streamed directly to ClickHouse via HTTP POST.
#[cfg(feature = "clickhouse")]
fn upload_chunk_to_clickhouse(
    client: &crate::export::ClickHouseClient,
    table: &str,
    rows: &[genohype_core::codec::EncodedValue],
    row_type: &genohype_core::codec::EncodedType,
    arrow_schema: std::sync::Arc<arrow::datatypes::Schema>,
    batch_size: usize,
) -> Result<usize> {
    use genohype_core::parquet::InMemoryParquetWriter;

    if rows.is_empty() {
        return Ok(0);
    }

    // Write rows to in-memory Parquet buffer
    let mut writer = InMemoryParquetWriter::new(row_type)?;

    for batch_start in (0..rows.len()).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(rows.len());
        let batch_rows = &rows[batch_start..batch_end];
        let record_batch = build_record_batch(batch_rows, row_type, arrow_schema.clone())?;
        writer.write_batch(&record_batch)?;
    }

    let parquet_bytes = writer.finish()?;
    let row_count = rows.len();

    // Upload to ClickHouse with retry logic
    let mut attempts = 0;
    loop {
        match client.insert_parquet_bytes(table, parquet_bytes.clone()) {
            Ok(_) => break,
            Err(e) => {
                attempts += 1;
                if attempts >= 3 {
                    return Err(crate::HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to upload chunk to ClickHouse after 3 attempts: {}", e)
                    )));
                }
                std::thread::sleep(std::time::Duration::from_secs(2 * attempts as u64));
            }
        }
    }

    Ok(row_count)
}

/// Process a Manhattan ingestion task (ingest phenotype data into ClickHouse).
#[cfg(feature = "clickhouse")]
fn process_ingest_manhattan(
    phenotype_id: &str,
    ancestry: &str,
    base_path: &str,
    clickhouse_url: &str,
    database: &str,
) -> Result<usize> {
    use crate::ingest::manhattan::run_ingest_task;

    println!("Processing ingestion task:");
    println!("  Phenotype: {}", phenotype_id);
    println!("  Ancestry: {}", ancestry);
    println!("  Base path: {}", base_path);
    println!("  ClickHouse: {}", clickhouse_url);
    println!("  Database: {}", database);

    let rows = run_ingest_task(phenotype_id, ancestry, base_path, clickhouse_url, database)?;

    println!("Ingestion complete: {} total rows", rows);
    Ok(rows)
}

/// Process a loci generation job.
fn process_loci(spec: &crate::distributed::message::LociSpec) -> Result<usize> {
    use crate::manhattan::aggregate::generate_loci_standalone;

    println!("Processing loci generation job:");
    println!("  Output dir: {}", spec.output_dir);
    println!("  Exome: {:?}", spec.exome_results);
    println!("  Genome: {:?}", spec.genome_results);
    println!("  Gene burden: {:?}", spec.gene_burden);
    println!("  Window: {}bp", spec.locus_window);
    println!("  Threshold: {}", spec.threshold);
    println!("  Gene threshold: {}", spec.gene_threshold);

    let loci = generate_loci_standalone(
        &spec.output_dir,
        spec.exome_results.as_deref(),
        spec.genome_results.as_deref(),
        spec.gene_burden.as_deref(),
        spec.locus_window,
        spec.threshold,
        spec.gene_threshold,
        8, // threads per worker
        spec.locus_plots,
        spec.min_variants_per_locus,
    )?;

    println!("Generated {} locus plots", loci.len());
    Ok(loci.len())
}

/// Parse filter strings back into KeyRanges.
///
/// Uses the shared filter parsing module to convert where clause strings
/// into KeyRange objects for row filtering.
fn parse_filter_strings(filters: &[String]) -> Vec<KeyRange> {
    use genohype_core::query::filter::parse_where_condition;

    filters
        .iter()
        .filter_map(|s| {
            let range = parse_where_condition(s);
            if range.is_none() {
                eprintln!("Warning: failed to parse filter condition: {}", s);
            }
            range
        })
        .collect()
}

/// Parse interval strings back into IntervalList.
///
/// Note: Similar to filters, interval parsing is complex and lives in main.rs.
/// For distributed mode, the coordinator handles interval validation.
fn parse_interval_strings(_intervals: &[String]) -> Option<Arc<IntervalList>> {
    // TODO: Move interval parsing to a shared module if needed on workers
    None
}

/// Process partitions and write to Parquet output (synchronous version).
/// Uses rayon to process partitions in parallel across all CPU cores.
fn process_parquet_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    filters: &[KeyRange],
    intervals: Option<&Arc<IntervalList>>,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use genohype_core::query::row_matches_intervals;
    use rayon::prelude::*;

    println!("Processing {} partitions to Parquet...", partitions.len());

    let output_is_cloud = is_cloud_path(output_path);

    // Clone refs for the parallel closure
    let input_path = input_path.to_string();
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            // Each thread opens its own QueryEngine (they share underlying caches)
            let engine = QueryEngine::open_path(&input_path)?;
            let row_type = engine.row_type().clone();
            let arrow_schema = Arc::new(genohype_core::parquet::schema::create_schema(&row_type)?);

            // Determine the output file path
            let output_file = if output_is_cloud {
                let base = output_path.trim_end_matches('/');
                format!("{}/part-{:05}.parquet", base, partition_id)
            } else {
                format!("{}/part-{:05}.parquet", output_path, partition_id)
            };

            const BATCH_SIZE: usize = 4096;
            let mut batch_rows = Vec::with_capacity(BATCH_SIZE);
            let mut partition_rows = 0;

            // Stream rows from this partition with filters
            let iter = engine.scan_partition_iter(partition_id, &filters)?;

            // Clone telemetry for this thread
            let ts = telemetry.clone();

            // Helper macro for processing with any writer type
            macro_rules! process_with_writer {
                ($writer:expr) => {{
                    let mut writer = $writer;

                    for row_result in iter {
                        let row = row_result?;

                        // Apply interval filtering if present
                        if let Some(ref ivl) = intervals {
                            if !row_matches_intervals(&row, ivl) {
                                continue;
                            }
                        }

                        batch_rows.push(row);

                        if batch_rows.len() >= BATCH_SIZE {
                            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                            writer.write_batch(&batch)?;
                            partition_rows += batch_rows.len();
                            // Update telemetry row count
                            if let Some(ref t) = ts {
                                t.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                            }
                            batch_rows.clear();
                        }
                    }

                    // Write remaining rows
                    if !batch_rows.is_empty() {
                        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                        writer.write_batch(&batch)?;
                        partition_rows += batch_rows.len();
                        if let Some(ref t) = ts {
                            t.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                        }
                    }

                    writer
                }};
            }

            if output_is_cloud {
                let cloud_writer = StreamingCloudWriter::new(&output_file)?;
                let writer = ParquetWriter::from_writer(cloud_writer, &row_type)?;
                let writer = process_with_writer!(writer);
                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
            } else {
                let writer = ParquetWriter::new(&output_file, &row_type)?;
                let writer = process_with_writer!(writer);
                writer.close()?;
            }

            println!(
                "  Partition {} complete: {} rows -> {}",
                partition_id, partition_rows, output_file
            );

            Ok(partition_rows)
        })
        .collect();

    // Check for any errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();

    // Don't return cached engine since we opened multiple in parallel
    Ok((total, None))
}

/// Process partitions and write to JSON output (NDJSON format).
fn process_json_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    filters: &[KeyRange],
    intervals: Option<&Arc<IntervalList>>,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::export::JsonWriter;
    use genohype_core::query::row_matches_intervals;
    use rayon::prelude::*;
    use std::fs::File;
    use std::io::BufWriter;

    println!("Processing {} partitions to JSON...", partitions.len());

    let output_is_cloud = is_cloud_path(output_path);

    // Clone refs for the parallel closure
    let input_path = input_path.to_string();
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            let engine = QueryEngine::open_path(&input_path)?;

            let output_file = if output_is_cloud {
                let base = output_path.trim_end_matches('/');
                format!("{}/part-{:05}.json", base, partition_id)
            } else {
                format!("{}/part-{:05}.json", output_path, partition_id)
            };

            let mut partition_rows = 0;

            // Stream rows from this partition with filters
            let iter = engine.scan_partition_iter(partition_id, &filters)?;

            let ts = telemetry.clone();

            macro_rules! process_json_with_writer {
                ($writer:expr) => {{
                    let mut json_writer = JsonWriter::new($writer);

                    for row_result in iter {
                        let row = row_result?;

                        // Apply interval filtering if present
                        if let Some(ref ivl) = intervals {
                            if !row_matches_intervals(&row, ivl) {
                                continue;
                            }
                        }

                        json_writer.write_row(&row)?;
                        partition_rows += 1;

                        if let Some(ref t) = ts {
                            t.total_rows.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    json_writer.flush()?;
                    json_writer.into_inner()
                }};
            }

            if output_is_cloud {
                let cloud_writer = StreamingCloudWriter::new(&output_file)?;
                let writer = process_json_with_writer!(cloud_writer);
                writer.finish()?;
            } else {
                let file = File::create(&output_file)?;
                let buf_writer = BufWriter::new(file);
                process_json_with_writer!(buf_writer);
            }

            println!(
                "  Partition {} complete: {} rows -> {}",
                partition_id, partition_rows, output_file
            );

            Ok(partition_rows)
        })
        .collect();

    // Check for any errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();
    Ok((total, None))
}

/// Process partitions and collect summary statistics.
/// Uses rayon to process partitions in parallel and merge stats.
fn process_summary(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, genohype_core::summary::stats::StatsAccumulator, Option<(String, QueryEngine)>)> {
    use genohype_core::summary::stats::StatsAccumulator;
    use rayon::prelude::*;

    println!("Processing {} partitions for summary...", partitions.len());

    let input_path = input_path.to_string();

    // Process partitions in parallel using rayon's fold/reduce
    // Each thread gets its own StatsAccumulator and they are merged at the end
    let (total_rows, stats) = partitions
        .par_iter()
        .fold(
            || (0usize, StatsAccumulator::new()),
            |(mut rows, mut acc), &partition_id| {
                // Track the active partition for this Rayon thread (RAII guard)
                let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

                match QueryEngine::open_path(&input_path) {
                    Ok(engine) => {
                        match engine.scan_partition_iter(partition_id, &[]) {
                            Ok(iter) => {
                                for row_result in iter {
                                    match row_result {
                                        Ok(row) => {
                                            acc.process_row(&row);
                                            rows += 1;

                                            // Update telemetry
                                            if let Some(ref t) = telemetry {
                                                t.total_rows.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Warning: Error reading row in partition {}: {}",
                                                partition_id, e
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: Failed to scan partition {}: {}",
                                    partition_id, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to open engine for partition {}: {}", partition_id, e);
                    }
                }
                (rows, acc)
            },
        )
        .reduce(
            || (0, StatsAccumulator::new()),
            |(rows_a, mut acc_a), (rows_b, acc_b)| {
                acc_a.merge(acc_b);
                (rows_a + rows_b, acc_a)
            },
        );

    println!("Summary complete: {} rows processed, {} fields tracked", total_rows, stats.stats.len());

    // Don't cache engine since we opened multiple in parallel
    Ok((total_rows, stats, None))
}

/// Process partitions for Manhattan scan phase (V2 pipeline).
///
/// This is the Phase 1 worker task. For each partition, it:
/// 1. Renders plot points to a partial PNG (transparent background)
/// 2. Writes significant hits (below threshold) to a Parquet file
///
/// Output structure:
/// - {output_path}/{source}/part-{id}.png
/// - {output_path}/{source}/part-{id}-sig.parquet
fn process_manhattan_scan_v2(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    spec: &ManhattanScanSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
    use crate::manhattan::data::{extract_plot_data, SigHitRow};
    use crate::manhattan::layout::ChromosomeLayout;
    use crate::manhattan::reference::{calculate_xpos, normalize_contig_name};
    use crate::manhattan::render::ManhattanRenderer;
    use crate::manhattan::sig_writer::SigHitWriter;
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::io::Write;

    let source_name = match spec.source {
        ManhattanSource::Exome => "exome",
        ManhattanSource::Genome => "genome",
    };

    // Get sequencing_type string for SigHitRow
    let sequencing_type = source_name.to_string();

    println!(
        "Processing {} partitions for Manhattan scan ({})...",
        partitions.len(),
        source_name
    );

    let layout = &spec.layout;
    let y_scale = &spec.y_scale;
    let table_path = &spec.table_path;
    let output_base = format!("{}/{}", spec.output_path.trim_end_matches('/'), source_name);
    let width = spec.width;
    let height = spec.height;
    let threshold = spec.threshold;
    let y_field = &spec.y_field;
    let phenotype = &spec.phenotype;
    let ancestry = &spec.ancestry;
    let contig_lengths = &spec.contig_lengths;
    let style = &spec.style;

    // Update telemetry with first partition
    if let Some(ref ts) = telemetry {
        if let Some(&part_id) = partitions.first() {
            ts.active_partition.store(part_id, Ordering::Relaxed);
        }
    }

    // Parallel scan+render+encode: each partition produces PNG + sig.parquet
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            let engine = QueryEngine::open_path(table_path)?;
            let iter = engine.scan_partition_iter(partition_id, &[])?;

            // Each thread has its own renderer with transparent background
            let mut renderer = ManhattanRenderer::new_transparent(width, height);

            // Per-chromosome renderers (lazy init)
            let mut chrom_renderers: HashMap<String, ManhattanRenderer> = HashMap::new();
            // Per-chromosome layouts (lazy init)
            let mut chrom_layouts: HashMap<String, ChromosomeLayout> = HashMap::new();

            let mut rows = 0usize;

            // Collect significant hits for this partition
            let mut sig_hits: Vec<SigHitRow> = Vec::new();

            for row_result in iter {
                let row = row_result?;
                rows += 1;

                if let Some(point) = extract_plot_data(&row, y_field) {
                    // For layout lookups, strip "chr" prefix if present
                    let contig_for_layout = if point.contig.starts_with("chr") {
                        &point.contig[3..]
                    } else {
                        &point.contig
                    };

                    // 1. Whole Genome Plot: Map to pixel coordinates and render
                    if let Some(x) = layout.get_x(contig_for_layout, point.position) {
                        let y = y_scale.get_y(point.neg_log10_p);
                        let color = layout.get_color(contig_for_layout);
                        renderer.render_point_with_radius(x, y, color, style.point_alpha, style.point_radius);
                    }

                    // 2. Per-Chromosome Plot
                    // Use normalized contig name for file/map keys (e.g., "chr1")
                    let normalized_contig = normalize_contig_name(&point.contig);

                    // Look up length using short name (e.g., "1") because that's what we have in map
                    if let Some(&len) = contig_lengths.get(contig_for_layout).or_else(|| contig_lengths.get(&normalized_contig)) {
                        // Initialize renderer and layout for this chromosome if needed
                        let chrom_layout = chrom_layouts.entry(normalized_contig.clone()).or_insert_with(|| {
                            // Create a layout where this single chromosome fills the width
                            ChromosomeLayout::new(&[(contig_for_layout.to_string(), len)], width, 0)
                        });

                        let chrom_renderer = chrom_renderers.entry(normalized_contig.clone()).or_insert_with(|| {
                            ManhattanRenderer::new_transparent(width, height)
                        });

                        if let Some(x) = chrom_layout.get_x(contig_for_layout, point.position) {
                            let y = y_scale.get_y(point.neg_log10_p);
                            // Use same color scheme as WG plot
                            let color = layout.get_color(contig_for_layout);
                            chrom_renderer.render_point_with_radius(x, y, color, style.point_alpha, style.point_radius);
                        }
                    }

                    // Check significance threshold
                    if point.pvalue < threshold {
                        // Extract additional fields for significant hit
                        let (ref_allele, alt_allele, beta, se, af) =
                            extract_sig_hit_fields(&row);

                        // Normalize contig to chr-prefixed format for output
                        let contig_normalized = normalize_contig_name(&point.contig);
                        // Calculate xpos for efficient ordering
                        let xpos = calculate_xpos(&point.contig, point.position);

                        sig_hits.push(SigHitRow {
                            phenotype: phenotype.clone(),
                            ancestry: ancestry.clone(),
                            sequencing_type: sequencing_type.clone(),
                            contig: contig_normalized,
                            position: point.position,
                            ref_allele,
                            alt_allele,
                            xpos,
                            pvalue: point.pvalue,
                            beta,
                            se,
                            af,
                            // Case/control and association fields not available in distributed scan phase
                            ac_cases: None,
                            ac_controls: None,
                            af_cases: None,
                            af_controls: None,
                            association_ac: None,
                        });
                    }
                }
            }

            // Encode WG PNG
            let png_data = renderer.encode_png()?;

            // Write WG PNG for this partition
            let png_file = format!("{}/part-{:05}.png", output_base, partition_id);
            if is_cloud_path(&png_file) {
                let mut writer = StreamingCloudWriter::new(&png_file)?;
                writer.write_all(&png_data)?;
                writer.finish()?;
            } else {
                std::fs::create_dir_all(&output_base)?;
                std::fs::write(&png_file, &png_data)?;
            }

            // Write Per-Chromosome PNGs
            // Structure: {output_root}/chroms/{chrom}/{source}/part-{id}.png
            let root = spec.output_path.trim_end_matches('/');
            for (chrom, chrom_renderer) in chrom_renderers {
                let chrom_png_data = chrom_renderer.encode_png()?;
                let chrom_path = format!("{}/chroms/{}/{}/part-{:05}.png", root, chrom, source_name, partition_id);

                if is_cloud_path(&chrom_path) {
                    let mut writer = StreamingCloudWriter::new(&chrom_path)?;
                    writer.write_all(&chrom_png_data)?;
                    writer.finish()?;
                } else {
                    if let Some(parent) = std::path::Path::new(&chrom_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&chrom_path, &chrom_png_data)?;
                }
            }

            // Write significant hits Parquet (even if empty - consistent output)
            let sig_file = format!("{}/part-{:05}-sig.parquet", output_base, partition_id);
            if is_cloud_path(&sig_file) {
                let cloud_writer = genohype_core::io::CloudWriter::new(&sig_file)?;
                let mut writer = SigHitWriter::from_writer(cloud_writer)?;
                for hit in sig_hits {
                    writer.write(hit)?;
                }
                // Get back the cloud writer and finish to trigger the upload
                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
            } else {
                let mut writer = SigHitWriter::new(&sig_file)?;
                for hit in sig_hits {
                    writer.write(hit)?;
                }
                writer.finish()?;
            }

            Ok(rows)
        })
        .collect();

    // Aggregate row counts
    let mut total_rows = 0usize;
    for result in results {
        total_rows += result?;
    }

    // Update telemetry
    if let Some(ref ts) = telemetry {
        ts.total_rows.fetch_add(total_rows, Ordering::Relaxed);
    }

    println!(
        "  Manhattan scan ({}) partitions {:?} complete: {} rows",
        source_name, partitions, total_rows
    );

    Ok((total_rows, None))
}


/// Extract fields for a significant hit from an encoded row.
fn extract_sig_hit_fields(row: &genohype_core::codec::EncodedValue) -> (String, String, Option<f64>, Option<f64>, Option<f64>) {
    use genohype_core::codec::EncodedValue;

    // Helper to get nested field
    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
        let mut current = value;
        for &field_name in path {
            if let EncodedValue::Struct(fields) = current {
                current = fields.iter().find(|(n, _)| n == field_name).map(|(_, v)| v)?;
            } else {
                return None;
            }
        }
        Some(current)
    }

    // Extract alleles
    let (ref_allele, alt_allele) = if let Some(EncodedValue::Array(alleles)) = get_field(row, &["alleles"]) {
        let ref_a = alleles.first()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let alt_a = alleles.get(1)
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        (ref_a, alt_a)
    } else {
        (String::new(), String::new())
    };

    // Extract BETA
    let beta = get_field(row, &["BETA"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    // Extract SE
    let se = get_field(row, &["SE"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    // Extract AF (AF_Allele2)
    let af = get_field(row, &["AF_Allele2"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    (ref_allele, alt_allele, beta, se, af)
}

/// Process Manhattan aggregate phase (V2 pipeline).
///
/// This is the Phase 2 worker task. It:
/// 1. Composites partial PNGs into final Manhattan plots
/// 2. Processes gene burden table (if provided)
/// 3. Merges pre-annotated significant hits
/// 4. Generates locus plots for significant regions
/// 5. Writes manifest.json
/// 6. Verifies outputs and writes to checkpoint
fn process_manhattan_aggregate(
    spec: &ManhattanAggregateSpec,
) -> Result<(usize, serde_json::Value)> {
    use crate::manhattan::aggregate::run_aggregation;

    let (rows, summary) = run_aggregation(spec)?;

    // Verify expected outputs exist and write to checkpoint
    if let Err(e) = verify_and_checkpoint(spec) {
        // Log but don't fail - aggregation succeeded
        eprintln!("Warning: checkpoint update failed: {}", e);
    }

    Ok((rows, summary))
}

/// A simple counting semaphore for limiting concurrency.
///
/// Used to bound memory usage by limiting how many partitions are processed
/// concurrently, independent of the number of CPU cores available.
#[cfg(feature = "clickhouse")]
#[derive(Clone)]
struct Semaphore {
    inner: Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>,
    max: usize,
}

#[cfg(feature = "clickhouse")]
impl Semaphore {
    fn new(max: usize) -> Self {
        Semaphore {
            inner: Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new())),
            max,
        }
    }

    fn acquire(&self) -> SemaphorePermit {
        let (lock, cvar) = &*self.inner;
        let mut count = lock.lock().unwrap();
        while *count >= self.max {
            count = cvar.wait(count).unwrap();
        }
        *count += 1;
        SemaphorePermit { sem: self.clone() }
    }
}

#[cfg(feature = "clickhouse")]
struct SemaphorePermit {
    sem: Semaphore,
}

#[cfg(feature = "clickhouse")]
impl Drop for SemaphorePermit {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.sem.inner;
        let mut count = lock.lock().unwrap();
        *count -= 1;
        cvar.notify_one();
    }
}

/// Verify expected outputs exist and append to checkpoint file.
fn verify_and_checkpoint(spec: &ManhattanAggregateSpec) -> Result<()> {
    use object_store::ObjectStore;
    use object_store::path::Path as ObjPath;

    // Build list of expected files
    // Note: significant.parquet files are only created when there are significant hits,
    // so we don't require them here. The manifest.json and PNG files are always created.
    let mut expected = vec!["manifest.json"];
    if spec.exome_results.is_some() {
        expected.push("exome_manhattan.png");
    }
    if spec.genome_results.is_some() {
        expected.push("genome_manhattan.png");
    }

    let url = url::Url::parse(&spec.output_path).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid output URL: {}", e),
        ))
    })?;

    #[cfg(feature = "gcp")]
    let store: std::sync::Arc<dyn ObjectStore> = {
        let bucket = url.host_str().ok_or_else(|| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing bucket in GCS URL",
            ))
        })?;
        genohype_core::io::get_gcs_client(bucket)?
    };

    #[cfg(not(feature = "gcp"))]
    return Ok(()); // Can't verify without GCS support

    #[cfg(feature = "gcp")]
    {
        let base_path = url.path().trim_start_matches('/');

        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            crate::HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Verify all expected files exist
        for file in &expected {
            let file_path = ObjPath::from(format!("{}/{}", base_path, file));
            rt.block_on(async {
                store.head(&file_path).await
            }).map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Missing expected output {}: {}", file, e),
                ))
            })?;
        }

        // Derive checkpoint path and relative phenotype path
        // output_path: gs://bucket/manhattans/meta/1234
        // checkpoint:  gs://bucket/manhattans/.completed
        // rel_path:    meta/1234
        let parts: Vec<&str> = base_path.rsplitn(3, '/').collect();
        if parts.len() < 3 {
            return Ok(()); // Can't determine checkpoint path
        }

        let phenotype_rel = format!("{}/{}", parts[1], parts[0]); // ancestry/id
        let base_dir = parts[2]; // everything before ancestry
        let checkpoint_path = ObjPath::from(format!("{}/.completed", base_dir));

        // Append to checkpoint file (read-modify-write with newline)
        let append_content = format!("{}\n", phenotype_rel);

        rt.block_on(async {
            // Try to read existing content
            let existing = match store.get(&checkpoint_path).await {
                Ok(result) => {
                    let bytes = result.bytes().await.unwrap_or_default();
                    String::from_utf8_lossy(&bytes).to_string()
                }
                Err(_) => String::new(),
            };

            // Check if already in checkpoint (idempotent)
            if existing.lines().any(|line| line.trim() == phenotype_rel) {
                return Ok::<(), object_store::Error>(());
            }

            // Append and write back
            let new_content = format!("{}{}", existing, append_content);
            store.put(&checkpoint_path, new_content.into()).await?;
            Ok(())
        }).map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to update checkpoint: {}", e),
            ))
        })?;

        println!("  Checkpoint: added {}", phenotype_rel);
        Ok(())
    }
}
