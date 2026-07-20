//! Stress test handler.
//!
//! Processes synthetic stress test workloads to validate distributed
//! processing infrastructure with controlled CPU, memory, and I/O loads.

use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::io::StreamingCloudWriter;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Process a synthetic stress test workload.
///
/// Simulates CPU, Memory, and Network I/O loads concurrently based on the `StressSpec` parameters.
/// Uses catch_unwind to handle panics (e.g., OOM) gracefully per-partition.
/// Limits parallelism based on available memory to prevent OOM.
pub fn process_stress(
    partitions: &[usize],
    spec: &crate::distributed::message::StressSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<usize> {
    use rayon::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use sysinfo::{MemoryRefreshKind, RefreshKind, System};

    // Check available memory BEFORE processing any partitions
    // Fail early if we can't possibly fit the batch (unless skip_memory_check is set)
    let max_parallel = if spec.memory_mb > 0 && !spec.skip_memory_check {
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::new().with_ram()),
        );
        sys.refresh_memory();
        let available_mb = sys.available_memory() / (1024 * 1024);
        // Allow 70% of available memory (30% headroom for system)
        let usable_mb = (available_mb as f64 * 0.7) as u64;
        let per_partition_mb = spec.memory_mb as u64;
        let safe_parallel = (usable_mb / per_partition_mb).max(1) as usize;

        // CRITICAL: If we can't even fit ONE partition, fail immediately
        if safe_parallel == 0 || available_mb < per_partition_mb {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                format!(
                    "Batch rejected: insufficient memory ({} MB available, need {} MB per partition, {} partitions requested)",
                    available_mb, per_partition_mb, partitions.len()
                ),
            )));
        }

        // If batch is larger than what we can safely handle, fail early
        // This prevents partial completion where some tasks succeed and others OOM
        if partitions.len() > safe_parallel {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                format!(
                    "Batch too large for available memory: {} partitions requested but only {} can fit ({} MB available, {} MB per partition)",
                    partitions.len(), safe_parallel, available_mb, per_partition_mb
                ),
            )));
        }

        println!(
            "Memory check passed: {} partitions, {} MB available, {} MB per partition",
            partitions.len(),
            available_mb,
            per_partition_mb
        );
        safe_parallel
    } else if spec.skip_memory_check {
        // Skip memory check - dangerous mode for OOM testing
        println!("WARNING: Skipping memory pre-flight check (--skip-memory-check)");
        rayon::current_num_threads()
    } else {
        rayon::current_num_threads()
    };

    println!(
        "Processing {} stress partitions (max {} parallel)...",
        partitions.len(),
        max_parallel
    );

    // Use a custom thread pool with limited parallelism
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(max_parallel.min(rayon::current_num_threads()))
        .build()
        .unwrap_or_else(|_| rayon::ThreadPoolBuilder::new().build().unwrap());

    let results: Vec<Result<usize>> = pool.install(|| {
        partitions
            .par_iter()
            .map(|&partition_id| {
                // Wrap each partition's work in catch_unwind to handle OOM panics gracefully
                let result = catch_unwind(AssertUnwindSafe(|| {
                    process_single_stress_partition(partition_id, spec, telemetry.as_ref())
                }));

                match result {
                    Ok(inner_result) => inner_result,
                    Err(panic_info) => {
                        let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Unknown panic".to_string()
                        };
                        eprintln!("Partition {} panicked: {}", partition_id, panic_msg);
                        Err(crate::HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Partition {} panicked: {}", partition_id, panic_msg),
                        )))
                    }
                }
            })
            .collect()
    });

    let mut total_rows = 0;
    let mut errors = Vec::new();
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(rows) => total_rows += rows,
            Err(e) => errors.push((partitions[i], e)),
        }
    }

    if !errors.is_empty() {
        // Return first error but log all
        let (part, first_err) = errors.remove(0);
        for (p, e) in &errors {
            eprintln!("Additional error for partition {}: {}", p, e);
        }
        return Err(crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "Partition {} failed: {} ({} other partitions also failed)",
                part,
                first_err,
                errors.len()
            ),
        )));
    }

    Ok(total_rows)
}

/// Process a single stress partition (extracted for catch_unwind).
fn process_single_stress_partition(
    partition_id: usize,
    spec: &crate::distributed::message::StressSpec,
    telemetry: Option<&Arc<TelemetryState>>,
) -> Result<usize> {
    use std::io::{Read, Write};
    use std::time::Instant;
    use sysinfo::{MemoryRefreshKind, RefreshKind, System};

    // Tag this thread for the dashboard's per-core task view (stress task type)
    let _core_guard =
        telemetry.map(|ts| CoreTaskGuard::custom(ts, "stress", partition_id.to_string()));

    // Calculate actual memory to allocate (with optional jitter)
    let actual_memory_mb = if let Some(pct) = spec.memory_jitter_pct {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        partition_id.hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
            .hash(&mut hasher);
        let pseudo_rand = (hasher.finish() % 1000) as f64 / 1000.0; // 0.0 to 1.0

        let factor = 1.0 + (pseudo_rand - 0.5) * 2.0 * (pct as f64 / 100.0);
        ((spec.memory_mb as f64 * factor) as usize).max(1)
    } else {
        spec.memory_mb
    };

    // 1. Memory Load: allocate and hold a large vector.
    // Pre-check available memory to avoid allocation failure (which aborts, not panics)
    let mut mem_hog: Vec<u64> = Vec::new();
    if actual_memory_mb > 0 && !spec.skip_memory_check {
        let required_bytes = actual_memory_mb as u64 * 1024 * 1024;

        // Check available memory before attempting allocation
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::new().with_ram()),
        );
        sys.refresh_memory();
        let available = sys.available_memory();

        // Require at least 20% headroom to avoid OOM
        let required_with_headroom = required_bytes + (required_bytes / 5);
        if available < required_with_headroom {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                format!(
                    "Partition {} skipped: insufficient memory ({} MB available, {} MB required)",
                    partition_id,
                    available / (1024 * 1024),
                    required_with_headroom / (1024 * 1024)
                ),
            )));
        }

        let elements = (actual_memory_mb * 1024 * 1024) / 8;
        mem_hog.resize(elements, 0u64);
        // Force the OS to actually allocate the physical pages by writing to them.
        for i in 0..elements {
            mem_hog[i] = i as u64;
        }
    } else if actual_memory_mb > 0 {
        // Skip memory check mode - allocate without pre-check (dangerous)
        let elements = (actual_memory_mb * 1024 * 1024) / 8;
        mem_hog.resize(elements, 0u64);
        for i in 0..elements {
            mem_hog[i] = i as u64;
        }
    }

    // 2. CPU Load with optional gradual memory leak
    // If leak_memory_mb is set, allocate memory gradually during CPU work
    let mut leaked_memory: Vec<Vec<u8>> = Vec::new();
    if spec.cpu_secs > 0.0 {
        let start = Instant::now();
        let mut dummy: f64 = 1.0;

        // Calculate leak parameters
        let leak_chunks = if let Some(leak_mb) = spec.leak_memory_mb {
            let chunk_size = 64 * 1024 * 1024; // 64MB chunks
            (leak_mb * 1024 * 1024) / chunk_size
        } else {
            0
        };

        let chunk_interval = if leak_chunks > 0 {
            spec.cpu_secs / leak_chunks as f64
        } else {
            f64::MAX
        };

        let mut next_leak_time = chunk_interval;

        while start.elapsed().as_secs_f64() < spec.cpu_secs {
            // Inner tight loop to avoid excessive clock reads
            for _ in 0..10_000 {
                dummy = dummy.sin().cos().exp();
            }

            // Gradual memory leak during CPU work
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed >= next_leak_time && spec.leak_memory_mb.is_some() {
                let chunk = vec![0xAA_u8; 64 * 1024 * 1024]; // 64MB chunk
                leaked_memory.push(chunk);
                next_leak_time += chunk_interval;
            }
        }
        // Prevent the compiler from optimizing away the math loop
        std::hint::black_box(dummy);
    }
    // Keep leaked memory alive
    std::hint::black_box(&leaked_memory);

    // 3. Generate read data if requested (write temp file, then read it back)
    let generated_read_path = if spec.generate_read_data {
        if let Some(write_dir) = &spec.write_dir {
            let temp_path = format!(
                "{}/stress_read_{}.bin",
                write_dir.trim_end_matches('/'),
                partition_id
            );

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
                if n == 0 {
                    break;
                }
            }
        }
    }

    // 5. Network TX Load: stream random bytes to a remote path (separate from generated read data).
    if let Some(write_dir) = &spec.write_dir {
        // Only write stress output files if not in generate-read-data-only mode
        // or if we want both TX and generated read data
        if !spec.generate_read_data {
            let out_path = format!(
                "{}/stress_{}.bin",
                write_dir.trim_end_matches('/'),
                partition_id
            );
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
    if let Some(t) = telemetry {
        t.total_rows.fetch_add(rows_simulated, Ordering::Relaxed);
    }

    Ok(rows_simulated)
}
