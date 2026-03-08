//! Worker client for distributed processing.
//!
//! The worker connects to a coordinator, requests work, processes partitions,
//! and reports completion. It loops until receiving an Exit response.
//!
//! Includes a background telemetry loop that sends heartbeats with system
//! metrics to the coordinator for the dashboard UI.

pub mod telemetry;
pub mod dispatch;
pub mod handlers;

use crate::distributed::message::{
    CompleteRequest, JobSpec, TaskType, WorkRequest, WorkResponse,
};
use crate::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub use dispatch::dispatch_job;
pub use telemetry::{TelemetryState, NO_ACTIVE_PARTITION, spawn_telemetry_loop};

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
    let telemetry_state = Arc::new(TelemetryState::new());

    // Spawn background telemetry heartbeat loop
    let heartbeat_handle = spawn_telemetry_loop(
        client.clone(),
        config.coordinator_url.clone(),
        config.worker_id.clone(),
        telemetry_state.clone(),
    );

    // Cache the QueryEngine between work requests to avoid re-reading metadata
    let mut cached_engine: Option<(String, genohype_core::query::QueryEngine)> = None;

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
            WorkResponse::UpdateBinary { gcs_url } => {
                // Use current executable path so we replace exactly what is running
                let current_exe = std::env::current_exe().unwrap_or_else(|_| {
                    std::path::PathBuf::from("/usr/local/bin/genohype")
                });
                let exe_path = current_exe.to_string_lossy().to_string();

                println!(
                    "Received UpdateBinary signal. Updating {} from {}...",
                    exe_path, gcs_url
                );

                let temp_path = "/tmp/genohype-new";

                // 1. Try gsutil first
                let status = std::process::Command::new("gsutil")
                    .args(["cp", &gcs_url, temp_path])
                    .status();

                if !status.map(|s| s.success()).unwrap_or(false) {
                    println!("gsutil failed, falling back to curl from coordinator...");
                    // 2. Fallback to pulling from coordinator API
                    let curl_url = format!("{}/api/binary", config.coordinator_url);
                    let curl_status = std::process::Command::new("curl")
                        .args(["-sL", "--retry", "3", &curl_url, "-o", temp_path])
                        .status();

                    if !curl_status.map(|s| s.success()).unwrap_or(false) {
                        eprintln!(
                            "Failed to download binary via both gsutil and curl. Retrying later."
                        );
                        tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
                        continue;
                    }
                }

                // Make executable
                std::process::Command::new("chmod")
                    .args(["+x", temp_path])
                    .status()
                    .ok();

                // Try standard rename first (works if same filesystem and permissions)
                if std::fs::rename(temp_path, &exe_path).is_err() {
                    // Fall back to sudo mv
                    let mv_status = std::process::Command::new("sudo")
                        .args(["mv", temp_path, &exe_path])
                        .status();

                    if !mv_status.map(|s| s.success()).unwrap_or(false) {
                        eprintln!("Failed to move binary to {}. Retrying later.", exe_path);
                        tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
                        continue;
                    }
                }

                println!("Binary updated at {}. Restarting worker...", exe_path);

                use std::os::unix::process::CommandExt;
                let args: Vec<String> = std::env::args().collect();
                let err = std::process::Command::new(&exe_path)
                    .args(&args[1..])
                    .exec();

                // We only reach here if exec fails
                return Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to exec new binary at {}: {}", exe_path, err),
                )));
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
                        let error_msg = format!("Task panicked: {}", e);
                        eprintln!("Task panicked processing tasks {:?}: {}", task_ids, e);
                        cached_engine = None;

                        // Report panic/crash to coordinator so tasks get requeued
                        let fail_req = CompleteRequest {
                            worker_id: config.worker_id.clone(),
                            tasks: task_ids.clone(),
                            items_processed: 0,
                            result_json: None,
                            error: Some(error_msg),
                        };
                        if let Err(post_err) = client.post(&complete_url).json(&fail_req).send().await {
                            eprintln!("Failed to report panic to coordinator: {}", post_err);
                        }
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
        build_version: Some(env!("GIT_HASH").to_string()),
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
