//! Generic worker loop for distributed task execution.
//!
//! This module provides a generic worker that polls a coordinator for work
//! and delegates task execution to a `TaskHandler` implementation.

use crate::distributed::message::{CompleteRequest, HeartbeatRequest, WorkRequest, WorkResponse};
use crate::traits::TaskHandler;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for a worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique identifier for this worker
    pub worker_id: String,
    /// URL of the coordinator
    pub coordinator_url: String,
    /// Polling interval in milliseconds
    pub poll_interval_ms: u64,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
    /// Git commit hash or package version of the worker binary
    pub build_version: Option<String>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: uuid::Uuid::new_v4().to_string(),
            coordinator_url: "http://localhost:3000".to_string(),
            poll_interval_ms: 1000,
            connect_timeout_secs: 10,
            build_version: None,
        }
    }
}

/// Run a worker loop that polls for work and delegates to the handler.
///
/// This function runs indefinitely until the coordinator signals exit
/// or an unrecoverable error occurs.
///
/// # Arguments
/// * `config` - Worker configuration
/// * `handler` - Task handler implementation
///
/// # Example
/// ```ignore
/// let handler = Arc::new(MyTaskHandler::new());
/// let config = WorkerConfig {
///     worker_id: "worker-1".to_string(),
///     coordinator_url: "http://coordinator:3000".to_string(),
///     ..Default::default()
/// };
/// run_worker(config, handler).await?;
/// ```
pub async fn run_worker(
    config: WorkerConfig,
    handler: Arc<dyn TaskHandler>,
) -> Result<(), anyhow::Error> {
    use crate::distributed::telemetry::SystemMetrics;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::signal::unix::{signal, SignalKind};

    println!(
        "Worker {} starting (version: {:?}), connecting to {}",
        config.worker_id, config.build_version, config.coordinator_url
    );

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .timeout(Duration::from_secs(300))
        .build()?;

    let work_url = format!("{}/work", config.coordinator_url);
    let complete_url = format!("{}/complete", config.coordinator_url);

    // Initialize System Metrics Collector
    let metrics = Arc::new(SystemMetrics::new());

    // Register Graceful Shutdown Hook (SIGTERM)
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown_flag.clone();
    tokio::spawn(async move {
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            sigterm.recv().await;
            println!("SIGTERM received. Finishing current task before shutting down...");
            shutdown_clone.store(true, Ordering::SeqCst);
        }
    });

    loop {
        // Request work from coordinator
        let work_response = match request_work(&client, &work_url, &config).await {
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
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
            WorkResponse::Task {
                tasks,
                input_path: _,
                payload,
                total_tasks: _,
                filters: _,
                intervals: _,
                session_id,
            } => {
                let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
                let task_labels: Vec<String> = tasks
                    .iter()
                    .map(|t| t.label.clone().unwrap_or_else(|| t.id.clone()))
                    .collect();
                println!("Received {} task(s): {:?}", tasks.len(), task_labels);

                // Spawn background heartbeat to keep coordinator from marking us dead
                // during long-running tasks. The heartbeat runs every 10s and stops
                // when the task completes (cancel token is dropped).
                let heartbeat_cancel = tokio_util::sync::CancellationToken::new();
                let heartbeat_handle = {
                    let cancel = heartbeat_cancel.clone();
                    let client = client.clone();
                    let heartbeat_url = format!("{}/heartbeat", config.coordinator_url);
                    let worker_id = config.worker_id.clone();
                    let build_version = config.build_version.clone();
                    let metrics = metrics.clone();

                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                                    let req = HeartbeatRequest {
                                        worker_id: worker_id.clone(),
                                        telemetry: metrics.snapshot(10.0),
                                        build_version: build_version.clone(),
                                    };
                                    let _ = client.post(&heartbeat_url).json(&req).send().await;
                                }
                                _ = cancel.cancelled() => break,
                            }
                        }
                    })
                };

                // Execute the task using the handler
                let result = handler.handle_task(&payload, tasks).await;

                // Stop heartbeat
                heartbeat_cancel.cancel();
                let _ = heartbeat_handle.await;

                let (items_processed, result_json, error) = match result {
                    Ok(task_result) => {
                        if task_result.is_success() {
                            metrics.record_task_completion(task_result.items_processed);
                            (task_result.items_processed, task_result.result_json, None)
                        } else {
                            (0, None, task_result.error)
                        }
                    }
                    Err(e) => (0, None, Some(format!("{}", e))),
                };

                // Report completion
                let request = CompleteRequest {
                    worker_id: config.worker_id.clone(),
                    tasks: task_ids.clone(),
                    items_processed,
                    result_json,
                    error,
                    session_id,
                };

                if let Err(e) = client.post(&complete_url).json(&request).send().await {
                    eprintln!("Failed to report completion: {}", e);
                }

                println!("Completed tasks {:?} ({} items)", task_ids, items_processed);
            }
            WorkResponse::UpdateBinary { gcs_url } => {
                println!(
                    "Received UpdateBinary request. Self-updating from {}",
                    gcs_url
                );

                async fn update_logic(client: &reqwest::Client, url: &str) -> anyhow::Result<()> {
                    use std::os::unix::fs::PermissionsExt;

                    // Download to temp file
                    let resp = client.get(url).send().await?;
                    let bytes = resp.bytes().await?;
                    let tmp_path = "/tmp/genohype-update";
                    std::fs::write(tmp_path, &bytes)?;

                    // Make executable and replace target
                    std::fs::set_permissions(tmp_path, std::fs::Permissions::from_mode(0o755))?;
                    std::fs::rename(tmp_path, "/usr/local/bin/genohype")?;

                    // Restart via systemd
                    std::process::Command::new("sudo")
                        .args(["systemctl", "restart", "genohype-worker"])
                        .spawn()?;

                    Ok(())
                }

                if let Err(e) = update_logic(&client, &gcs_url).await {
                    eprintln!(
                        "Failed to perform self-update: {}. Continuing with old binary.",
                        e
                    );
                    continue;
                }

                // Give systemd a moment to cleanly terminate us
                tokio::time::sleep(Duration::from_secs(1)).await;
                break;
            }
        }

        // Evaluate graceful shutdown after each operation
        if shutdown_flag.load(std::sync::atomic::Ordering::SeqCst) {
            println!("Graceful shutdown active: Exiting worker loop safely.");
            break;
        }
    }

    Ok(())
}

/// Request work from the coordinator.
async fn request_work(
    client: &reqwest::Client,
    url: &str,
    config: &WorkerConfig,
) -> Result<WorkResponse, anyhow::Error> {
    let request = WorkRequest {
        worker_id: config.worker_id.clone(),
        hardware: None,
        build_version: config.build_version.clone(),
    };

    let response = client.post(url).json(&request).send().await?;

    let work_response: WorkResponse = response.json().await?;
    Ok(work_response)
}
