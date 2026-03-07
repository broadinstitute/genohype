//! Generic worker loop for distributed task execution.
//!
//! This module provides a generic worker that polls a coordinator for work
//! and delegates task execution to a `TaskHandler` implementation.

use crate::distributed::message::{CompleteRequest, WorkRequest, WorkResponse};
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
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: uuid::Uuid::new_v4().to_string(),
            coordinator_url: "http://localhost:3000".to_string(),
            poll_interval_ms: 1000,
            connect_timeout_secs: 10,
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
    println!(
        "Worker {} starting, connecting to {}",
        config.worker_id, config.coordinator_url
    );

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .timeout(Duration::from_secs(300))
        .build()?;

    let work_url = format!("{}/work", config.coordinator_url);
    let complete_url = format!("{}/complete", config.coordinator_url);

    loop {
        // Request work from coordinator
        let work_response = match request_work(&client, &work_url, &config.worker_id).await {
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
            } => {
                let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
                let task_labels: Vec<String> = tasks
                    .iter()
                    .map(|t| t.label.clone().unwrap_or_else(|| t.id.clone()))
                    .collect();
                println!(
                    "Received {} task(s): {:?}",
                    tasks.len(),
                    task_labels
                );

                // Execute the task using the handler
                let result = handler.handle_task(&payload, tasks).await;

                let (items_processed, result_json, error) = match result {
                    Ok(task_result) => {
                        if task_result.is_success() {
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
                };

                if let Err(e) = client.post(&complete_url).json(&request).send().await {
                    eprintln!("Failed to report completion: {}", e);
                }

                println!(
                    "Completed tasks {:?} ({} items)",
                    task_ids, items_processed
                );
            }
            WorkResponse::UpdateBinary { gcs_url } => {
                println!("Received UpdateBinary request: {}", gcs_url);
                // TODO: Implement binary update logic
            }
        }
    }

    Ok(())
}

/// Request work from the coordinator.
async fn request_work(
    client: &reqwest::Client,
    url: &str,
    worker_id: &str,
) -> Result<WorkResponse, anyhow::Error> {
    let request = WorkRequest {
        worker_id: worker_id.to_string(),
        hardware: None,
        build_version: None, // Pool crate doesn't have GIT_HASH; CLI worker sets this
    };

    let response = client.post(url).json(&request).send().await?;

    let work_response: WorkResponse = response.json().await?;
    Ok(work_response)
}
