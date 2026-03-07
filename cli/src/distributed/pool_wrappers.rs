//! Wrapper implementations for genohype-pool traits.
//!
//! This module bridges the domain-specific CLI logic with the generic pool
//! infrastructure by implementing the TaskHandler and CoordinatorPlugin traits.
//!
//! # Design
//!
//! The wrappers serve as the boundary between:
//! - Generic pool infrastructure (HTTP server, worker loop, cloud orchestration)
//! - Domain-specific logic (JobSpec parsing, Manhattan pipeline, Hail queries)
//!
//! This allows the pool crate to be completely decoupled from the CLI's domain types.

use crate::distributed::message::{JobSpec, TaskType};
use crate::distributed::worker::dispatch_job;
use genohype_pool::distributed::TaskDescriptor;
use genohype_pool::{async_trait, CoordinatorPlugin, TaskHandler, TaskResult, WorkAssignment};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Task handler implementation that wraps the existing dispatch_job function.
///
/// This handler deserializes the generic JSON payload into a JobSpec and
/// delegates to the existing job dispatch logic.
pub struct CliTaskHandler {
    /// Input path for the Hail table
    pub input_path: String,
    /// Filters to apply during processing
    pub filters: Vec<String>,
    /// Interval filters to apply
    pub intervals: Vec<String>,
}

impl CliTaskHandler {
    /// Create a new CLI task handler.
    pub fn new(input_path: String, filters: Vec<String>, intervals: Vec<String>) -> Self {
        Self {
            input_path,
            filters,
            intervals,
        }
    }
}

#[async_trait]
impl TaskHandler for CliTaskHandler {
    async fn handle_task(
        &self,
        payload: &Value,
        tasks: Vec<TaskDescriptor>,
    ) -> Result<TaskResult, anyhow::Error> {
        // Deserialize the generic payload into our domain-specific JobSpec
        let job_spec: JobSpec = serde_json::from_value(payload.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize JobSpec: {}", e))?;

        // Extract partition indices from task descriptors for backwards compatibility
        // with the existing dispatch_job interface
        let partitions: Vec<usize> = tasks
            .iter()
            .filter_map(|t| {
                if let Ok(task_type) = serde_json::from_value::<TaskType>(t.payload.clone()) {
                    if let TaskType::Partition { partition_index, .. } = task_type {
                        return Some(partition_index);
                    }
                }
                // Fallback: try to parse the task ID as a partition index
                t.id.parse::<usize>().ok()
            })
            .collect();

        let input_path = self.input_path.clone();
        let filters = self.filters.clone();
        let intervals = self.intervals.clone();

        // Run on blocking thread since dispatch_job uses blocking I/O
        let result = tokio::task::spawn_blocking(move || {
            dispatch_job(
                None, // No cached engine for now
                &partitions,
                &input_path,
                &job_spec,
                &filters,
                &intervals,
                None, // No telemetry state for now
            )
        })
        .await?;

        match result {
            Ok((rows, result_json, _cached_engine)) => Ok(TaskResult::success(rows, result_json)),
            Err(e) => Ok(TaskResult::failure(format!("{}", e))),
        }
    }
}

/// Simple work queue state for the coordinator plugin.
///
/// This is a minimal implementation that demonstrates the abstraction.
/// In a full implementation, this would wrap the existing CoordinatorData
/// with all its batch/pipeline state machines.
pub struct SimpleWorkQueue {
    /// Partitions waiting to be assigned
    pub pending: VecDeque<usize>,
    /// Partitions being processed: partition_id -> (worker_id, start_time)
    pub processing: HashMap<usize, (String, Instant)>,
    /// Completed partitions
    pub completed: HashSet<usize>,
    /// Total partitions
    pub total: usize,
    /// Job specification (serialized to JSON for workers)
    pub job_spec: Option<Value>,
    /// Input path
    pub input_path: String,
    /// Filters
    pub filters: Vec<String>,
}

impl SimpleWorkQueue {
    /// Create a new work queue with the given number of partitions.
    pub fn new(total_partitions: usize, input_path: String) -> Self {
        Self {
            pending: (0..total_partitions).collect(),
            processing: HashMap::new(),
            completed: HashSet::new(),
            total: total_partitions,
            job_spec: None,
            input_path,
            filters: Vec::new(),
        }
    }

    /// Set the job specification.
    pub fn set_job_spec(&mut self, spec: &JobSpec) {
        self.job_spec = serde_json::to_value(spec).ok();
    }
}

/// Coordinator plugin implementation using a simple work queue.
///
/// This demonstrates the abstraction pattern. In later sessions, this will
/// be extended to wrap the full CoordinatorData with batch/pipeline support.
pub struct CliCoordinatorPlugin {
    /// Work queue state
    state: Arc<Mutex<SimpleWorkQueue>>,
}

impl CliCoordinatorPlugin {
    /// Create a new coordinator plugin with the given work queue.
    pub fn new(state: Arc<Mutex<SimpleWorkQueue>>) -> Self {
        Self { state }
    }

    /// Create with initial partitions.
    pub fn with_partitions(total_partitions: usize, input_path: String) -> Self {
        Self {
            state: Arc::new(Mutex::new(SimpleWorkQueue::new(total_partitions, input_path))),
        }
    }

    /// Get a reference to the state for configuration.
    pub fn state(&self) -> Arc<Mutex<SimpleWorkQueue>> {
        self.state.clone()
    }
}

#[async_trait]
impl CoordinatorPlugin for CliCoordinatorPlugin {
    async fn get_work(&self, worker_id: &str) -> Option<WorkAssignment> {
        let mut queue = self.state.lock().ok()?;

        // Get next pending partition
        let partition_id = queue.pending.pop_front()?;

        // Mark as processing
        queue
            .processing
            .insert(partition_id, (worker_id.to_string(), Instant::now()));

        // Create a TaskDescriptor for this partition
        let task = TaskDescriptor::partition(partition_id, queue.total);

        Some(WorkAssignment {
            tasks: vec![task],
            payload: queue.job_spec.clone().unwrap_or_else(|| serde_json::json!({})),
            input_path: Some(queue.input_path.clone()),
            filters: queue.filters.clone(),
        })
    }

    async fn complete_work(
        &self,
        _worker_id: &str,
        tasks: &[String],
        result: TaskResult,
    ) -> Result<(), anyhow::Error> {
        let mut queue = self
            .state
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock state: {}", e))?;

        // Convert task IDs back to partition indices and update state
        for task_id in tasks {
            if let Ok(partition_id) = task_id.parse::<usize>() {
                if result.is_success() {
                    queue.processing.remove(&partition_id);
                    queue.completed.insert(partition_id);
                } else {
                    // On failure, re-queue the partition
                    queue.processing.remove(&partition_id);
                    queue.pending.push_back(partition_id);
                }
            }
        }

        Ok(())
    }

    async fn is_complete(&self) -> bool {
        let queue = match self.state.lock() {
            Ok(q) => q,
            Err(_) => return false,
        };

        queue.pending.is_empty() && queue.processing.is_empty()
    }

    async fn get_status(&self) -> (usize, usize, usize, usize) {
        let queue = match self.state.lock() {
            Ok(q) => q,
            Err(_) => return (0, 0, 0, 0),
        };

        (
            queue.pending.len(),
            queue.processing.len(),
            queue.completed.len(),
            queue.total,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_plugin_basic() {
        let plugin = CliCoordinatorPlugin::with_partitions(5, "/test/path".to_string());

        // Check initial status
        let (pending, processing, completed, total) = plugin.get_status().await;
        assert_eq!(pending, 5);
        assert_eq!(processing, 0);
        assert_eq!(completed, 0);
        assert_eq!(total, 5);

        // Get work
        let work = plugin.get_work("worker-1").await;
        assert!(work.is_some());
        let work = work.unwrap();
        let task_ids: Vec<String> = work.tasks.iter().map(|t| t.id.clone()).collect();

        // Check status after get_work
        let (pending, processing, _, _) = plugin.get_status().await;
        assert_eq!(pending, 4);
        assert_eq!(processing, 1);

        // Complete work using the actual task IDs
        let result = TaskResult::success(100, None);
        plugin.complete_work("worker-1", &task_ids, result).await.unwrap();

        // Check status after complete
        let (pending, processing, completed, _) = plugin.get_status().await;
        assert_eq!(pending, 4);
        assert_eq!(processing, 0);
        assert_eq!(completed, 1);
    }
}
