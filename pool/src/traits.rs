//! Core traits for the distributed worker pool.
//!
//! These traits define the abstraction boundary between the generic pool infrastructure
//! and domain-specific task handling logic. By implementing these traits, applications
//! can use the pool's worker management, coordinator, and cloud orchestration without
//! coupling to specific job types.

use async_trait::async_trait;
use serde_json::Value;

use crate::distributed::message::TaskDescriptor;

/// Result of task execution.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// Number of items processed (e.g., rows, records)
    pub items_processed: usize,
    /// Optional result data (e.g., aggregated stats, computed values)
    pub result_json: Option<Value>,
    /// Error message if the task failed (None = success)
    pub error: Option<String>,
}

impl TaskResult {
    /// Create a successful result.
    pub fn success(items_processed: usize, result_json: Option<Value>) -> Self {
        Self {
            items_processed,
            result_json,
            error: None,
        }
    }

    /// Create a failed result.
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            items_processed: 0,
            result_json: None,
            error: Some(error.into()),
        }
    }

    /// Check if the task succeeded.
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

/// Trait for handling task execution in a worker.
///
/// Implementors receive generic JSON payloads and task descriptors,
/// deserialize them into domain-specific types, and execute the work.
///
/// # Example
///
/// ```ignore
/// struct MyTaskHandler;
///
/// #[async_trait]
/// impl TaskHandler for MyTaskHandler {
///     async fn handle_task(
///         &self,
///         payload: &Value,
///         tasks: Vec<TaskDescriptor>,
///     ) -> Result<TaskResult, anyhow::Error> {
///         let job: MyJobSpec = serde_json::from_value(payload.clone())?;
///         // Process the job using task descriptors...
///         Ok(TaskResult::success(rows_processed, None))
///     }
/// }
/// ```
#[async_trait]
pub trait TaskHandler: Send + Sync {
    /// Handle a task with the given payload and task descriptors.
    ///
    /// # Arguments
    /// * `payload` - JSON-serialized job specification
    /// * `tasks` - Self-describing task descriptors to process
    ///
    /// # Returns
    /// * `Ok(TaskResult)` - Task completed (check `is_success()` for status)
    /// * `Err(...)` - Unrecoverable error during task execution
    async fn handle_task(
        &self,
        payload: &Value,
        tasks: Vec<TaskDescriptor>,
    ) -> Result<TaskResult, anyhow::Error>;
}

/// Work assignment from the coordinator.
#[derive(Debug, Clone)]
pub struct WorkAssignment {
    /// Self-describing task descriptors to process
    pub tasks: Vec<TaskDescriptor>,
    /// JSON-serialized job specification
    pub payload: Value,
    /// Optional: input path for the data source
    pub input_path: Option<String>,
    /// Optional: filter conditions
    pub filters: Vec<String>,
}

/// Trait for coordinator state management.
///
/// This trait abstracts the work queue and state machine logic, allowing
/// the generic coordinator HTTP server to work with different job types
/// and scheduling strategies.
///
/// Implementors manage their own state (e.g., task queues, batch tracking,
/// multi-phase pipelines) and expose a simple interface for the coordinator.
#[async_trait]
pub trait CoordinatorPlugin: Send + Sync {
    /// Get work for a worker.
    ///
    /// Returns `Some(WorkAssignment)` if work is available, or `None` if
    /// the worker should wait.
    ///
    /// # Arguments
    /// * `worker_id` - Unique identifier for the requesting worker
    async fn get_work(&self, worker_id: &str) -> Option<WorkAssignment>;

    /// Mark work as complete.
    ///
    /// # Arguments
    /// * `worker_id` - Worker that completed the work
    /// * `tasks` - Task IDs that were completed (matches TaskDescriptor.id values)
    /// * `result` - Result of task execution
    async fn complete_work(
        &self,
        worker_id: &str,
        tasks: &[String],
        result: TaskResult,
    ) -> Result<(), anyhow::Error>;

    /// Check if all work is complete.
    ///
    /// Returns `true` when there is no pending or in-progress work.
    async fn is_complete(&self) -> bool;

    /// Get the current status for reporting.
    ///
    /// Returns (pending, processing, completed, total) counts.
    async fn get_status(&self) -> (usize, usize, usize, usize);
}
