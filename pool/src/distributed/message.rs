//! Generic message types for Coordinator/Worker communication.
//!
//! These messages define the network protocol between coordinators and workers.
//! They use `serde_json::Value` for job specifications to remain decoupled from
//! domain-specific types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A self-describing work unit in the coordinator's queue.
///
/// Unlike bare indices, `TaskDescriptor` carries full context for logging,
/// dashboard display, and type-safe dispatch. The `payload` field holds
/// domain-specific operation details (serialized as JSON).
///
/// # Example
///
/// ```ignore
/// TaskDescriptor {
///     id: "15".to_string(),
///     task_type: "partition".to_string(),
///     label: Some("Partition 15 → Parquet".to_string()),
///     index: Some(15),
///     total: Some(200),
///     payload: serde_json::json!({
///         "type": "partition",
///         "table_path": "gs://bucket/table.ht",
///         "partition_index": 15,
///         "op": { "op": "export_parquet", "output_path": "gs://bucket/output/" }
///     }),
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDescriptor {
    /// Unique identifier within this job (e.g., "0", "PHENO_123", "stress_42")
    pub id: String,

    /// What kind of work this represents (e.g., "partition", "phenotype", "stress")
    pub task_type: String,

    /// Human-readable label for dashboard display (e.g., "Blood Pressure (EUR)")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Progress tracking: which item is this out of how many?
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<usize>,

    /// Total items in this category (for progress bars)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,

    /// Domain-specific operation details (serialized TaskType enum)
    pub payload: Value,
}

impl TaskDescriptor {
    /// Create a simple partition task (backwards compatibility helper).
    pub fn partition(index: usize, total: usize) -> Self {
        Self {
            id: index.to_string(),
            task_type: "partition".to_string(),
            label: Some(format!("Partition {}", index + 1)),
            index: Some(index),
            total: Some(total),
            payload: serde_json::json!({
                "partition_index": index
            }),
        }
    }

    /// Convert to CoreTaskInfo for per-core telemetry tracking.
    pub fn to_core_task_info(&self) -> CoreTaskInfo {
        CoreTaskInfo {
            task_type: self.task_type.clone(),
            task_id: self.id.clone(),
            label: self.label.clone(),
            parent: None,
        }
    }
}

/// Hardware capabilities of the worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareSpec {
    /// Number of logical CPU cores
    pub num_cores: usize,
    /// Total system memory in megabytes
    pub total_memory_mb: u64,
}

/// Request from a worker asking for work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkRequest {
    /// Unique identifier for this worker
    pub worker_id: String,
    /// Worker hardware specification
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware: Option<HardwareSpec>,
    /// Git commit hash of the worker binary
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
}

/// Response from coordinator with work assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WorkResponse {
    /// Work is available - process these tasks
    #[serde(rename = "task")]
    Task {
        /// Self-describing task descriptors (replaces task_id + partitions)
        tasks: Vec<TaskDescriptor>,
        /// Path to input data
        input_path: String,
        /// Job specification (domain-specific, serialized as JSON)
        payload: Value,
        /// Total number of tasks in the job (for progress tracking)
        total_tasks: usize,
        /// Filter conditions
        #[serde(default)]
        filters: Vec<String>,
        /// Interval filters
        #[serde(default)]
        intervals: Vec<String>,
    },
    /// No work available but job is still in progress - wait and retry
    #[serde(rename = "wait")]
    Wait,
    /// All work is complete - worker should exit
    #[serde(rename = "exit")]
    Exit,
    /// Update binary and restart
    #[serde(rename = "update")]
    UpdateBinary { gcs_url: String },
}

/// Request to update the fleet binary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateFleetRequest {
    pub gcs_url: String,
}

/// Request from a worker reporting completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRequest {
    /// Worker that completed the work
    pub worker_id: String,
    /// Task IDs that were completed (or failed) - matches TaskDescriptor.id values
    pub tasks: Vec<String>,
    /// Number of items processed
    pub items_processed: usize,
    /// Optional result data for aggregation
    #[serde(default)]
    pub result_json: Option<Value>,
    /// Error message if the task failed (None = success)
    #[serde(default)]
    pub error: Option<String>,
}

/// Response to completion request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteResponse {
    /// Whether the completion was acknowledged
    pub acknowledged: bool,
}

/// Status query response from coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Number of tasks pending
    pub pending_tasks: usize,
    /// Number of tasks currently being processed
    pub processing_tasks: usize,
    /// Number of tasks completed
    pub completed_tasks: usize,
    /// Total tasks in the job
    pub total_tasks: usize,
    /// Total items processed so far
    pub total_items: usize,
    /// Number of tasks that permanently failed
    pub failed_tasks: usize,
    /// Whether the job is complete
    pub is_complete: bool,
}

/// Describes what work a specific CPU core/thread is currently executing.
///
/// This provides rich context for dashboard visualization, supporting any job type
/// and nested parallelism (e.g., phenotype → locus plots).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreTaskInfo {
    /// Type of work unit (e.g., "partition", "phenotype", "locus_plot", "aggregation")
    pub task_type: String,

    /// Primary identifier for the work unit (partition number, phenotype ID, etc.)
    pub task_id: String,

    /// Optional human-readable label for dashboard display
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Optional parent context (e.g., which phenotype a locus plot belongs to)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<Box<CoreTaskInfo>>,
}

impl CoreTaskInfo {
    /// Create a simple partition task.
    pub fn partition(partition_id: usize) -> Self {
        Self {
            task_type: "partition".to_string(),
            task_id: partition_id.to_string(),
            label: None,
            parent: None,
        }
    }

    /// Create a phenotype task.
    pub fn phenotype(phenotype_id: impl Into<String>, label: Option<String>) -> Self {
        Self {
            task_type: "phenotype".to_string(),
            task_id: phenotype_id.into(),
            label,
            parent: None,
        }
    }

    /// Create a task with a parent context (for nested parallelism).
    pub fn with_parent(mut self, parent: CoreTaskInfo) -> Self {
        self.parent = Some(Box::new(parent));
        self
    }

    /// Create a custom task type.
    pub fn custom(task_type: impl Into<String>, task_id: impl Into<String>) -> Self {
        Self {
            task_type: task_type.into(),
            task_id: task_id.into(),
            label: None,
            parent: None,
        }
    }
}

/// A point-in-time telemetry snapshot from a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    /// Unix timestamp in milliseconds
    pub timestamp_ms: u64,
    /// CPU usage percentage (0-100)
    pub cpu_percent: Option<f32>,
    /// Memory used in bytes
    pub memory_used_bytes: Option<u64>,
    /// Memory total in bytes
    pub memory_total_bytes: Option<u64>,
    /// Items processed per second
    pub items_per_sec: f64,
    /// Total items processed so far by this worker
    pub total_items: usize,
    /// Currently active partition, if any
    pub active_partition: Option<usize>,
    /// Partitions completed by this worker
    pub partitions_completed: usize,

    // Extended metrics for dashboard

    /// Per-core CPU usage percentages
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_per_core: Option<Vec<f32>>,
    /// Disk read rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_read_bytes_sec: Option<f64>,
    /// Disk write rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_sec: Option<f64>,
    /// Disk space used in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_used_bytes: Option<u64>,
    /// Disk space total in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_total_bytes: Option<u64>,
    /// Network receive rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_rx_bytes_sec: Option<f64>,
    /// Network transmit rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_tx_bytes_sec: Option<f64>,

    /// Map of CPU core index (Rayon thread ID) to currently executing task info
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_tasks: Option<std::collections::HashMap<usize, CoreTaskInfo>>,

    /// The current dynamically adjusted batch size for this worker
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_batch_size: Option<usize>,
}

/// Heartbeat request from worker to coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// Worker sending the heartbeat
    pub worker_id: String,
    /// Current telemetry snapshot
    pub telemetry: TelemetrySnapshot,
    /// Git commit hash of the worker binary
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
}

/// Heartbeat response from coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was acknowledged
    pub acknowledged: bool,
}

/// Dashboard summary for the overall job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSummary {
    /// Job progress percentage (0-100)
    pub progress_percent: f64,
    /// Total tasks in the job
    pub total_tasks: usize,
    /// Number of tasks assigned per work request
    #[serde(default)]
    pub batch_size: usize,
    /// Tasks completed
    pub completed_tasks: usize,
    /// Tasks currently processing
    pub processing_tasks: usize,
    /// Tasks pending
    pub pending_tasks: usize,
    /// Tasks permanently failed
    pub failed_tasks: usize,
    /// Total items processed across all workers
    pub total_items: usize,
    /// Aggregate items per second across all workers
    pub cluster_items_per_sec: f64,
    /// Job elapsed time in seconds
    pub elapsed_secs: f64,
    /// Estimated time remaining in seconds, if calculable
    pub eta_secs: Option<f64>,
    /// Whether the job is complete
    pub is_complete: bool,
    /// Input path being processed
    pub input_path: String,
    /// Job specification (serialized as JSON)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_spec: Option<Value>,
    /// Whether the coordinator is idle
    #[serde(default)]
    pub idle: bool,
}

/// Worker information for dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardWorker {
    /// Worker ID
    pub worker_id: String,
    /// Worker status
    pub status: String,
    /// Current dynamically adjusted batch size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_batch_size: Option<usize>,
    /// Current telemetry
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetrySnapshot>,
    /// Time since last heartbeat in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_secs: Option<f64>,
    /// Git commit hash of the worker binary
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
}

/// Dashboard metrics response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardMetrics {
    /// Job summary
    pub summary: DashboardSummary,
    /// Worker information
    pub workers: Vec<DashboardWorker>,
    /// Bottleneck information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bottleneck: Option<DashboardBottleneck>,
}

/// Bottleneck analysis for dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardBottleneck {
    /// Bottleneck type
    pub bottleneck_type: String,
    /// Bottleneck description
    pub description: String,
    /// Severity (0-100)
    pub severity: f64,
}
