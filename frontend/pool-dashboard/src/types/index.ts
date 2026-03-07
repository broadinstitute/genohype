/**
 * TypeScript interfaces matching the Rust data models from:
 * - pool/src/distributed/message.rs
 * - cli/src/distributed/message.rs
 */

/**
 * Describes what work a specific CPU core/thread is currently executing.
 * Supports any job type and nested parallelism (e.g., phenotype → locus plots).
 */
export interface CoreTaskInfo {
  /** Type of work unit (e.g., "partition", "phenotype", "locus_plot", "aggregation") */
  task_type: string;
  /** Primary identifier for the work unit (partition number, phenotype ID, etc.) */
  task_id: string;
  /** Optional human-readable label for dashboard display */
  label?: string;
  /** Optional parent context (e.g., which phenotype a locus plot belongs to) */
  parent?: CoreTaskInfo;
}

/**
 * A point-in-time telemetry snapshot from a worker.
 */
export interface TelemetrySnapshot {
  /** Unix timestamp in milliseconds */
  timestamp_ms: number;
  /** CPU usage percentage (0-100) */
  cpu_percent?: number;
  /** Memory used in bytes */
  memory_used_bytes?: number;
  /** Memory total in bytes */
  memory_total_bytes?: number;
  /** Items processed per second */
  items_per_sec: number;
  /** Total items processed so far by this worker */
  total_items: number;
  /** Currently active partition, if any */
  active_partition?: number;
  /** Partitions completed by this worker */
  partitions_completed: number;
  /** Per-core CPU usage percentages */
  cpu_per_core?: number[];
  /** Disk read rate in bytes per second */
  disk_read_bytes_sec?: number;
  /** Disk write rate in bytes per second */
  disk_write_bytes_sec?: number;
  /** Disk space used in bytes */
  disk_used_bytes?: number;
  /** Disk space total in bytes */
  disk_total_bytes?: number;
  /** Network receive rate in bytes per second */
  network_rx_bytes_sec?: number;
  /** Network transmit rate in bytes per second */
  network_tx_bytes_sec?: number;
  /** Map of CPU core index (Rayon thread ID) to currently executing task info */
  core_tasks?: Record<number, CoreTaskInfo>;
  /** Current dynamically adjusted batch size */
  current_batch_size?: number;
}

/**
 * Dashboard summary for the overall job.
 */
export interface DashboardSummary {
  /** Job progress percentage (0-100) */
  progress_percent: number;
  /** Total tasks in the job */
  total_tasks: number;
  /** Number of tasks assigned per work request */
  batch_size?: number;
  /** Tasks completed */
  completed_tasks: number;
  /** Tasks currently processing */
  processing_tasks: number;
  /** Tasks pending */
  pending_tasks: number;
  /** Tasks permanently failed */
  failed_tasks: number;
  /** Total items processed across all workers */
  total_items: number;
  /** Aggregate items per second across all workers */
  cluster_items_per_sec: number;
  /** Job elapsed time in seconds */
  elapsed_secs: number;
  /** Estimated time remaining in seconds, if calculable */
  eta_secs?: number;
  /** Whether the job is complete */
  is_complete: boolean;
  /** Input path being processed */
  input_path: string;
  /** Job specification (serialized as JSON) */
  job_spec?: unknown;
  /** Whether the coordinator is idle */
  idle: boolean;
  /** Local path to the SQLite database */
  db_path?: string;
  /** GCS path for backup (if configured) */
  backup_path?: string;
  /** Timestamp of last successful backup (milliseconds since epoch) */
  last_backup_at?: number;
}

/**
 * Worker information for dashboard.
 */
export interface DashboardWorker {
  /** Worker ID */
  worker_id: string;
  /** Worker status */
  status: string;
  /** Current dynamically adjusted batch size */
  current_batch_size?: number;
  /** Current telemetry */
  telemetry?: TelemetrySnapshot;
  /** Time since last heartbeat in seconds */
  last_heartbeat_secs?: number;
}

/**
 * Time-series data for a single worker.
 */
export interface WorkerMetricsSeries {
  /** Worker identifier */
  worker_id: string;
  /** Telemetry snapshots (most recent last) */
  snapshots: TelemetrySnapshot[];
}

/**
 * Dashboard metrics response containing per-worker time-series data.
 */
export interface DashboardMetrics {
  /** Per-worker time-series data */
  workers: WorkerMetricsSeries[];
}

/**
 * Bottleneck analysis for dashboard.
 * Provides real-time bottleneck analysis based on aggregated worker telemetry.
 */
export interface DashboardBottleneck {
  /** Bottleneck identifier: "CPU", "Memory", "Network RX", "Network TX", "I/O Wait", "Mixed", "Idle" */
  bottleneck: string;
  /** Human-readable description of the bottleneck state */
  description: string;
  /** Average CPU utilization across active workers (0-100%) */
  avg_cpu_percent: number;
  /** Average memory utilization across active workers (0-100%) */
  avg_mem_percent: number;
  /** Average network download rate across active workers (MB/s) */
  avg_network_rx_mb: number;
  /** Average network upload rate across active workers (MB/s) */
  avg_network_tx_mb: number;
}

/**
 * A historical event in the cluster.
 */
export interface JobEvent {
  /** Unix timestamp in milliseconds */
  timestamp_ms: number;
  /** Event type: "assigned", "completed", "failed", "requeued" */
  event_type: string;
  /** Worker ID if applicable */
  worker_id?: string;
  /** Phenotype ID if applicable */
  phenotype_id?: string;
  /** Human-readable event details */
  details: string;
}

/**
 * A record of a failed task.
 */
export interface FailureRecord {
  /** Unix timestamp in milliseconds */
  timestamp_ms: number;
  /** Phenotype ID if applicable */
  phenotype_id?: string;
  /** Task IDs that failed */
  tasks: string[];
  /** Worker that experienced the failure */
  worker_id: string;
  /** Error message */
  error: string;
  /** Number of retry attempts */
  retry_count: number;
  /** Time wasted on this failed task (milliseconds) */
  wasted_duration_ms: number;
}

/**
 * Status of a single phenotype in a batch job.
 */
export interface PhenotypeStatus {
  /** Phenotype identifier */
  id: string;
  /** Stage: "queued", "scanning", "aggregating", "completed", "failed" */
  stage: string;
  /** Partitions completed for this phenotype */
  partitions_done: number;
  /** Total partitions for this phenotype */
  partitions_total: number;
  /** Result data if completed */
  result?: unknown;
  /** Error message if failed */
  error?: string;
  /** Total duration in seconds (from scan start to completion) */
  duration_secs?: number;
  /** Accumulated CPU core-seconds for this phenotype */
  cpu_core_secs?: number;
}

/**
 * Response containing status of all phenotypes in a batch.
 */
export interface BatchStatusResponse {
  /** Status of each phenotype in the batch */
  phenotypes: PhenotypeStatus[];
}

/**
 * Response from GET /api/events endpoint.
 */
export interface EventsResponse {
  events: JobEvent[];
}

/**
 * Response from GET /api/failures endpoint.
 */
export interface FailuresResponse {
  failures: FailureRecord[];
}
