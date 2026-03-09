//! Coordinator state structures.
//!
//! This module contains all the core data structures used by the coordinator,
//! including configuration, worker tracking, job state, and batch processing state.

use crate::distributed::message::{
    ActiveTaskInfo, ExecutionMode, HardwareSpec, JobEvent, JobSpec, ManhattanAggregateSpec,
    ManhattanSource, ManhattanSpec, PhenotypeStatus, TelemetrySnapshot,
};
use crate::distributed::metrics_db::MetricsDb;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Maximum number of telemetry snapshots to keep per worker.
pub(crate) const MAX_METRICS_HISTORY: usize = 300; // ~10 min at 2s intervals

/// Seconds without a heartbeat before a worker is considered suspect.
pub(crate) const WORKER_SUSPECT_TIMEOUT_SECS: u64 = 30;

/// Maximum retries for aggregate tasks before permanent failure.
pub(crate) const MAX_AGGREGATE_RETRIES: usize = 3;

/// Maximum number of phenotypes to process concurrently.
pub(crate) const BATCH_ACTIVE_LIMIT: usize = 20;

/// Minimum batch size for aggregation (unless no other work available).
pub(crate) const AGGREGATE_BATCH_SIZE: usize = 10;

/// Maximum events in the ring buffer
pub(crate) const MAX_EVENTS: usize = 1000;

/// Maximum failures in the ring buffer
pub(crate) const MAX_FAILURES: usize = 100;

/// Represents the current execution state for a job.
///
/// Since a coordinator only runs one job type at a time, these states are mutually exclusive.
/// This enum makes illegal states unrepresentable (e.g., having both batch and manhattan state).
#[derive(Debug, Default)]
pub(crate) enum JobExecutionState {
    /// No specialized state (standard partition-based jobs like ExportParquet)
    #[default]
    Standard,
    /// Single Manhattan pipeline job
    Manhattan(ManhattanPipelineState),
    /// Manhattan batch job (multiple phenotypes)
    Batch(BatchState),
    /// Manhattan ingestion into ClickHouse
    Ingestion(IngestionState),
}

/// Configuration for the coordinator.
#[allow(dead_code)]
pub struct CoordinatorConfig {
    /// Port to listen on
    pub port: u16,
    /// Path to input Hail table
    pub input_path: String,
    /// Job specification (what operation to perform)
    pub job_spec: Option<JobSpec>,
    /// Total number of partitions to process
    pub total_tasks: usize,
    /// Number of partitions to assign per work request (batching)
    pub batch_size: usize,
    /// Timeout before rescheduling work (seconds)
    pub timeout_secs: u64,
    /// Timeout for stuck jobs making no progress (seconds)
    pub stuck_timeout_secs: u64,
    /// Filter conditions (where clauses)
    pub filters: Vec<String>,
    /// Interval filters
    pub intervals: Vec<String>,
    /// Hint for memory required per partition in MB
    pub memory_weight_mb: Option<u64>,
    /// Local path to SQLite database file
    pub db_path: String,
    /// GCS path for backup/restore (e.g., "gs://bucket/pool-ops/dev-pool/ops.db")
    pub backup_path: Option<String>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            input_path: String::new(),
            job_spec: None,
            total_tasks: 0,
            batch_size: 1, // Default to 1 partition per request for fine-grained retry
            timeout_secs: 600, // 10 minutes
            stuck_timeout_secs: 600, // 10 minutes for stuck job detection
            filters: Vec::new(),
            intervals: Vec::new(),
            memory_weight_mb: None,
            db_path: "/var/lib/genohype/ops.db".to_string(),
            backup_path: None,
        }
    }
}

/// Current status of a worker.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WorkerStatus {
    /// Worker is actively sending heartbeats
    Active,
    /// Worker is idle (requested work, got Wait)
    Idle,
    /// Worker has not sent a heartbeat recently
    SuspectedDead,
}

impl WorkerStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Active => "active",
            WorkerStatus::Idle => "idle",
            WorkerStatus::SuspectedDead => "suspected_dead",
        }
    }
}

/// Phase of a Manhattan pipeline job.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ManhattanPhase {
    /// Scanning partitions (outputs partial PNGs + sig.parquet)
    Scan,
    /// Aggregating results (compositing, annotation joins, locus plots)
    Aggregate,
    /// Pipeline complete
    Complete,
}

/// Tracks what a specific task_id corresponds to in batch mode.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum ActiveTask {
    /// A scan task for a specific phenotype and partition
    Scan {
        /// Unique ID for the phenotype (e.g., analysis_id)
        phenotype_id: String,
        /// Which partition is being scanned
        partition_id: usize,
        /// Source (exome or genome)
        source: ManhattanSource,
        /// Timestamp when this task was assigned (milliseconds since epoch)
        started_at_ms: u64,
    },
    /// An aggregate batch task processing multiple phenotypes
    AggregateBatch {
        /// IDs of phenotypes being aggregated
        phenotype_ids: Vec<String>,
        /// Timestamp when this task was assigned (milliseconds since epoch)
        started_at_ms: u64,
    },
}

/// State for managing a batch of Manhattan phenotypes.
#[derive(Debug)]
pub(crate) struct BatchState {
    /// Execution mode for the batch
    pub(crate) mode: ExecutionMode,
    /// Tracking status for all phenotypes (for dashboard)
    pub(crate) phenotype_statuses: HashMap<String, PhenotypeStatus>,
    /// Start times for phenotypes (for duration calculation)
    pub(crate) phenotype_start_times: HashMap<String, Instant>,
    /// Accumulated CPU core-seconds per phenotype
    pub(crate) phenotype_cpu_secs: HashMap<String, f64>,

    /// Phenotypes waiting to be activated (lazy loading)
    pub(crate) pending_queue: VecDeque<ManhattanSpec>,
    /// Currently active phenotypes: phenotype_id -> pipeline state
    pub(crate) active_phenotypes: HashMap<String, ManhattanPipelineState>,
    /// Phenotypes that have finished scanning and are ready to aggregate
    pub(crate) ready_to_aggregate: Vec<(String, ManhattanAggregateSpec)>,
    /// Count of successfully completed phenotypes
    pub(crate) completed_count: usize,
    /// Count of permanently failed phenotypes (exceeded max retries)
    pub(crate) failed_count: usize,
    /// Total number of phenotypes in the batch
    pub(crate) total_phenotypes: usize,
    /// Retry counts for aggregate tasks: phenotype_id -> retry count
    pub(crate) aggregate_retry_counts: HashMap<String, usize>,
    /// Aggregate specs for phenotypes that may need retry (phenotype_id -> spec)
    pub(crate) aggregate_specs: HashMap<String, ManhattanAggregateSpec>,
}

/// State for managing Manhattan ingestion into ClickHouse.
#[derive(Debug)]
pub(crate) struct IngestionState {
    /// Pending phenotypes to ingest: (phenotype_id, ancestry, base_path)
    pub(crate) pending_tasks: VecDeque<(String, String, String)>,
    /// Currently processing tasks: task_id -> (phenotype_id, ancestry, base_path, worker_id, start_time)
    pub(crate) active_tasks: HashMap<String, (String, String, String, String, Instant)>,
    /// ClickHouse connection URL
    pub(crate) clickhouse_url: String,
    /// Target database
    pub(crate) database: String,
    /// Number of completed tasks
    pub(crate) completed_count: usize,
    /// Number of failed tasks
    pub(crate) failed_count: usize,
    /// Total tasks discovered
    pub(crate) total_tasks: usize,
}

/// State for tracking Manhattan pipeline phases.
#[derive(Debug)]
pub(crate) struct ManhattanPipelineState {
    /// Execution mode
    pub(crate) mode: ExecutionMode,
    /// Current phase
    pub(crate) phase: ManhattanPhase,
    /// Original ManhattanSpec from job submission
    pub(crate) original_spec: ManhattanSpec,
    /// Pre-computed layout (shared by scan and aggregate)
    pub(crate) layout: Option<crate::manhattan::layout::ChromosomeLayout>,
    /// Pre-computed Y scale
    pub(crate) y_scale: Option<crate::manhattan::layout::YScale>,
    /// Contig lengths for per-chromosome plots
    pub(crate) contig_lengths: HashMap<String, u32>,

    // Exome scan tracking
    /// Total exome partitions
    pub(crate) exome_total_tasks: usize,
    /// Pending exome partitions
    pub(crate) exome_pending: VecDeque<usize>,
    /// Processing exome partitions: partition_id -> (worker_id, start_time)
    pub(crate) exome_processing: HashMap<usize, (String, Instant)>,
    /// Completed exome partitions
    pub(crate) exome_completed: HashSet<usize>,

    // Genome scan tracking
    /// Total genome partitions
    pub(crate) genome_total_tasks: usize,
    /// Pending genome partitions
    pub(crate) genome_pending: VecDeque<usize>,
    /// Processing genome partitions: partition_id -> (worker_id, start_time)
    pub(crate) genome_processing: HashMap<usize, (String, Instant)>,
    /// Completed genome partitions
    pub(crate) genome_completed: HashSet<usize>,

    /// Whether aggregate task has been dispatched
    pub(crate) aggregate_dispatched: bool,
    /// Whether aggregate task is complete
    pub(crate) aggregate_complete: bool,
}

/// Tracked state for a single worker.
pub(crate) struct WorkerState {
    /// Last time we heard from this worker (heartbeat or work request)
    pub(crate) last_seen: Instant,
    /// Current status
    pub(crate) status: WorkerStatus,
    /// Recent telemetry snapshots (newest last)
    pub(crate) metrics_history: VecDeque<TelemetrySnapshot>,
    /// Total rows reported completed by this worker
    pub(crate) total_rows: usize,
    /// Total partitions completed by this worker
    pub(crate) partitions_completed: usize,
    /// The task this worker is currently processing
    pub(crate) current_task: Option<ActiveTaskInfo>,
    /// Latest log tail received via heartbeat
    pub(crate) latest_log_tail: Option<Vec<String>>,
    /// Hardware specifications reported by worker
    pub(crate) hardware: Option<HardwareSpec>,
    /// Dynamically adjusted batch size for this worker (AIMD algorithm)
    pub(crate) current_batch_size: Option<usize>,
    /// Learned maximum batch capacity from "batch too large" errors
    /// This caps AIMD growth to prevent repeated OOM failures
    pub(crate) max_batch_capacity: Option<usize>,
    /// Git commit hash of the worker binary
    pub(crate) build_version: Option<String>,
}

/// Internal state of the coordinator.
pub(crate) struct CoordinatorData {
    /// Partitions waiting to be assigned
    pub(crate) pending_partitions: VecDeque<usize>,
    /// Partitions currently being processed: partition_id -> (worker_id, start_time)
    pub(crate) processing_partitions: HashMap<usize, (String, Instant)>,
    /// Partitions that have been completed
    pub(crate) completed_tasks: HashSet<usize>,
    /// Configuration
    pub(crate) config: CoordinatorConfig,
    /// Total rows processed (reported by workers)
    pub(crate) total_rows: usize,
    /// Cumulative CPU-seconds spent in scan phase
    pub(crate) scan_cpu_secs: f64,
    /// Cumulative CPU-seconds spent in aggregate phase
    pub(crate) aggregate_cpu_secs: f64,
    /// Cumulative CPU-seconds wasted due to failures/preemption
    pub(crate) wasted_cpu_secs: f64,
    /// Track retry counts per partition
    pub(crate) retry_counts: HashMap<usize, usize>,
    /// Partitions that permanently failed (exceeded max retries)
    pub(crate) failed_partitions: HashSet<usize>,
    /// Registry of all known workers and their telemetry
    pub(crate) worker_registry: HashMap<String, WorkerState>,
    /// When the job started
    pub(crate) job_start_time: Instant,
    /// Last time progress was made (job start or last partition completion)
    pub(crate) last_progress_time: Instant,
    /// Whether the coordinator is idle (waiting for job submission via /api/job)
    pub(crate) idle: bool,
    /// SQLite database for persistent metrics storage
    pub(crate) metrics_db: MetricsDb,
    /// Aggregated results from workers (for Summary/Validate jobs)
    pub(crate) aggregated_results: Vec<serde_json::Value>,
    /// Current job execution state (unified state for Manhattan, Batch, Ingestion, or Standard)
    pub(crate) job_state: JobExecutionState,
    /// Active tasks: task_id -> ActiveTask (for batch mode tracking)
    pub(crate) active_tasks: HashMap<String, ActiveTask>,
    /// Last error message from a failed task
    pub(crate) last_error: Option<String>,
    /// Ring buffer of recent events (max 1000)
    pub(crate) events: VecDeque<JobEvent>,
    /// Ring buffer of recent failures (max 100)
    pub(crate) failures: VecDeque<crate::distributed::message::FailureRecord>,
    /// Number of events since last GCS backup (for periodic backup trigger)
    pub(crate) events_since_backup: usize,
    /// Timestamp of last successful backup (milliseconds since epoch)
    pub(crate) last_backup_at: Option<u64>,
    /// URL to the new binary for fleet updates
    pub(crate) update_fleet_url: Option<String>,
    /// Set of workers that have already received the update signal
    pub(crate) updated_workers: HashSet<String>,
    /// Unique ID for the currently running job (for history tracking)
    pub(crate) current_job_id: Option<String>,
}

pub(crate) type SharedState = Arc<Mutex<CoordinatorData>>;

impl CoordinatorData {
    /// Ensure a worker exists in the registry and update last_seen.
    pub(crate) fn touch_worker(
        &mut self,
        worker_id: &str,
        hardware: Option<HardwareSpec>,
        build_version: Option<String>,
    ) {
        use std::time::Instant;
        let worker = self
            .worker_registry
            .entry(worker_id.to_string())
            .or_insert_with(|| WorkerState {
                last_seen: Instant::now(),
                status: WorkerStatus::Idle,
                metrics_history: VecDeque::new(),
                total_rows: 0,
                partitions_completed: 0,
                current_task: None,
                latest_log_tail: None,
                hardware: None,
                current_batch_size: None,
                max_batch_capacity: None,
                build_version: None,
            });
        worker.last_seen = Instant::now();
        if hardware.is_some() {
            worker.hardware = hardware;
        }
        if build_version.is_some() {
            worker.build_version = build_version;
        }
    }

    /// Log an event to the ring buffer and persist to database.
    pub(crate) fn log_event(&mut self, event: JobEvent) {
        // Persist to database if we have a current job
        if let Some(ref job_id) = self.current_job_id {
            if let Err(e) = self.metrics_db.log_event(job_id, &event) {
                eprintln!("Warning: failed to persist event to DB: {}", e);
            }
        }
        // Also keep in ring buffer for live dashboard
        self.events.push_back(event);
        if self.events.len() > MAX_EVENTS {
            self.events.pop_front();
        }
    }

    /// Log a failure to the ring buffer and persist to database.
    pub(crate) fn log_failure(&mut self, failure: crate::distributed::message::FailureRecord) {
        // Persist to database if we have a current job
        if let Some(ref job_id) = self.current_job_id {
            if let Err(e) = self.metrics_db.log_failure(job_id, &failure) {
                eprintln!("Warning: failed to persist failure to DB: {}", e);
            }
        }
        // Also keep in ring buffer for live dashboard
        self.failures.push_back(failure);
        if self.failures.len() > MAX_FAILURES {
            self.failures.pop_front();
        }
    }

    /// Get current timestamp in milliseconds
    pub(crate) fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Requeue all tasks assigned to a worker that is suspected dead.
    pub(crate) fn requeue_worker_tasks(&mut self, dead_worker_id: &str) {
        let now_ms = CoordinatorData::now_ms();

        // 1. Standard partitions
        let mut lost_parts = Vec::new();
        for (&part_id, (worker, _)) in &self.processing_partitions {
            if worker == dead_worker_id {
                lost_parts.push(part_id);
            }
        }
        // Sort descending so push_front preserves ascending order
        lost_parts.sort_by(|a, b| b.cmp(a));

        for part_id in lost_parts {
            self.processing_partitions.remove(&part_id);
            let retry_count = {
                let retries = self.retry_counts.entry(part_id).or_insert(0);
                *retries += 1;
                *retries
            };
            if retry_count > 3 {
                println!("Partition {} exceeded max retries (worker {} dead). Marking as failed.", part_id, dead_worker_id);
                self.failed_partitions.insert(part_id);
            } else {
                println!("Worker {} died. Requeuing partition {} (retry {}/3)", dead_worker_id, part_id, retry_count);
                self.pending_partitions.push_front(part_id);

                // Add REQUEUED event to dashboard
                self.log_event(JobEvent {
                    timestamp_ms: now_ms,
                    event_type: "requeued".to_string(),
                    worker_id: Some(dead_worker_id.to_string()),
                    phenotype_id: None,
                    details: format!("Task {} requeued after worker death (retry {}/3)", part_id, retry_count),
                });
            }
        }

        // 2. Job-specific state requeuing
        match &mut self.job_state {
            JobExecutionState::Manhattan(ref mut manhattan) => {
                let mut lost_exome = Vec::new();
                for (&part_id, (worker, _)) in &manhattan.exome_processing {
                    if worker == dead_worker_id {
                        lost_exome.push(part_id);
                    }
                }
                lost_exome.sort_by(|a, b| b.cmp(a));
                for part_id in lost_exome {
                    manhattan.exome_processing.remove(&part_id);
                    println!("Worker {} died. Requeuing exome partition {}", dead_worker_id, part_id);
                    manhattan.exome_pending.push_front(part_id);
                }

                let mut lost_genome = Vec::new();
                for (&part_id, (worker, _)) in &manhattan.genome_processing {
                    if worker == dead_worker_id {
                        lost_genome.push(part_id);
                    }
                }
                lost_genome.sort_by(|a, b| b.cmp(a));
                for part_id in lost_genome {
                    manhattan.genome_processing.remove(&part_id);
                    println!("Worker {} died. Requeuing genome partition {}", dead_worker_id, part_id);
                    manhattan.genome_pending.push_front(part_id);
                }
            }
            JobExecutionState::Ingestion(ref mut ingestion) => {
                let mut lost_tasks = Vec::new();
                for (task_id, (pheno, ancestry, base_path, worker, _)) in &ingestion.active_tasks {
                    if worker == dead_worker_id {
                        lost_tasks.push((task_id.clone(), pheno.clone(), ancestry.clone(), base_path.clone()));
                    }
                }
                for (task_id, pheno, ancestry, base_path) in lost_tasks {
                    ingestion.active_tasks.remove(&task_id);
                    println!("Worker {} died. Requeuing ingestion task {}/{}", dead_worker_id, ancestry, pheno);
                    ingestion.pending_tasks.push_front((pheno, ancestry, base_path));
                }
            }
            JobExecutionState::Batch(ref mut batch) => {
                for (pheno_id, state) in &mut batch.active_phenotypes {
                    let mut lost_exome = Vec::new();
                    for (&part_id, (worker, _)) in &state.exome_processing {
                        if worker == dead_worker_id {
                            lost_exome.push(part_id);
                        }
                    }
                    lost_exome.sort_by(|a, b| b.cmp(a));
                    for part_id in lost_exome {
                        state.exome_processing.remove(&part_id);
                        println!("Worker {} died. Requeuing exome partition {} for {}", dead_worker_id, part_id, pheno_id);
                        state.exome_pending.push_front(part_id);
                    }

                    let mut lost_genome = Vec::new();
                    for (&part_id, (worker, _)) in &state.genome_processing {
                        if worker == dead_worker_id {
                            lost_genome.push(part_id);
                        }
                    }
                    lost_genome.sort_by(|a, b| b.cmp(a));
                    for part_id in lost_genome {
                        state.genome_processing.remove(&part_id);
                        println!("Worker {} died. Requeuing genome partition {} for {}", dead_worker_id, part_id, pheno_id);
                        state.genome_pending.push_front(part_id);
                    }
                }
            }
            JobExecutionState::Standard => {
                // No additional state to requeue for standard jobs
            }
        }

        // 5. Check worker's current_task to handle tasks that don't track worker_id in a map (like AggregateBatch)
        let mut failed_active_tasks = Vec::new();
        if let Some(worker_state) = self.worker_registry.get_mut(dead_worker_id) {
            if let Some(task_info) = worker_state.current_task.take() {
                failed_active_tasks.push(task_info);
            }
        }

        for task_info in failed_active_tasks {
            let task_id = task_info.task_id;
            if let Some(task) = self.active_tasks.remove(&task_id) {
                if let ActiveTask::AggregateBatch { phenotype_ids, .. } = task {
                    if let JobExecutionState::Batch(ref mut batch) = self.job_state {
                        for pheno_id in phenotype_ids {
                            let retries = batch.aggregate_retry_counts.entry(pheno_id.clone()).or_insert(0);
                            *retries += 1;
                            if *retries > MAX_AGGREGATE_RETRIES {
                                println!("Phenotype {} exceeded max aggregate retries (worker {} dead). Marking as failed.", pheno_id, dead_worker_id);
                                batch.failed_count += 1;
                                batch.aggregate_specs.remove(&pheno_id);
                            } else if let Some(spec) = batch.aggregate_specs.get(&pheno_id).cloned() {
                                println!("Worker {} died. Requeuing aggregate task for {} (retry {})", dead_worker_id, pheno_id, retries);
                                batch.ready_to_aggregate.push((pheno_id, spec));
                            }
                        }
                    }
                }
            }
        }
    }
}
