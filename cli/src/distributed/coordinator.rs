//! Coordinator server for distributed processing.
//!
//! The coordinator maintains the state of a distributed job:
//! - Queue of pending partitions
//! - Map of partitions currently being processed (with timestamps)
//! - Set of completed partitions
//!
//! Workers poll `/work` to get assignments and `/complete` to report completion.
//! A background task monitors for timed-out workers and reschedules their work.

use crate::distributed::message::{
    ActiveTaskInfo, BatchStatusResponse, CancelRequest, CancelResponse, CompleteRequest,
    CompleteResponse, DashboardBatchProgress, DashboardBottleneck, DashboardMetrics,
    DashboardSummary, DashboardWorker, EventsResponse, ExportMetricsRequest, ExportMetricsResponse,
    FailureRecord, FailuresResponse, HeartbeatRequest, HeartbeatResponse, JobConfigRequest,
    JobConfigResponse, JobEvent, JobResultResponse, JobSpec, ManhattanAggregateSpec,
    ManhattanScanSpec, ManhattanSource, ManhattanSpec, PartitionOp, PhenotypeOp, PhenotypeStatus,
    StatusResponse, TaskDescriptor, TaskType, TelemetrySnapshot, WorkRequest, WorkResponse,
    WorkerMetricsSeries,
};
use crate::distributed::metrics_db::MetricsDb;
use crate::manhattan::config::PlotType;
use crate::Result;
use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;
use mime_guess;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Backup the database to GCS.
///
/// This function checkpoints the WAL to ensure all data is in the main file,
/// then uploads the database to the specified GCS path.
async fn backup_db(metrics_db: &MetricsDb, db_path: &str, backup_path: &str) {
    // Checkpoint the WAL to ensure the main DB file is up-to-date
    if let Err(e) = metrics_db.checkpoint_for_backup() {
        eprintln!("Warning: failed to checkpoint DB before backup: {}", e);
    }

    // Read the database file and upload to GCS in a blocking task
    let db_path = db_path.to_string();
    let backup_path = backup_path.to_string();
    let backup_path_display = backup_path.clone();
    let result = tokio::task::spawn_blocking(move || {
        use genohype_core::io::CloudWriter;
        use std::io::Write;

        let db_contents = match std::fs::read(&db_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Warning: failed to read database file at {}: {}", db_path, e);
                return false;
            }
        };

        let mut writer = match CloudWriter::new(&backup_path) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("Warning: failed to create cloud writer for {}: {}", backup_path, e);
                return false;
            }
        };

        if let Err(e) = writer.write_all(&db_contents) {
            eprintln!("Warning: failed to write backup data to {}: {}", backup_path, e);
            return false;
        }

        if let Err(e) = writer.finish() {
            eprintln!("Warning: failed to finalize backup upload to {}: {}", backup_path, e);
            return false;
        }

        true
    }).await;

    if result.unwrap_or(false) {
        println!("Job history backed up to {}", backup_path_display);
    }
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

/// Maximum number of telemetry snapshots to keep per worker.
const MAX_METRICS_HISTORY: usize = 300; // ~10 min at 2s intervals

/// Seconds without a heartbeat before a worker is considered suspect.
const WORKER_SUSPECT_TIMEOUT_SECS: u64 = 30;

/// Current status of a worker.
#[derive(Debug, Clone, PartialEq)]
enum WorkerStatus {
    /// Worker is actively sending heartbeats
    Active,
    /// Worker is idle (requested work, got Wait)
    Idle,
    /// Worker has not sent a heartbeat recently
    SuspectedDead,
}

impl WorkerStatus {
    fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Active => "active",
            WorkerStatus::Idle => "idle",
            WorkerStatus::SuspectedDead => "suspected_dead",
        }
    }
}

/// Phase of a Manhattan pipeline job.
#[derive(Debug, Clone, PartialEq)]
enum ManhattanPhase {
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
enum ActiveTask {
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
struct BatchState {
    /// Execution mode for the batch
    mode: crate::distributed::message::ExecutionMode,
    /// Tracking status for all phenotypes (for dashboard)
    phenotype_statuses: HashMap<String, PhenotypeStatus>,
    /// Start times for phenotypes (for duration calculation)
    phenotype_start_times: HashMap<String, Instant>,
    /// Accumulated CPU core-seconds per phenotype
    phenotype_cpu_secs: HashMap<String, f64>,

    /// Phenotypes waiting to be activated (lazy loading)
    pending_queue: VecDeque<ManhattanSpec>,
    /// Currently active phenotypes: phenotype_id -> pipeline state
    active_phenotypes: HashMap<String, ManhattanPipelineState>,
    /// Phenotypes that have finished scanning and are ready to aggregate
    ready_to_aggregate: Vec<(String, ManhattanAggregateSpec)>,
    /// Count of successfully completed phenotypes
    completed_count: usize,
    /// Count of permanently failed phenotypes (exceeded max retries)
    failed_count: usize,
    /// Total number of phenotypes in the batch
    total_phenotypes: usize,
    /// Retry counts for aggregate tasks: phenotype_id -> retry count
    aggregate_retry_counts: HashMap<String, usize>,
    /// Aggregate specs for phenotypes that may need retry (phenotype_id -> spec)
    aggregate_specs: HashMap<String, ManhattanAggregateSpec>,
}

/// Maximum retries for aggregate tasks before permanent failure.
const MAX_AGGREGATE_RETRIES: usize = 3;

/// Maximum number of phenotypes to process concurrently.
const BATCH_ACTIVE_LIMIT: usize = 20;

/// State for managing Manhattan ingestion into ClickHouse.
#[derive(Debug)]
struct IngestionState {
    /// Pending phenotypes to ingest: (phenotype_id, ancestry, base_path)
    pending_tasks: VecDeque<(String, String, String)>,
    /// Currently processing tasks: task_id -> (phenotype_id, ancestry, base_path, worker_id, start_time)
    active_tasks: HashMap<String, (String, String, String, String, Instant)>,
    /// ClickHouse connection URL
    clickhouse_url: String,
    /// Target database
    database: String,
    /// Number of completed tasks
    completed_count: usize,
    /// Number of failed tasks
    failed_count: usize,
    /// Total tasks discovered
    total_tasks: usize,
}

/// Minimum batch size for aggregation (unless no other work available).
const AGGREGATE_BATCH_SIZE: usize = 10;

/// State for tracking Manhattan pipeline phases.
#[derive(Debug)]
struct ManhattanPipelineState {
    /// Execution mode
    mode: crate::distributed::message::ExecutionMode,
    /// Current phase
    phase: ManhattanPhase,
    /// Original ManhattanSpec from job submission
    original_spec: ManhattanSpec,
    /// Pre-computed layout (shared by scan and aggregate)
    layout: Option<crate::manhattan::layout::ChromosomeLayout>,
    /// Pre-computed Y scale
    y_scale: Option<crate::manhattan::layout::YScale>,
    /// Contig lengths for per-chromosome plots
    contig_lengths: HashMap<String, u32>,

    // Exome scan tracking
    /// Total exome partitions
    exome_total_tasks: usize,
    /// Pending exome partitions
    exome_pending: VecDeque<usize>,
    /// Processing exome partitions: partition_id -> (worker_id, start_time)
    exome_processing: HashMap<usize, (String, Instant)>,
    /// Completed exome partitions
    exome_completed: HashSet<usize>,

    // Genome scan tracking
    /// Total genome partitions
    genome_total_tasks: usize,
    /// Pending genome partitions
    genome_pending: VecDeque<usize>,
    /// Processing genome partitions: partition_id -> (worker_id, start_time)
    genome_processing: HashMap<usize, (String, Instant)>,
    /// Completed genome partitions
    genome_completed: HashSet<usize>,

    /// Whether aggregate task has been dispatched
    aggregate_dispatched: bool,
    /// Whether aggregate task is complete
    aggregate_complete: bool,
}

/// Tracked state for a single worker.
struct WorkerState {
    /// Last time we heard from this worker (heartbeat or work request)
    last_seen: Instant,
    /// Current status
    status: WorkerStatus,
    /// Recent telemetry snapshots (newest last)
    metrics_history: VecDeque<TelemetrySnapshot>,
    /// Total rows reported completed by this worker
    total_rows: usize,
    /// Total partitions completed by this worker
    partitions_completed: usize,
    /// The task this worker is currently processing
    current_task: Option<ActiveTaskInfo>,
    /// Latest log tail received via heartbeat
    latest_log_tail: Option<Vec<String>>,
    /// Hardware specifications reported by worker
    hardware: Option<crate::distributed::message::HardwareSpec>,
    /// Dynamically adjusted batch size for this worker (AIMD algorithm)
    current_batch_size: Option<usize>,
}

/// Internal state of the coordinator.
struct CoordinatorData {
    /// Partitions waiting to be assigned
    pending_partitions: VecDeque<usize>,
    /// Partitions currently being processed: partition_id -> (worker_id, start_time)
    processing_partitions: HashMap<usize, (String, Instant)>,
    /// Partitions that have been completed
    completed_tasks: HashSet<usize>,
    /// Configuration
    config: CoordinatorConfig,
    /// Total rows processed (reported by workers)
    total_rows: usize,
    /// Cumulative CPU-seconds spent in scan phase
    scan_cpu_secs: f64,
    /// Cumulative CPU-seconds spent in aggregate phase
    aggregate_cpu_secs: f64,
    /// Cumulative CPU-seconds wasted due to failures/preemption
    wasted_cpu_secs: f64,
    /// Track retry counts per partition
    retry_counts: HashMap<usize, usize>,
    /// Partitions that permanently failed (exceeded max retries)
    failed_partitions: HashSet<usize>,
    /// Registry of all known workers and their telemetry
    worker_registry: HashMap<String, WorkerState>,
    /// When the job started
    job_start_time: Instant,
    /// Last time progress was made (job start or last partition completion)
    last_progress_time: Instant,
    /// Whether the coordinator is idle (waiting for job submission via /api/job)
    idle: bool,
    /// SQLite database for persistent metrics storage
    metrics_db: MetricsDb,
    /// Aggregated results from workers (for Summary/Validate jobs)
    aggregated_results: Vec<serde_json::Value>,
    /// Manhattan pipeline state (only set for single Manhattan jobs)
    manhattan_state: Option<ManhattanPipelineState>,
    /// Batch state (only set for ManhattanBatch jobs)
    batch_state: Option<BatchState>,
    /// Active tasks: task_id -> ActiveTask (for batch mode tracking)
    active_tasks: HashMap<String, ActiveTask>,
    /// Last error message from a failed task
    last_error: Option<String>,
    /// Ingestion state (for IngestManhattan jobs)
    ingestion_state: Option<IngestionState>,
    /// Ring buffer of recent events (max 1000)
    events: VecDeque<JobEvent>,
    /// Ring buffer of recent failures (max 100)
    failures: VecDeque<FailureRecord>,
    /// Number of events since last GCS backup (for periodic backup trigger)
    events_since_backup: usize,
}

type SharedState = Arc<Mutex<CoordinatorData>>;

/// Maximum events in the ring buffer
const MAX_EVENTS: usize = 1000;
/// Maximum failures in the ring buffer
const MAX_FAILURES: usize = 100;

impl CoordinatorData {
    /// Log an event to the ring buffer
    fn log_event(&mut self, event: JobEvent) {
        self.events.push_back(event);
        if self.events.len() > MAX_EVENTS {
            self.events.pop_front();
        }
    }

    /// Log a failure to the ring buffer
    fn log_failure(&mut self, failure: FailureRecord) {
        self.failures.push_back(failure);
        if self.failures.len() > MAX_FAILURES {
            self.failures.pop_front();
        }
    }

    /// Get current timestamp in milliseconds
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// Start the coordinator server using a config struct.
///
/// This function blocks until the job is complete or the server is interrupted.
#[allow(dead_code)]
pub async fn start_coordinator(config: CoordinatorConfig) -> Result<()> {
    // Extract output_path from job_spec for backward compatibility
    let output_path = config
        .job_spec
        .as_ref()
        .and_then(|js| js.output_path())
        .map(String::from)
        .unwrap_or_default();

    run_coordinator(
        config.port,
        config.db_path,
        config.backup_path,
        config.input_path,
        output_path,
        config.total_tasks,
        config.batch_size,
        config.timeout_secs,
    )
    .await
}

/// Properly structured coordinator startup.
///
/// Note: For backward compatibility, `output_path` is converted to a default
/// ExportParquet JobSpec. New code should use the API endpoint with JobSpec directly.
pub async fn run_coordinator(
    port: u16,
    db_path: String,
    backup_path: Option<String>,
    input_path: String,
    output_path: String,
    total_tasks: usize,
    batch_size: usize,
    timeout_secs: u64,
) -> Result<()> {
    use axum::{routing::{get, post}, Router};
    use tokio::net::TcpListener;

    // Determine if starting in idle mode (no job configured yet)
    let idle = total_tasks == 0;

    // Convert output_path to JobSpec for backward compatibility
    let job_spec = if output_path.is_empty() {
        None
    } else {
        Some(JobSpec::ExportParquet {
            output_path: output_path.clone(),
        })
    };

    if idle {
        println!(
            "Coordinator starting on port {} in IDLE mode (waiting for job submission)",
            port
        );
        println!("  Submit a job via POST /api/job or pool submit");
    } else {
        println!(
            "Coordinator starting on port {} with {} partitions",
            port, total_tasks
        );
        println!("  Input: {}", input_path);
        if let Some(ref spec) = job_spec {
            println!("  Job: {}", spec.description());
            if let Some(out) = spec.output_path() {
                println!("  Output: {}", out);
            }
        }
    }
    println!("  Batch size: {}", batch_size);
    println!("  Timeout: {}s", timeout_secs);

    // Try to restore from backup path if configured
    if let Some(ref bp) = backup_path {
        println!("  Checking for database backup at {}", bp);
        let bp_clone = bp.clone();
        let db_path_clone = db_path.clone();
        let restore_result = tokio::task::spawn_blocking(move || {
            if let Ok(mut reader) = genohype_core::io::get_reader(&bp_clone) {
                let mut data = Vec::new();
                if std::io::Read::read_to_end(&mut reader, &mut data).is_ok() && !data.is_empty() {
                    if let Some(parent) = std::path::Path::new(&db_path_clone).parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if std::fs::write(&db_path_clone, data).is_ok() {
                        return true;
                    }
                }
            }
            false
        }).await;
        if restore_result.unwrap_or(false) {
            println!("  Restored ops database from {}", bp);
        }
    }

    // Initialize SQLite database for metrics persistence
    let metrics_db = MetricsDb::open(&db_path).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to open metrics database: {}", e),
        ))
    })?;
    println!("  Metrics DB: {}", db_path);
    if let Some(ref bp) = backup_path {
        println!("  Backup path: {}", bp);
    }

    let state = Arc::new(Mutex::new(CoordinatorData {
        pending_partitions: (0..total_tasks).collect(),
        processing_partitions: HashMap::new(),
        completed_tasks: HashSet::new(),
        config: CoordinatorConfig {
            port,
            input_path,
            job_spec,
            total_tasks,
            batch_size,
            timeout_secs,
            stuck_timeout_secs: 600, // Default 10 minutes
            filters: Vec::new(),
            intervals: Vec::new(),
            memory_weight_mb: None,
            db_path: db_path.clone(),
            backup_path: backup_path.clone(),
        },
        total_rows: 0,
        scan_cpu_secs: 0.0,
        aggregate_cpu_secs: 0.0,
        wasted_cpu_secs: 0.0,
        retry_counts: HashMap::new(),
        failed_partitions: HashSet::new(),
        worker_registry: HashMap::new(),
        job_start_time: Instant::now(),
        last_progress_time: Instant::now(),
        idle,
        metrics_db,
        aggregated_results: Vec::new(),
        manhattan_state: None,
        batch_state: None,
        active_tasks: HashMap::new(),
        last_error: None,
        ingestion_state: None,
        events: VecDeque::new(),
        failures: VecDeque::new(),
        events_since_backup: 0,
    }));

    // Start background timeout monitor
    let monitor_state = state.clone();
    tokio::spawn(async move {
        let mut last_backup_time = Instant::now();
        let mut last_events_count = 0usize;

        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            check_timeouts(&monitor_state, timeout_secs);
            check_worker_liveness(&monitor_state);

            // Track events for periodic backup trigger
            let current_events = { monitor_state.lock().unwrap().events.len() };
            let events_delta = current_events.saturating_sub(last_events_count);
            last_events_count = current_events;

            // Check for stuck jobs (R3)
            let stuck_timeout = {
                let data = monitor_state.lock().unwrap();
                data.config.stuck_timeout_secs
            };
            check_stuck_job(&monitor_state, stuck_timeout);

            // Print periodic status
            // For batch/Manhattan jobs, use their state; otherwise use standard counters
            let (pending, processing, completed, failed, total, rows, is_idle, is_batch_complete) = {
                let data = monitor_state.lock().unwrap();
                if let Some(ref batch) = data.batch_state {
                    // Batch jobs use phenotype-level tracking
                    let total = batch.total_phenotypes;
                    let completed = batch.completed_count;
                    let pending = batch.pending_queue.len();
                    let processing = batch.active_phenotypes.len() + batch.ready_to_aggregate.len();
                    let is_complete = batch.pending_queue.is_empty()
                        && batch.active_phenotypes.is_empty()
                        && batch.ready_to_aggregate.is_empty()
                        && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;
                    (
                        pending,
                        processing,
                        completed,
                        batch.failed_count,
                        total,
                        data.total_rows,
                        data.idle,
                        is_complete,
                    )
                } else if let Some(ref m) = data.manhattan_state {
                    // Single Manhattan pipeline uses separate tracking
                    let total_parts = m.exome_total_tasks + m.genome_total_tasks + 1; // +1 for aggregate
                    let completed_parts = m.exome_completed.len() + m.genome_completed.len() + if m.aggregate_complete { 1 } else { 0 };
                    let processing_parts = m.exome_processing.len() + m.genome_processing.len() + if m.aggregate_dispatched && !m.aggregate_complete { 1 } else { 0 };
                    let pending_parts = m.exome_pending.len() + m.genome_pending.len();
                    let is_complete = m.phase == ManhattanPhase::Complete;
                    (
                        pending_parts,
                        processing_parts,
                        completed_parts,
                        data.failed_partitions.len(),
                        total_parts,
                        data.total_rows,
                        data.idle,
                        is_complete,
                    )
                } else {
                    (
                        data.pending_partitions.len(),
                        data.processing_partitions.len(),
                        data.completed_tasks.len(),
                        data.failed_partitions.len(),
                        data.config.total_tasks,
                        data.total_rows,
                        data.idle,
                        false, // Standard jobs don't use this flag
                    )
                }
            };

            // Don't print progress or exit if idle
            if is_idle {
                println!("Coordinator idle, waiting for job submission...");
                continue;
            }

            if completed > 0 || failed > 0 {
                println!(
                    "Progress: {}/{} partitions ({:.1}%), {} failed, {} rows processed",
                    completed,
                    total,
                    (completed as f64 / total as f64) * 100.0,
                    failed,
                    rows
                );
            }

            // Periodic / threshold backup fallback
            // Backup every 1000 events or every 30 minutes
            let (db_path, backup_path, events_since, metrics_db) = {
                let mut data = monitor_state.lock().unwrap();
                data.events_since_backup += events_delta;
                (
                    data.config.db_path.clone(),
                    data.config.backup_path.clone(),
                    data.events_since_backup,
                    data.metrics_db.clone(),
                )
            };

            if let Some(ref bp) = backup_path {
                let should_backup = events_since > 1000 || last_backup_time.elapsed().as_secs() > 60;
                if should_backup {
                    backup_db(&metrics_db, &db_path, bp).await;
                    last_backup_time = Instant::now();
                    if let Ok(mut data) = monitor_state.lock() {
                        data.events_since_backup = 0;
                    }
                }
            }

            // Check if job is complete
            // For batch/Manhattan: use the phase flag; for standard jobs: check partition counts
            let job_complete = is_batch_complete ||
                (total > 0 && (completed + failed) == total && processing == 0 && pending == 0);

            if job_complete {
                if failed > 0 {
                    println!(
                        "Job finished with {} failed partitions out of {}. Total rows: {}",
                        failed, total, rows
                    );
                } else {
                    println!("All {} partitions completed! Total rows: {}", total, rows);
                }

                // For Manhattan jobs, run the composite step to merge partial PNGs
                let manhattan_spec = {
                    let data = monitor_state.lock().unwrap();
                    if let Some(JobSpec::Manhattan { ref spec, .. }) = data.config.job_spec {
                        Some(spec.clone())
                    } else {
                        None
                    }
                };

                if let Some(spec) = manhattan_spec {
                    if spec.skip_composite {
                        println!("Skipping composite step (--no-composite). Run manually with:");
                        println!("  genohype manhattan --from-shards {}", spec.output_path);
                    } else {
                    println!("Running post-job composite for Manhattan plot...");

                    let output_dir = spec.output_path.trim_end_matches('/');
                    let final_png = format!("{}/manhattan.png", output_dir);

                    // Run composite in a blocking thread to avoid nested runtime issues
                    let output_path = spec.output_path.clone();
                    let final_png_clone = final_png.clone();
                    let width = spec.width;
                    let height = spec.height;
                    let threshold = spec.threshold;

                    let result = tokio::task::spawn_blocking(move || {
                        crate::manhattan::pipeline::composite_partial_pngs(
                            &output_path,
                            &final_png_clone,
                            width,
                            height,
                            threshold,
                        )
                    })
                    .await;

                    match result {
                        Ok(Ok(())) => println!("Composite complete: {}", final_png),
                        Ok(Err(e)) => eprintln!("Warning: Composite failed: {}", e),
                        Err(e) => eprintln!("Warning: Composite task panicked: {}", e),
                    }
                    }
                }

                // Save aggregated results to file before exiting
                {
                    let data = monitor_state.lock().unwrap();
                    let result = JobResultResponse {
                        available: true,
                        result: Some(serde_json::Value::Array(data.aggregated_results.clone())),
                        error: None,
                    };
                    if let Ok(json) = serde_json::to_string_pretty(&result) {
                        if let Err(e) = std::fs::write("/tmp/job_result.json", &json) {
                            eprintln!("Warning: Failed to save results to file: {}", e);
                        } else {
                            println!("Results saved to /tmp/job_result.json");
                        }
                    }
                }

                // Perform final backup to GCS before exiting
                {
                    let (db_path, backup_path, metrics_db) = {
                        let data = monitor_state.lock().unwrap();
                        (
                            data.config.db_path.clone(),
                            data.config.backup_path.clone(),
                            data.metrics_db.clone(),
                        )
                    };
                    if let Some(bp) = backup_path {
                        backup_db(&metrics_db, &db_path, &bp).await;
                    }
                }

                println!("Coordinator will exit in 10 seconds...");
                tokio::time::sleep(Duration::from_secs(10)).await;
                std::process::exit(0);
            }
        }
    });

    let app = Router::new()
        .route("/work", post(get_work))
        .route("/complete", post(complete_work))
        .route("/status", get(get_status))
        .route("/heartbeat", post(handle_heartbeat))
        // API routes
        .route("/api/dashboard/summary", get(get_dashboard_summary))
        .route("/api/dashboard/bottlenecks", get(get_dashboard_bottlenecks))
        .route("/api/dashboard/workers", get(get_dashboard_workers))
        .route("/api/dashboard/metrics", get(get_dashboard_metrics))
        .route("/api/dashboard/batch", get(get_batch_status))
        .route("/api/job", post(submit_job))
        .route("/api/cancel", post(cancel_job))
        .route("/api/result", get(get_job_result))
        .route("/api/binary", get(serve_binary))
        .route("/api/export-metrics", post(export_metrics))
        .route("/api/events", get(get_events))
        .route("/api/failures", get(get_failures))
        .route("/api/workers/:worker_id/logs", get(get_worker_logs))
        .with_state(state)
        // Serve embedded SPA dashboard
        .route("/dashboard", get(serve_dashboard_index))
        .route("/dashboard/*path", get(serve_dashboard_asset));

    println!("Dashboard available at http://0.0.0.0:{}/dashboard", port);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Coordinator listening on {}", addr);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(crate::HailError::Io)?;
    axum::serve(listener, app)
        .await
        .map_err(|e| crate::HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    Ok(())
}

/// Check if the job is stuck (no progress for N minutes) and auto-cancel if needed.
fn check_stuck_job(state: &SharedState, timeout_secs: u64) {
    let mut data = state.lock().unwrap();

    // Only check if job is running
    if data.idle {
        return;
    }

    // Only consider it stuck if ZERO partitions have completed
    // (if some completed but now it's stalled, that's different - likely worker issues handled by timeouts)
    // Check batch state, Manhattan state, and standard counters
    let has_progress = if let Some(ref batch) = data.batch_state {
        // For batch jobs, check if any phenotypes completed
        batch.completed_count > 0 || !batch.active_phenotypes.values().all(|s| {
            s.exome_completed.is_empty() && s.genome_completed.is_empty()
        })
    } else if let Some(ref m) = data.manhattan_state {
        // For single Manhattan jobs, check if any exome/genome partitions completed
        !m.exome_completed.is_empty() || !m.genome_completed.is_empty() || m.aggregate_complete
    } else {
        // For standard jobs, check standard counter
        !data.completed_tasks.is_empty()
    };

    if has_progress {
        return;
    }

    let elapsed = data.last_progress_time.elapsed();
    if elapsed.as_secs() > timeout_secs {
        println!(
            "Job stuck (0 progress for {}s). Auto-cancelling.",
            elapsed.as_secs()
        );

        // Reset state
        data.pending_partitions.clear();
        data.processing_partitions.clear();
        data.manhattan_state = None;
        data.batch_state = None;
        data.active_tasks.clear();
        data.ingestion_state = None;
        data.idle = true;
    }
}

/// Discover phenotypes for ingestion by scanning GCS for manifest.json files.
///
/// Expected directory structure: {input_dir}/{ancestry}/{phenotype_id}/manifest.json
/// Returns: Vec of (phenotype_id, ancestry, base_path)
fn discover_phenotypes_for_ingestion(input_dir: &str) -> crate::Result<Vec<(String, String, String)>> {
    use std::process::Command;

    let input_dir = input_dir.trim_end_matches('/');

    // Use gsutil to list all manifest.json files recursively
    // This is more reliable than object_store listing for discovering subdirectories
    let output = Command::new("gsutil")
        .args(["-m", "ls", "-r", &format!("{}/**/manifest.json", input_dir)])
        .output()
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to run gsutil: {}", e),
            ))
        })?;

    if !output.status.success() {
        // If no files found, return empty vec (not an error)
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("matched no objects") || stderr.contains("CommandException") {
            return Ok(Vec::new());
        }
        return Err(crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("gsutil failed: {}", stderr),
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut phenotypes = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if !line.ends_with("/manifest.json") {
            continue;
        }

        // Parse path: gs://bucket/base/{ancestry}/{phenotype_id}/manifest.json
        // Remove /manifest.json to get base_path
        let base_path = line.trim_end_matches("/manifest.json");

        // Extract ancestry and phenotype_id from path
        // Take last two segments
        let segments: Vec<&str> = base_path.split('/').collect();
        if segments.len() < 2 {
            continue;
        }

        let phenotype_id = segments[segments.len() - 1].to_string();
        let ancestry = segments[segments.len() - 2].to_string();

        // Skip if ancestry looks like a bucket or path component
        if ancestry.is_empty() || phenotype_id.is_empty() {
            continue;
        }

        phenotypes.push((phenotype_id, ancestry, base_path.to_string()));
    }

    Ok(phenotypes)
}

/// Check for timed-out work and reschedule.
fn check_timeouts(state: &SharedState, timeout_secs: u64) {
    let mut data = state.lock().unwrap();
    let now = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    let mut timed_out = Vec::new();
    for (part_id, (worker, start_time)) in &data.processing_partitions {
        if now.duration_since(*start_time) > timeout {
            timed_out.push((*part_id, worker.clone(), *start_time));
        }
    }

    for (part_id, worker, start_time) in timed_out {
        data.processing_partitions.remove(&part_id);

        // Track the wasted time from this timeout
        let elapsed_secs = now.duration_since(start_time).as_secs_f64();
        data.wasted_cpu_secs += elapsed_secs;

        let retries = data.retry_counts.entry(part_id).or_insert(0);
        *retries += 1;

        if *retries > 3 {
            println!(
                "Partition {} exceeded max retries (worker: {}). Marking as failed. (wasted {:.1}s)",
                part_id, worker, elapsed_secs
            );
            data.failed_partitions.insert(part_id);
        } else {
            println!(
                "Partition {} timed out (worker: {}), rescheduling (retry {}) (wasted {:.1}s)",
                part_id, worker, retries, elapsed_secs
            );
            data.pending_partitions.push_front(part_id);
        }
    }
}

/// Ensure a worker exists in the registry and update last_seen.
fn touch_worker(
    data: &mut CoordinatorData,
    worker_id: &str,
    hardware: Option<crate::distributed::message::HardwareSpec>,
) {
    let worker = data
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
        });
    worker.last_seen = Instant::now();
    if hardware.is_some() {
        worker.hardware = hardware;
    }
}

/// Activate phenotypes from the pending queue up to the active limit.
///
/// This implements lazy loading - phenotypes are only initialized when
/// there's capacity to process them, rather than all at once at job submission.
fn activate_next_phenotypes(batch: &mut BatchState) {
    while batch.active_phenotypes.len() < BATCH_ACTIVE_LIMIT && !batch.pending_queue.is_empty() {
        let spec = batch.pending_queue.pop_front().unwrap();

        // Generate a unique ID for this phenotype
        // Use the output_path as the ID since it should be unique per phenotype
        let phenotype_id = spec.output_path.clone();

        // Get partition counts from the spec (set by CLI/pool submit)
        // If not set, fall back to 0 (skip that source)
        let exome_partitions = spec.exome_partitions.unwrap_or(0);
        let genome_partitions = spec.genome_partitions.unwrap_or(0);

        if exome_partitions == 0 && genome_partitions == 0 {
            // No partitions to scan - this phenotype needs partition counts
            // For now, skip it with a warning (CLI should set partition counts)
            println!(
                "Warning: Phenotype {} has no partition counts set, skipping",
                phenotype_id
            );
            batch.failed_count += 1;
            // Update status to failed
            if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                status.stage = "failed".to_string();
                status.error = Some("No partition counts set".to_string());
            }
            continue;
        }

        println!(
            "Activating phenotype {} ({} exome, {} genome partitions)",
            phenotype_id, exome_partitions, genome_partitions
        );

        // Update status tracking and record start time
        if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
            status.stage = "scanning".to_string();
            status.partitions_total = exome_partitions + genome_partitions;
        }
        batch.phenotype_start_times.insert(phenotype_id.clone(), Instant::now());
        batch.phenotype_cpu_secs.insert(phenotype_id.clone(), 0.0);

        // Initialize the pipeline state
        let pipeline_state = ManhattanPipelineState {
            mode: batch.mode,
            phase: ManhattanPhase::Scan,
            original_spec: spec.clone(),
            layout: spec.layout.clone(),
            y_scale: spec.y_scale.clone(),
            contig_lengths: spec.contig_lengths.clone().unwrap_or_default(),
            exome_total_tasks: exome_partitions,
            exome_pending: (0..exome_partitions).collect(),
            exome_processing: HashMap::new(),
            exome_completed: HashSet::new(),
            genome_total_tasks: genome_partitions,
            genome_pending: (0..genome_partitions).collect(),
            genome_processing: HashMap::new(),
            genome_completed: HashSet::new(),
            aggregate_dispatched: false,
            aggregate_complete: false,
        };

        batch.active_phenotypes.insert(phenotype_id, pipeline_state);
    }
}

/// Determine the optimal batch size for a worker based on its hardware and the job type.
fn determine_batch_size(
    default_size: usize,
    hardware: Option<&crate::distributed::message::HardwareSpec>,
    job_spec: &Option<JobSpec>,
    memory_weight_mb: Option<u64>,
) -> usize {
    if let Some(hw) = hardware {
        // Different jobs have different memory characteristics.
        // - Parquet/JSON: streaming, low memory, scale well
        // - Manhattan: higher memory footprint per partition (point rendering)
        // - Summary: very memory efficient, saturates cores easily
        let core_multiplier = match job_spec {
            Some(JobSpec::ExportParquet { .. }) | Some(JobSpec::ExportJson { .. }) => 2.0,
            Some(JobSpec::ManhattanScan(_))
            | Some(JobSpec::ManhattanBatch { .. })
            | Some(JobSpec::Manhattan { .. }) => 1.0,
            Some(JobSpec::Summary) => 3.0,
            _ => 1.5,
        };

        let core_based = (hw.num_cores as f64 * core_multiplier).ceil() as usize;

        // Phase 3: Use job-specific memory weight if provided, otherwise infer from job type
        let mem_per_partition_mb = memory_weight_mb.unwrap_or_else(|| match job_spec {
            Some(JobSpec::ManhattanScan(_))
            | Some(JobSpec::ManhattanBatch { .. })
            | Some(JobSpec::Manhattan { .. }) => 1024, // 1GB per partition for Manhattan
            Some(JobSpec::ExportParquet { .. }) | Some(JobSpec::ExportJson { .. }) => 256, // 256MB
            Some(JobSpec::Summary) => 64, // 64MB, very light
            _ => 500,
        });

        let max_by_memory = (hw.total_memory_mb / mem_per_partition_mb).max(1) as usize;

        // We want at least the default_size (so we don't regress if someone manually specified a good default),
        // but if memory dictates a lower cap, we respect it unless the default size itself exceeds memory.
        let target = core_based.max(default_size).min(max_by_memory.max(default_size));

        target
    } else {
        default_size
    }
}

/// Get work for a batch Manhattan job (multi-phenotype scheduling).
/// Get work for an ingestion job.
fn get_ingestion_work(
    data: &mut CoordinatorData,
    ingestion: &mut IngestionState,
    worker_id: &str,
) -> axum::Json<WorkResponse> {
    // Check if there's a pending task
    if let Some((phenotype_id, ancestry, base_path)) = ingestion.pending_tasks.pop_front() {
        let task_id = Uuid::new_v4().to_string();

        // Track this task
        ingestion.active_tasks.insert(
            task_id.clone(),
            (
                phenotype_id.clone(),
                ancestry.clone(),
                base_path.clone(),
                worker_id.to_string(),
                Instant::now(),
            ),
        );

        // Update worker status
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
        }

        println!(
            "Assigned 1 ingest task to {} [{}/{}] ({} pending, {} active, {} done)",
            worker_id,
            phenotype_id,
            ancestry,
            ingestion.pending_tasks.len(),
            ingestion.active_tasks.len(),
            ingestion.completed_count
        );

        // Create IngestManhattanTask job spec
        let job_spec = JobSpec::IngestManhattanTask {
            phenotype_id: phenotype_id.clone(),
            ancestry: ancestry.clone(),
            base_path,
            clickhouse_url: ingestion.clickhouse_url.clone(),
            database: ingestion.database.clone(),
        };

        // Create TaskDescriptor for this ingestion task
        let task = TaskType::Phenotype {
            phenotype_id: phenotype_id.clone(),
            ancestry: Some(ancestry),
            operation: PhenotypeOp::Ingest {
                clickhouse_url: ingestion.clickhouse_url.clone(),
                database: ingestion.database.clone(),
            },
        }
        .into_descriptor(
            task_id.clone(),
            Some(format!("{} → Ingest", phenotype_id)),
            None,
            Some(ingestion.total_tasks),
        );

        return axum::Json(WorkResponse::Task {
            tasks: vec![task],
            input_path: String::new(), // Not used for ingestion tasks
            payload: serde_json::to_value(&job_spec).unwrap_or_default(),
            total_tasks: ingestion.total_tasks,
            filters: Vec::new(),
            intervals: Vec::new(),
        });
    }

    // Check if there's active work in progress
    if !ingestion.active_tasks.is_empty() {
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Idle;
        }
        return axum::Json(WorkResponse::Wait);
    }

    // All work complete
    println!(
        "Ingestion complete: {} succeeded, {} failed",
        ingestion.completed_count, ingestion.failed_count
    );
    axum::Json(WorkResponse::Exit)
}

fn get_batch_work(
    data: &mut CoordinatorData,
    batch: &mut BatchState,
    worker_id: &str,
) -> axum::Json<WorkResponse> {
    let now = Instant::now();
    let worker_hw = data
        .worker_registry
        .get(worker_id)
        .and_then(|w| w.hardware.as_ref());

    let max_batch_size = determine_batch_size(data.config.batch_size, worker_hw, &data.config.job_spec, data.config.memory_weight_mb);
    let partition_batch_size = data.worker_registry.get(worker_id)
        .and_then(|w| w.current_batch_size)
        .unwrap_or_else(|| (max_batch_size / 10).max(2).min(max_batch_size));

    // Step 1: Activate phenotypes to fill the active pool
    activate_next_phenotypes(batch);

    // Step 2: Priority 1 - Check for aggregation batches
    // If we have enough ready or no other work available
    let has_scan_work = batch.active_phenotypes.values().any(|state| {
        !state.exome_pending.is_empty() || !state.genome_pending.is_empty()
    });

    let should_aggregate = batch.ready_to_aggregate.len() >= AGGREGATE_BATCH_SIZE
        || (!batch.ready_to_aggregate.is_empty() && !has_scan_work && batch.pending_queue.is_empty());

    if should_aggregate {
        // Drain aggregation specs (up to batch size)
        let count = std::cmp::min(batch.ready_to_aggregate.len(), AGGREGATE_BATCH_SIZE);
        let specs_to_aggregate: Vec<_> = batch.ready_to_aggregate.drain(..count).collect();

        let phenotype_ids: Vec<String> = specs_to_aggregate.iter().map(|(id, _)| id.clone()).collect();
        let aggregate_specs: Vec<ManhattanAggregateSpec> = specs_to_aggregate.into_iter().map(|(_, spec)| spec).collect();

        let task_id = Uuid::new_v4().to_string();

        // Update status for all phenotypes in this batch
        for pid in &phenotype_ids {
            if let Some(status) = batch.phenotype_statuses.get_mut(pid) {
                status.stage = "aggregating".to_string();
            }
        }

        // Track this task
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::AggregateBatch {
                phenotype_ids: phenotype_ids.clone(),
                started_at_ms: CoordinatorData::now_ms(),
            },
        );

        // Update worker status
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
        }

        println!(
            "Assigned {} aggregate task(s) to {} [{:?}] ({} queued, {} done)",
            phenotype_ids.len(),
            worker_id,
            phenotype_ids,
            batch.ready_to_aggregate.len(),
            batch.completed_count
        );

        // Create TaskDescriptors for each phenotype in the batch
        let tasks: Vec<TaskDescriptor> = phenotype_ids
            .iter()
            .enumerate()
            .map(|(i, pid)| {
                TaskType::Phenotype {
                    phenotype_id: pid.clone(),
                    ancestry: None,
                    operation: PhenotypeOp::ManhattanAggregate,
                }
                .into_descriptor(
                    pid.clone(),
                    Some(format!("{} → Aggregate", pid)),
                    Some(i),
                    Some(phenotype_ids.len()),
                )
            })
            .collect();

        return axum::Json(WorkResponse::Task {
            tasks,
            input_path: String::new(),
            payload: serde_json::to_value(&JobSpec::ManhattanAggregateBatch { specs: aggregate_specs }).unwrap_or_default(),
            total_tasks: phenotype_ids.len(),
            filters: Vec::new(),
            intervals: Vec::new(),
        });
    }

    // Step 3: Priority 2 - Find scan work from active phenotypes
    for (phenotype_id, state) in batch.active_phenotypes.iter_mut() {
        // Try exome first, then genome
        let (source, partitions, table_path) = if let Some(part_id) = state.exome_pending.pop_front() {
            let mut parts = vec![part_id];
            while parts.len() < partition_batch_size {
                if let Some(p) = state.exome_pending.pop_front() {
                    parts.push(p);
                } else {
                    break;
                }
            }
            for &p in &parts {
                state.exome_processing.insert(p, (worker_id.to_string(), now));
            }
            (
                ManhattanSource::Exome,
                parts,
                state.original_spec.exome.clone().unwrap_or_default(),
            )
        } else if let Some(part_id) = state.genome_pending.pop_front() {
            let mut parts = vec![part_id];
            while parts.len() < partition_batch_size {
                if let Some(p) = state.genome_pending.pop_front() {
                    parts.push(p);
                } else {
                    break;
                }
            }
            for &p in &parts {
                state.genome_processing.insert(p, (worker_id.to_string(), now));
            }
            (
                ManhattanSource::Genome,
                parts,
                state.original_spec.genome.clone().unwrap_or_default(),
            )
        } else {
            // No pending work for this phenotype, continue to next
            continue;
        };

        let task_id = Uuid::new_v4().to_string();

        // Track this task (using first partition as identifier)
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::Scan {
                phenotype_id: phenotype_id.clone(),
                partition_id: partitions[0],
                source,
                started_at_ms: CoordinatorData::now_ms(),
            },
        );

        let source_name = match source {
            ManhattanSource::Exome => "exome",
            ManhattanSource::Genome => "genome",
        };

        // Create TaskDescriptors for each partition
        let total_tasks = state.exome_total_tasks + state.genome_total_tasks;
        let tasks: Vec<TaskDescriptor> = partitions
            .iter()
            .map(|&i| {
                TaskType::Partition {
                    table_path: table_path.clone(),
                    partition_index: i,
                    operation: PartitionOp::ManhattanScan {
                        phenotype_id: phenotype_id.clone(),
                        source: source_name.to_string(),
                    },
                }
                .into_descriptor(
                    i.to_string(),
                    Some(format!("Partition {} → Scan ({})", i + 1, source_name)),
                    Some(i),
                    Some(total_tasks),
                )
            })
            .collect();
        let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();

        // Update worker status and track task info for AIMD duration tracking
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
            w.current_task = Some(ActiveTaskInfo {
                task_id: task_id.clone(),
                phenotype_id: Some(phenotype_id.clone()),
                phase: "scan".to_string(),
                source: Some(source_name.to_string()),
                tasks: task_ids.clone(),
                started_at_ms: CoordinatorData::now_ms(),
            });
        }

        let pending_scans = state.exome_pending.len() + state.genome_pending.len();
        let processing_scans = state.exome_processing.len() + state.genome_processing.len();
        let completed_scans = state.exome_completed.len() + state.genome_completed.len();
        println!(
            "Assigned {} {} scan task(s) to {} [{}] ({} pending, {} processing, {} done)",
            tasks.len(),
            source_name,
            worker_id,
            phenotype_id,
            pending_scans,
            processing_scans,
            completed_scans
        );

        // Build ManhattanScanSpec with identity metadata
        // Extract phenotype and ancestry from the original spec, with fallbacks
        let phenotype = state.original_spec.phenotype.clone()
            .unwrap_or_else(|| phenotype_id.clone());
        let ancestry = state.original_spec.ancestry.clone()
            .unwrap_or_else(|| "unknown".to_string());

        // Resolve style based on source type
        let plot_type = match source {
            ManhattanSource::Exome => PlotType::Exome,
            ManhattanSource::Genome => PlotType::Genome,
        };
        let style = state.original_spec.styling.resolve(plot_type);

        let scan_spec = ManhattanScanSpec {
            phenotype,
            ancestry,
            source,
            table_path,
            output_path: state.original_spec.output_path.clone(),
            threshold: state.original_spec.threshold,
            y_field: state.original_spec.y_field.clone(),
            layout: state.layout.clone().unwrap_or_default(),
            y_scale: state.y_scale.clone().unwrap_or_default(),
            width: state.original_spec.width,
            height: state.original_spec.height,
            contig_lengths: state.contig_lengths.clone(),
            style,
        };

        return axum::Json(WorkResponse::Task {
            tasks,
            input_path: String::new(),
            payload: serde_json::to_value(&JobSpec::ManhattanScan(scan_spec)).unwrap_or_default(),
            total_tasks,
            filters: Vec::new(),
            intervals: Vec::new(),
        });
    }

    // Step 4: Check if batch is complete
    // Must verify all phenotypes are actually done, not just that queues are empty
    // (queues can be empty while aggregate tasks are still in-flight with workers)
    let all_done = batch.pending_queue.is_empty()
        && batch.active_phenotypes.is_empty()
        && batch.ready_to_aggregate.is_empty()
        && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;

    if all_done {
        println!(
            "Manhattan batch complete: {} completed, {} failed",
            batch.completed_count, batch.failed_count
        );
        return axum::Json(WorkResponse::Exit);
    }

    // Step 5: Work in progress, tell worker to wait
    if let Some(w) = data.worker_registry.get_mut(worker_id) {
        w.status = WorkerStatus::Idle;
    }
    axum::Json(WorkResponse::Wait)
}

/// Handler for POST /work - worker requests work.
async fn get_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<WorkRequest>,
) -> axum::Json<WorkResponse> {
    let mut data = state.lock().unwrap();
    touch_worker(&mut data, &req.worker_id, req.hardware.clone());

    // If coordinator is idle (no job configured), tell workers to wait
    if data.idle {
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
            w.status = WorkerStatus::Idle;
        }
        return axum::Json(WorkResponse::Wait);
    }

    // Check for Manhattan batch job (multi-phenotype scheduling)
    if data.batch_state.is_some() {
        let mut batch = data.batch_state.take().unwrap();
        let result = get_batch_work(&mut data, &mut batch, &req.worker_id);
        data.batch_state = Some(batch);
        return result;
    }

    // Check for Manhattan pipeline job (two-phase execution)
    if data.manhattan_state.is_some() {
        // Take manhattan state out temporarily to avoid borrow issues
        let mut manhattan = data.manhattan_state.take().unwrap();
        let result = get_manhattan_work(&mut data, &mut manhattan, &req.worker_id);
        data.manhattan_state = Some(manhattan);
        return result;
    }

    // Check for ingestion job
    if data.ingestion_state.is_some() {
        let mut ingestion = data.ingestion_state.take().unwrap();
        let result = get_ingestion_work(&mut data, &mut ingestion, &req.worker_id);
        data.ingestion_state = Some(ingestion);
        return result;
    }

    // Standard (non-Manhattan) job: check if there's pending work
    if let Some(part_id) = data.pending_partitions.pop_front() {
        // Collect batch of partitions
        let mut partitions = vec![part_id];
        let worker_hw = data
            .worker_registry
            .get(&req.worker_id)
            .and_then(|w| w.hardware.as_ref());

        let max_batch_size = determine_batch_size(data.config.batch_size, worker_hw, &data.config.job_spec, data.config.memory_weight_mb);
        let batch_size = data.worker_registry.get(&req.worker_id)
            .and_then(|w| w.current_batch_size)
            .unwrap_or_else(|| (max_batch_size / 10).max(2).min(max_batch_size));

        while partitions.len() < batch_size {
            if let Some(next_id) = data.pending_partitions.pop_front() {
                partitions.push(next_id);
            } else {
                break;
            }
        }

        // Mark all as processing
        let now = Instant::now();
        for &p in &partitions {
            data.processing_partitions
                .insert(p, (req.worker_id.clone(), now));
        }

        // Get job_spec, or return Wait if not configured (shouldn't happen since we check idle)
        let job_spec = match data.config.job_spec.clone() {
            Some(spec) => spec,
            None => {
                // Put partitions back
                for p in partitions.into_iter().rev() {
                    data.pending_partitions.push_front(p);
                }
                return axum::Json(WorkResponse::Wait);
            }
        };

        // Create TaskDescriptors for each partition
        let total_tasks = data.config.total_tasks;
        let input_path = data.config.input_path.clone();
        // Pre-generate all tasks once, then select the ones we need
        let all_tasks = job_spec.generate_tasks(&input_path, total_tasks);
        let tasks: Vec<TaskDescriptor> = partitions
            .iter()
            .map(|&i| {
                // Try to find by index first (most common case), then by ID
                all_tasks
                    .iter()
                    .find(|t| t.index == Some(i))
                    .cloned()
                    .or_else(|| all_tasks.iter().find(|t| t.id == i.to_string()).cloned())
                    .unwrap_or_else(|| TaskDescriptor::partition(i, total_tasks))
            })
            .collect();
        let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
        let task_labels: Vec<String> = tasks
            .iter()
            .map(|t| t.label.clone().unwrap_or_else(|| t.id.clone()))
            .collect();
        let task_type = tasks.first().map(|t| t.task_type.as_str()).unwrap_or("unknown");

        // Update worker status and assign task info for AIMD duration tracking
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
            w.status = WorkerStatus::Active;
            w.current_task = Some(ActiveTaskInfo {
                task_id: task_ids.first().cloned().unwrap_or_default(),
                phenotype_id: None,
                phase: task_type.to_string(),
                source: None,
                tasks: task_ids.clone(),
                started_at_ms: CoordinatorData::now_ms(),
            });
        }

        println!(
            "Assigned {} {} task(s) to {} [{:?}] ({} pending, {} processing, {} done)",
            tasks.len(),
            task_type,
            req.worker_id,
            task_labels,
            data.pending_partitions.len(),
            data.processing_partitions.len(),
            data.completed_tasks.len()
        );

        axum::Json(WorkResponse::Task {
            tasks,
            input_path: data.config.input_path.clone(),
            payload: serde_json::to_value(&job_spec).unwrap_or_default(),
            total_tasks: data.config.total_tasks,
            filters: data.config.filters.clone(),
            intervals: data.config.intervals.clone(),
        })
    } else if !data.processing_partitions.is_empty() {
        // Work in progress but nothing pending - tell worker to wait
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
            w.status = WorkerStatus::Idle;
        }
        axum::Json(WorkResponse::Wait)
    } else {
        // All done
        println!(
            "Worker {} requested work, but all partitions complete",
            req.worker_id
        );
        axum::Json(WorkResponse::Exit)
    }
}

/// Get work for a Manhattan pipeline job (two-phase execution).
fn get_manhattan_work(
    data: &mut CoordinatorData,
    manhattan: &mut ManhattanPipelineState,
    worker_id: &str,
) -> axum::Json<WorkResponse> {
    let now = Instant::now();
    let worker_hw = data
        .worker_registry
        .get(worker_id)
        .and_then(|w| w.hardware.as_ref());

    let max_batch_size = determine_batch_size(data.config.batch_size, worker_hw, &data.config.job_spec, data.config.memory_weight_mb);
    let batch_size = data.worker_registry.get(worker_id)
        .and_then(|w| w.current_batch_size)
        .unwrap_or_else(|| (max_batch_size / 10).max(2).min(max_batch_size));

    // Generate unique task ID for tracking
    let task_id = Uuid::new_v4().to_string();

    match manhattan.phase {
        ManhattanPhase::Scan => {
            // Try to get exome work first, then genome
            let (source, partitions, table_path) =
                if let Some(part_id) = manhattan.exome_pending.pop_front() {
                    let mut parts = vec![part_id];
                    while parts.len() < batch_size {
                        if let Some(p) = manhattan.exome_pending.pop_front() {
                            parts.push(p);
                        } else {
                            break;
                        }
                    }
                    for &p in &parts {
                        manhattan.exome_processing.insert(p, (worker_id.to_string(), now));
                    }
                    (
                        ManhattanSource::Exome,
                        parts,
                        manhattan.original_spec.exome.clone().unwrap_or_default(),
                    )
                } else if let Some(part_id) = manhattan.genome_pending.pop_front() {
                    let mut parts = vec![part_id];
                    while parts.len() < batch_size {
                        if let Some(p) = manhattan.genome_pending.pop_front() {
                            parts.push(p);
                        } else {
                            break;
                        }
                    }
                    for &p in &parts {
                        manhattan.genome_processing.insert(p, (worker_id.to_string(), now));
                    }
                    (
                        ManhattanSource::Genome,
                        parts,
                        manhattan.original_spec.genome.clone().unwrap_or_default(),
                    )
                } else if !manhattan.exome_processing.is_empty()
                    || !manhattan.genome_processing.is_empty()
                {
                    // Scan work still processing, tell worker to wait
                    if let Some(w) = data.worker_registry.get_mut(worker_id) {
                        w.status = WorkerStatus::Idle;
                    }
                    return axum::Json(WorkResponse::Wait);
                } else {
                    // All scan work complete
                    if manhattan.mode == crate::distributed::message::ExecutionMode::ScanOnly {
                        println!("Manhattan scan phase complete (ScanOnly mode) - job finished!");
                        manhattan.phase = ManhattanPhase::Complete;
                        return axum::Json(WorkResponse::Exit);
                    } else {
                        // Transition to Aggregate phase
                        println!("Manhattan scan phase complete, transitioning to Aggregate phase");
                        manhattan.phase = ManhattanPhase::Aggregate;
                        return get_manhattan_work(data, manhattan, worker_id);
                    }
                };

            let source_name = match source {
                ManhattanSource::Exome => "exome",
                ManhattanSource::Genome => "genome",
            };

            // Build identity metadata for dashboard task mapping
            let phenotype = manhattan.original_spec.phenotype.clone()
                .unwrap_or_else(|| {
                    manhattan.original_spec.output_path
                        .trim_end_matches('/')
                        .rsplit('/')
                        .next()
                        .unwrap_or("unknown")
                        .to_string()
                });

            // Create TaskDescriptors for each partition
            let total_tasks = manhattan.exome_total_tasks + manhattan.genome_total_tasks;
            let tasks: Vec<TaskDescriptor> = partitions
                .iter()
                .map(|&i| {
                    TaskType::Partition {
                        table_path: table_path.clone(),
                        partition_index: i,
                        operation: PartitionOp::ManhattanScan {
                            phenotype_id: phenotype.clone(),
                            source: source_name.to_string(),
                        },
                    }
                    .into_descriptor(
                        i.to_string(),
                        Some(format!("Partition {} → Scan ({})", i + 1, source_name)),
                        Some(i),
                        Some(total_tasks),
                    )
                })
                .collect();
            let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();

            // Update worker status and task info for AIMD duration tracking
            if let Some(w) = data.worker_registry.get_mut(worker_id) {
                w.status = WorkerStatus::Active;
                w.current_task = Some(ActiveTaskInfo {
                    task_id: task_id.clone(),
                    phenotype_id: Some(phenotype.clone()),
                    phase: "scan".to_string(),
                    source: Some(source_name.to_string()),
                    tasks: task_ids.clone(),
                    started_at_ms: CoordinatorData::now_ms(),
                });
            }

            let pending_scans = manhattan.exome_pending.len() + manhattan.genome_pending.len();
            let processing_scans = manhattan.exome_processing.len() + manhattan.genome_processing.len();
            let completed_scans = manhattan.exome_completed.len() + manhattan.genome_completed.len();
            println!(
                "Assigned {} {} scan task(s) to {} [{}] ({} pending, {} processing, {} done)",
                tasks.len(),
                source_name,
                worker_id,
                phenotype,
                pending_scans,
                processing_scans,
                completed_scans
            );

            // Build ManhattanScanSpec with identity metadata
            let ancestry = manhattan.original_spec.ancestry.clone()
                .unwrap_or_else(|| "unknown".to_string());

            // Resolve style based on source type
            let plot_type = match source {
                ManhattanSource::Exome => PlotType::Exome,
                ManhattanSource::Genome => PlotType::Genome,
            };
            let style = manhattan.original_spec.styling.resolve(plot_type);

            let scan_spec = ManhattanScanSpec {
                phenotype,
                ancestry,
                source,
                table_path,
                output_path: manhattan.original_spec.output_path.clone(),
                threshold: manhattan.original_spec.threshold,
                y_field: manhattan.original_spec.y_field.clone(),
                layout: manhattan.layout.clone().unwrap_or_default(),
                y_scale: manhattan.y_scale.clone().unwrap_or_default(),
                width: manhattan.original_spec.width,
                height: manhattan.original_spec.height,
                contig_lengths: manhattan.contig_lengths.clone(),
                style,
            };

            axum::Json(WorkResponse::Task {
                tasks,
                input_path: String::new(), // Not used for ManhattanScan
                payload: serde_json::to_value(&JobSpec::ManhattanScan(scan_spec)).unwrap_or_default(),
                total_tasks: manhattan.exome_total_tasks + manhattan.genome_total_tasks,
                filters: Vec::new(),
                intervals: Vec::new(),
            })
        }

        ManhattanPhase::Aggregate => {
            if manhattan.aggregate_dispatched && !manhattan.aggregate_complete {
                // Aggregate in progress, tell worker to wait
                if let Some(w) = data.worker_registry.get_mut(worker_id) {
                    w.status = WorkerStatus::Idle;
                }
                return axum::Json(WorkResponse::Wait);
            }

            if manhattan.aggregate_complete {
                // All done
                manhattan.phase = ManhattanPhase::Complete;
                return axum::Json(WorkResponse::Exit);
            }

            // Dispatch aggregate task
            manhattan.aggregate_dispatched = true;

            if let Some(w) = data.worker_registry.get_mut(worker_id) {
                w.status = WorkerStatus::Active;
            }

            let phenotype_id = manhattan.original_spec.phenotype.clone().unwrap_or_default();
            println!(
                "Assigned 1 aggregate task to {} [{}] (final aggregation)",
                worker_id,
                phenotype_id
            );

            // Build ManhattanAggregateSpec
            let aggregate_spec = ManhattanAggregateSpec {
                output_path: manhattan.original_spec.output_path.clone(),
                phenotype_id: manhattan.original_spec.phenotype.clone(),
                ancestry: manhattan.original_spec.ancestry.clone(),
                exome_results: manhattan.original_spec.exome.clone(),
                genome_results: manhattan.original_spec.genome.clone(),
                gene_burden: manhattan.original_spec.gene_burden.clone(),
                exome_exp_p: manhattan.original_spec.exome_exp_p.clone(),
                genome_exp_p: manhattan.original_spec.genome_exp_p.clone(),
                exome_annotations: manhattan.original_spec.exome_annotations.clone(),
                genome_annotations: manhattan.original_spec.genome_annotations.clone(),
                genes: manhattan.original_spec.genes.clone(),
                threshold: manhattan.original_spec.threshold,
                gene_threshold: manhattan.original_spec.gene_threshold,
                locus_threshold: manhattan.original_spec.locus_threshold,
                locus_window: manhattan.original_spec.locus_window,
                locus_plots: manhattan.original_spec.locus_plots,
                min_variants_per_locus: manhattan.original_spec.min_variants_per_locus,
                width: manhattan.original_spec.width,
                height: manhattan.original_spec.height,
                layout: manhattan.layout.clone().unwrap_or_default(),
                y_scale: manhattan.y_scale.clone().unwrap_or_default(),
                cleanup: false, // TODO: Add cleanup option to ManhattanSpec
                styling: manhattan.original_spec.styling.clone(),
            };

            // Create TaskDescriptor for aggregate task
            let phenotype_id = manhattan.original_spec.phenotype.clone().unwrap_or_default();
            let task = TaskType::Phenotype {
                phenotype_id: phenotype_id.clone(),
                ancestry: manhattan.original_spec.ancestry.clone(),
                operation: PhenotypeOp::ManhattanAggregate,
            }
            .into_descriptor(
                task_id.clone(),
                Some(format!("{} → Aggregate", phenotype_id)),
                Some(0),
                Some(1),
            );

            axum::Json(WorkResponse::Task {
                tasks: vec![task],
                input_path: String::new(),
                payload: serde_json::to_value(&JobSpec::ManhattanAggregate(aggregate_spec)).unwrap_or_default(),
                total_tasks: 1,
                filters: Vec::new(),
                intervals: Vec::new(),
            })
        }

        ManhattanPhase::Complete => {
            axum::Json(WorkResponse::Exit)
        }
    }
}

/// Handle completion for Manhattan pipeline jobs.
fn complete_manhattan_work(
    manhattan: &mut ManhattanPipelineState,
    req: &CompleteRequest,
    last_progress_time: &mut Instant,
) {
    *last_progress_time = Instant::now();

    // Extract partition indices from task IDs
    let partitions: Vec<usize> = req
        .tasks
        .iter()
        .filter_map(|t| t.parse::<usize>().ok())
        .collect();

    match manhattan.phase {
        ManhattanPhase::Scan => {
            // Try to find partitions in exome or genome processing maps
            for &part_id in &partitions {
                if manhattan.exome_processing.remove(&part_id).is_some() {
                    manhattan.exome_completed.insert(part_id);
                    println!(
                        "Exome partition {} complete ({}/{} exome done)",
                        part_id,
                        manhattan.exome_completed.len(),
                        manhattan.exome_total_tasks
                    );
                } else if manhattan.genome_processing.remove(&part_id).is_some() {
                    manhattan.genome_completed.insert(part_id);
                    println!(
                        "Genome partition {} complete ({}/{} genome done)",
                        part_id,
                        manhattan.genome_completed.len(),
                        manhattan.genome_total_tasks
                    );
                } else {
                    // Partition wasn't in processing (maybe timed out and reassigned)
                    println!(
                        "Warning: partition {} completed but wasn't in processing map",
                        part_id
                    );
                }
            }

            // Check if scan phase is complete
            let exome_done = manhattan.exome_completed.len() == manhattan.exome_total_tasks;
            let genome_done = manhattan.genome_completed.len() == manhattan.genome_total_tasks;
            let exome_idle = manhattan.exome_pending.is_empty() && manhattan.exome_processing.is_empty();
            let genome_idle = manhattan.genome_pending.is_empty() && manhattan.genome_processing.is_empty();

            if (exome_done || manhattan.exome_total_tasks == 0)
                && (genome_done || manhattan.genome_total_tasks == 0)
                && exome_idle
                && genome_idle
            {
                if manhattan.mode == crate::distributed::message::ExecutionMode::ScanOnly {
                    println!("Manhattan scan phase complete (ScanOnly mode) - job finished!");
                    manhattan.phase = ManhattanPhase::Complete;
                } else {
                    println!(
                        "Manhattan scan phase complete: {} exome, {} genome partitions done. Transitioning to Aggregate phase",
                        manhattan.exome_completed.len(),
                        manhattan.genome_completed.len()
                    );
                    manhattan.phase = ManhattanPhase::Aggregate;
                }
            }
        }

        ManhattanPhase::Aggregate => {
            // Aggregate task completed
            manhattan.aggregate_complete = true;
            manhattan.phase = ManhattanPhase::Complete;
            println!("Manhattan aggregate phase complete - job finished!");
        }

        ManhattanPhase::Complete => {
            // Already complete, nothing to do
        }
    }
}

/// Handle completion for batch Manhattan jobs.
/// Complete an ingestion task.
fn complete_ingestion_work(ingestion: &mut IngestionState, req: &CompleteRequest) {
    // Extract task ID from tasks list
    let task_id = req.tasks.first().cloned().unwrap_or_default();

    // Remove from active tasks
    if let Some((phenotype_id, ancestry, _base_path, _worker_id, start_time)) =
        ingestion.active_tasks.remove(&task_id)
    {
        let duration = start_time.elapsed();
        ingestion.completed_count += 1;

        println!(
            "Ingestion complete: {}/{} ({} rows in {:.1}s) [{}/{}]",
            phenotype_id,
            ancestry,
            req.items_processed,
            duration.as_secs_f64(),
            ingestion.completed_count,
            ingestion.total_tasks
        );
    } else {
        // Task not found - might have already been handled
        println!(
            "Warning: Ingestion task {} not found in active_tasks",
            task_id
        );
        ingestion.completed_count += 1;
    }
}

///
/// Uses task_id to lookup the active task and update the appropriate state.
fn complete_batch_work(
    data: &mut CoordinatorData,
    batch: &mut BatchState,
    req: &CompleteRequest,
) {
    data.last_progress_time = Instant::now();

    // Extract task ID from tasks list
    let task_id = req.tasks.first().cloned().unwrap_or_default();

    // Extract partition indices from task IDs
    let partitions: Vec<usize> = req
        .tasks
        .iter()
        .filter_map(|t| t.parse::<usize>().ok())
        .collect();

    // Look up the task by task_id
    let task = match data.active_tasks.remove(&task_id) {
        Some(task) => task,
        None => {
            // Task not found - might be a duplicate completion or old task
            println!(
                "Warning: task {} not found in active_tasks (completion from {})",
                task_id, req.worker_id
            );
            return;
        }
    };

    let now_ms = CoordinatorData::now_ms();

    match task {
        ActiveTask::Scan { phenotype_id, partition_id: _, source, started_at_ms } => {
            // Track CPU time for this scan task
            let duration_secs = (now_ms.saturating_sub(started_at_ms)) as f64 / 1000.0;
            data.scan_cpu_secs += duration_secs;

            // Find the phenotype's pipeline state
            let state = match batch.active_phenotypes.get_mut(&phenotype_id) {
                Some(state) => state,
                None => {
                    println!(
                        "Warning: phenotype {} not found in active_phenotypes for scan completion",
                        phenotype_id
                    );
                    return;
                }
            };

            // Mark partitions as complete
            for &part_id in &partitions {
                match source {
                    ManhattanSource::Exome => {
                        if state.exome_processing.remove(&part_id).is_some() {
                            state.exome_completed.insert(part_id);
                        }
                    }
                    ManhattanSource::Genome => {
                        if state.genome_processing.remove(&part_id).is_some() {
                            state.genome_completed.insert(part_id);
                        }
                    }
                }
            }

            // Update status partitions count
            if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                status.partitions_done = state.exome_completed.len() + state.genome_completed.len();
            }

            // Check if scan phase is complete for this phenotype
            let exome_done = state.exome_completed.len() == state.exome_total_tasks;
            let genome_done = state.genome_completed.len() == state.genome_total_tasks;
            let exome_idle = state.exome_pending.is_empty() && state.exome_processing.is_empty();
            let genome_idle = state.genome_pending.is_empty() && state.genome_processing.is_empty();

            if (exome_done || state.exome_total_tasks == 0)
                && (genome_done || state.genome_total_tasks == 0)
                && exome_idle
                && genome_idle
            {
                if batch.mode == crate::distributed::message::ExecutionMode::ScanOnly {
                    println!("Phenotype {} scan complete (ScanOnly mode), marking as fully complete", phenotype_id);
                    batch.completed_count += 1;
                    if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                        status.stage = "completed".to_string();
                    }
                    batch.active_phenotypes.remove(&phenotype_id);
                } else {
                    println!(
                        "Phenotype {} scan complete, moving to aggregate queue",
                        phenotype_id
                    );

                    // Build aggregate spec and move to ready_to_aggregate
                    let original = &state.original_spec;
                    let aggregate_spec = ManhattanAggregateSpec {
                        output_path: original.output_path.clone(),
                        phenotype_id: original.phenotype.clone(),
                        ancestry: original.ancestry.clone(),
                        exome_results: original.exome.clone(),
                        genome_results: original.genome.clone(),
                        gene_burden: original.gene_burden.clone(),
                        exome_exp_p: original.exome_exp_p.clone(),
                        genome_exp_p: original.genome_exp_p.clone(),
                        exome_annotations: original.exome_annotations.clone(),
                        genome_annotations: original.genome_annotations.clone(),
                        genes: original.genes.clone(),
                        threshold: original.threshold,
                        gene_threshold: original.gene_threshold,
                        locus_threshold: original.locus_threshold,
                        locus_window: original.locus_window,
                        locus_plots: original.locus_plots,
                        min_variants_per_locus: original.min_variants_per_locus,
                        width: original.width,
                        height: original.height,
                        layout: state.layout.clone().unwrap_or_default(),
                        y_scale: state.y_scale.clone().unwrap_or_default(),
                        cleanup: false,
                        styling: original.styling.clone(),
                    };

                    // Store spec for potential retries
                    batch.aggregate_specs.insert(phenotype_id.clone(), aggregate_spec.clone());
                    batch.ready_to_aggregate.push((phenotype_id.clone(), aggregate_spec));

                    // Remove from active phenotypes
                    batch.active_phenotypes.remove(&phenotype_id);
                }
            } else {
                // Log progress
                let source_name = match source {
                    ManhattanSource::Exome => "exome",
                    ManhattanSource::Genome => "genome",
                };
                let (done, total) = match source {
                    ManhattanSource::Exome => (state.exome_completed.len(), state.exome_total_tasks),
                    ManhattanSource::Genome => (state.genome_completed.len(), state.genome_total_tasks),
                };
                println!(
                    "Phenotype {} {} progress: {}/{} partitions",
                    phenotype_id, source_name, done, total
                );
            }
        }

        ActiveTask::AggregateBatch { phenotype_ids, started_at_ms } => {
            // Track CPU time for this aggregate task
            let duration_secs = (now_ms.saturating_sub(started_at_ms)) as f64 / 1000.0;
            data.aggregate_cpu_secs += duration_secs;

            // Extract individual summaries if available
            let results_map: HashMap<String, serde_json::Value> =
                if let Some(ref json) = req.result_json {
                    if let Some(results_array) = json.get("batch_results").and_then(|v| v.as_array())
                    {
                        // Results array corresponds to phenotype_ids order
                        if results_array.len() == phenotype_ids.len() {
                            phenotype_ids
                                .iter()
                                .zip(results_array.iter())
                                .map(|(id, res)| (id.clone(), res.clone()))
                                .collect()
                        } else {
                            HashMap::new()
                        }
                    } else {
                        HashMap::new()
                    }
                } else {
                    HashMap::new()
                };

            // Aggregate batch completed - mark all phenotypes as done
            for phenotype_id in phenotype_ids {
                batch.completed_count += 1;

                // Calculate duration from start time
                let duration_secs = batch
                    .phenotype_start_times
                    .get(&phenotype_id)
                    .map(|start| start.elapsed().as_secs_f64());

                // Get accumulated CPU core-seconds
                let cpu_core_secs = batch.phenotype_cpu_secs.get(&phenotype_id).copied();

                // Update status
                if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                    status.stage = "completed".to_string();
                    status.duration_secs = duration_secs;
                    status.cpu_core_secs = cpu_core_secs;
                    if let Some(res) = results_map.get(&phenotype_id) {
                        status.result = Some(res.clone());
                    }
                }

                // Clean up tracking data
                batch.phenotype_start_times.remove(&phenotype_id);
                batch.phenotype_cpu_secs.remove(&phenotype_id);
                batch.aggregate_specs.remove(&phenotype_id);
                batch.aggregate_retry_counts.remove(&phenotype_id);

                let duration_str = duration_secs
                    .map(|d| format!("{:.1}s", d))
                    .unwrap_or_else(|| "--".to_string());
                println!(
                    "Phenotype {} complete ({}/{}) [{}]",
                    phenotype_id, batch.completed_count, batch.total_phenotypes, duration_str
                );
            }
        }
    }
}

/// Handler for POST /complete - worker reports completion.
async fn complete_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<CompleteRequest>,
) -> axum::Json<CompleteResponse> {
    let mut data = state.lock().unwrap();
    let now_ms = CoordinatorData::now_ms();

    // Extract config values before borrowing worker_registry mutably
    let config_batch_size = data.config.batch_size;
    let job_spec_ref = data.config.job_spec.clone();
    let memory_weight_mb = data.config.memory_weight_mb;

    // Clear the current_task from the worker and capture it for duration tracking
    // Also extract hardware info for AIMD calculation
    let (completed_task, worker_hardware) = if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
        (w.current_task.take(), w.hardware.clone())
    } else {
        (None, None)
    };

    // Extract task IDs and partition indices from request
    let task_ids = &req.tasks;
    let task_id = task_ids.first().cloned().unwrap_or_default();
    // Extract partition indices from task IDs. Task IDs may be:
    // - Raw numbers: "0", "1", "2" (legacy format)
    // - Prefixed: "stress_0", "partition_1", etc. (new TaskDescriptor format)
    let partitions: Vec<usize> = task_ids
        .iter()
        .filter_map(|t| {
            // First try direct parse (legacy format)
            t.parse::<usize>().ok().or_else(|| {
                // Try extracting number after underscore (e.g., "stress_0" -> 0)
                t.rsplit('_').next().and_then(|s| s.parse::<usize>().ok())
            })
        })
        .collect();

    // AIMD Batch Size Adjustment
    if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
        let max_batch = determine_batch_size(config_batch_size, worker_hardware.as_ref(), &job_spec_ref, memory_weight_mb);
        // Start conservative if we don't have a baseline yet
        let current_batch = w.current_batch_size.unwrap_or((max_batch / 10).max(2).min(max_batch));

        if req.error.is_some() {
            // Multiplicative Decrease: halve the batch size on failure/timeout
            w.current_batch_size = Some((current_batch / 2).max(1));
        } else if let Some(task) = &completed_task {
            let duration_secs = (now_ms.saturating_sub(task.started_at_ms)) as f64 / 1000.0;
            let num_tasks = task_ids.len() as f64;

            if num_tasks > 0.0 && duration_secs > 0.0 {
                let time_per_task = duration_secs / num_tasks;
                // Target an ideal turnaround of 60 seconds
                let target_batch = (60.0 / time_per_task).round() as usize;
                let clamped_target = target_batch.clamp(1, max_batch);

                let next_batch = if clamped_target > current_batch {
                    // Additive Increase / Slow Start: grow safely up to the optimal target
                    let growth = (current_batch / 4).max(2);
                    (current_batch + growth).min(clamped_target)
                } else {
                    // Multiplicative Decrease (soft): instantly drop to the optimal target if taking too long
                    clamped_target
                };
                w.current_batch_size = Some(next_batch);
            }
        }
    }

    // Check if this is a failure report
    if let Some(ref error) = req.error {
        // Look up task BEFORE removing (need to know what type of task failed and when it started)
        let failed_task = data.active_tasks.remove(&task_id);

        // Calculate wasted time based on when the task started
        let wasted_duration_ms = match &failed_task {
            Some(ActiveTask::Scan { started_at_ms, .. }) => now_ms.saturating_sub(*started_at_ms),
            Some(ActiveTask::AggregateBatch { started_at_ms, .. }) => now_ms.saturating_sub(*started_at_ms),
            None => 0,
        };

        // Track wasted CPU time
        data.wasted_cpu_secs += (wasted_duration_ms as f64) / 1000.0;

        // Log the error prominently
        println!(
            "ERROR from worker {}: tasks {:?} failed: {} (wasted {:.1}s)",
            req.worker_id, task_ids, error, wasted_duration_ms as f64 / 1000.0
        );

        // Store the error for dashboard display
        data.last_error = Some(format!(
            "Worker {} failed on tasks {:?}: {}",
            req.worker_id, task_ids, error
        ));

        // Log failure to ring buffer (with wasted duration)
        data.log_failure(FailureRecord {
            timestamp_ms: now_ms,
            phenotype_id: None,
            tasks: task_ids.clone(),
            worker_id: req.worker_id.clone(),
            error: error.clone(),
            retry_count: 0,
            wasted_duration_ms,
        });

        // Log event
        data.log_event(JobEvent {
            timestamp_ms: now_ms,
            event_type: "failed".to_string(),
            worker_id: Some(req.worker_id.clone()),
            phenotype_id: None,
            details: format!("Failed tasks {:?}: {} (wasted {:.1}s)", task_ids, error, wasted_duration_ms as f64 / 1000.0),
        });

        // Remove from processing and retry or mark as failed (for non-batch jobs)
        for &part_id in &partitions {
            data.processing_partitions.remove(&part_id);

            // Use same retry logic as timeouts
            let retries = data.retry_counts.entry(part_id).or_insert(0);
            *retries += 1;

            if *retries > 3 {
                println!(
                    "Partition {} exceeded max retries ({}), marking as permanently failed",
                    part_id, retries
                );
                data.failed_partitions.insert(part_id);
            } else {
                println!(
                    "Re-queuing partition {} for retry ({}/3)",
                    part_id, retries
                );
                data.pending_partitions.push_front(part_id);
            }
        }

        // For batch jobs, handle retry logic based on task type
        if let Some(ref mut batch) = data.batch_state {
            match failed_task {
                Some(ActiveTask::AggregateBatch { phenotype_ids, .. }) => {
                    // Re-queue aggregate tasks for retry
                    for phenotype_id in phenotype_ids {
                        let retries = batch.aggregate_retry_counts.entry(phenotype_id.clone()).or_insert(0);
                        *retries += 1;

                        if *retries > MAX_AGGREGATE_RETRIES {
                            println!(
                                "Phenotype {} exceeded max aggregate retries ({}), marking as failed",
                                phenotype_id, MAX_AGGREGATE_RETRIES
                            );
                            batch.failed_count += 1;

                            // Write error.json to the output path before removing the spec
                            if let Some(spec) = batch.aggregate_specs.remove(&phenotype_id) {
                                let err_path = format!("{}/error.json", spec.output_path.trim_end_matches('/'));
                                let timestamp_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);
                                let err_json = serde_json::json!({
                                    "phenotype": phenotype_id,
                                    "status": "MANHATTAN_FAILED",
                                    "error": format!("Exceeded max aggregate retries ({})", MAX_AGGREGATE_RETRIES),
                                    "timestamp_ms": timestamp_ms
                                });

                                // Write error.json in a background thread to avoid blocking
                                std::thread::spawn(move || {
                                    if genohype_core::io::is_cloud_path(&err_path) {
                                        use genohype_core::io::CloudWriter;
                                        use std::io::Write;
                                        if let Ok(mut writer) = CloudWriter::new(&err_path) {
                                            let _ = writer.write_all(err_json.to_string().as_bytes());
                                            let _ = writer.finish();
                                        }
                                    } else {
                                        let _ = std::fs::write(&err_path, err_json.to_string());
                                    }
                                });
                            }
                        } else if let Some(spec) = batch.aggregate_specs.get(&phenotype_id).cloned() {
                            println!(
                                "Re-queuing phenotype {} for aggregate retry ({}/{})",
                                phenotype_id, retries, MAX_AGGREGATE_RETRIES
                            );
                            batch.ready_to_aggregate.push((phenotype_id, spec));
                        } else {
                            // No spec to retry with - this shouldn't happen but handle gracefully
                            println!(
                                "Warning: No aggregate spec for {} to retry, marking as failed",
                                phenotype_id
                            );
                            batch.failed_count += 1;
                        }
                    }
                }
                Some(ActiveTask::Scan { phenotype_id, source, .. }) => {
                    // Re-queue scan partitions back to the phenotype
                    if let Some(state) = batch.active_phenotypes.get_mut(&phenotype_id) {
                        // Put partitions back in pending
                        for &part_id in &partitions {
                            match source {
                                ManhattanSource::Exome => {
                                    state.exome_processing.remove(&part_id);
                                    state.exome_pending.push_back(part_id);
                                }
                                ManhattanSource::Genome => {
                                    state.genome_processing.remove(&part_id);
                                    state.genome_pending.push_back(part_id);
                                }
                            }
                        }
                        println!(
                            "Re-queued {} scan tasks for phenotype {} (source: {:?})",
                            partitions.len(), phenotype_id, source
                        );
                    } else {
                        println!(
                            "Warning: Phenotype {} not found for scan retry, tasks lost",
                            phenotype_id
                        );
                    }
                }
                None => {
                    // Task not found - might have already been handled or was a legacy task
                    println!("Warning: Failed task {} not found in active_tasks", task_id);
                }
            }
        }

        // For Manhattan jobs, also update the manhattan state
        if let Some(ref mut manhattan) = data.manhattan_state {
            for &part_id in &partitions {
                manhattan.exome_processing.remove(&part_id);
                manhattan.genome_processing.remove(&part_id);
                // If this was the aggregate task, mark it failed
                if manhattan.aggregate_dispatched && !manhattan.aggregate_complete {
                    println!("Aggregate task failed - job cannot complete without fixing the error");
                }
            }
        }

        // For ingestion jobs, mark task as failed
        if let Some(ref mut ingestion) = data.ingestion_state {
            if let Some((phenotype_id, ancestry, _base_path, _worker_id, _start_time)) =
                ingestion.active_tasks.remove(&task_id)
            {
                println!(
                    "Ingestion failed: {}/{} - {}",
                    phenotype_id, ancestry, req.error.as_ref().unwrap()
                );
                ingestion.failed_count += 1;
            }
        }

        return axum::Json(CompleteResponse { acknowledged: true });
    }

    // Check if this is an ingestion job
    if data.ingestion_state.is_some() {
        let mut ingestion = data.ingestion_state.take().unwrap();
        complete_ingestion_work(&mut ingestion, &req);
        data.ingestion_state = Some(ingestion);
    } else if data.batch_state.is_some() {
        // Check if this is a batch Manhattan job
        let mut batch = data.batch_state.take().unwrap();
        complete_batch_work(&mut data, &mut batch, &req);
        data.batch_state = Some(batch);
    } else if data.manhattan_state.is_some() {
        // Check if this is a single Manhattan pipeline job
        let mut manhattan = data.manhattan_state.take().unwrap();
        complete_manhattan_work(&mut manhattan, &req, &mut data.last_progress_time);
        data.manhattan_state = Some(manhattan);
    } else {
        // Standard job completion
        for &part_id in &partitions {
            if data.processing_partitions.remove(&part_id).is_some() {
                data.completed_tasks.insert(part_id);
                // Update progress timestamp (R3)
                data.last_progress_time = Instant::now();
            } else {
                // Partition wasn't in processing (maybe timed out and reassigned)
                // Still mark as complete if not already
                if !data.completed_tasks.contains(&part_id) {
                    println!(
                        "Warning: task {} completed by {} but wasn't in processing map",
                        part_id, req.worker_id
                    );
                    data.completed_tasks.insert(part_id);
                }
            }
        }
    }

    data.total_rows += req.items_processed;

    // Store result_json if present (for Summary/Validate jobs)
    if let Some(result) = req.result_json.clone() {
        data.aggregated_results.push(result);
    }

    // Update per-worker stats
    touch_worker(&mut data, &req.worker_id, None);
    if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
        w.total_rows += req.items_processed;
        w.partitions_completed += task_ids.len();
    }

    // Log completion event (only for successful completions, not errors)
    if req.error.is_none() {
        data.log_event(JobEvent {
            timestamp_ms: now_ms,
            event_type: "completed".to_string(),
            worker_id: Some(req.worker_id.clone()),
            phenotype_id: None,
            details: format!("Completed tasks {:?} ({} rows)", task_ids, req.items_processed),
        });
    }

    let total = data.config.total_tasks;
    let done = data.completed_tasks.len();

    // Log progress periodically
    if done % 10 == 0 || done == total {
        println!(
            "Progress: {}/{} partitions ({:.1}%), {} total rows",
            done,
            total,
            (done as f64 / total as f64) * 100.0,
            data.total_rows
        );
    }

    axum::Json(CompleteResponse { acknowledged: true })
}

/// Handler for GET /status - query job status.
async fn get_status(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<StatusResponse> {
    let data = state.lock().unwrap();

    // Check for batch Manhattan job
    let (pending, processing, completed, total, is_complete) = if let Some(ref batch) = data.batch_state {
        let total = batch.total_phenotypes;
        let completed = batch.completed_count;
        let pending = batch.pending_queue.len();
        let processing = batch.active_phenotypes.len() + batch.ready_to_aggregate.len();
        let is_complete = batch.pending_queue.is_empty()
            && batch.active_phenotypes.is_empty()
            && batch.ready_to_aggregate.is_empty()
            && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;
        (pending, processing, completed, total, is_complete)
    } else if let Some(ref m) = data.manhattan_state {
        // Check if this is a single Manhattan pipeline job
        let total_parts = m.exome_total_tasks + m.genome_total_tasks;
        let completed_parts = m.exome_completed.len() + m.genome_completed.len();
        let processing_parts = m.exome_processing.len() + m.genome_processing.len();
        let pending_parts = m.exome_pending.len() + m.genome_pending.len();

        // Add aggregate phase (+1 task)
        let total = total_parts + 1;
        let completed = completed_parts + if m.aggregate_complete { 1 } else { 0 };
        let processing = processing_parts + if m.aggregate_dispatched && !m.aggregate_complete { 1 } else { 0 };
        let pending = pending_parts + if !m.aggregate_dispatched && m.phase == ManhattanPhase::Aggregate { 1 } else { 0 };
        let is_complete = m.phase == ManhattanPhase::Complete;

        (pending, processing, completed, total, is_complete)
    } else {
        let failed = data.failed_partitions.len();
        let completed = data.completed_tasks.len();
        let is_complete = (completed + failed) == data.config.total_tasks;
        (
            data.pending_partitions.len(),
            data.processing_partitions.len(),
            completed,
            data.config.total_tasks,
            is_complete,
        )
    };

    let failed = data.failed_partitions.len();

    axum::Json(StatusResponse {
        pending_tasks: pending,
        processing_tasks: processing,
        completed_tasks: completed,
        total_tasks: total,
        total_items: data.total_rows,
        failed_tasks: failed,
        is_complete,
    })
}

/// Check worker liveness based on heartbeat timestamps.
fn check_worker_liveness(state: &SharedState) {
    let mut data = state.lock().unwrap();
    let now = Instant::now();
    let timeout = Duration::from_secs(WORKER_SUSPECT_TIMEOUT_SECS);

    for (worker_id, worker) in data.worker_registry.iter_mut() {
        if now.duration_since(worker.last_seen) > timeout
            && worker.status != WorkerStatus::SuspectedDead
        {
            println!("Worker {} not seen for >{}s, marking as suspected dead", worker_id, WORKER_SUSPECT_TIMEOUT_SECS);
            worker.status = WorkerStatus::SuspectedDead;
        }
    }
}

/// Handler for POST /heartbeat - worker sends telemetry.
async fn handle_heartbeat(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(mut req): axum::Json<HeartbeatRequest>,
) -> axum::Json<HeartbeatResponse> {
    let mut data = state.lock().unwrap();
    touch_worker(&mut data, &req.worker_id, None);

    // Extract config value before mutable borrow of worker_registry
    let default_batch_size = data.config.batch_size;

    // Track if we need to log an event (collected outside the mutable borrow)
    let mut batch_reduction_event: Option<(String, usize, f64)> = None;

    if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
        // Revive if previously suspected dead
        if w.status == WorkerStatus::SuspectedDead {
            w.status = WorkerStatus::Active;
        }

        // Phase 3: Memory-based batch reduction heuristic
        // If memory usage exceeds 85%, aggressively slash batch size to prevent OOM
        if let (Some(used), Some(total)) = (req.telemetry.memory_used_bytes, req.telemetry.memory_total_bytes) {
            if total > 0 {
                let mem_usage_pct = (used as f64 / total as f64) * 100.0;
                if mem_usage_pct > 85.0 {
                    // Aggressively slash batch size to prevent OOM
                    let current_batch = w.current_batch_size.unwrap_or(default_batch_size);
                    let new_batch = (current_batch / 2).max(1);
                    if new_batch < current_batch {
                        println!(
                            "Worker {} memory usage at {:.1}%. Reducing batch size from {} to {}",
                            req.worker_id, mem_usage_pct, current_batch, new_batch
                        );

                        // Collect event info to log after releasing worker borrow
                        batch_reduction_event = Some((req.worker_id.clone(), new_batch, mem_usage_pct));

                        w.current_batch_size = Some(new_batch);
                    }
                }
            }
        }

        // Inject the current batch size into telemetry before persistence
        req.telemetry.current_batch_size = w.current_batch_size;

        // Store telemetry snapshot in memory (for quick access to latest)
        w.metrics_history.push_back(req.telemetry.clone());
        if w.metrics_history.len() > MAX_METRICS_HISTORY {
            w.metrics_history.pop_front();
        }
    }

    // Log batch reduction event for the UI (outside the worker borrow)
    if let Some((worker_id, new_batch, mem_pct)) = batch_reduction_event {
        data.log_event(JobEvent {
            timestamp_ms: CoordinatorData::now_ms(),
            event_type: "warning".to_string(),
            worker_id: Some(worker_id),
            phenotype_id: None,
            details: format!("Reduced batch size to {} (Memory at {:.1}%)", new_batch, mem_pct),
        });
    }

    // Persist to SQLite (fire-and-forget, don't block on DB errors)
    if let Err(e) = data.metrics_db.insert_snapshot(&req.worker_id, &req.telemetry) {
        eprintln!("Warning: failed to persist metrics to DB: {}", e);
    }

    axum::Json(HeartbeatResponse { acknowledged: true })
}

/// Handler for POST /api/job - submit a job to an idle coordinator.
async fn submit_job(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<JobConfigRequest>,
) -> axum::Json<JobConfigResponse> {
    // Handle ExportClickhouse table creation BEFORE acquiring lock
    // This avoids holding MutexGuard across await points
    #[cfg(feature = "clickhouse")]
    if let JobSpec::ExportClickhouse { ref clickhouse_url, ref table_name } = req.job_spec {
        use crate::export::{ClickHouseClient, generate_create_table};
        use genohype_core::query::QueryEngine;

        println!("  Creating ClickHouse table '{}' before dispatching...", table_name);

        let input_path = req.input_path.clone();
        let table_name_clone = table_name.clone();
        let clickhouse_url_clone = clickhouse_url.clone();

        // Run blocking I/O operations in spawn_blocking to avoid
        // "cannot start a runtime from within a runtime" panic.
        // QueryEngine::open_path() uses IO_RUNTIME.block_on() internally.
        let result = tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
            let engine = QueryEngine::open_path(&input_path)
                .map_err(|e| format!("Failed to open input table for schema: {}", e))?;
            let schema = engine.row_type().clone();

            let create_sql = generate_create_table(&table_name_clone, &schema, &[])
                .map_err(|e| format!("Failed to generate DDL: {}", e))?;

            let client = ClickHouseClient::new(&clickhouse_url_clone);
            client.execute(&create_sql)
                .map_err(|e| format!("Failed to create table {}: {}", table_name_clone, e))?;

            println!("    Created table: {}", table_name_clone);
            Ok(())
        }).await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return axum::Json(JobConfigResponse {
                    acknowledged: false,
                    error: Some(e),
                });
            }
            Err(e) => {
                return axum::Json(JobConfigResponse {
                    acknowledged: false,
                    error: Some(format!("Task panicked: {}", e)),
                });
            }
        }
    }

    let mut data = state.lock().unwrap();

    // R1: Check if workers are available
    let active_workers = data
        .worker_registry
        .values()
        .filter(|w| w.status != WorkerStatus::SuspectedDead)
        .count();

    // Allow if we have workers OR if force is used (force bypasses worker check too for testing)
    if active_workers == 0 && !req.force {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some(
                "No active workers connected. Scale up workers first or use --force.".to_string(),
            ),
        });
    }

    // R4: Handle running jobs
    if !data.idle {
        if !req.force {
            return axum::Json(JobConfigResponse {
                acknowledged: false,
                error: Some(
                    "Coordinator already has a job running. Use --force to supersede.".to_string(),
                ),
            });
        }
        println!("Superseding running job (--force)...");
        // Clear existing state will happen below
    }

    // Validate request
    // ManhattanBatch and IngestManhattan jobs don't use total_tasks (they manage tasks internally)
    let is_batch_job = matches!(&req.job_spec, JobSpec::ManhattanBatch { .. });
    let is_ingest_job = matches!(&req.job_spec, JobSpec::IngestManhattan { .. });
    if req.total_tasks == 0 && !is_batch_job && !is_ingest_job {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("total_tasks must be greater than 0".to_string()),
        });
    }

    // ManhattanBatch, Manhattan, IngestManhattan, and Stress jobs don't require input_path (they use per-spec paths)
    let needs_input_path = !is_batch_job && !is_ingest_job && !matches!(&req.job_spec, JobSpec::Manhattan { .. }) && !matches!(&req.job_spec, JobSpec::Stress(_));
    if req.input_path.is_empty() && needs_input_path {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("input_path is required".to_string()),
        });
    }

    // Configure the job
    data.config.input_path = req.input_path.clone();
    data.config.job_spec = Some(req.job_spec.clone());
    data.config.total_tasks = req.total_tasks;
    data.config.filters = req.filters.clone();
    data.config.intervals = req.intervals.clone();
    data.config.memory_weight_mb = req.memory_weight_mb;
    if let Some(batch_size) = req.batch_size {
        data.config.batch_size = batch_size;
    }

    // Fill pending queue with partition indices
    data.pending_partitions = (0..req.total_tasks).collect();

    // Reset job state
    data.completed_tasks.clear();
    data.processing_partitions.clear();
    data.failed_partitions.clear();
    data.retry_counts.clear();
    data.total_rows = 0;
    data.job_start_time = Instant::now();
    data.last_progress_time = Instant::now();
    data.aggregated_results.clear();
    data.manhattan_state = None;
    data.batch_state = None;
    data.active_tasks.clear();

    // Don't clear worker registry on resubmission/superseding, as we want to keep connected workers
    // Only clear if this is a fresh start and we want to purge stale workers
    // data.worker_registry.clear();

    // Clear metrics database for new job
    if let Err(e) = data.metrics_db.clear() {
        eprintln!("Warning: failed to clear metrics DB: {}", e);
    }

    // Mark as no longer idle
    data.idle = false;

    // Handle ManhattanBatch jobs (batch scheduling mode)
    if let JobSpec::ManhattanBatch { ref specs, mode } = req.job_spec {
        let total_phenotypes = specs.len();

        // Clear standard partition tracking - batch mode uses its own
        data.pending_partitions.clear();

        println!(
            "Initializing Manhattan batch: {} phenotypes (lazy loading, max {} active, mode={:?})",
            total_phenotypes,
            BATCH_ACTIVE_LIMIT,
            mode
        );

        // Initialize status map for all phenotypes
        let mut phenotype_statuses = HashMap::new();
        for spec in specs {
            let id = spec.output_path.clone();
            phenotype_statuses.insert(
                id.clone(),
                PhenotypeStatus {
                    id,
                    stage: if mode == crate::distributed::message::ExecutionMode::AggregateOnly { "aggregating".to_string() } else { "queued".to_string() },
                    partitions_done: 0,
                    partitions_total: 0, // Updated when activated
                    result: None,
                    error: None,
                    duration_secs: None,
                    cpu_core_secs: None,
                },
            );
        }

        let mut batch_state = BatchState {
            mode,
            phenotype_statuses,
            phenotype_start_times: HashMap::new(),
            phenotype_cpu_secs: HashMap::new(),
            pending_queue: VecDeque::new(),
            active_phenotypes: HashMap::new(),
            ready_to_aggregate: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            total_phenotypes,
            aggregate_retry_counts: HashMap::new(),
            aggregate_specs: HashMap::new(),
        };

        if mode == crate::distributed::message::ExecutionMode::AggregateOnly {
            for spec in specs {
                let id = spec.output_path.clone();
                let aggregate_spec = ManhattanAggregateSpec {
                    output_path: spec.output_path.clone(),
                    phenotype_id: spec.phenotype.clone(),
                    ancestry: spec.ancestry.clone(),
                    exome_results: spec.exome.clone(),
                    genome_results: spec.genome.clone(),
                    gene_burden: spec.gene_burden.clone(),
                    exome_exp_p: spec.exome_exp_p.clone(),
                    genome_exp_p: spec.genome_exp_p.clone(),
                    exome_annotations: spec.exome_annotations.clone(),
                    genome_annotations: spec.genome_annotations.clone(),
                    genes: spec.genes.clone(),
                    threshold: spec.threshold,
                    gene_threshold: spec.gene_threshold,
                    locus_threshold: spec.locus_threshold,
                    locus_window: spec.locus_window,
                    locus_plots: spec.locus_plots,
                    min_variants_per_locus: spec.min_variants_per_locus,
                    width: spec.width,
                    height: spec.height,
                    layout: spec.layout.clone().unwrap_or_default(),
                    y_scale: spec.y_scale.clone().unwrap_or_default(),
                    cleanup: false,
                    styling: spec.styling.clone(),
                };
                batch_state.aggregate_specs.insert(id.clone(), aggregate_spec.clone());
                batch_state.ready_to_aggregate.push((id, aggregate_spec));
            }
        } else {
            batch_state.pending_queue = specs.iter().cloned().collect();
        }

        data.batch_state = Some(batch_state);

        let output_desc = specs.first()
            .map(|s| s.output_path.clone())
            .unwrap_or_else(|| "(no output)".to_string());
        println!(
            "Job submitted: {} ({} phenotypes, output={})",
            req.job_spec.description(),
            total_phenotypes,
            output_desc
        );

        return axum::Json(JobConfigResponse {
            acknowledged: true,
            error: None,
        });
    }

    // Handle IngestManhattan jobs (discover phenotypes and queue ingestion tasks)
    if let JobSpec::IngestManhattan { ref input_dir, ref clickhouse_url, ref database, ref init_strategy } = req.job_spec {
        // Clear standard partition tracking - ingestion uses its own state
        data.pending_partitions.clear();

        println!(
            "Initializing Manhattan ingestion from {} to ClickHouse {} (init_strategy: {:?})",
            input_dir, clickhouse_url, init_strategy
        );

        // Execute DDL based on init_strategy (before dispatching tasks)
        #[cfg(feature = "clickhouse")]
        {
            use crate::ingest::get_manhattan_schemas;
            use crate::export::ClickHouseClient;
            use super::message::InitStrategy;

            if *init_strategy != InitStrategy::Append {
                println!("  Initializing tables...");
                let client = ClickHouseClient::new(clickhouse_url);
                let schemas = get_manhattan_schemas();

                for (table_name, create_sql) in schemas {
                    // If replace strategy, drop the table first
                    if *init_strategy == InitStrategy::Replace {
                        let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
                        match client.execute(&drop_sql) {
                            Ok(_) => println!("    Dropped table: {}", table_name),
                            Err(e) => {
                                return axum::Json(JobConfigResponse {
                                    acknowledged: false,
                                    error: Some(format!("Failed to drop table {}: {}", table_name, e)),
                                });
                            }
                        }
                    }

                    // Create the table (for both Create and Replace strategies)
                    match client.execute(create_sql) {
                        Ok(_) => println!("    Created table: {}", table_name),
                        Err(e) => {
                            return axum::Json(JobConfigResponse {
                                acknowledged: false,
                                error: Some(format!("Failed to create table {}: {}", table_name, e)),
                            });
                        }
                    }
                }
                println!("  Table initialization complete.");
            }
        }

        // Discover phenotypes by scanning for manifest.json files
        // Structure: {input_dir}/{ancestry}/{phenotype_id}/manifest.json
        match discover_phenotypes_for_ingestion(input_dir) {
            Ok(phenotypes) => {
                let total = phenotypes.len();
                println!("  Discovered {} phenotypes for ingestion", total);

                data.ingestion_state = Some(IngestionState {
                    pending_tasks: phenotypes.into_iter().collect(),
                    active_tasks: HashMap::new(),
                    clickhouse_url: clickhouse_url.clone(),
                    database: database.clone(),
                    completed_count: 0,
                    failed_count: 0,
                    total_tasks: total,
                });

                return axum::Json(JobConfigResponse {
                    acknowledged: true,
                    error: None,
                });
            }
            Err(e) => {
                return axum::Json(JobConfigResponse {
                    acknowledged: false,
                    error: Some(format!("Failed to discover phenotypes: {}", e)),
                });
            }
        }
    }

    // Initialize Manhattan pipeline state if this is a single Manhattan job
    data.manhattan_state = if let JobSpec::Manhattan { ref spec, mode } = req.job_spec {
        // For Manhattan jobs, we manage exome/genome partitions separately
        // Use partition counts from spec if available, otherwise fall back to total_tasks
        let exome_partitions = spec.exome_partitions.unwrap_or_else(|| {
            if spec.exome.is_some() { req.total_tasks } else { 0 }
        });
        let genome_partitions = spec.genome_partitions.unwrap_or_else(|| {
            if spec.genome.is_some() && spec.exome.is_none() { req.total_tasks } else { 0 }
        });

        // Clear the standard partition tracking since Manhattan uses its own
        data.pending_partitions.clear();

        let initial_phase = if mode == crate::distributed::message::ExecutionMode::AggregateOnly {
            ManhattanPhase::Aggregate
        } else {
            ManhattanPhase::Scan
        };

        println!(
            "Initializing Manhattan pipeline: {} exome partitions, {} genome partitions (mode={:?})",
            exome_partitions, genome_partitions, mode
        );

        Some(ManhattanPipelineState {
            mode,
            phase: initial_phase,
            original_spec: spec.clone(),
            layout: spec.layout.clone(),
            y_scale: spec.y_scale.clone(),
            contig_lengths: spec.contig_lengths.clone().unwrap_or_default(),
            exome_total_tasks: exome_partitions,
            exome_pending: (0..exome_partitions).collect(),
            exome_processing: HashMap::new(),
            exome_completed: HashSet::new(),
            genome_total_tasks: genome_partitions,
            genome_pending: (0..genome_partitions).collect(),
            genome_processing: HashMap::new(),
            genome_completed: HashSet::new(),
            aggregate_dispatched: false,
            aggregate_complete: false,
        })
    } else {
        None
    };

    let output_desc = req
        .job_spec
        .output_path()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "(no output)".to_string());
    println!(
        "Job submitted: {} ({} partitions, input={}, output={})",
        req.job_spec.description(),
        req.total_tasks,
        req.input_path,
        output_desc
    );

    axum::Json(JobConfigResponse {
        acknowledged: true,
        error: None,
    })
}

/// Handler for POST /api/cancel - cancel the running job.
async fn cancel_job(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<CancelRequest>,
) -> axum::Json<CancelResponse> {
    let mut data = state.lock().unwrap();

    if data.idle {
        return axum::Json(CancelResponse {
            success: false,
            message: "No job is currently running".to_string(),
        });
    }

    // Reset job state
    data.pending_partitions.clear();
    data.processing_partitions.clear();
    data.manhattan_state = None;
    data.batch_state = None;
    data.active_tasks.clear();
    data.ingestion_state = None;
    data.idle = true;

    let reason = req.reason.unwrap_or_else(|| "User request".to_string());
    println!("Job cancelled: {}", reason);

    axum::Json(CancelResponse {
        success: true,
        message: format!("Job cancelled: {}", reason),
    })
}

/// Handler for GET /api/result - retrieve aggregated job results.
///
/// For Summary and Validate jobs, this returns the collected results from all workers.
async fn get_job_result(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<JobResultResponse> {
    let data = state.lock().unwrap();

    // Check if the job is complete
    let failed = data.failed_partitions.len();
    let completed = data.completed_tasks.len();
    let total = data.config.total_tasks;
    let is_complete = total > 0 && (completed + failed) == total;

    if data.idle {
        return axum::Json(JobResultResponse {
            available: false,
            result: None,
            error: Some("No job is running".to_string()),
        });
    }

    if !is_complete {
        return axum::Json(JobResultResponse {
            available: false,
            result: None,
            error: Some(format!(
                "Job not complete: {}/{} partitions done",
                completed, total
            )),
        });
    }

    // Return aggregated results as a JSON array
    axum::Json(JobResultResponse {
        available: true,
        result: Some(serde_json::Value::Array(data.aggregated_results.clone())),
        error: None,
    })
}

/// Handler for GET /api/dashboard/summary - overall job summary.
async fn get_dashboard_summary(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardSummary> {
    let data = state.lock().unwrap();

    let elapsed = data.job_start_time.elapsed().as_secs_f64();
    let failed = data.failed_partitions.len();

    // Check for batch Manhattan job (multi-phenotype mode)
    let (completed, processing, pending, total, is_complete) = if let Some(ref batch) = data.batch_state {
        // For batch jobs, track phenotype-level progress
        let total = batch.total_phenotypes;
        let completed = batch.completed_count;

        // Count active phenotypes (currently being scanned)
        let active_count = batch.active_phenotypes.len();

        // Count phenotypes ready to aggregate
        let ready_count = batch.ready_to_aggregate.len();

        // Pending = queued + active + ready (not yet fully complete)
        // Processing = active phenotypes being scanned
        let pending = batch.pending_queue.len();
        let processing = active_count + ready_count;

        let is_complete = batch.pending_queue.is_empty()
            && batch.active_phenotypes.is_empty()
            && batch.ready_to_aggregate.is_empty()
            && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;

        (completed, processing, pending, total, is_complete)
    } else if let Some(ref m) = data.manhattan_state {
        // Check if this is a single Manhattan pipeline job
        let total_parts = m.exome_total_tasks + m.genome_total_tasks;
        let completed_parts = m.exome_completed.len() + m.genome_completed.len();
        let processing_parts = m.exome_processing.len() + m.genome_processing.len();
        let pending_parts = m.exome_pending.len() + m.genome_pending.len();

        // Add aggregate phase (+1 task)
        let total = total_parts + 1;
        let completed = completed_parts + if m.aggregate_complete { 1 } else { 0 };
        let processing = processing_parts + if m.aggregate_dispatched && !m.aggregate_complete { 1 } else { 0 };
        let pending = pending_parts + if !m.aggregate_dispatched && m.phase == ManhattanPhase::Aggregate { 1 } else { 0 };
        let is_complete = m.phase == ManhattanPhase::Complete;

        (completed, processing, pending, total, is_complete)
    } else {
        let completed = data.completed_tasks.len();
        let is_complete = (completed + failed) == data.config.total_tasks;
        (
            completed,
            data.processing_partitions.len(),
            data.pending_partitions.len(),
            data.config.total_tasks,
            is_complete,
        )
    };

    let progress_percent = if total > 0 {
        (completed as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    // Compute cluster-wide items/sec from worker telemetry
    let cluster_items_per_sec: f64 = data
        .worker_registry
        .values()
        .filter(|w| w.status == WorkerStatus::Active)
        .filter_map(|w| w.metrics_history.back())
        .map(|s| s.items_per_sec)
        .sum();

    // ETA based on partition completion rate
    let eta_secs = if completed > 0 && !is_complete {
        let remaining = total - completed - failed;
        let secs_per_partition = elapsed / completed as f64;
        Some(remaining as f64 * secs_per_partition)
    } else {
        None
    };

    // Compute batch progress if in batch mode
    let batch_progress = if let Some(ref batch) = data.batch_state {
        // Count phenotypes in each aggregation state
        let in_aggregate = batch
            .phenotype_statuses
            .values()
            .filter(|s| s.stage == "aggregating")
            .count();

        Some(DashboardBatchProgress {
            total: batch.total_phenotypes,
            queued: batch.pending_queue.len(),
            active_scan: batch.active_phenotypes.len(),
            active_aggregate: in_aggregate,
            completed: batch.completed_count,
            failed: batch.failed_count,
        })
    } else {
        None
    };

    axum::Json(DashboardSummary {
        progress_percent,
        total_tasks: total,
        batch_size: data.config.batch_size,
        completed_tasks: completed,
        processing_tasks: processing,
        pending_tasks: pending,
        failed_tasks: failed,
        total_items: data.total_rows,
        cluster_items_per_sec,
        elapsed_secs: elapsed,
        eta_secs,
        is_complete,
        input_path: data.config.input_path.clone(),
        job_spec: data.config.job_spec.clone(),
        idle: data.idle,
        last_error: data.last_error.clone(),
        batch_progress,
        build_version: Some(env!("GIT_HASH").to_string()),
        scan_cpu_secs: data.scan_cpu_secs,
        aggregate_cpu_secs: data.aggregate_cpu_secs,
        wasted_cpu_secs: data.wasted_cpu_secs,
        db_path: Some(data.config.db_path.clone()),
        backup_path: data.config.backup_path.clone(),
    })
}

/// Handler for GET /api/dashboard/bottlenecks - analyze cluster bottlenecks.
///
/// Aggregates telemetry from active workers to identify the current limiting factor:
/// - CPU: High CPU utilization indicates compute-bound workload (scanning/decoding)
/// - Memory: High memory usage indicates aggregate-phase or high partition counts
/// - Network RX: High download rate indicates GCS fetch bottleneck
/// - Network TX: High upload rate indicates GCS write bottleneck
/// - I/O Wait: Low CPU with active workers indicates waiting on external I/O
async fn get_dashboard_bottlenecks(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardBottleneck> {
    let data = state.lock().unwrap();

    let mut active_count = 0;
    let mut total_cpu = 0.0_f32;
    let mut total_mem_pct = 0.0_f64;
    let mut total_rx = 0.0_f64;
    let mut total_tx = 0.0_f64;
    let mut total_batch_size = 0usize;
    let mut mem_constrained_workers = 0usize;

    for worker in data.worker_registry.values() {
        if worker.status == WorkerStatus::Active {
            if let Some(telemetry) = worker.metrics_history.back() {
                active_count += 1;
                total_cpu += telemetry.cpu_percent.unwrap_or(0.0);

                if let (Some(used), Some(total)) =
                    (telemetry.memory_used_bytes, telemetry.memory_total_bytes)
                {
                    if total > 0 {
                        let pct = (used as f64 / total as f64) * 100.0;
                        total_mem_pct += pct;
                        if pct > 85.0 {
                            mem_constrained_workers += 1;
                        }
                    }
                }
                total_rx += telemetry.network_rx_bytes_sec.unwrap_or(0.0);
                total_tx += telemetry.network_tx_bytes_sec.unwrap_or(0.0);
                total_batch_size += worker.current_batch_size.unwrap_or(data.config.batch_size);
            }
        }
    }

    if active_count == 0 {
        return axum::Json(DashboardBottleneck {
            bottleneck: "Idle".to_string(),
            description: "No active workers available for analysis".to_string(),
            avg_cpu_percent: 0.0,
            avg_mem_percent: 0.0,
            avg_network_rx_mb: 0.0,
            avg_network_tx_mb: 0.0,
        });
    }

    let avg_cpu = total_cpu / active_count as f32;
    let avg_mem = (total_mem_pct / active_count as f64) as f32;
    let avg_rx_mb = (total_rx / active_count as f64) / 1_048_576.0;
    let avg_tx_mb = (total_tx / active_count as f64) / 1_048_576.0;
    let avg_batch = total_batch_size / active_count;

    let (bottleneck, description) = if avg_cpu > 85.0 {
        (
            "CPU".to_string(),
            format!(
                "CPU Bound ({:.1}% utilization) - Scanning phase or heavy decoding. (Avg batch: {})",
                avg_cpu, avg_batch
            ),
        )
    } else if mem_constrained_workers > 0 || avg_mem > 85.0 {
        (
            "Memory".to_string(),
            format!(
                "Memory Bound ({:.1}% utilization) - {} workers throttled to prevent OOM. (Avg batch: {})",
                avg_mem, mem_constrained_workers.max(1), avg_batch
            ),
        )
    } else if avg_rx_mb > 800.0 {
        (
            "Network RX".to_string(),
            format!(
                "Network Downlink Bound ({:.1} MB/s) - Saturated VM network fetching from GCS. (Avg batch: {})",
                avg_rx_mb, avg_batch
            ),
        )
    } else if avg_tx_mb > 500.0 {
        (
            "Network TX".to_string(),
            format!(
                "Network Uplink Bound ({:.1} MB/s) - Saturated VM network writing to GCS. (Avg batch: {})",
                avg_tx_mb, avg_batch
            ),
        )
    } else if avg_cpu < 30.0 {
        (
            "I/O Wait".to_string(),
            format!(
                "Low Utilization ({:.1}% CPU). Likely waiting on external I/O or single-threaded aggregation. (Avg batch: {})",
                avg_cpu, avg_batch
            ),
        )
    } else {
        (
            "Mixed".to_string(),
            format!(
                "Healthy distribution. CPU: {:.1}%, Mem: {:.1}% (Avg batch: {})",
                avg_cpu, avg_mem, avg_batch
            ),
        )
    };

    axum::Json(DashboardBottleneck {
        bottleneck,
        description,
        avg_cpu_percent: avg_cpu,
        avg_mem_percent: avg_mem,
        avg_network_rx_mb: avg_rx_mb,
        avg_network_tx_mb: avg_tx_mb,
    })
}

/// Handler for GET /api/dashboard/workers - list all workers.
async fn get_dashboard_workers(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<Vec<DashboardWorker>> {
    let data = state.lock().unwrap();
    let now = Instant::now();

    let workers: Vec<DashboardWorker> = data
        .worker_registry
        .iter()
        .map(|(id, w)| DashboardWorker {
            worker_id: id.clone(),
            status: w.status.as_str().to_string(),
            current_batch_size: w.current_batch_size,
            last_seen_secs: now.duration_since(w.last_seen).as_secs_f64(),
            telemetry: w.metrics_history.back().cloned(),
            total_items: w.total_rows,
            tasks_completed: w.partitions_completed,
            current_task: w.current_task.clone(),
        })
        .collect();

    axum::Json(workers)
}

/// Handler for GET /api/dashboard/metrics - time-series metrics for charts.
async fn get_dashboard_metrics(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardMetrics> {
    let data = state.lock().unwrap();

    // Get all worker IDs from the database (includes historical workers)
    let worker_ids = match data.metrics_db.get_worker_ids() {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("Warning: failed to get worker IDs from DB: {}", e);
            // Fall back to in-memory registry
            data.worker_registry.keys().cloned().collect()
        }
    };

    let workers: Vec<WorkerMetricsSeries> = worker_ids
        .iter()
        .map(|id| {
            // Query all snapshots from DB for this worker
            let snapshots = data
                .metrics_db
                .get_worker_snapshots(id)
                .unwrap_or_else(|_| Vec::new());

            WorkerMetricsSeries {
                worker_id: id.clone(),
                snapshots,
            }
        })
        .collect();

    axum::Json(DashboardMetrics { workers })
}

/// Handler for GET /api/dashboard/batch - get status of batch phenotypes.
async fn get_batch_status(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<BatchStatusResponse> {
    let data = state.lock().unwrap();
    let phenotypes = if let Some(ref batch) = data.batch_state {
        let mut list: Vec<PhenotypeStatus> = batch.phenotype_statuses.values().cloned().collect();
        // Sort by ID for stability
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    } else {
        Vec::new()
    };
    axum::Json(BatchStatusResponse { phenotypes })
}

/// Query parameters for GET /api/events
#[derive(serde::Deserialize)]
struct EventsQuery {
    #[serde(default)]
    since_ms: u64,
}

/// Handler for GET /api/events - get recent events.
async fn get_events(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
) -> axum::Json<EventsResponse> {
    let data = state.lock().unwrap();
    let events = data
        .events
        .iter()
        .filter(|e| e.timestamp_ms > query.since_ms)
        .cloned()
        .collect();
    axum::Json(EventsResponse { events })
}

/// Handler for GET /api/failures - get recent failures.
async fn get_failures(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<FailuresResponse> {
    let data = state.lock().unwrap();
    let failures = data.failures.iter().cloned().collect();
    axum::Json(FailuresResponse { failures })
}

/// Handler for GET /api/workers/:worker_id/logs - get worker log tail.
async fn get_worker_logs(
    axum::extract::Path(worker_id): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<Vec<String>> {
    let data = state.lock().unwrap();
    let logs = data
        .worker_registry
        .get(&worker_id)
        .and_then(|w| w.latest_log_tail.clone())
        .unwrap_or_else(|| vec!["No logs available for this worker".to_string()]);
    axum::Json(logs)
}

// ============================================================================
// Embedded Dashboard SPA
// ============================================================================

/// Embedded static files for the pool-dashboard SPA.
/// Files are included at compile time from cli/static/dist.
#[derive(Embed)]
#[folder = "static/dist"]
struct DashboardAssets;

/// Handler for serving embedded dashboard assets.
/// Serves files from the embedded SPA, falling back to index.html for SPA routing.
async fn serve_dashboard_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> impl IntoResponse {
    serve_embedded_file(&path)
}

/// Handler for the dashboard root - serves index.html.
async fn serve_dashboard_index() -> impl IntoResponse {
    serve_embedded_file("index.html")
}

/// Serve an embedded file by path, with SPA fallback to index.html.
fn serve_embedded_file(path: &str) -> Response {
    // Try to get the requested file
    let file = DashboardAssets::get(path).or_else(|| {
        // For SPA routing: if the path doesn't exist, serve index.html
        // (unless it looks like a file request with an extension)
        if !path.contains('.') {
            DashboardAssets::get("index.html")
        } else {
            None
        }
    });

    match file {
        Some(content) => {
            // Determine content type from file extension
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not found"))
            .unwrap(),
    }
}

/// Handler for GET /api/binary - serve the genohype binary.
///
/// This endpoint allows workers to download the genohype binary directly
/// from the coordinator over the fast GCP internal network, instead of each
/// worker receiving it via slow SCP from the client machine.
///
/// We serve from the fixed install path rather than current_exe() because:
/// - current_exe() returns /proc/self/exe which becomes stale when binary is replaced
/// - The fixed path always points to the latest uploaded binary
const BINARY_INSTALL_PATH: &str = "/usr/local/bin/genohype";

async fn serve_binary() -> impl axum::response::IntoResponse {
    use axum::http::{header, StatusCode};
    use axum::response::Response;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;

    // Use the fixed install path - this always points to the latest binary
    // even after updates (unlike /proc/self/exe which becomes "(deleted)")
    let exe_path = Path::new(BINARY_INSTALL_PATH);

    // Read the file synchronously (it's a one-time operation per request)
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<(Vec<u8>, u64)> {
        let mut file = File::open(exe_path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let mut buffer = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut buffer)?;
        Ok((buffer, file_size))
    })
    .await;

    match result {
        Ok(Ok((buffer, file_size))) => {
            println!(
                "Serving binary {} ({} bytes) to worker",
                BINARY_INSTALL_PATH, file_size
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, file_size)
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"genohype\"",
                )
                .body(Body::from(buffer))
                .unwrap()
        }
        Ok(Err(e)) => {
            eprintln!("Failed to read binary file {}: {}", BINARY_INSTALL_PATH, e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to read binary: {}", e)))
                .unwrap()
        }
        Err(e) => {
            eprintln!("Task panicked reading binary: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Internal error: {}", e)))
                .unwrap()
        }
    }
}

/// Handler for POST /api/export-metrics - export metrics database to GCS.
///
/// This endpoint reads the SQLite metrics database and uploads it to a GCS path.
/// Used by `pool destroy --metrics-bucket` to save metrics before deleting VMs.
async fn export_metrics(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<ExportMetricsRequest>,
) -> axum::Json<ExportMetricsResponse> {
    use genohype_core::io::CloudWriter;
    use std::io::Write;

    // Get db_path from config
    let db_path = { state.lock().unwrap().config.db_path.clone() };

    // Read the metrics database file
    let db_contents = match std::fs::read(&db_path) {
        Ok(contents) => contents,
        Err(e) => {
            return axum::Json(ExportMetricsResponse {
                success: false,
                path: None,
                error: Some(format!("Failed to read metrics database at {}: {}", db_path, e)),
            });
        }
    };

    if db_contents.is_empty() {
        return axum::Json(ExportMetricsResponse {
            success: false,
            path: None,
            error: Some("Metrics database is empty".to_string()),
        });
    }

    // Create cloud writer and upload
    let destination = req.destination.trim_end_matches('/');
    let upload_path = if destination.ends_with(".db") {
        destination.to_string()
    } else {
        format!("{}/metrics.db", destination)
    };

    let mut writer = match CloudWriter::new(&upload_path) {
        Ok(w) => w,
        Err(e) => {
            return axum::Json(ExportMetricsResponse {
                success: false,
                path: None,
                error: Some(format!("Failed to create cloud writer: {}", e)),
            });
        }
    };

    if let Err(e) = writer.write_all(&db_contents) {
        return axum::Json(ExportMetricsResponse {
            success: false,
            path: None,
            error: Some(format!("Failed to write metrics data: {}", e)),
        });
    }

    match writer.finish() {
        Ok(bytes) => {
            println!(
                "Exported metrics database ({} bytes) to {}",
                bytes, upload_path
            );
            axum::Json(ExportMetricsResponse {
                success: true,
                path: Some(upload_path),
                error: None,
            })
        }
        Err(e) => axum::Json(ExportMetricsResponse {
            success: false,
            path: None,
            error: Some(format!("Failed to upload metrics: {}", e)),
        }),
    }
}
