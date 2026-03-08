//! Coordinator server for distributed processing.
//!
//! The coordinator maintains the state of a distributed job:
//! - Queue of pending partitions
//! - Map of partitions currently being processed (with timestamps)
//! - Set of completed partitions
//!
//! Workers poll `/work` to get assignments and `/complete` to report completion.
//! A background task monitors for timed-out workers and reschedules their work.
//!
//! ## Module Structure
//!
//! - `state`: Core data structures (CoordinatorConfig, CoordinatorData, WorkerState, etc.)
//! - `ui`: Dashboard SPA serving
//! - `monitor`: Background monitoring functions (timeouts, liveness, backups)
//! - `api/`: HTTP API handlers
//!   - `dashboard`: Dashboard-related endpoints
//!   - `history`: Job history endpoints
//!   - `jobs`: Job submission and management endpoints
//! - `scheduler/`: Work distribution logic (kept in mod.rs due to tight coupling)

pub mod api;
pub mod monitor;
pub mod scheduler;
pub mod state;
pub mod ui;

// Re-export CoordinatorConfig as public (used by callers)
pub use state::CoordinatorConfig;

// Re-export internal state types for use within the crate
pub(crate) use state::{
    ActiveTask, BatchState, CoordinatorData, IngestionState,
    ManhattanPhase, ManhattanPipelineState, SharedState, WorkerState, WorkerStatus,
    AGGREGATE_BATCH_SIZE, BATCH_ACTIVE_LIMIT, MAX_AGGREGATE_RETRIES, MAX_METRICS_HISTORY,
};

use crate::distributed::message::{
    ActiveTaskInfo, CompleteRequest, CompleteResponse, FailureRecord,
    HeartbeatRequest, HeartbeatResponse, JobEvent, JobResultResponse, JobSpec,
    ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource, PartitionOp,
    PhenotypeOp, StatusResponse, TaskDescriptor, TaskType, WorkRequest, WorkResponse,
};
use crate::distributed::metrics_db::MetricsDb;
use crate::manhattan::config::PlotType;
use crate::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

// Import functions from extracted modules
use api::dashboard::build_dashboard_summary;
use monitor::{backup_db, check_stuck_job, check_timeouts, check_worker_liveness};

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
    use axum::{routing::{delete, get, post}, Router};
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
        eprintln!("  Checking for database backup at {}", bp);
        let bp_clone = bp.clone();
        let db_path_clone = db_path.clone();

        let restore_result = tokio::task::spawn_blocking(move || {
            // First, ensure the parent directory exists
            if let Some(parent) = std::path::Path::new(&db_path_clone).parent() {
                let _ = std::fs::create_dir_all(parent);
            }

            // Clean up any existing db files that might corrupt the restored state
            // If a stale -wal file exists, SQLite will overwrite the restored .db!
            let _ = std::fs::remove_file(&db_path_clone);
            let _ = std::fs::remove_file(format!("{}-wal", db_path_clone));
            let _ = std::fs::remove_file(format!("{}-shm", db_path_clone));

            if bp_clone.starts_with("gs://") {
                eprintln!("  Running gsutil cp {} {}", bp_clone, db_path_clone);
                match std::process::Command::new("gsutil")
                    .args(["cp", &bp_clone, &db_path_clone])
                    .status()
                {
                    Ok(status) if status.success() => {
                        // Verify file exists and has size
                        if let Ok(metadata) = std::fs::metadata(&db_path_clone) {
                            if metadata.len() > 0 {
                                eprintln!(
                                    "  Successfully restored DB ({} bytes)",
                                    metadata.len()
                                );
                                return true;
                            } else {
                                eprintln!("  Warning: Restored DB file is empty");
                            }
                        } else {
                            eprintln!(
                                "  Warning: gsutil succeeded but file not found at {}",
                                db_path_clone
                            );
                        }
                    }
                    Ok(status) => eprintln!("  gsutil cp failed with status: {}", status),
                    Err(e) => eprintln!("  Failed to execute gsutil: {}", e),
                }
            } else {
                eprintln!("  Warning: Automatic restore only supported for gs:// paths");
            }
            false
        })
        .await;

        if restore_result.unwrap_or(false) {
            eprintln!("  Restored ops database from {}", bp);
        } else {
            eprintln!("  Starting with fresh database (restore skipped or failed)");
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
        last_backup_at: None,
        update_fleet_url: None,
        updated_workers: HashSet::new(),
        current_job_id: None,
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
                    if backup_db(&metrics_db, &db_path, bp).await {
                        last_backup_time = Instant::now();
                        if let Ok(mut data) = monitor_state.lock() {
                            data.events_since_backup = 0;
                            data.last_backup_at = Some(CoordinatorData::now_ms());
                        }
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

                // Perform final backup to GCS
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
                        if backup_db(&metrics_db, &db_path, &bp).await {
                            if let Ok(mut data) = monitor_state.lock() {
                                data.last_backup_at = Some(CoordinatorData::now_ms());
                            }
                        }
                    }
                }

                // Reset to idle mode instead of exiting - allows coordinator to accept new jobs
                {
                    let mut data = monitor_state.lock().unwrap();

                    // Update job status in database before clearing state
                    if let Some(ref job_id) = data.current_job_id {
                        let end_time_ms = CoordinatorData::now_ms();
                        // Build a summary for persistence
                        let summary = build_dashboard_summary(&data);
                        let summary_json = serde_json::to_string(&summary).ok();
                        if let Err(e) = data.metrics_db.update_job_status(
                            job_id,
                            "completed",
                            Some(end_time_ms),
                            summary_json.as_deref(),
                        ) {
                            eprintln!("Warning: failed to update job status in DB: {}", e);
                        }
                    }

                    data.pending_partitions.clear();
                    data.processing_partitions.clear();
                    data.completed_tasks.clear();
                    data.failed_partitions.clear();
                    data.retry_counts.clear();
                    data.manhattan_state = None;
                    data.batch_state = None;
                    data.active_tasks.clear();
                    data.ingestion_state = None;
                    data.aggregated_results.clear();
                    data.total_rows = 0;
                    data.scan_cpu_secs = 0.0;
                    data.aggregate_cpu_secs = 0.0;
                    data.wasted_cpu_secs = 0.0;
                    data.last_error = None;
                    // Note: We intentionally keep current_job_id so the dashboard continues
                    // to display the finished job's metrics until a new job is submitted.
                    data.idle = true;
                }
                println!("Job complete. Coordinator returning to idle mode, ready for next job.");
            }
        }
    });

    let app = Router::new()
        // Core worker endpoints (scheduler)
        .route("/work", post(get_work))
        .route("/complete", post(complete_work))
        .route("/status", get(get_status))
        .route("/heartbeat", post(handle_heartbeat))
        // Dashboard API
        .route("/api/dashboard/summary", get(api::dashboard::get_dashboard_summary))
        .route("/api/dashboard/bottlenecks", get(api::dashboard::get_dashboard_bottlenecks))
        .route("/api/dashboard/workers", get(api::dashboard::get_dashboard_workers))
        .route("/api/dashboard/metrics", get(api::dashboard::get_dashboard_metrics))
        .route("/api/dashboard/batch", get(api::dashboard::get_batch_status))
        // Job management API
        .route("/api/job", post(api::jobs::submit_job))
        .route("/api/cancel", post(api::jobs::cancel_job))
        .route("/api/result", get(api::jobs::get_job_result))
        .route("/api/binary", get(api::jobs::serve_binary))
        .route("/api/export-metrics", post(api::jobs::export_metrics))
        .route("/api/events", get(api::jobs::get_events))
        .route("/api/failures", get(api::jobs::get_failures))
        .route("/api/workers/:worker_id/logs", get(api::jobs::get_worker_logs))
        .route("/api/workers/:worker_id/reset-capacity", post(api::jobs::reset_worker_capacity))
        .route("/api/update-fleet", post(api::jobs::update_fleet))
        .route("/api/update-coordinator", post(api::jobs::update_coordinator))
        // History API
        .route("/api/history/jobs", get(api::history::get_history_jobs))
        .route("/api/history/jobs/:job_id/summary", get(api::history::get_history_job_summary))
        .route("/api/history/jobs/:job_id/metrics", get(api::history::get_history_job_metrics))
        .route("/api/history/jobs/:job_id/events", get(api::history::get_history_job_events))
        .route("/api/history/jobs/:job_id/failures", get(api::history::get_history_job_failures))
        .route("/api/history/jobs/:job_id/batch", get(api::history::get_history_job_batch))
        .route("/api/history/jobs/:job_id", delete(api::history::delete_history_job))
        .with_state(state)
        // Embedded dashboard SPA
        .route("/dashboard", get(ui::serve_dashboard_index))
        .route("/dashboard/*path", get(ui::serve_dashboard_asset));

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

/// Ensure a worker exists in the registry and update last_seen.
fn touch_worker(
    data: &mut CoordinatorData,
    worker_id: &str,
    hardware: Option<crate::distributed::message::HardwareSpec>,
    build_version: Option<String>,
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

    // All work complete - return Wait so worker stays alive for next job
    println!(
        "Ingestion complete: {} succeeded, {} failed",
        ingestion.completed_count, ingestion.failed_count
    );
    axum::Json(WorkResponse::Wait)
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
    // Respect learned capacity ceiling if it exists
    let worker_cap = data.worker_registry.get(worker_id).and_then(|w| w.max_batch_capacity);
    let effective_max = worker_cap.unwrap_or(max_batch_size).min(max_batch_size);
    let partition_batch_size = data.worker_registry.get(worker_id)
        .and_then(|w| w.current_batch_size)
        .unwrap_or_else(|| (effective_max / 10).max(2).min(effective_max));

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
        // Return Wait so worker stays alive for next job
        return axum::Json(WorkResponse::Wait);
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
    touch_worker(&mut data, &req.worker_id, req.hardware.clone(), req.build_version.clone());

    // Check for pending binary update
    if let Some(gcs_url) = data.update_fleet_url.clone() {
        if !data.updated_workers.contains(&req.worker_id) {
            data.updated_workers.insert(req.worker_id.clone());
            return axum::Json(WorkResponse::UpdateBinary { gcs_url });
        }
    }

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
        // Respect learned capacity ceiling if it exists
        let worker_cap = data.worker_registry.get(&req.worker_id).and_then(|w| w.max_batch_capacity);
        let effective_max = worker_cap.unwrap_or(max_batch_size).min(max_batch_size);
        let batch_size = data.worker_registry.get(&req.worker_id)
            .and_then(|w| w.current_batch_size)
            .unwrap_or_else(|| (effective_max / 10).max(2).min(effective_max));

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
        // All done - return Wait so worker stays alive for next job
        println!(
            "Worker {} requested work, but all partitions complete",
            req.worker_id
        );
        axum::Json(WorkResponse::Wait)
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
    // Respect learned capacity ceiling if it exists
    let worker_cap = data.worker_registry.get(worker_id).and_then(|w| w.max_batch_capacity);
    let effective_max = worker_cap.unwrap_or(max_batch_size).min(max_batch_size);
    let batch_size = data.worker_registry.get(worker_id)
        .and_then(|w| w.current_batch_size)
        .unwrap_or_else(|| (effective_max / 10).max(2).min(effective_max));

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
                        // Return Wait so worker stays alive for next job
                        return axum::Json(WorkResponse::Wait);
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
                // All done - return Wait so worker stays alive for next job
                manhattan.phase = ManhattanPhase::Complete;
                return axum::Json(WorkResponse::Wait);
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
            // Return Wait so worker stays alive for next job
            axum::Json(WorkResponse::Wait)
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

/// Extract the batch capacity from a "batch too large" error message.
/// Returns the number of partitions the worker can handle, or None if not found.
/// Expected format: "... only N can fit ..."
fn extract_capacity_from_error(msg: &str) -> Option<usize> {
    let prefix = "only ";
    let suffix = " can fit";

    if let Some(start_idx) = msg.find(prefix) {
        let remainder = &msg[start_idx + prefix.len()..];
        if let Some(end_idx) = remainder.find(suffix) {
            let num_str = remainder[..end_idx].trim();
            return num_str.parse::<usize>().ok();
        }
    }
    None
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

        if let Some(ref err_msg) = req.error {
            // Check if this is a "batch too large" error with capacity info
            if let Some(capacity) = extract_capacity_from_error(err_msg) {
                println!(
                    "Worker {} reported memory capacity limit: {} partitions (was trying {})",
                    req.worker_id, capacity, current_batch
                );
                // Store the learned ceiling
                w.max_batch_capacity = Some(capacity);
                // Drop to half the known capacity (safe margin)
                w.current_batch_size = Some((capacity / 2).max(1));
            } else {
                // Standard Multiplicative Decrease: halve the batch size on failure/timeout
                w.current_batch_size = Some((current_batch / 2).max(1));
            }
        } else if let Some(task) = &completed_task {
            let duration_secs = (now_ms.saturating_sub(task.started_at_ms)) as f64 / 1000.0;
            let num_tasks = task_ids.len() as f64;

            if num_tasks > 0.0 && duration_secs > 0.0 {
                let time_per_task = duration_secs / num_tasks;
                // Target an ideal turnaround of 60 seconds
                let target_batch = (60.0 / time_per_task).round() as usize;
                // Respect the learned capacity ceiling if it exists
                let effective_max = w.max_batch_capacity.unwrap_or(max_batch).min(max_batch);
                let clamped_target = target_batch.clamp(1, effective_max);

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
    touch_worker(&mut data, &req.worker_id, None, None);
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

/// Handler for POST /heartbeat - worker sends telemetry.
async fn handle_heartbeat(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(mut req): axum::Json<HeartbeatRequest>,
) -> axum::Json<HeartbeatResponse> {
    let mut data = state.lock().unwrap();
    touch_worker(&mut data, &req.worker_id, None, req.build_version.clone());

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

        // Inject the current batch size and capacity ceiling into telemetry before persistence
        req.telemetry.current_batch_size = w.current_batch_size;
        req.telemetry.max_batch_capacity = w.max_batch_capacity;

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
    let job_id = data.current_job_id.clone();
    if let Err(e) = data.metrics_db.insert_snapshot_with_job_id(&req.worker_id, &req.telemetry, job_id.as_deref()) {
        eprintln!("Warning: failed to persist metrics to DB: {}", e);
    }

    axum::Json(HeartbeatResponse { acknowledged: true })
}
