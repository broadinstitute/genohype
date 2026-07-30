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
pub mod services;
pub mod state;
pub mod ui;

// Re-export CoordinatorConfig as public (used by callers)
pub use state::CoordinatorConfig;

// Re-export internal state types for use within the crate
pub(crate) use state::{
    ActiveTask, BatchState, CoordinatorData, IngestionState, JobExecutionState, ManhattanPhase,
    ManhattanPipelineState, SharedState, WorkerStatus, AGGREGATE_BATCH_SIZE, BATCH_ACTIVE_LIMIT,
    MAX_AGGREGATE_RETRIES, MAX_METRICS_HISTORY,
};

use crate::distributed::message::{
    ActiveTaskInfo, CompleteRequest, CompleteResponse, FailureRecord, HeartbeatRequest,
    HeartbeatResponse, JobEvent, JobResultResponse, JobSpec, ManhattanSource, StatusResponse,
    TaskDescriptor, WorkRequest, WorkResponse, CUSTOM_WORKER_PROTOCOL_VERSION,
};
use crate::distributed::metrics_db::{CustomCompletionOutcome, DurableCustomAssignment, MetricsDb};
use crate::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

// Import functions from extracted modules
use api::dashboard::build_dashboard_summary;
use monitor::{
    backup_db, check_cpu_status_consistency, check_stuck_job, check_timeouts, check_worker_liveness,
};
use scheduler::{
    complete_batch_work, complete_ingestion_work, complete_manhattan_work, determine_batch_size,
    extract_capacity_from_error, get_batch_work, get_ingestion_work, get_manhattan_work,
};

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
        config.pool_name,
        config.gcp_project,
        config.gcp_zone,
        config.machine_type,
        config.spot,
        config.network,
        config.subnet,
        config.public_ip,
        config.manage_firewall,
        config.worker_service_account,
    )
    .await
}

fn restore_and_reconcile_database<F>(
    db_path: &str,
    copy_backup: F,
) -> std::result::Result<bool, String>
where
    F: FnOnce(&str) -> std::result::Result<bool, String>,
{
    if let Some(parent) = std::path::Path::new(db_path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create database directory: {error}"))?;
    }

    // A stale WAL can overwrite the newly copied main database, so backup
    // installation and reconciliation always begin from an empty destination.
    for path in [
        db_path.to_string(),
        format!("{db_path}-wal"),
        format!("{db_path}-shm"),
    ] {
        if let Err(error) = std::fs::remove_file(&path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(format!(
                    "failed to remove stale database file {path}: {error}"
                ));
            }
        }
    }

    if !copy_backup(db_path)? {
        return Ok(false);
    }
    let metadata = std::fs::metadata(db_path)
        .map_err(|error| format!("restored database file is unavailable: {error}"))?;
    if metadata.len() == 0 {
        return Err("restored database file is empty".to_string());
    }
    eprintln!("  Successfully restored DB ({} bytes)", metadata.len());

    let restored = MetricsDb::open(db_path)
        .map_err(|error| format!("failed to open restored metrics database: {error}"))?;
    let reconciled = restored
        .reconcile_restored_custom_jobs(CoordinatorData::now_ms())
        .map_err(|error| format!("failed to reconcile restored custom jobs: {error}"))?;
    if reconciled > 0 {
        eprintln!(
            "  Marked {reconciled} restored running custom job(s) failed and fenced assignments"
        );
    }
    Ok(true)
}

/// Properly structured coordinator startup.
///
/// Note: For backward compatibility, `output_path` is converted to a default
/// ExportParquet JobSpec. New code should use the API endpoint with JobSpec directly.
#[allow(clippy::too_many_arguments)]
pub async fn run_coordinator(
    port: u16,
    db_path: String,
    backup_path: Option<String>,
    input_path: String,
    output_path: String,
    total_tasks: usize,
    batch_size: usize,
    timeout_secs: u64,
    pool_name: Option<String>,
    gcp_project: Option<String>,
    gcp_zone: Option<String>,
    machine_type: Option<String>,
    spot: Option<bool>,
    network: Option<String>,
    subnet: Option<String>,
    public_ip: Option<bool>,
    manage_firewall: Option<bool>,
    worker_service_account: Option<String>,
) -> Result<()> {
    use axum::{
        routing::{delete, get, post},
        Router,
    };
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

    // Try to restore from backup path if configured. Any successfully copied
    // database is reconciled before the server or its receipt endpoints exist.
    if let Some(ref bp) = backup_path {
        eprintln!("  Checking for database backup at {}", bp);
        let bp_clone = bp.clone();
        let db_path_clone = db_path.clone();

        let restore_result = tokio::task::spawn_blocking(move || {
            restore_and_reconcile_database(&db_path_clone, |destination| {
                if !bp_clone.starts_with("gs://") {
                    eprintln!("  Warning: Automatic restore only supported for gs:// paths");
                    return Ok(false);
                }
                eprintln!("  Running gsutil cp {} {}", bp_clone, destination);
                match std::process::Command::new("gsutil")
                    .args(["cp", &bp_clone, destination])
                    .status()
                {
                    Ok(status) if status.success() => Ok(true),
                    Ok(status) => {
                        eprintln!("  gsutil cp failed with status: {}", status);
                        Ok(false)
                    }
                    Err(error) => {
                        eprintln!("  Failed to execute gsutil: {}", error);
                        Ok(false)
                    }
                }
            })
        })
        .await
        .map_err(|error| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("database restore task failed: {error}"),
            ))
        })?
        .map_err(|error| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("database restore reconciliation failed: {error}"),
            ))
        })?;

        if restore_result {
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
            pool_name: pool_name.or_else(|| {
                // Auto-detect pool name from hostname (e.g., "heavy-coordinator" -> "heavy")
                std::process::Command::new("hostname")
                    .output()
                    .ok()
                    .and_then(|o| {
                        let h = String::from_utf8_lossy(&o.stdout).trim().to_string();
                        h.strip_suffix("-coordinator").map(String::from)
                    })
            }),
            gcp_project: gcp_project.or_else(|| {
                // Auto-detect from gcloud config
                std::process::Command::new("gcloud")
                    .args(["config", "get-value", "project"])
                    .output()
                    .ok()
                    .and_then(|o| {
                        let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    })
            }),
            gcp_zone: gcp_zone.or_else(|| {
                // Auto-detect from gcloud config or instance metadata
                std::process::Command::new("gcloud")
                    .args(["config", "get-value", "compute/zone"])
                    .output()
                    .ok()
                    .and_then(|o| {
                        let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    })
            }),
            machine_type,
            spot,
            network,
            subnet,
            public_ip,
            manage_firewall,
            worker_service_account,
        },
        total_rows: 0,
        scan_cpu_secs: 0.0,
        aggregate_cpu_secs: 0.0,
        wasted_cpu_secs: 0.0,
        retry_counts: HashMap::new(),
        custom_assignment_attempts: HashMap::new(),
        custom_assignments: HashMap::new(),
        failed_partitions: HashSet::new(),
        worker_registry: HashMap::new(),
        job_start_time: Instant::now(),
        last_progress_time: Instant::now(),
        idle,
        metrics_db,
        aggregated_results: Vec::new(),
        job_state: JobExecutionState::Standard,
        active_tasks: HashMap::new(),
        last_error: None,
        events: VecDeque::new(),
        failures: VecDeque::new(),
        events_since_backup: 0,
        last_backup_at: None,
        update_fleet_url: None,
        updated_workers: HashSet::new(),
        current_job_id: None,
        session_id: Uuid::new_v4().to_string(),
        catalog: None,
        ingested_phenotypes: HashSet::new(),
        completed_phenotypes: HashSet::new(),
        last_completed_batch: None,
        cached_vms: None,
        deleted_workers: HashSet::new(),
    }));

    // Log session ID for debugging
    {
        let data = state.lock().unwrap();
        println!("  Session ID: {}", &data.session_id[..8]);
    }

    // Start background ClickHouse health monitor (AIMD for ingestion batch sizing)
    let ch_monitor_state = state.clone();
    tokio::spawn(async move {
        monitor::monitor_clickhouse_health(ch_monitor_state).await;
    });

    // Start background timeout monitor
    let monitor_state = state.clone();
    tokio::spawn(async move {
        let mut last_backup_time = Instant::now();
        let mut last_events_count = 0usize;

        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            check_timeouts(&monitor_state, timeout_secs);
            check_worker_liveness(&monitor_state);
            check_cpu_status_consistency(&monitor_state);

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
                match &data.job_state {
                    JobExecutionState::Batch(batch) => {
                        // Batch jobs use phenotype-level tracking
                        let total = batch.total_phenotypes;
                        let completed = batch.completed_count;
                        let pending = batch.pending_queue.len();
                        let processing =
                            batch.active_phenotypes.len() + batch.ready_to_aggregate.len();
                        let is_complete = batch.pending_queue.is_empty()
                            && batch.active_phenotypes.is_empty()
                            && batch.ready_to_aggregate.is_empty()
                            && (batch.completed_count + batch.failed_count)
                                == batch.total_phenotypes;
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
                    }
                    JobExecutionState::Manhattan(m) => {
                        // Single Manhattan pipeline uses separate tracking
                        let total_parts = m.exome_total_tasks + m.genome_total_tasks + 1; // +1 for aggregate
                        let completed_parts = m.exome_completed.len()
                            + m.genome_completed.len()
                            + if m.aggregate_complete { 1 } else { 0 };
                        let processing_parts = m.exome_processing.len()
                            + m.genome_processing.len()
                            + if m.aggregate_dispatched && !m.aggregate_complete {
                                1
                            } else {
                                0
                            };
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
                    }
                    JobExecutionState::Ingestion(ing) => {
                        let is_complete = ing.pending_tasks.is_empty()
                            && ing.active_tasks.is_empty()
                            && (ing.completed_count + ing.failed_count) == ing.total_tasks;
                        (
                            ing.pending_tasks.len(),
                            ing.active_tasks.len(),
                            ing.completed_count,
                            ing.failed_count,
                            ing.total_tasks,
                            data.total_rows,
                            data.idle,
                            is_complete,
                        )
                    }
                    JobExecutionState::Standard => {
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
                let should_backup =
                    events_since > 1000 || last_backup_time.elapsed().as_secs() > 60;
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
            let job_complete = is_batch_complete
                || (total > 0 && (completed + failed) == total && processing == 0 && pending == 0);

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

                    // Capture batch state before resetting and transfer completed phenotypes
                    // Collect completed phenotypes first to avoid borrow conflicts
                    let completed: Vec<(String, String)> = match &data.job_state {
                        JobExecutionState::Batch(batch) => {
                            let mut result = Vec::new();
                            if let Some(crate::distributed::message::JobSpec::ManhattanBatch {
                                ref specs,
                                ..
                            }) = data.config.job_spec
                            {
                                for spec in specs {
                                    if let Some(status) =
                                        batch.phenotype_statuses.get(&spec.output_path)
                                    {
                                        if status.stage == "completed" {
                                            if let (Some(id), Some(ancestry)) =
                                                (&spec.phenotype, &spec.ancestry)
                                            {
                                                result.push((id.clone(), ancestry.clone()));
                                            }
                                        }
                                    }
                                }
                            }
                            result
                        }
                        JobExecutionState::Manhattan(m) if m.phase == ManhattanPhase::Complete => {
                            let mut result = Vec::new();
                            if let Some(crate::distributed::message::JobSpec::Manhattan {
                                ref spec,
                                ..
                            }) = data.config.job_spec
                            {
                                if let (Some(id), Some(ancestry)) =
                                    (&spec.phenotype, &spec.ancestry)
                                {
                                    result.push((id.clone(), ancestry.clone()));
                                }
                            }
                            result
                        }
                        _ => Vec::new(),
                    };

                    if let JobExecutionState::Batch(ref batch) = data.job_state {
                        data.last_completed_batch = Some(batch.phenotype_statuses.clone());
                    }

                    for (id, ancestry) in completed {
                        data.completed_phenotypes.insert((id, ancestry));
                    }

                    data.pending_partitions.clear();
                    data.processing_partitions.clear();
                    data.completed_tasks.clear();
                    data.failed_partitions.clear();
                    data.retry_counts.clear();
                    data.custom_assignment_attempts.clear();
                    data.custom_assignments.clear();
                    data.job_state = JobExecutionState::Standard;
                    data.active_tasks.clear();
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
        .route(
            "/api/dashboard/summary",
            get(api::dashboard::get_dashboard_summary),
        )
        .route(
            "/api/dashboard/bottlenecks",
            get(api::dashboard::get_dashboard_bottlenecks),
        )
        .route(
            "/api/dashboard/workers",
            get(api::dashboard::get_dashboard_workers),
        )
        .route(
            "/api/dashboard/metrics",
            get(api::dashboard::get_dashboard_metrics),
        )
        .route(
            "/api/dashboard/batch",
            get(api::dashboard::get_batch_status),
        )
        // Cluster management API
        .route("/api/cluster/config", get(api::cluster::get_config))
        .route("/api/cluster/vms", get(api::cluster::get_vms))
        .route("/api/cluster/scale", post(api::cluster::scale_cluster))
        // ClickHouse API
        .route(
            "/api/clickhouse/info",
            get(api::clickhouse::get_clickhouse_info),
        )
        // Catalog API
        .route("/api/catalog/load", post(api::catalog::load_catalog_api))
        .route("/api/catalog", get(api::catalog::get_catalog_api))
        .route("/api/catalog/config", get(api::catalog::get_config_api))
        .route(
            "/api/catalog/process",
            post(api::catalog::process_catalog_api),
        )
        .route(
            "/api/catalog/ingest",
            post(api::catalog::ingest_catalog_api),
        )
        // Job management API
        .route("/api/job", post(api::jobs::submit_job))
        .route("/api/cancel", post(api::jobs::cancel_job))
        .route("/api/result", get(api::jobs::get_job_result))
        .route(
            "/api/jobs/:job_id/custom-receipts",
            get(api::jobs::get_custom_receipts),
        )
        .route("/api/binary", get(api::jobs::serve_binary))
        .route("/api/export-metrics", post(api::jobs::export_metrics))
        .route("/api/events", get(api::jobs::get_events))
        .route("/api/failures", get(api::jobs::get_failures))
        .route(
            "/api/workers/:worker_id/logs",
            get(api::jobs::get_worker_logs),
        )
        .route(
            "/api/workers/:worker_id/reset-capacity",
            post(api::jobs::reset_worker_capacity),
        )
        .route("/api/update-fleet", post(api::jobs::update_fleet))
        .route(
            "/api/update-coordinator",
            post(api::jobs::update_coordinator),
        )
        // History API
        .route("/api/history/jobs", get(api::history::get_history_jobs))
        .route(
            "/api/history/jobs/:job_id/summary",
            get(api::history::get_history_job_summary),
        )
        .route(
            "/api/history/jobs/:job_id/metrics",
            get(api::history::get_history_job_metrics),
        )
        .route(
            "/api/history/jobs/:job_id/events",
            get(api::history::get_history_job_events),
        )
        .route(
            "/api/history/jobs/:job_id/failures",
            get(api::history::get_history_job_failures),
        )
        .route(
            "/api/history/jobs/:job_id/batch",
            get(api::history::get_history_job_batch),
        )
        .route(
            "/api/history/jobs/:job_id",
            delete(api::history::delete_history_job),
        )
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

/// Handler for POST /work - worker requests work.
async fn get_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<WorkRequest>,
) -> axum::Json<WorkResponse> {
    let mut data = state.lock().unwrap();

    // Custom tasks require workers that understand lease/session fencing. Reject
    // legacy custom workers before assignment so incompatibility is immediate
    // rather than surfacing as repeated completion timeouts.
    if matches!(data.config.job_spec, Some(JobSpec::Custom { .. }))
        && req.protocol_version.unwrap_or(0) < CUSTOM_WORKER_PROTOCOL_VERSION
    {
        return axum::Json(WorkResponse::Incompatible {
            required_protocol_version: CUSTOM_WORKER_PROTOCOL_VERSION,
            message: format!(
                "custom jobs require worker protocol {} or newer; worker reported {}",
                CUSTOM_WORKER_PROTOCOL_VERSION,
                req.protocol_version.unwrap_or(0)
            ),
        });
    }

    data.touch_worker(
        &req.worker_id,
        req.hardware.clone(),
        req.build_version.clone(),
    );

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

    // Check for specialized job types using the JobExecutionState enum
    // We need to temporarily swap out the state to avoid borrow issues
    let current_state = std::mem::take(&mut data.job_state);
    match current_state {
        JobExecutionState::Batch(mut batch) => {
            let result = get_batch_work(&mut data, &mut batch, &req.worker_id);
            data.job_state = JobExecutionState::Batch(batch);
            return result;
        }
        JobExecutionState::Manhattan(mut manhattan) => {
            let result = get_manhattan_work(&mut data, &mut manhattan, &req.worker_id);
            data.job_state = JobExecutionState::Manhattan(manhattan);
            return result;
        }
        JobExecutionState::Ingestion(mut ingestion) => {
            let result = get_ingestion_work(&mut data, &mut ingestion, &req.worker_id);
            data.job_state = JobExecutionState::Ingestion(ingestion);
            return result;
        }
        JobExecutionState::Standard => {
            // Continue with standard job handling below
            data.job_state = JobExecutionState::Standard;
        }
    }

    // Standard (non-Manhattan) job: check if there's pending work
    if let Some(part_id) = data.pending_partitions.pop_front() {
        // Collect batch of partitions
        let mut partitions = vec![part_id];
        let worker_hw = data
            .worker_registry
            .get(&req.worker_id)
            .and_then(|w| w.hardware.as_ref());

        let max_batch_size = determine_batch_size(
            data.config.batch_size,
            worker_hw,
            &data.config.job_spec,
            data.config.memory_weight_mb,
        );
        // Respect learned capacity ceiling if it exists
        let worker_cap = data
            .worker_registry
            .get(&req.worker_id)
            .and_then(|w| w.max_batch_capacity);
        let effective_max = worker_cap.unwrap_or(max_batch_size).min(max_batch_size);
        let batch_size = data
            .worker_registry
            .get(&req.worker_id)
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
        let mut tasks: Vec<TaskDescriptor> = partitions
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

        // Custom tasks are side-effecting and may outlive a timeout or Spot
        // preemption. Fence each assignment independently so a late worker can
        // never complete a newer retry of the same manifest task.
        if matches!(job_spec, JobSpec::Custom { .. }) {
            for (&partition_id, task) in partitions.iter().zip(tasks.iter_mut()) {
                let lease = data.issue_custom_assignment(&task.id, partition_id, &req.worker_id);
                task.assignment_attempt = Some(lease.assignment_attempt);
                task.lease_token = Some(lease.lease_token);
            }
            let Some(job_id) = data.current_job_id.clone() else {
                for (&partition_id, task) in partitions.iter().zip(tasks.iter()).rev() {
                    data.processing_partitions.remove(&partition_id);
                    data.custom_assignments.remove(&task.id);
                    data.pending_partitions.push_front(partition_id);
                }
                return axum::Json(WorkResponse::Wait);
            };
            let durable: Vec<_> = partitions
                .iter()
                .zip(tasks.iter())
                .map(|(&partition_id, task)| DurableCustomAssignment {
                    task_id: &task.id,
                    partition_id,
                    assignment_attempt: task.assignment_attempt.expect("custom attempt"),
                    lease_token: task.lease_token.as_deref().expect("custom lease"),
                })
                .collect();
            if let Err(error) = data.metrics_db.persist_custom_assignments(
                &job_id,
                &data.session_id,
                &req.worker_id,
                // Coordinator-observed but worker-self-reported. This value is
                // assignment-bound, not authenticated build attestation.
                req.build_version.as_deref(),
                &durable,
                CoordinatorData::now_ms(),
            ) {
                eprintln!("Rejected unsafe custom assignment dispatch: {error}");
                for (&partition_id, task) in partitions.iter().zip(tasks.iter()).rev() {
                    data.processing_partitions.remove(&partition_id);
                    data.custom_assignments.remove(&task.id);
                    data.pending_partitions.push_front(partition_id);
                }
                return axum::Json(WorkResponse::Wait);
            }
        }
        let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
        let task_labels: Vec<String> = tasks
            .iter()
            .map(|t| t.label.clone().unwrap_or_else(|| t.id.clone()))
            .collect();
        let task_type = tasks
            .first()
            .map(|t| t.task_type.as_str())
            .unwrap_or("unknown");

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

        // For Custom jobs, send the inner payload (e.g. {"clickhouse_url": "..."})
        // instead of the entire serialized JobSpec enum wrapper.
        // This lets custom worker binaries read payload fields directly.
        let work_payload = if let JobSpec::Custom { ref payload, .. } = job_spec {
            payload.clone()
        } else {
            serde_json::to_value(&job_spec).unwrap_or_default()
        };

        axum::Json(WorkResponse::Task {
            tasks,
            input_path: data.config.input_path.clone(),
            payload: work_payload,
            total_tasks: data.config.total_tasks,
            filters: data.config.filters.clone(),
            intervals: data.config.intervals.clone(),
            session_id: Some(data.session_id.clone()),
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

/// Handler for POST /complete - worker reports completion.
async fn complete_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<CompleteRequest>,
) -> axum::Json<CompleteResponse> {
    let mut data = state.lock().unwrap();
    let now_ms = CoordinatorData::now_ms();

    // Check session_id to detect stale completions from a previous coordinator session
    // This happens when coordinator restarts while workers continue running
    if let Some(ref req_session_id) = req.session_id {
        if *req_session_id != data.session_id {
            // Silently ignore stale completions - don't spam warnings
            // The work will be re-assigned if still pending, or the worker will get new work
            return axum::Json(CompleteResponse {
                acknowledged: false,
            });
        }
    }

    let is_custom_job = matches!(data.config.job_spec, Some(JobSpec::Custom { .. }));
    if is_custom_job {
        // Fencing cannot be optional for side-effecting custom tasks: accepting a
        // legacy completion here would reintroduce the stale-writer race.
        let Some(job_id) = data.current_job_id.clone() else {
            return axum::Json(CompleteResponse {
                acknowledged: false,
            });
        };
        if req.session_id.as_deref() != Some(data.session_id.as_str()) {
            return axum::Json(CompleteResponse {
                acknowledged: false,
            });
        }

        // An exact retry after an uncertain response is idempotently acknowledged
        // from durable state and must not replay any in-memory state transition.
        match data
            .metrics_db
            .is_identical_custom_completion(&job_id, &req)
        {
            Ok(true) => {
                return axum::Json(CompleteResponse { acknowledged: true });
            }
            Ok(false) => {}
            Err(error) => {
                eprintln!("Rejected custom completion: durable receipt lookup failed: {error}");
                return axum::Json(CompleteResponse {
                    acknowledged: false,
                });
            }
        }

        if let Err(reason) = data.validate_custom_assignments(
            req.session_id.as_deref(),
            &req.worker_id,
            &req.tasks,
            &req.assignments,
        ) {
            println!(
                "Rejected custom completion from {}: {}",
                req.worker_id, reason
            );
            return axum::Json(CompleteResponse {
                acknowledged: false,
            });
        }
        match data
            .metrics_db
            .accept_custom_completion(&job_id, &req, now_ms)
        {
            Ok(CustomCompletionOutcome::Stored) => {
                for task_id in &req.tasks {
                    data.custom_assignments.remove(task_id);
                }
            }
            Ok(CustomCompletionOutcome::Duplicate) => {
                return axum::Json(CompleteResponse { acknowledged: true });
            }
            Err(error) => {
                eprintln!("Rejected custom completion: durable acceptance failed: {error}");
                return axum::Json(CompleteResponse {
                    acknowledged: false,
                });
            }
        }
    }
    // Missing session/lease metadata remains accepted only for legacy built-in jobs.

    // Extract config values before borrowing worker_registry mutably
    let config_batch_size = data.config.batch_size;
    let job_spec_ref = data.config.job_spec.clone();
    let memory_weight_mb = data.config.memory_weight_mb;

    // Clear the current_task from the worker and capture it for duration tracking
    // Also extract hardware info for AIMD calculation
    let (completed_task, worker_hardware) =
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
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
        let max_batch = determine_batch_size(
            config_batch_size,
            worker_hardware.as_ref(),
            &job_spec_ref,
            memory_weight_mb,
        );
        // Start conservative if we don't have a baseline yet
        let current_batch = w
            .current_batch_size
            .unwrap_or((max_batch / 10).max(2).min(max_batch));

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
                    let growth = (current_batch / 10).max(1);
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
            Some(ActiveTask::AggregateBatch { started_at_ms, .. }) => {
                now_ms.saturating_sub(*started_at_ms)
            }
            None => 0,
        };

        // Track wasted CPU time
        data.wasted_cpu_secs += (wasted_duration_ms as f64) / 1000.0;

        // Log the error prominently
        println!(
            "ERROR from worker {}: tasks {:?} failed: {} (wasted {:.1}s)",
            req.worker_id,
            task_ids,
            error,
            wasted_duration_ms as f64 / 1000.0
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

        // Log event as warning (will be retried) - individual REQUEUED events show retry status
        data.log_event(JobEvent {
            timestamp_ms: now_ms,
            event_type: "warning".to_string(),
            worker_id: Some(req.worker_id.clone()),
            phenotype_id: None,
            details: format!(
                "Batch failed: {} (wasted {:.1}s)",
                error,
                wasted_duration_ms as f64 / 1000.0
            ),
        });

        // Only process standard job partitions if they are actually in processing_partitions
        // This implicitly skips batch/manhattan tasks which use different tracking maps
        let mut parts_to_requeue = Vec::new();
        for &part_id in &partitions {
            // Check ownership to prevent race conditions with timeouts stealing tasks
            if let Some((current_worker, _)) = data.processing_partitions.get(&part_id) {
                if current_worker != &req.worker_id {
                    println!(
                        "Warning: Worker {} reported failure for task {} but it is assigned to {}. Ignoring.",
                        req.worker_id, part_id, current_worker
                    );
                    continue;
                }
            } else {
                // Not in processing partitions (could be a batch job or timed out task)
                continue;
            }

            data.processing_partitions.remove(&part_id);
            parts_to_requeue.push(part_id);
        }

        // Process requeues in reverse order to preserve original queue ordering
        for &part_id in parts_to_requeue.iter().rev() {
            // Use same retry logic as timeouts
            let retry_count = {
                let retries = data.retry_counts.entry(part_id).or_insert(0);
                *retries += 1;
                *retries
            };

            if retry_count > 3 {
                println!(
                    "Partition {} exceeded max retries ({}), marking as permanently failed",
                    part_id, retry_count
                );
                data.failed_partitions.insert(part_id);

                // Log permanent failure event
                data.log_event(JobEvent {
                    timestamp_ms: now_ms,
                    event_type: "failed".to_string(),
                    worker_id: None,
                    phenotype_id: None,
                    details: format!(
                        "Task {} permanently failed after {} retries",
                        part_id, retry_count
                    ),
                });
            } else {
                println!(
                    "Re-queuing partition {} for retry ({}/3)",
                    part_id, retry_count
                );
                data.pending_partitions.push_front(part_id);

                // Add REQUEUED event to dashboard
                data.log_event(JobEvent {
                    timestamp_ms: now_ms,
                    event_type: "requeued".to_string(),
                    worker_id: None,
                    phenotype_id: None,
                    details: format!(
                        "Task {} requeued after failure (retry {}/3)",
                        part_id, retry_count
                    ),
                });
            }
        }

        // For batch jobs, handle retry logic based on task type
        if let JobExecutionState::Batch(ref mut batch) = data.job_state {
            match failed_task {
                Some(ActiveTask::AggregateBatch { phenotype_ids, .. }) => {
                    // Re-queue aggregate tasks for retry
                    for phenotype_id in phenotype_ids {
                        let retries = batch
                            .aggregate_retry_counts
                            .entry(phenotype_id.clone())
                            .or_insert(0);
                        *retries += 1;

                        if *retries > MAX_AGGREGATE_RETRIES {
                            println!(
                                "Phenotype {} exceeded max aggregate retries ({}), marking as failed",
                                phenotype_id, MAX_AGGREGATE_RETRIES
                            );
                            batch.failed_count += 1;

                            // Write error.json to the output path before removing the spec
                            if let Some(spec) = batch.aggregate_specs.remove(&phenotype_id) {
                                let err_path = format!(
                                    "{}/error.json",
                                    spec.output_path.trim_end_matches('/')
                                );
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
                                            let _ =
                                                writer.write_all(err_json.to_string().as_bytes());
                                            let _ = writer.finish();
                                        }
                                    } else {
                                        let _ = std::fs::write(&err_path, err_json.to_string());
                                    }
                                });
                            }
                        } else if let Some(spec) = batch.aggregate_specs.get(&phenotype_id).cloned()
                        {
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
                Some(ActiveTask::Scan {
                    phenotype_id,
                    source,
                    ..
                }) => {
                    // Re-queue scan partitions back to the phenotype
                    if let Some(state) = batch.active_phenotypes.get_mut(&phenotype_id) {
                        let mut valid_parts = Vec::new();

                        // Check ownership
                        for &part_id in &partitions {
                            let is_owned = match source {
                                ManhattanSource::Exome => state
                                    .exome_processing
                                    .get(&part_id)
                                    .map_or(false, |(w, _)| w == &req.worker_id),
                                ManhattanSource::Genome => state
                                    .genome_processing
                                    .get(&part_id)
                                    .map_or(false, |(w, _)| w == &req.worker_id),
                            };

                            if is_owned {
                                match source {
                                    ManhattanSource::Exome => {
                                        state.exome_processing.remove(&part_id);
                                    }
                                    ManhattanSource::Genome => {
                                        state.genome_processing.remove(&part_id);
                                    }
                                }
                                valid_parts.push(part_id);
                            } else {
                                println!("Warning: Ignoring failure for {} partition {} from {} (not owned)", phenotype_id, part_id, req.worker_id);
                            }
                        }

                        // Requeue in reverse order using push_front to preserve original sequence
                        for &part_id in valid_parts.iter().rev() {
                            match source {
                                ManhattanSource::Exome => state.exome_pending.push_front(part_id),
                                ManhattanSource::Genome => state.genome_pending.push_front(part_id),
                            }
                        }
                        println!(
                            "Re-queued {} scan tasks for phenotype {} (source: {:?})",
                            valid_parts.len(),
                            phenotype_id,
                            source
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
        if let JobExecutionState::Manhattan(ref mut manhattan) = data.job_state {
            let mut valid_exome = Vec::new();
            let mut valid_genome = Vec::new();

            for &part_id in &partitions {
                if manhattan
                    .exome_processing
                    .get(&part_id)
                    .map_or(false, |(w, _)| w == &req.worker_id)
                {
                    manhattan.exome_processing.remove(&part_id);
                    valid_exome.push(part_id);
                }

                if manhattan
                    .genome_processing
                    .get(&part_id)
                    .map_or(false, |(w, _)| w == &req.worker_id)
                {
                    manhattan.genome_processing.remove(&part_id);
                    valid_genome.push(part_id);
                }

                // If this was the aggregate task, mark it failed
                if manhattan.aggregate_dispatched && !manhattan.aggregate_complete {
                    println!(
                        "Aggregate task failed - job cannot complete without fixing the error"
                    );
                }
            }

            for &part_id in valid_exome.iter().rev() {
                manhattan.exome_pending.push_front(part_id);
            }
            for &part_id in valid_genome.iter().rev() {
                manhattan.genome_pending.push_front(part_id);
            }
        }

        // For ingestion jobs, mark all batch tasks as failed
        if let JobExecutionState::Ingestion(ref mut ingestion) = data.job_state {
            for t_id in task_ids {
                let mut is_owned = false;
                if let Some((_, _, _, worker_id, _)) = ingestion.active_tasks.get(t_id) {
                    if worker_id == &req.worker_id {
                        is_owned = true;
                    } else {
                        println!("Warning: Ignoring failure for ingestion task {} from {} (assigned to {})", t_id, req.worker_id, worker_id);
                    }
                }

                if is_owned {
                    if let Some((phenotype_id, ancestry, _base_path, _worker_id, _start_time)) =
                        ingestion.active_tasks.remove(t_id)
                    {
                        println!(
                            "Ingestion failed: {}/{} - {}",
                            phenotype_id,
                            ancestry,
                            req.error.as_deref().unwrap_or("unknown")
                        );
                        ingestion.failed_count += 1;
                    }
                }
            }
        }

        return axum::Json(CompleteResponse { acknowledged: true });
    }

    // Handle completion based on job type using the JobExecutionState enum
    let current_state = std::mem::take(&mut data.job_state);
    match current_state {
        JobExecutionState::Ingestion(mut ingestion) => {
            complete_ingestion_work(&mut data, &mut ingestion, &req);
            data.job_state = JobExecutionState::Ingestion(ingestion);
        }
        JobExecutionState::Batch(mut batch) => {
            complete_batch_work(&mut data, &mut batch, &req);
            data.job_state = JobExecutionState::Batch(batch);
        }
        JobExecutionState::Manhattan(mut manhattan) => {
            complete_manhattan_work(&mut manhattan, &req, &mut data.last_progress_time);
            data.job_state = JobExecutionState::Manhattan(manhattan);
        }
        JobExecutionState::Standard => {
            data.job_state = JobExecutionState::Standard;
            // Standard job completion
            for &part_id in &partitions {
                let mut valid_completion = false;

                if let Some((worker_id, _)) = data.processing_partitions.get(&part_id) {
                    if worker_id == &req.worker_id {
                        valid_completion = true;
                    } else {
                        println!(
                            "Warning: task {} completed by {} but is currently assigned to {}",
                            part_id, req.worker_id, worker_id
                        );
                        // It was reassigned, but the slow worker finished it. We still count it as done.
                    }
                } else {
                    println!(
                        "Warning: task {} completed by {} but wasn't in processing map",
                        part_id, req.worker_id
                    );
                }

                if valid_completion {
                    data.processing_partitions.remove(&part_id);
                }

                // Mark as complete regardless of ownership (work is done!)
                if !data.completed_tasks.contains(&part_id) {
                    data.completed_tasks.insert(part_id);
                    // Update progress timestamp (R3)
                    data.last_progress_time = Instant::now();
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
    data.touch_worker(&req.worker_id, None, None);
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
            details: format!(
                "Completed tasks {:?} ({} rows)",
                task_ids, req.items_processed
            ),
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

    // Check for job type using the JobExecutionState enum
    let (pending, processing, completed, total, is_complete) = match &data.job_state {
        JobExecutionState::Batch(batch) => {
            let total = batch.total_phenotypes;
            let completed = batch.completed_count;
            let pending = batch.pending_queue.len();
            let processing = batch.active_phenotypes.len() + batch.ready_to_aggregate.len();
            let is_complete = batch.pending_queue.is_empty()
                && batch.active_phenotypes.is_empty()
                && batch.ready_to_aggregate.is_empty()
                && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;
            (pending, processing, completed, total, is_complete)
        }
        JobExecutionState::Manhattan(m) => {
            let total_parts = m.exome_total_tasks + m.genome_total_tasks;
            let completed_parts = m.exome_completed.len() + m.genome_completed.len();
            let processing_parts = m.exome_processing.len() + m.genome_processing.len();
            let pending_parts = m.exome_pending.len() + m.genome_pending.len();

            // Add aggregate phase (+1 task)
            let total = total_parts + 1;
            let completed = completed_parts + if m.aggregate_complete { 1 } else { 0 };
            let processing = processing_parts
                + if m.aggregate_dispatched && !m.aggregate_complete {
                    1
                } else {
                    0
                };
            let pending = pending_parts
                + if !m.aggregate_dispatched && m.phase == ManhattanPhase::Aggregate {
                    1
                } else {
                    0
                };
            let is_complete = m.phase == ManhattanPhase::Complete;

            (pending, processing, completed, total, is_complete)
        }
        JobExecutionState::Ingestion(ing) => {
            let is_complete = ing.pending_tasks.is_empty()
                && ing.active_tasks.is_empty()
                && (ing.completed_count + ing.failed_count) == ing.total_tasks;
            (
                ing.pending_tasks.len(),
                ing.active_tasks.len(),
                ing.completed_count,
                ing.total_tasks,
                is_complete,
            )
        }
        JobExecutionState::Standard => {
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
        }
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

    if matches!(data.config.job_spec, Some(JobSpec::Custom { .. })) {
        let mut expected_tasks: Vec<String> = data
            .custom_assignments
            .iter()
            .filter(|(_, assignment)| assignment.worker_id == req.worker_id)
            .map(|(task_id, _)| task_id.clone())
            .collect();
        expected_tasks.sort();
        if !expected_tasks.is_empty() || !req.assignments.is_empty() {
            if req.session_id.as_deref() != Some(data.session_id.as_str())
                || data
                    .validate_custom_assignments(
                        req.session_id.as_deref(),
                        &req.worker_id,
                        &expected_tasks,
                        &req.assignments,
                    )
                    .is_err()
            {
                return axum::Json(HeartbeatResponse {
                    acknowledged: false,
                });
            }
        }
    }

    data.touch_worker(&req.worker_id, None, req.build_version.clone());

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
        // If memory usage exceeds 80%, aggressively slash batch size to prevent OOM
        if let (Some(used), Some(total)) = (
            req.telemetry.memory_used_bytes,
            req.telemetry.memory_total_bytes,
        ) {
            if total > 0 {
                let mem_usage_pct = (used as f64 / total as f64) * 100.0;
                if mem_usage_pct > 80.0 {
                    // Aggressively slash batch size to prevent OOM
                    let current_batch = w.current_batch_size.unwrap_or(default_batch_size);
                    let new_batch = (current_batch / 2).max(1);
                    if new_batch < current_batch {
                        println!(
                            "Worker {} memory usage at {:.1}%. Reducing batch size from {} to {}",
                            req.worker_id, mem_usage_pct, current_batch, new_batch
                        );

                        // Collect event info to log after releasing worker borrow
                        batch_reduction_event =
                            Some((req.worker_id.clone(), new_batch, mem_usage_pct));

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
            details: format!(
                "Reduced batch size to {} (Memory at {:.1}%)",
                new_batch, mem_pct
            ),
        });
    }

    // Persist to SQLite (fire-and-forget, don't block on DB errors)
    let job_id = data.current_job_id.clone();
    if let Err(e) = data.metrics_db.insert_snapshot_with_job_id(
        &req.worker_id,
        &req.telemetry,
        job_id.as_deref(),
    ) {
        eprintln!("Warning: failed to persist metrics to DB: {}", e);
    }

    axum::Json(HeartbeatResponse { acknowledged: true })
}

#[cfg(test)]
mod lease_coordinator_tests {
    use super::*;
    use crate::distributed::message::{
        AssignmentLease, CancelRequest, CompleteResponse, HardwareSpec, HeartbeatResponse,
        JobRecord, TelemetrySnapshot,
    };
    use axum::{
        routing::{get, post},
        Router,
    };

    fn custom_job(tasks: usize) -> JobSpec {
        JobSpec::Custom {
            payload: serde_json::json!({"test": true}),
            tasks,
            manifest: None,
        }
    }

    fn test_state(tasks: usize) -> SharedState {
        let mut config = CoordinatorConfig::default();
        config.job_spec = Some(custom_job(tasks));
        config.total_tasks = tasks;
        config.batch_size = 1;
        let metrics_db = MetricsDb::in_memory().unwrap();
        metrics_db
            .insert_job(&JobRecord {
                job_id: "job-1".to_string(),
                status: "running".to_string(),
                start_time_ms: 1,
                end_time_ms: None,
                job_spec_json: serde_json::to_value(custom_job(tasks)).ok(),
                input_path: "input".to_string(),
                total_tasks: tasks,
                job_type: Some("custom".to_string()),
            })
            .unwrap();
        Arc::new(Mutex::new(CoordinatorData {
            pending_partitions: (0..tasks).collect(),
            processing_partitions: HashMap::new(),
            completed_tasks: HashSet::new(),
            config,
            total_rows: 0,
            scan_cpu_secs: 0.0,
            aggregate_cpu_secs: 0.0,
            wasted_cpu_secs: 0.0,
            retry_counts: HashMap::new(),
            custom_assignment_attempts: HashMap::new(),
            custom_assignments: HashMap::new(),
            failed_partitions: HashSet::new(),
            worker_registry: HashMap::new(),
            job_start_time: Instant::now(),
            last_progress_time: Instant::now(),
            idle: false,
            metrics_db,
            aggregated_results: Vec::new(),
            job_state: JobExecutionState::Standard,
            active_tasks: HashMap::new(),
            last_error: None,
            events: VecDeque::new(),
            failures: VecDeque::new(),
            events_since_backup: 0,
            last_backup_at: None,
            update_fleet_url: None,
            updated_workers: HashSet::new(),
            current_job_id: Some("job-1".to_string()),
            session_id: "session-1".to_string(),
            catalog: None,
            ingested_phenotypes: HashSet::new(),
            completed_phenotypes: HashSet::new(),
            last_completed_batch: None,
            cached_vms: None,
            deleted_workers: HashSet::new(),
        }))
    }

    async fn assign(state: &SharedState, worker_id: &str) -> (String, String, AssignmentLease) {
        let response = get_work(
            axum::extract::State(state.clone()),
            axum::Json(WorkRequest {
                worker_id: worker_id.to_string(),
                hardware: None,
                build_version: Some("test-build".to_string()),
                protocol_version: Some(CUSTOM_WORKER_PROTOCOL_VERSION),
            }),
        )
        .await
        .0;
        match response {
            WorkResponse::Task {
                tasks, session_id, ..
            } => {
                assert_eq!(tasks.len(), 1);
                let task = tasks.into_iter().next().unwrap();
                let lease = task.assignment_lease().expect("custom task lease");
                (task.id, session_id.expect("coordinator session"), lease)
            }
            other => panic!("expected task assignment, got {other:?}"),
        }
    }

    fn completion(
        worker_id: &str,
        task_id: &str,
        session_id: &str,
        lease: AssignmentLease,
        error: Option<&str>,
    ) -> CompleteRequest {
        CompleteRequest {
            worker_id: worker_id.to_string(),
            tasks: vec![task_id.to_string()],
            items_processed: if error.is_none() { 17 } else { 0 },
            result_json: error
                .is_none()
                .then(|| serde_json::json!({"task": task_id})),
            error: error.map(str::to_string),
            session_id: Some(session_id.to_string()),
            assignments: vec![lease],
        }
    }

    #[derive(Debug, PartialEq)]
    struct StateSignature {
        pending: Vec<usize>,
        processing: Vec<(usize, String)>,
        completed: Vec<usize>,
        attempts: Vec<(String, u64)>,
        assignments: Vec<(String, usize, String, u64, String)>,
        retry_counts: Vec<(usize, usize)>,
        failed: Vec<usize>,
        total_rows: usize,
        wasted_cpu_bits: u64,
        aggregated_results: Vec<serde_json::Value>,
        last_error: Option<String>,
        events: usize,
        failures: usize,
        last_progress_time: Instant,
        workers: Vec<(
            String,
            Instant,
            usize,
            usize,
            usize,
            Option<String>,
            Option<String>,
        )>,
    }

    fn signature(state: &SharedState) -> StateSignature {
        let data = state.lock().unwrap();
        let mut processing: Vec<_> = data
            .processing_partitions
            .iter()
            .map(|(id, (worker, _))| (*id, worker.clone()))
            .collect();
        processing.sort();
        let mut completed: Vec<_> = data.completed_tasks.iter().copied().collect();
        completed.sort();
        let mut attempts: Vec<_> = data
            .custom_assignment_attempts
            .iter()
            .map(|(task, attempt)| (task.clone(), *attempt))
            .collect();
        attempts.sort();
        let mut assignments: Vec<_> = data
            .custom_assignments
            .iter()
            .map(|(task, assignment)| {
                (
                    task.clone(),
                    assignment.partition_id,
                    assignment.worker_id.clone(),
                    assignment.assignment_attempt,
                    assignment.lease_token.clone(),
                )
            })
            .collect();
        assignments.sort();
        let mut retry_counts: Vec<_> = data.retry_counts.iter().map(|(k, v)| (*k, *v)).collect();
        retry_counts.sort();
        let mut failed: Vec<_> = data.failed_partitions.iter().copied().collect();
        failed.sort();
        let mut workers: Vec<_> = data
            .worker_registry
            .iter()
            .map(|(id, worker)| {
                (
                    id.clone(),
                    worker.last_seen,
                    worker.metrics_history.len(),
                    worker.total_rows,
                    worker.partitions_completed,
                    worker.build_version.clone(),
                    worker
                        .current_task
                        .as_ref()
                        .map(|task| task.task_id.clone()),
                )
            })
            .collect();
        workers.sort_by(|a, b| a.0.cmp(&b.0));
        StateSignature {
            pending: data.pending_partitions.iter().copied().collect(),
            processing,
            completed,
            attempts,
            assignments,
            retry_counts,
            failed,
            total_rows: data.total_rows,
            wasted_cpu_bits: data.wasted_cpu_secs.to_bits(),
            aggregated_results: data.aggregated_results.clone(),
            last_error: data.last_error.clone(),
            events: data.events.len(),
            failures: data.failures.len(),
            last_progress_time: data.last_progress_time,
            workers,
        }
    }

    #[tokio::test]
    async fn valid_completion_cleans_current_assignment_and_updates_real_coordinator_state() {
        let state = test_state(1);
        let (task_id, session, lease) = assign(&state, "worker-a").await;
        let raw_lease = lease.lease_token.clone();
        let request = completion("worker-a", &task_id, &session, lease, None);

        // The same worker ID may report different mutable registry metadata,
        // but the outstanding durable assignment remains bound to build A.
        let replacement = get_work(
            axum::extract::State(state.clone()),
            axum::Json(WorkRequest {
                worker_id: "worker-a".to_string(),
                hardware: None,
                build_version: Some("replacement-build-b".to_string()),
                protocol_version: Some(CUSTOM_WORKER_PROTOCOL_VERSION),
            }),
        )
        .await;
        assert!(matches!(replacement.0, WorkResponse::Wait));

        let response = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(request.clone()),
        )
        .await;
        assert!(response.0.acknowledged);

        let accepted_signature = signature(&state);
        let duplicate = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(request.clone()),
        )
        .await;
        assert!(duplicate.0.acknowledged);
        assert_eq!(signature(&state), accepted_signature);

        let mut conflict = request;
        conflict.result_json = Some(serde_json::json!({"conflicting": true}));
        let rejected =
            complete_work(axum::extract::State(state.clone()), axum::Json(conflict)).await;
        assert!(!rejected.0.acknowledged);
        assert_eq!(signature(&state), accepted_signature);

        let data = state.lock().unwrap();
        assert!(data.custom_assignments.is_empty());
        assert!(data.processing_partitions.is_empty());
        assert_eq!(data.completed_tasks, HashSet::from([0]));
        assert_eq!(data.total_rows, 17);
        assert_eq!(
            data.aggregated_results,
            vec![serde_json::json!({"task": task_id})]
        );
        assert_eq!(data.custom_assignment_attempts.get("custom_0"), Some(&1));
        let receipts = data.metrics_db.get_custom_receipts("job-1").unwrap();
        assert!(receipts.complete);
        assert_eq!(receipts.accepted_count, 1);
        assert_eq!(receipts.terminal_receipt_count, 1);
        assert!(receipts.canonical_sha256.is_some());
        let machine_json = serde_json::to_string(&receipts).unwrap();
        assert!(!machine_json.contains(&raw_lease));
        assert_ne!(receipts.receipts[0].lease_identity_sha256, raw_lease);
        assert_eq!(
            receipts.receipts[0].worker_build_version.as_deref(),
            Some("test-build")
        );
        assert_eq!(
            data.worker_registry["worker-a"].build_version.as_deref(),
            Some("replacement-build-b")
        );
    }

    #[tokio::test]
    async fn failed_custom_cancellation_is_not_acknowledged_and_retains_live_state() {
        let state = test_state(1);
        let (task_id, session, lease) = assign(&state, "worker-a").await;
        let accepted = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(completion("worker-a", &task_id, &session, lease, None)),
        )
        .await;
        assert!(accepted.0.acknowledged);
        let before = signature(&state);
        state
            .lock()
            .unwrap()
            .metrics_db
            .execute_test_sql(
                "CREATE TEMP TRIGGER fail_cancel BEFORE UPDATE OF status ON jobs
             WHEN NEW.status = 'cancelled' BEGIN SELECT RAISE(FAIL, 'injected'); END;",
            )
            .unwrap();

        let cancelled = api::jobs::cancel_job(
            axum::extract::State(state.clone()),
            axum::Json(CancelRequest {
                reason: Some("fault".to_string()),
            }),
        )
        .await;
        assert!(!cancelled.0.success);
        assert_eq!(signature(&state), before);
        let data = state.lock().unwrap();
        assert!(!data.idle);
        let receipts = data.metrics_db.get_custom_receipts("job-1").unwrap();
        assert_eq!(receipts.job_status.as_deref(), Some("running"));
        assert!(receipts.complete);
    }

    #[test]
    fn restored_running_custom_jobs_fail_closed_before_startup_serves() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let backup_path = dir.path().join("older-valid-backup.db");
        let restored_path = dir.path().join("restored.db");
        let source = MetricsDb::open(&source_path).unwrap();

        for job_id in ["job-accepted", "job-assigned"] {
            source
                .insert_job(&JobRecord {
                    job_id: job_id.to_string(),
                    status: "running".to_string(),
                    start_time_ms: 1,
                    end_time_ms: None,
                    job_spec_json: serde_json::to_value(custom_job(1)).ok(),
                    input_path: "input".to_string(),
                    total_tasks: 1,
                    job_type: Some(custom_job(1).description().to_string()),
                })
                .unwrap();
        }

        source
            .persist_custom_assignments(
                "job-accepted",
                "old-session",
                "worker-accepted",
                Some("assignment-build-a"),
                &[DurableCustomAssignment {
                    task_id: "custom_0",
                    partition_id: 0,
                    assignment_attempt: 1,
                    lease_token: "accepted-lease",
                }],
                10,
            )
            .unwrap();
        source
            .accept_custom_completion(
                "job-accepted",
                &completion(
                    "worker-accepted",
                    "custom_0",
                    "old-session",
                    AssignmentLease {
                        task_id: "custom_0".to_string(),
                        assignment_attempt: 1,
                        lease_token: "accepted-lease".to_string(),
                    },
                    None,
                ),
                20,
            )
            .unwrap();
        assert!(source.get_custom_receipts("job-accepted").unwrap().complete);

        source
            .persist_custom_assignments(
                "job-assigned",
                "old-session",
                "worker-assigned",
                Some("assignment-build-b"),
                &[DurableCustomAssignment {
                    task_id: "custom_0",
                    partition_id: 0,
                    assignment_attempt: 1,
                    lease_token: "current-lease",
                }],
                30,
            )
            .unwrap();
        let stale_completion = completion(
            "worker-assigned",
            "custom_0",
            "old-session",
            AssignmentLease {
                task_id: "custom_0".to_string(),
                assignment_attempt: 1,
                lease_token: "current-lease".to_string(),
            },
            None,
        );
        source.write_verified_backup_snapshot(&backup_path).unwrap();
        drop(source);

        let restored = restore_and_reconcile_database(restored_path.to_str().unwrap(), |dest| {
            std::fs::copy(&backup_path, dest)
                .map(|_| true)
                .map_err(|error| error.to_string())
        })
        .unwrap();
        assert!(restored);

        let db = MetricsDb::open(&restored_path).unwrap();
        let accepted = db.get_custom_receipts("job-accepted").unwrap();
        assert_eq!(accepted.job_status.as_deref(), Some("failed"));
        assert_eq!(accepted.accepted_count, 1);
        assert_eq!(accepted.expected_task_count, 1);
        assert!(!accepted.complete);
        assert_eq!(
            accepted.receipts[0].worker_build_version.as_deref(),
            Some("assignment-build-a")
        );
        assert_eq!(
            db.current_custom_assignment_count("job-assigned").unwrap(),
            0
        );
        assert!(db
            .accept_custom_completion("job-assigned", &stale_completion, 40)
            .unwrap_err()
            .contains("not durably running"));
        let summary = db
            .get_job_summary("job-accepted")
            .unwrap()
            .expect("restore interruption summary");
        assert!(summary.contains("interrupted"));
        assert!(summary.contains("not completion authority"));
    }

    #[test]
    fn restore_reconciliation_failure_is_fatal_and_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let backup_path = dir.path().join("backup.db");
        let restored_path = dir.path().join("restored.db");
        let source = MetricsDb::open(&source_path).unwrap();
        source
            .insert_job(&JobRecord {
                job_id: "job-blocked".to_string(),
                status: "running".to_string(),
                start_time_ms: 1,
                end_time_ms: None,
                job_spec_json: serde_json::to_value(custom_job(1)).ok(),
                input_path: "input".to_string(),
                total_tasks: 1,
                job_type: Some(custom_job(1).description().to_string()),
            })
            .unwrap();
        source
            .persist_custom_assignments(
                "job-blocked",
                "old-session",
                "worker-a",
                Some("build-a"),
                &[DurableCustomAssignment {
                    task_id: "custom_0",
                    partition_id: 0,
                    assignment_attempt: 1,
                    lease_token: "current-lease",
                }],
                10,
            )
            .unwrap();
        source
            .execute_test_sql(
                "CREATE TRIGGER block_restore_reconciliation
                 BEFORE UPDATE OF status ON jobs
                 WHEN OLD.job_id = 'job-blocked'
                 BEGIN SELECT RAISE(FAIL, 'injected restore failure'); END;",
            )
            .unwrap();
        source.write_verified_backup_snapshot(&backup_path).unwrap();
        drop(source);

        let error = restore_and_reconcile_database(restored_path.to_str().unwrap(), |dest| {
            std::fs::copy(&backup_path, dest)
                .map(|_| true)
                .map_err(|error| error.to_string())
        })
        .unwrap_err();
        assert!(error.contains("failed to reconcile restored custom jobs"));
        assert!(error.contains("injected restore failure"));

        let db = MetricsDb::open(&restored_path).unwrap();
        assert_eq!(
            db.get_custom_receipts("job-blocked")
                .unwrap()
                .job_status
                .as_deref(),
            Some("running")
        );
        assert_eq!(
            db.current_custom_assignment_count("job-blocked").unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn stale_completion_after_timeout_and_reassignment_mutates_no_state() {
        let state = test_state(1);
        let (task_id, session, stale_lease) = assign(&state, "worker-old").await;
        {
            let mut data = state.lock().unwrap();
            data.processing_partitions.get_mut(&0).unwrap().1 =
                Instant::now() - Duration::from_secs(10);
        }
        check_timeouts(&state, 1);
        let (_, _, fresh_lease) = assign(&state, "worker-new").await;
        assert_eq!(
            fresh_lease.assignment_attempt,
            stale_lease.assignment_attempt + 1
        );
        assert_ne!(fresh_lease.lease_token, stale_lease.lease_token);

        let before = signature(&state);
        let response = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(completion(
                "worker-old",
                &task_id,
                &session,
                stale_lease,
                None,
            )),
        )
        .await;
        assert!(!response.0.acknowledged);
        assert_eq!(signature(&state), before);
        assert_eq!(
            state
                .lock()
                .unwrap()
                .metrics_db
                .get_custom_receipts("job-1")
                .unwrap()
                .terminal_receipt_count,
            0
        );
    }

    #[tokio::test]
    async fn cancelled_and_legacy_unfenced_custom_completions_create_no_receipt() {
        let cancelled_state = test_state(1);
        let (task_id, session, lease) = assign(&cancelled_state, "worker-a").await;
        let cancelled = api::jobs::cancel_job(
            axum::extract::State(cancelled_state.clone()),
            axum::Json(CancelRequest {
                reason: Some("test cancellation".to_string()),
            }),
        )
        .await;
        assert!(cancelled.0.success);
        let response = complete_work(
            axum::extract::State(cancelled_state.clone()),
            axum::Json(completion("worker-a", &task_id, &session, lease, None)),
        )
        .await;
        assert!(!response.0.acknowledged);
        let cancelled_receipts = cancelled_state
            .lock()
            .unwrap()
            .metrics_db
            .get_custom_receipts("job-1")
            .unwrap();
        assert_eq!(cancelled_receipts.terminal_receipt_count, 0);
        assert!(!cancelled_receipts.complete);

        let legacy_state = test_state(1);
        let (task_id, _, _) = assign(&legacy_state, "legacy-worker").await;
        let legacy = CompleteRequest {
            worker_id: "legacy-worker".to_string(),
            tasks: vec![task_id],
            items_processed: 1,
            result_json: Some(serde_json::json!({"unsafe": true})),
            error: None,
            session_id: None,
            assignments: Vec::new(),
        };
        let response = complete_work(
            axum::extract::State(legacy_state.clone()),
            axum::Json(legacy),
        )
        .await;
        assert!(!response.0.acknowledged);
        assert_eq!(
            legacy_state
                .lock()
                .unwrap()
                .metrics_db
                .get_custom_receipts("job-1")
                .unwrap()
                .terminal_receipt_count,
            0
        );
    }

    async fn assert_rejected_heartbeat_unchanged(state: &SharedState, req: HeartbeatRequest) {
        let before = signature(state);
        let response = handle_heartbeat(axum::extract::State(state.clone()), axum::Json(req)).await;
        assert!(!response.0.acknowledged);
        assert_eq!(signature(state), before, "rejected heartbeat mutated state");
    }

    fn heartbeat(
        worker_id: &str,
        session_id: Option<&str>,
        lease: AssignmentLease,
    ) -> HeartbeatRequest {
        let mut telemetry = TelemetrySnapshot::empty();
        telemetry.cpu_percent = Some(99.0);
        HeartbeatRequest {
            worker_id: worker_id.to_string(),
            telemetry,
            build_version: Some("rejected-build".to_string()),
            session_id: session_id.map(str::to_string),
            assignments: vec![lease],
        }
    }

    #[tokio::test]
    async fn invalid_heartbeats_do_not_refresh_liveness_or_mutate_telemetry() {
        let state = test_state(1);
        let (_, session, lease) = assign(&state, "worker-a").await;

        assert_rejected_heartbeat_unchanged(&state, heartbeat("worker-a", None, lease.clone()))
            .await;
        assert_rejected_heartbeat_unchanged(
            &state,
            heartbeat("worker-a", Some("stale-session"), lease.clone()),
        )
        .await;

        let mut wrong_attempt = lease.clone();
        wrong_attempt.assignment_attempt += 1;
        assert_rejected_heartbeat_unchanged(
            &state,
            heartbeat("worker-a", Some(&session), wrong_attempt),
        )
        .await;

        let mut wrong_token = lease.clone();
        wrong_token.lease_token = "wrong-token".to_string();
        assert_rejected_heartbeat_unchanged(
            &state,
            heartbeat("worker-a", Some(&session), wrong_token),
        )
        .await;
        assert_rejected_heartbeat_unchanged(
            &state,
            heartbeat("worker-b", Some(&session), lease.clone()),
        )
        .await;

        {
            let mut data = state.lock().unwrap();
            data.processing_partitions.get_mut(&0).unwrap().1 =
                Instant::now() - Duration::from_secs(10);
        }
        check_timeouts(&state, 1);
        let _ = assign(&state, "worker-b").await;
        assert_rejected_heartbeat_unchanged(&state, heartbeat("worker-a", Some(&session), lease))
            .await;
    }

    #[tokio::test]
    async fn stock_cli_preflight_and_fenced_reports_use_real_http_endpoints() {
        let state = test_state(2);
        let app = Router::new()
            .route("/work", post(get_work))
            .route("/complete", post(complete_work))
            .route("/heartbeat", post(handle_heartbeat))
            .route(
                "/api/jobs/:job_id/custom-receipts",
                get(api::jobs::get_custom_receipts),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let base_url = format!("http://{address}");

        // Exercise the exact request built by the stock CLI worker. Because that
        // worker has no arbitrary-custom-payload handler, it must fail preflight
        // without registering itself or consuming an assignment.
        let stock_request = crate::distributed::worker::build_work_request(
            "stock-cli",
            &HardwareSpec {
                num_cores: 4,
                total_memory_mb: 8192,
            },
        );
        assert_eq!(stock_request.protocol_version, None);
        let before_preflight = signature(&state);
        let preflight: WorkResponse = client
            .post(format!("{base_url}/work"))
            .json(&stock_request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(matches!(
            preflight,
            WorkResponse::Incompatible {
                required_protocol_version: CUSTOM_WORKER_PROTOCOL_VERSION,
                ..
            }
        ));
        assert_eq!(signature(&state), before_preflight);

        // A handler-backed protocol-v1 worker receives a fenced assignment over
        // the same endpoint and must echo the exact identity on both report paths.
        let capable_request = WorkRequest {
            worker_id: "capable-worker".to_string(),
            hardware: None,
            build_version: Some("integration-test".to_string()),
            protocol_version: Some(CUSTOM_WORKER_PROTOCOL_VERSION),
        };
        let assignment: WorkResponse = client
            .post(format!("{base_url}/work"))
            .json(&capable_request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let (task_id, session, lease) = match assignment {
            WorkResponse::Task {
                tasks, session_id, ..
            } => {
                let task = tasks.into_iter().next().unwrap();
                let lease = task.assignment_lease().unwrap();
                (task.id, session_id.unwrap(), lease)
            }
            other => panic!("expected task assignment, got {other:?}"),
        };

        let accepted_heartbeat: HeartbeatResponse = client
            .post(format!("{base_url}/heartbeat"))
            .json(&heartbeat("capable-worker", Some(&session), lease.clone()))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(accepted_heartbeat.acknowledged);

        let mut wrong_lease = lease.clone();
        wrong_lease.lease_token = "wrong-token".to_string();
        let rejected_heartbeat: HeartbeatResponse = client
            .post(format!("{base_url}/heartbeat"))
            .json(&heartbeat(
                "capable-worker",
                Some(&session),
                wrong_lease.clone(),
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!rejected_heartbeat.acknowledged);

        let stale_completion = completion(
            "capable-worker",
            &task_id,
            "stale-session",
            lease.clone(),
            None,
        );
        let rejected_stale: CompleteResponse = client
            .post(format!("{base_url}/complete"))
            .json(&stale_completion)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!rejected_stale.acknowledged);

        let rejected_wrong: CompleteResponse = client
            .post(format!("{base_url}/complete"))
            .json(&completion(
                "capable-worker",
                &task_id,
                &session,
                wrong_lease,
                None,
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!rejected_wrong.acknowledged);

        let accepted_completion: CompleteResponse = client
            .post(format!("{base_url}/complete"))
            .json(&completion(
                "capable-worker",
                &task_id,
                &session,
                lease,
                None,
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(accepted_completion.acknowledged);
        assert_eq!(state.lock().unwrap().completed_tasks, HashSet::from([0]));

        let receipt_set: crate::distributed::message::CustomReceiptSet = client
            .get(format!("{base_url}/api/jobs/job-1/custom-receipts"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(receipt_set.job_id, "job-1");
        assert_eq!(receipt_set.accepted_count, 1);
        assert_eq!(receipt_set.receipts[0].task_id, task_id);
        let missing: crate::distributed::message::CustomReceiptSet = client
            .get(format!("{base_url}/api/jobs/other-job/custom-receipts"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!missing.job_found);
        assert_eq!(missing.accepted_count, 0);
        assert!(missing.receipts.is_empty());
        assert!(missing.error.is_some());

        server.abort();
    }

    #[tokio::test]
    async fn every_failure_death_and_timeout_requeue_gets_monotonic_fresh_identity() {
        let state = test_state(1);
        let (task_id, session, first) = assign(&state, "worker-1").await;
        let failed = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(completion(
                "worker-1",
                &task_id,
                &session,
                first.clone(),
                Some("explicit failure"),
            )),
        )
        .await;
        assert!(failed.0.acknowledged);
        assert!(state.lock().unwrap().custom_assignments.is_empty());

        let (_, _, second) = assign(&state, "worker-2").await;
        state.lock().unwrap().requeue_worker_tasks("worker-2");
        assert!(state.lock().unwrap().custom_assignments.is_empty());

        let (_, _, third) = assign(&state, "worker-3").await;
        {
            let mut data = state.lock().unwrap();
            data.processing_partitions.get_mut(&0).unwrap().1 =
                Instant::now() - Duration::from_secs(10);
        }
        check_timeouts(&state, 1);
        assert!(state.lock().unwrap().custom_assignments.is_empty());
        let (_, _, fourth) = assign(&state, "worker-4").await;

        assert_eq!(
            [
                first.assignment_attempt,
                second.assignment_attempt,
                third.assignment_attempt,
                fourth.assignment_attempt,
            ],
            [1, 2, 3, 4]
        );
        let accepted = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(completion(
                "worker-4",
                &task_id,
                &session,
                fourth.clone(),
                None,
            )),
        )
        .await;
        assert!(accepted.0.acknowledged);
        let receipt_set = state
            .lock()
            .unwrap()
            .metrics_db
            .get_custom_receipts("job-1")
            .unwrap();
        assert!(receipt_set.complete);
        assert_eq!(receipt_set.accepted_count, 1);
        assert_eq!(receipt_set.failed_attempt_count, 1);
        assert_eq!(receipt_set.terminal_receipt_count, 2);
        assert_eq!(
            receipt_set
                .receipts
                .iter()
                .map(|receipt| (receipt.assignment_attempt, receipt.terminal_status.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "failed"), (4, "accepted")]
        );

        let tokens: HashSet<_> = [first, second, third, fourth]
            .into_iter()
            .map(|lease| lease.lease_token)
            .collect();
        assert_eq!(tokens.len(), 4);
    }

    #[tokio::test]
    async fn restart_cancel_and_supersede_paths_reject_or_clean_fenced_state() {
        let state = test_state(1);
        let (task_id, old_session, lease) = assign(&state, "worker-a").await;
        state.lock().unwrap().session_id = "session-2".to_string();
        let before = signature(&state);
        let stale = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(completion("worker-a", &task_id, &old_session, lease, None)),
        )
        .await;
        assert!(!stale.0.acknowledged);
        assert_eq!(signature(&state), before);

        let cancelled = api::jobs::cancel_job(
            axum::extract::State(state.clone()),
            axum::Json(CancelRequest {
                reason: Some("test".to_string()),
            }),
        )
        .await;
        assert!(cancelled.0.success);
        {
            let data = state.lock().unwrap();
            assert!(data.custom_assignments.is_empty());
            assert!(data.custom_assignment_attempts.is_empty());
        }

        {
            let mut data = state.lock().unwrap();
            data.idle = false;
            data.custom_assignment_attempts.insert("old".to_string(), 9);
            data.custom_assignments.insert(
                "old".to_string(),
                state::CustomAssignment {
                    partition_id: 9,
                    worker_id: "old-worker".to_string(),
                    assignment_attempt: 9,
                    lease_token: "old-token".to_string(),
                },
            );
            services::start_new_job(
                &mut data,
                custom_job(2),
                "input".to_string(),
                2,
                Some(1),
                None,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            assert!(data.custom_assignments.is_empty());
            assert!(data.custom_assignment_attempts.is_empty());
            assert_eq!(
                data.pending_partitions.iter().copied().collect::<Vec<_>>(),
                vec![0, 1]
            );
        }
    }

    #[tokio::test]
    async fn superseded_job_completion_cannot_create_a_receipt_in_either_job() {
        let state = test_state(1);
        let (task_id, session, lease) = assign(&state, "old-worker").await;
        let stale_request = completion("old-worker", &task_id, &session, lease, None);
        let new_job_id = {
            let mut data = state.lock().unwrap();
            services::start_new_job(
                &mut data,
                custom_job(1),
                "new-input".to_string(),
                1,
                Some(1),
                None,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            data.current_job_id.clone().unwrap()
        };
        let response = complete_work(
            axum::extract::State(state.clone()),
            axum::Json(stale_request),
        )
        .await;
        assert!(!response.0.acknowledged);
        let data = state.lock().unwrap();
        let old = data.metrics_db.get_custom_receipts("job-1").unwrap();
        let new = data.metrics_db.get_custom_receipts(&new_job_id).unwrap();
        assert_eq!(old.job_status.as_deref(), Some("superseded"));
        assert_eq!(old.terminal_receipt_count, 0);
        assert_eq!(new.terminal_receipt_count, 0);
    }

    #[tokio::test]
    async fn mixed_version_custom_worker_is_rejected_before_state_mutation() {
        let state = test_state(1);
        let legacy: WorkRequest = serde_json::from_value(serde_json::json!({
            "worker_id": "legacy-worker",
            "build_version": "old"
        }))
        .unwrap();
        assert_eq!(legacy.protocol_version, None);
        let before = signature(&state);
        let response = get_work(axum::extract::State(state.clone()), axum::Json(legacy))
            .await
            .0;
        assert!(matches!(
            response,
            WorkResponse::Incompatible {
                required_protocol_version: CUSTOM_WORKER_PROTOCOL_VERSION,
                ..
            }
        ));
        assert_eq!(signature(&state), before);

        let builtin = test_state(1);
        builtin.lock().unwrap().config.job_spec = Some(JobSpec::Summary);
        let response = get_work(
            axum::extract::State(builtin),
            axum::Json(WorkRequest {
                worker_id: "legacy-built-in".to_string(),
                hardware: None,
                build_version: None,
                protocol_version: None,
            }),
        )
        .await
        .0;
        assert!(matches!(response, WorkResponse::Task { .. }));
    }
}
