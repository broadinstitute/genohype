//! Job management API handlers.
//!
//! Handlers for job submission, cancellation, result retrieval,
//! metrics export, fleet management, and binary serving.

use crate::distributed::coordinator::services;
use crate::distributed::coordinator::state::{
    CoordinatorData, JobExecutionState, SharedState, WorkerStatus,
};
use crate::distributed::message::{
    CancelRequest, CancelResponse, EventsResponse, ExportMetricsRequest, ExportMetricsResponse,
    FailuresResponse, JobConfigRequest, JobConfigResponse, JobResultResponse,
    JobSpec, UpdateFleetRequest,
};

/// Query parameters for GET /api/events
#[derive(serde::Deserialize)]
pub(crate) struct EventsQuery {
    #[serde(default)]
    pub(crate) since_ms: u64,
}

/// Handler for POST /api/job - submit a job to an idle coordinator.
pub(crate) async fn submit_job(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<JobConfigRequest>,
) -> axum::Json<JobConfigResponse> {
    // Handle ExportClickhouse table creation BEFORE acquiring lock
    // This avoids holding MutexGuard across await points
    #[cfg(feature = "clickhouse")]
    if let JobSpec::ExportClickhouse {
        ref clickhouse_url,
        ref table_name,
    } = req.job_spec
    {
        use crate::export::{generate_create_table, ClickHouseClient};
        use genohype_core::query::QueryEngine;

        println!(
            "  Creating ClickHouse table '{}' before dispatching...",
            table_name
        );

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
            client
                .execute(&create_sql)
                .map_err(|e| format!("Failed to create table {}: {}", table_name_clone, e))?;

            println!("    Created table: {}", table_name_clone);
            Ok(())
        })
        .await;

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

    // ManhattanBatch, Manhattan, IngestManhattan, and Stress jobs don't require input_path
    let needs_input_path = !is_batch_job
        && !is_ingest_job
        && !matches!(&req.job_spec, JobSpec::Manhattan { .. })
        && !matches!(&req.job_spec, JobSpec::Stress(_));
    if req.input_path.is_empty() && needs_input_path {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("input_path is required".to_string()),
        });
    }

    // Schedule catalog auto-load in the background (don't block job submission).
    // Uses a plain OS thread because load_catalog_from_config does sync GCS I/O
    // via read_gcs_file, which creates its own tokio runtime.
    if let JobSpec::ManhattanBatch { ref config, .. } = req.job_spec {
        if let Some(job_config) = config {
            let state_clone = state.clone();
            let cfg = job_config.clone();
            std::thread::spawn(move || {
                match services::catalog::load_catalog_from_config(cfg) {
                    Ok((catalog, completed, ingested)) => {
                        println!("Auto-loaded catalog with {} phenotypes", catalog.entries.len());
                        let mut data = state_clone.lock().expect("state lock poisoned");
                        data.completed_phenotypes.extend(completed);
                        data.ingested_phenotypes.extend(ingested);
                        data.catalog = Some(catalog);
                    }
                    Err(e) => {
                        println!("Warning: Failed to auto-load catalog: {}", e);
                    }
                }
            });
        }
    }

    if let JobSpec::ManhattanBatch { ref specs, .. } = req.job_spec {
        if specs.is_empty() {
            // Idle batch: keep coordinator in idle mode so catalog UI can start jobs
            data.idle = true;
            println!("Received idle batch request. Catalog is loading in background...");
            return axum::Json(JobConfigResponse {
                acknowledged: true,
                error: None,
            });
        }
    }

    // Use the central job service to reset queues, persist to DB, and build states
    if let Err(e) = services::start_new_job(
        &mut data,
        req.job_spec.clone(),
        req.input_path.clone(),
        req.total_tasks,
        req.batch_size,
        req.memory_weight_mb,
        req.filters.clone(),
        req.intervals.clone(),
    ) {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some(e),
        });
    }

    // Handle IngestManhattan jobs (discover phenotypes and queue ingestion tasks)
    // We do this after start_new_job because it requires blocking I/O scanning GCS.
    if let JobSpec::IngestManhattan {
        ref input_dir,
        ref clickhouse_url,
        ref database,
        ref init_strategy,
        ref phenotypes,
    } = req.job_spec
    {
        println!(
            "Initializing Manhattan ingestion from {} to ClickHouse {} (init_strategy: {:?})",
            input_dir, clickhouse_url, init_strategy
        );

        // Execute DDL based on init_strategy (before dispatching tasks)
        #[cfg(feature = "clickhouse")]
        {
            if let Err(e) = services::init_clickhouse_tables(clickhouse_url, init_strategy) {
                return axum::Json(JobConfigResponse {
                    acknowledged: false,
                    error: Some(e),
                });
            }
        }

        match services::discover_phenotypes_for_ingestion(input_dir, phenotypes.as_deref()) {
            Ok(phenotypes) => {
                let total = phenotypes.len();
                println!("  Discovered {} phenotypes for ingestion", total);

                #[cfg(feature = "clickhouse")]
                {
                    data.job_state = JobExecutionState::Ingestion(services::create_ingestion_state(
                        phenotypes,
                        clickhouse_url,
                        database,
                    ));
                }
            }
            Err(e) => {
                return axum::Json(JobConfigResponse {
                    acknowledged: false,
                    error: Some(format!("Failed to discover phenotypes: {}", e)),
                });
            }
        }
    }

    // Log submission details
    if let JobSpec::ManhattanBatch { ref specs, .. } = req.job_spec {
        let output_desc = specs
            .first()
            .map(|s| s.output_path.clone())
            .unwrap_or_else(|| "(no output)".to_string());
        println!(
            "Job submitted: {} ({} phenotypes, output={})",
            req.job_spec.description(),
            specs.len(),
            output_desc
        );
    } else {
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
    }

    axum::Json(JobConfigResponse {
        acknowledged: true,
        error: None,
    })
}

/// Handler for POST /api/cancel - cancel the running job.
pub(crate) async fn cancel_job(
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

    // Update job status in database
    if let Some(ref job_id) = data.current_job_id {
        let end_time_ms = CoordinatorData::now_ms();
        if let Err(e) =
            data.metrics_db
                .update_job_status(job_id, "cancelled", Some(end_time_ms), None)
        {
            eprintln!("Warning: failed to update job status in DB: {}", e);
        }
    }

    // Reset job state
    data.pending_partitions.clear();
    data.processing_partitions.clear();
    data.job_state = JobExecutionState::Standard;
    data.active_tasks.clear();
    data.last_completed_batch = None;
    data.idle = true;
    // Note: We intentionally keep current_job_id so the dashboard continues
    // to display the cancelled job's metrics until a new job is submitted.

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
pub(crate) async fn get_job_result(
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

/// Handler for GET /api/events - get recent events.
pub(crate) async fn get_events(
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
pub(crate) async fn get_failures(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<FailuresResponse> {
    let data = state.lock().unwrap();
    let failures = data.failures.iter().cloned().collect();
    axum::Json(FailuresResponse { failures })
}

/// Handler for GET /api/workers/:worker_id/logs - get worker log tail.
pub(crate) async fn get_worker_logs(
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

/// Handler for POST /api/export-metrics - export metrics database to GCS.
///
/// This endpoint reads the SQLite metrics database and uploads it to a GCS path.
/// Used by `pool destroy --metrics-bucket` to save metrics before deleting VMs.
pub(crate) async fn export_metrics(
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
                error: Some(format!(
                    "Failed to read metrics database at {}: {}",
                    db_path, e
                )),
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

/// Handler for POST /api/update-fleet - trigger workers to self-update.
pub(crate) async fn update_fleet(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<UpdateFleetRequest>,
) -> impl axum::response::IntoResponse {
    let mut data = state.lock().unwrap();
    data.update_fleet_url = Some(req.gcs_url);
    data.updated_workers.clear();
    (axum::http::StatusCode::OK, "Fleet update initiated")
}

/// Handler for POST /api/update-coordinator - coordinator self-updates.
///
/// Downloads the new binary from GCS and exec()s itself to restart seamlessly.
pub(crate) async fn update_coordinator(
    axum::Json(req): axum::Json<UpdateFleetRequest>,
) -> impl axum::response::IntoResponse {
    use std::os::unix::process::CommandExt;

    println!(
        "Received UpdateCoordinator signal. Updating from {}...",
        req.gcs_url
    );

    // Download binary from GCS
    let status = std::process::Command::new("gsutil")
        .args(["cp", &req.gcs_url, "/tmp/genohype"])
        .status();

    match status {
        Ok(s) if s.success() => {}
        Ok(s) => {
            let msg = format!("gsutil cp failed with status: {}", s);
            eprintln!("{}", msg);
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg);
        }
        Err(e) => {
            let msg = format!("Failed to run gsutil: {}", e);
            eprintln!("{}", msg);
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg);
        }
    }

    // Make executable and move into place
    if let Err(e) = std::process::Command::new("chmod")
        .args(["+x", "/tmp/genohype"])
        .status()
    {
        let msg = format!("chmod failed: {}", e);
        eprintln!("{}", msg);
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg);
    }

    if let Err(e) = std::process::Command::new("sudo")
        .args(["mv", "/tmp/genohype", "/usr/local/bin/genohype"])
        .status()
    {
        let msg = format!("mv failed: {}", e);
        eprintln!("{}", msg);
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg);
    }

    println!("Binary updated successfully. Restarting coordinator...");

    // Spawn the restart in a background task so we can return the response first
    tokio::spawn(async move {
        // Small delay to allow response to be sent
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let args: Vec<String> = std::env::args().collect();
        let err = std::process::Command::new("/usr/local/bin/genohype")
            .args(&args[1..])
            .exec();

        // exec() only returns on error
        eprintln!("Failed to exec new binary: {}", err);
        std::process::exit(1);
    });

    (
        axum::http::StatusCode::OK,
        "Coordinator update initiated, restarting...".to_string(),
    )
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
pub(crate) const BINARY_INSTALL_PATH: &str = "/usr/local/bin/genohype";

pub(crate) async fn serve_binary() -> impl axum::response::IntoResponse {
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
        Ok(Ok((buffer, file_size))) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, file_size)
            .header(
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"genohype\"",
            )
            .body(axum::body::Body::from(buffer))
            .unwrap(),
        Ok(Err(e)) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::from(format!(
                "Failed to read binary: {}",
                e
            )))
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::from(format!("Task panicked: {}", e)))
            .unwrap(),
    }
}

/// Handler for POST /api/workers/:worker_id/reset-capacity
///
/// Resets the learned max_batch_capacity for a worker, allowing it to
/// probe for larger batch sizes again. Useful when a worker was throttled
/// due to a transient memory spike or anomalously large partition.
pub(crate) async fn reset_worker_capacity(
    axum::extract::Path(worker_id): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> impl axum::response::IntoResponse {
    let mut data = state.lock().unwrap();
    if let Some(w) = data.worker_registry.get_mut(&worker_id) {
        let old_cap = w.max_batch_capacity;
        w.max_batch_capacity = None;
        println!(
            "Reset max_batch_capacity for worker {} (was {:?})",
            worker_id, old_cap
        );
        return (
            axum::http::StatusCode::OK,
            axum::Json(serde_json::json!({ "success": true, "previous_capacity": old_cap })),
        );
    }
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({ "error": "Worker not found" })),
    )
}
