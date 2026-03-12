//! History API handlers.
//!
//! Handlers for retrieving historical job data from the metrics database.

use crate::distributed::coordinator::api::dashboard::build_dashboard_summary;
use crate::distributed::coordinator::state::{JobExecutionState, SharedState};
use crate::distributed::message::{
    BatchStatusResponse, DashboardMetrics, DashboardSummary, EventsResponse, FailuresResponse,
    JobRecord, PhenotypeStatus, WorkerMetricsSeries,
};
use axum::response::IntoResponse;

/// Handler for GET /api/history/jobs - list all historical jobs.
pub(crate) async fn get_history_jobs(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<Vec<JobRecord>> {
    let data = state.lock().unwrap();
    match data.metrics_db.get_jobs() {
        Ok(jobs) => axum::Json(jobs),
        Err(e) => {
            eprintln!("Warning: failed to fetch jobs from DB: {}", e);
            axum::Json(vec![])
        }
    }
}

/// Handler for GET /api/history/jobs/:job_id/summary - get job summary.
///
/// If job_id matches current_job_id, returns live data. Otherwise returns persisted summary.
pub(crate) async fn get_history_job_summary(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> axum::response::Response {
    let data = state.lock().unwrap();

    // If requesting the current active job, return live data
    if let Some(ref current_id) = data.current_job_id {
        if current_id == &job_id {
            let summary = build_dashboard_summary(&data);
            return axum::Json(summary).into_response();
        }
    }

    // Otherwise, fetch from database
    match data.metrics_db.get_job_summary(&job_id) {
        Ok(Some(json_str)) => {
            if let Ok(summary) = serde_json::from_str::<DashboardSummary>(&json_str) {
                return axum::Json(summary).into_response();
            }
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!("Warning: failed to fetch job summary from DB: {}", e);
        }
    }

    // If no persisted summary, try to construct a minimal one from the job record
    match data.metrics_db.get_job(&job_id) {
        Ok(Some(job)) => {
            let summary = DashboardSummary {
                progress_percent: if job.status == "completed" {
                    100.0
                } else {
                    0.0
                },
                total_tasks: job.total_tasks,
                batch_size: 1,
                completed_tasks: if job.status == "completed" {
                    job.total_tasks
                } else {
                    0
                },
                processing_tasks: 0,
                pending_tasks: if job.status == "completed" {
                    0
                } else {
                    job.total_tasks
                },
                failed_tasks: if job.status == "failed" {
                    job.total_tasks
                } else {
                    0
                },
                total_items: 0,
                cluster_items_per_sec: 0.0,
                elapsed_secs: job
                    .end_time_ms
                    .map(|e| (e - job.start_time_ms) as f64 / 1000.0)
                    .unwrap_or(0.0),
                eta_secs: None,
                is_complete: job.status == "completed",
                input_path: job.input_path,
                job_spec: job.job_spec_json.and_then(|v| serde_json::from_value(v).ok()),
                idle: true,
                last_error: if job.status == "failed" {
                    Some("Job failed".to_string())
                } else {
                    None
                },
                batch_progress: None,
                build_version: None,
                scan_cpu_secs: 0.0,
                aggregate_cpu_secs: 0.0,
                wasted_cpu_secs: 0.0,
                db_path: None,
                backup_path: None,
                last_backup_at: None,
            };
            axum::Json(summary).into_response()
        }
        _ => (axum::http::StatusCode::NOT_FOUND, "Job not found").into_response(),
    }
}

/// Handler for GET /api/history/jobs/:job_id/metrics - get job metrics.
pub(crate) async fn get_history_job_metrics(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> axum::Json<DashboardMetrics> {
    let data = state.lock().unwrap();

    // If requesting the current active job, return live data
    if let Some(ref current_id) = data.current_job_id {
        if current_id == &job_id {
            let workers: Vec<WorkerMetricsSeries> = data
                .worker_registry
                .iter()
                .map(|(id, state)| WorkerMetricsSeries {
                    worker_id: id.clone(),
                    snapshots: state.metrics_history.iter().cloned().collect(),
                })
                .collect();
            return axum::Json(DashboardMetrics { workers });
        }
    }

    // Fetch from database
    match data.metrics_db.get_job_metrics(&job_id) {
        Ok(worker_data) => {
            let workers = worker_data
                .into_iter()
                .map(|(worker_id, snapshots)| WorkerMetricsSeries {
                    worker_id,
                    snapshots,
                })
                .collect();
            axum::Json(DashboardMetrics { workers })
        }
        Err(e) => {
            eprintln!("Warning: failed to fetch job metrics from DB: {}", e);
            axum::Json(DashboardMetrics { workers: vec![] })
        }
    }
}

/// Handler for GET /api/history/jobs/:job_id/events - get job events.
pub(crate) async fn get_history_job_events(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> axum::Json<EventsResponse> {
    let data = state.lock().unwrap();

    // If requesting the current active job, return live data
    if let Some(ref current_id) = data.current_job_id {
        if current_id == &job_id {
            return axum::Json(EventsResponse {
                events: data.events.iter().cloned().collect(),
            });
        }
    }

    // Fetch from database
    match data.metrics_db.get_job_events(&job_id) {
        Ok(events) => axum::Json(EventsResponse { events }),
        Err(e) => {
            eprintln!("Warning: failed to fetch job events from DB: {}", e);
            axum::Json(EventsResponse { events: vec![] })
        }
    }
}

/// Handler for GET /api/history/jobs/:job_id/failures - get job failures.
pub(crate) async fn get_history_job_failures(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> axum::Json<FailuresResponse> {
    let data = state.lock().unwrap();

    // If requesting the current active job, return live data
    if let Some(ref current_id) = data.current_job_id {
        if current_id == &job_id {
            return axum::Json(FailuresResponse {
                failures: data.failures.iter().cloned().collect(),
            });
        }
    }

    // Fetch from database
    match data.metrics_db.get_job_failures(&job_id) {
        Ok(failures) => axum::Json(FailuresResponse { failures }),
        Err(e) => {
            eprintln!("Warning: failed to fetch job failures from DB: {}", e);
            axum::Json(FailuresResponse { failures: vec![] })
        }
    }
}

/// Handler for DELETE /api/history/jobs/:job_id - delete a job and all its data.
///
/// If the job being deleted is the currently active/displayed job, the context is cleared.
pub(crate) async fn delete_history_job(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> axum::response::Response {
    let mut data = state.lock().unwrap();

    // If deleting the currently active/displayed job, clear the context
    if data.current_job_id.as_deref() == Some(job_id.as_str()) {
        data.current_job_id = None;
    }

    match data.metrics_db.delete_job(&job_id) {
        Ok(_) => (axum::http::StatusCode::OK, "Job deleted").into_response(),
        Err(e) => {
            eprintln!("Warning: failed to delete job {}: {}", job_id, e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete job: {}", e),
            )
                .into_response()
        }
    }
}

/// Handler for GET /api/history/jobs/:job_id/batch - get batch phenotype status.
pub(crate) async fn get_history_job_batch(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> axum::Json<BatchStatusResponse> {
    let data = state.lock().unwrap();

    // If requesting the current active job, return live data
    if let Some(ref current_id) = data.current_job_id {
        if current_id == &job_id {
            if let JobExecutionState::Batch(batch) = &data.job_state {
                let phenotypes: Vec<PhenotypeStatus> =
                    batch.phenotype_statuses.values().cloned().collect();
                return axum::Json(BatchStatusResponse { phenotypes });
            } else if let Some(ref last_batch) = data.last_completed_batch {
                let phenotypes: Vec<PhenotypeStatus> =
                    last_batch.values().cloned().collect();
                return axum::Json(BatchStatusResponse { phenotypes });
            }
        }
    }

    // Fetch from database
    match data.metrics_db.get_job_batch_phenotypes(&job_id) {
        Ok(phenotypes) => axum::Json(BatchStatusResponse { phenotypes }),
        Err(e) => {
            eprintln!("Warning: failed to fetch batch phenotypes from DB: {}", e);
            axum::Json(BatchStatusResponse { phenotypes: vec![] })
        }
    }
}
