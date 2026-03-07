//! Dashboard API handlers.
//!
//! Handlers for dashboard-related API endpoints including summary,
//! bottleneck analysis, worker status, and metrics.

use crate::distributed::coordinator::state::{
    CoordinatorData, ManhattanPhase, SharedState, WorkerStatus,
};
use crate::distributed::message::{
    BatchStatusResponse, DashboardBatchProgress, DashboardBottleneck, DashboardMetrics,
    DashboardSummary, DashboardWorker, PhenotypeStatus, WorkerMetricsSeries,
};
use std::time::Instant;

/// Build a DashboardSummary from coordinator data (used for persistence and API).
pub(crate) fn build_dashboard_summary(data: &CoordinatorData) -> DashboardSummary {
    let elapsed = data.job_start_time.elapsed().as_secs_f64();
    let failed = data.failed_partitions.len();

    // Check for batch Manhattan job (multi-phenotype mode)
    let (completed, processing, pending, total, is_complete) =
        if let Some(ref batch) = data.batch_state {
            // For batch jobs, track phenotype-level progress
            let total = batch.total_phenotypes;
            let completed = batch.completed_count;
            let active_count = batch.active_phenotypes.len();
            let ready_count = batch.ready_to_aggregate.len();
            let pending = batch.pending_queue.len();
            let processing = active_count + ready_count;

            let is_complete = batch.pending_queue.is_empty()
                && batch.active_phenotypes.is_empty()
                && batch.ready_to_aggregate.is_empty()
                && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;

            (completed, processing, pending, total, is_complete)
        } else if let Some(ref m) = data.manhattan_state {
            let total_parts = m.exome_total_tasks + m.genome_total_tasks;
            let completed_parts = m.exome_completed.len() + m.genome_completed.len();
            let processing_parts = m.exome_processing.len() + m.genome_processing.len();
            let pending_parts = m.exome_pending.len() + m.genome_pending.len();

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

    let cluster_items_per_sec: f64 = data
        .worker_registry
        .values()
        .filter(|w| w.status == WorkerStatus::Active)
        .filter_map(|w| w.metrics_history.back())
        .map(|s| s.items_per_sec)
        .sum();

    let eta_secs = if completed > 0 && !is_complete {
        let remaining = total - completed - failed;
        let secs_per_partition = elapsed / completed as f64;
        Some(remaining as f64 * secs_per_partition)
    } else {
        None
    };

    let batch_progress = if let Some(ref batch) = data.batch_state {
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

    DashboardSummary {
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
        last_backup_at: data.last_backup_at,
    }
}

/// Handler for GET /api/dashboard/summary.
pub(crate) async fn get_dashboard_summary(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardSummary> {
    let data = state.lock().unwrap();
    axum::Json(build_dashboard_summary(&data))
}

/// Handler for GET /api/dashboard/bottlenecks - analyze cluster bottlenecks.
///
/// Aggregates telemetry from active workers to identify the current limiting factor:
/// - CPU: High CPU utilization indicates compute-bound workload (scanning/decoding)
/// - Memory: High memory usage indicates aggregate-phase or high partition counts
/// - Network RX: High download rate indicates GCS fetch bottleneck
/// - Network TX: High upload rate indicates GCS write bottleneck
/// - I/O Wait: Low CPU with active workers indicates waiting on external I/O
pub(crate) async fn get_dashboard_bottlenecks(
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
pub(crate) async fn get_dashboard_workers(
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
            build_version: w.build_version.clone(),
        })
        .collect();

    axum::Json(workers)
}

/// Handler for GET /api/dashboard/metrics - time-series metrics for charts.
///
/// Returns metrics scoped to the current job (if any) so that charts display
/// only the data for the active/most recent job rather than all historical data.
pub(crate) async fn get_dashboard_metrics(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardMetrics> {
    let data = state.lock().unwrap();

    // Scope metrics to the current job if one exists
    let workers = if let Some(ref job_id) = data.current_job_id {
        match data.metrics_db.get_job_metrics(job_id) {
            Ok(worker_data) => worker_data
                .into_iter()
                .map(|(worker_id, snapshots)| WorkerMetricsSeries {
                    worker_id,
                    snapshots,
                })
                .collect(),
            Err(e) => {
                eprintln!("Warning: failed to fetch job metrics from DB: {}", e);
                vec![]
            }
        }
    } else {
        // No active job - return empty metrics
        vec![]
    };

    axum::Json(DashboardMetrics { workers })
}

/// Handler for GET /api/dashboard/batch - get status of batch phenotypes.
pub(crate) async fn get_batch_status(
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
