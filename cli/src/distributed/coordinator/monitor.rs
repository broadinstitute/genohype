//! Background monitoring functions.
//!
//! This module contains functions that run in background tasks to monitor
//! worker health, detect stuck jobs, handle timeouts, and perform backups.

use crate::distributed::coordinator::state::{
    CoordinatorData, JobExecutionState, SharedState, WorkerStatus, WORKER_SUSPECT_TIMEOUT_SECS,
};
use crate::distributed::metrics_db::MetricsDb;
use std::time::{Duration, Instant};

/// Backup the database to GCS.
///
/// This function checkpoints the WAL to ensure all data is in the main file,
/// then uploads the database to the specified GCS path.
/// Returns true if backup succeeded.
pub(crate) async fn backup_db(metrics_db: &MetricsDb, db_path: &str, backup_path: &str) -> bool {
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
                eprintln!(
                    "Warning: failed to read database file at {}: {}",
                    db_path, e
                );
                return false;
            }
        };

        let mut writer = match CloudWriter::new(&backup_path) {
            Ok(w) => w,
            Err(e) => {
                eprintln!(
                    "Warning: failed to create cloud writer for {}: {}",
                    backup_path, e
                );
                return false;
            }
        };

        if let Err(e) = writer.write_all(&db_contents) {
            eprintln!(
                "Warning: failed to write backup data to {}: {}",
                backup_path, e
            );
            return false;
        }

        if let Err(e) = writer.finish() {
            eprintln!(
                "Warning: failed to finalize backup upload to {}: {}",
                backup_path, e
            );
            return false;
        }

        true
    })
    .await;

    let success = result.unwrap_or(false);
    if success {
        println!("Job history backed up to {}", backup_path_display);
    }
    success
}

/// Check if the job is stuck (no progress for N minutes) and auto-cancel if needed.
pub(crate) fn check_stuck_job(state: &SharedState, timeout_secs: u64) {
    let mut data = state.lock().unwrap();

    // Only check if job is running
    if data.idle {
        return;
    }

    // Only consider it stuck if ZERO partitions have completed
    // (if some completed but now it's stalled, that's different - likely worker issues handled by timeouts)
    // Check batch state, Manhattan state, and standard counters
    let has_progress = match &data.job_state {
        JobExecutionState::Batch(batch) => {
            // For batch jobs, check if any phenotypes completed
            batch.completed_count > 0
                || !batch
                    .active_phenotypes
                    .values()
                    .all(|s| s.exome_completed.is_empty() && s.genome_completed.is_empty())
        }
        JobExecutionState::Manhattan(m) => {
            // For single Manhattan jobs, check if any exome/genome partitions completed
            !m.exome_completed.is_empty() || !m.genome_completed.is_empty() || m.aggregate_complete
        }
        JobExecutionState::Ingestion(ing) => {
            // For ingestion jobs, check if any tasks completed
            ing.completed_count > 0
        }
        JobExecutionState::Standard => {
            // For standard jobs, check standard counter
            !data.completed_tasks.is_empty()
        }
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

        // Update job status in database
        if let Some(ref job_id) = data.current_job_id {
            let end_time_ms = CoordinatorData::now_ms();
            if let Err(e) =
                data.metrics_db
                    .update_job_status(job_id, "failed", Some(end_time_ms), None)
            {
                eprintln!("Warning: failed to update job status in DB: {}", e);
            }
            if let Err(e) = data.metrics_db.clear_current_custom_assignments(job_id) {
                eprintln!("Warning: failed to clear failed custom assignments: {}", e);
            }
        }

        // Reset state
        data.pending_partitions.clear();
        data.processing_partitions.clear();
        data.custom_assignments.clear();
        data.job_state = JobExecutionState::Standard;
        data.active_tasks.clear();
        // Note: We intentionally keep current_job_id so the dashboard continues
        // to display the failed job's metrics until a new job is submitted.
        data.idle = true;
    }
}

/// Check for timed-out work and reschedule.
pub(crate) fn check_timeouts(state: &SharedState, timeout_secs: u64) {
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
        data.custom_assignments
            .retain(|_, assignment| assignment.partition_id != part_id);

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

/// Check worker liveness based on heartbeat timestamps.
pub(crate) fn check_worker_liveness(state: &SharedState) {
    let mut data = state.lock().unwrap();
    let now = Instant::now();
    let timeout = Duration::from_secs(WORKER_SUSPECT_TIMEOUT_SECS);

    let mut dead_workers = Vec::new();

    for (worker_id, worker) in data.worker_registry.iter_mut() {
        if now.duration_since(worker.last_seen) > timeout
            && worker.status != WorkerStatus::SuspectedDead
        {
            println!(
                "Worker {} not seen for >{}s, marking as suspected dead",
                worker_id, WORKER_SUSPECT_TIMEOUT_SECS
            );
            worker.status = WorkerStatus::SuspectedDead;
            dead_workers.push(worker_id.clone());
        }
    }

    for worker_id in dead_workers {
        data.requeue_worker_tasks(&worker_id);
    }
}

/// Check for CPU/status mismatches and log warnings.
///
/// This detects situations where a worker reports "idle" status but has high CPU utilization,
/// which indicates the worker is actually computing (likely in a synchronous block before
/// state update). The telemetry provides accurate CPU data even when status is stale.
///
/// Also updates worker.effective_status field to reflect actual computational state:
/// - "computing" if CPU > 70% regardless of reported status
/// - "idle" if CPU < 20% and status is Idle
/// - follows reported status otherwise
pub(crate) fn check_cpu_status_consistency(state: &SharedState) {
    let mut data = state.lock().unwrap();

    for (worker_id, worker) in data.worker_registry.iter_mut() {
        if let Some(telemetry) = worker.metrics_history.back() {
            let cpu = telemetry.cpu_percent.unwrap_or(0.0);

            // Detect mismatch: worker says idle but CPU is high
            if worker.status == WorkerStatus::Idle && cpu > 70.0 {
                // Log anomaly (only once per occurrence to avoid spam)
                if worker.effective_status.as_deref() != Some("computing") {
                    println!(
                        "Warning: Worker {} reporting idle but CPU is {:.1}% - likely computing in synchronous block",
                        worker_id, cpu
                    );
                }
                worker.effective_status = Some("computing".to_string());
            } else if worker.status == WorkerStatus::Active {
                // Active workers showing what they're working on
                let phase = telemetry.current_phase.as_deref().unwrap_or("active");
                if let Some(phenotype) = telemetry.current_phenotype_id.as_ref() {
                    let source = telemetry.current_source.as_deref().unwrap_or("");
                    let source_suffix = if source.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", source)
                    };
                    worker.effective_status =
                        Some(format!("{} {}{}", phase, phenotype, source_suffix));
                } else {
                    worker.effective_status = Some(phase.to_string());
                }
            } else if cpu < 20.0 && worker.status == WorkerStatus::Idle {
                worker.effective_status = Some("idle".to_string());
            } else {
                // Follow reported status
                worker.effective_status = Some(worker.status.as_str().to_string());
            }
        }
    }
}

/// Background loop that polls ClickHouse system metrics and adjusts
/// the ingestion batch size using AIMD (Additive Increase, Multiplicative Decrease).
///
/// Only active during ingestion jobs. Queries `system.asynchronous_metrics`
/// for OS memory and `system.metrics` for HTTP connections every 5 seconds.
pub(crate) async fn monitor_clickhouse_health(state: SharedState) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Extract ClickHouse URL and current state without holding the lock during HTTP
        let (ch_url, current_batch, max_batch) = {
            let data = state.lock().unwrap();
            if data.idle {
                continue;
            }
            if let JobExecutionState::Ingestion(ref ing) = data.job_state {
                (
                    ing.clickhouse_url.clone(),
                    ing.dynamic_batch_size,
                    ing.max_batch_size,
                )
            } else {
                continue;
            }
        };

        // Query ClickHouse for memory and HTTP connection metrics
        let query = "SELECT metric, value FROM system.asynchronous_metrics \
                     WHERE metric IN ('OSMemoryAvailable', 'OSMemoryTotal') \
                     UNION ALL \
                     SELECT metric, value FROM system.metrics \
                     WHERE metric = 'HTTPConnection' \
                     FORMAT JSON";
        let url = format!("{}/?default_format=JSON", ch_url.trim_end_matches('/'));

        let resp = match client.post(&url).body(query).send().await {
            Ok(r) => r,
            Err(_) => continue, // Silently ignore transient network errors
        };

        let json = match resp.json::<serde_json::Value>().await {
            Ok(j) => j,
            Err(_) => continue,
        };

        let mut mem_avail = 0.0_f64;
        let mut mem_total = 0.0_f64;
        let mut http_conns = 0.0_f64;

        if let Some(data_arr) = json.get("data").and_then(|d| d.as_array()) {
            for row in data_arr {
                let metric = row.get("metric").and_then(|m| m.as_str()).unwrap_or("");
                // CH JSON format may return values as strings or numbers
                let value = row
                    .get("value")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);

                match metric {
                    "OSMemoryAvailable" => mem_avail = value,
                    "OSMemoryTotal" => mem_total = value,
                    "HTTPConnection" => http_conns = value,
                    _ => {}
                }
            }
        }

        if mem_total <= 0.0 {
            continue;
        }

        let mem_usage_pct = ((mem_total - mem_avail) / mem_total) * 100.0;

        // AIMD logic
        let mut new_batch = current_batch;
        let mut log_warning = None;

        if mem_usage_pct > 80.0 || http_conns > 100.0 {
            // Multiplicative Decrease — cut in half, floor at 1
            new_batch = (current_batch / 2).max(1);
            if new_batch < current_batch {
                log_warning = Some(format!(
                    "ClickHouse under pressure (Mem: {:.1}%, HTTP conns: {:.0}). Reduced ingest batch size {} → {}",
                    mem_usage_pct, http_conns, current_batch, new_batch
                ));
            }
        } else if mem_usage_pct < 60.0 && http_conns < 50.0 {
            // Additive Increase — increment by 1 up to ceiling
            new_batch = (current_batch + 1).min(max_batch);
        }

        // Apply back to state
        if new_batch != current_batch || log_warning.is_some() {
            let mut data = state.lock().unwrap();
            if let JobExecutionState::Ingestion(ref mut ing) = data.job_state {
                ing.dynamic_batch_size = new_batch;
            }
            if let Some(msg) = log_warning {
                println!("AIMD: {}", msg);
                data.log_event(crate::distributed::message::JobEvent {
                    timestamp_ms: CoordinatorData::now_ms(),
                    event_type: "warning".to_string(),
                    worker_id: None,
                    phenotype_id: None,
                    details: msg,
                });
            }
        }
    }
}
