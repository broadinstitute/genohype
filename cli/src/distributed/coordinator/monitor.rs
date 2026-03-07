//! Background monitoring functions.
//!
//! This module contains functions that run in background tasks to monitor
//! worker health, detect stuck jobs, handle timeouts, and perform backups.

use crate::distributed::coordinator::state::{
    CoordinatorData, SharedState, WorkerStatus, WORKER_SUSPECT_TIMEOUT_SECS,
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
    let has_progress = if let Some(ref batch) = data.batch_state {
        // For batch jobs, check if any phenotypes completed
        batch.completed_count > 0
            || !batch.active_phenotypes.values().all(|s| {
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

        // Update job status in database
        if let Some(ref job_id) = data.current_job_id {
            let end_time_ms = CoordinatorData::now_ms();
            if let Err(e) =
                data.metrics_db
                    .update_job_status(job_id, "failed", Some(end_time_ms), None)
            {
                eprintln!("Warning: failed to update job status in DB: {}", e);
            }
        }

        // Reset state
        data.pending_partitions.clear();
        data.processing_partitions.clear();
        data.manhattan_state = None;
        data.batch_state = None;
        data.active_tasks.clear();
        data.ingestion_state = None;
        data.current_job_id = None;
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

    for (worker_id, worker) in data.worker_registry.iter_mut() {
        if now.duration_since(worker.last_seen) > timeout
            && worker.status != WorkerStatus::SuspectedDead
        {
            println!(
                "Worker {} not seen for >{}s, marking as suspected dead",
                worker_id, WORKER_SUSPECT_TIMEOUT_SECS
            );
            worker.status = WorkerStatus::SuspectedDead;
        }
    }
}
