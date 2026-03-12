//! Task completion handlers for different job types.
//!
//! This module contains functions for handling task completions from workers,
//! updating state, and transitioning between job phases.

use crate::distributed::coordinator::{
    ActiveTask, BatchState, CoordinatorData, IngestionState, ManhattanPhase, ManhattanPipelineState,
};
use crate::distributed::message::{CompleteRequest, JobEvent, ManhattanAggregateSpec, ManhattanSource};
use std::collections::HashMap;
use std::time::Instant;

/// Handle completion for Manhattan pipeline jobs.
pub(crate) fn complete_manhattan_work(
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

/// Complete an ingestion task.
pub(crate) fn complete_ingestion_work(data: &mut CoordinatorData, ingestion: &mut IngestionState, req: &CompleteRequest) {
    // Extract task ID from tasks list
    let task_id = req.tasks.first().cloned().unwrap_or_default();

    // Remove from active tasks
    if let Some((phenotype_id, ancestry, _base_path, _worker_id, start_time)) =
        ingestion.active_tasks.remove(&task_id)
    {
        // Mark as ingested in coordinator state on success
        if req.error.is_none() {
            data.ingested_phenotypes.insert((phenotype_id.clone(), ancestry.clone()));
        }
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

/// Handle completion for batch Manhattan jobs.
///
/// Uses task_id to lookup the active task and update the appropriate state.
/// The task_id is the first element in req.tasks (a UUID generated by the coordinator),
/// and the partition_ids are stored in the ActiveTask to avoid parsing issues.
pub(crate) fn complete_batch_work(
    data: &mut CoordinatorData,
    batch: &mut BatchState,
    req: &CompleteRequest,
) {
    data.last_progress_time = Instant::now();

    // Extract task ID from tasks list (first element is the coordinator's UUID)
    let task_id = req.tasks.first().cloned().unwrap_or_default();

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
        ActiveTask::Scan { phenotype_id, partition_ids, source, started_at_ms } => {
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

            // Mark partitions as complete (using partition_ids from ActiveTask)
            for &part_id in &partition_ids {
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
                    data.log_event(JobEvent {
                        timestamp_ms: now_ms,
                        event_type: "completed".to_string(),
                        worker_id: Some(req.worker_id.clone()),
                        phenotype_id: Some(phenotype_id.clone()),
                        details: format!("Scan complete (ScanOnly mode) [{}/{}]", batch.completed_count, batch.total_phenotypes),
                    });
                    batch.active_phenotypes.remove(&phenotype_id);
                } else {
                    println!(
                        "Phenotype {} scan complete, moving to aggregate queue",
                        phenotype_id
                    );
                    data.log_event(JobEvent {
                        timestamp_ms: now_ms,
                        event_type: "phase".to_string(),
                        worker_id: Some(req.worker_id.clone()),
                        phenotype_id: Some(phenotype_id.clone()),
                        details: "Scan complete, queued for aggregation".to_string(),
                    });

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

            // Check if this is a global batch error or if we have individual results
            let is_global_error = req.error.is_some() && req.result_json.is_none();

            // Extract individual summaries if available
            // Each summary may have its own "error" field for per-phenotype failures
            let results_map: HashMap<String, (serde_json::Value, bool)> =
                if let Some(ref json) = req.result_json {
                    if let Some(results_array) = json.get("batch_results").and_then(|v| v.as_array())
                    {
                        // Results array corresponds to phenotype_ids order
                        if results_array.len() == phenotype_ids.len() {
                            phenotype_ids
                                .iter()
                                .zip(results_array.iter())
                                .map(|(id, res)| {
                                    // Check if this specific phenotype had an error
                                    let has_error = res.get("error").and_then(|e| e.as_str()).is_some()
                                        || res.get("status").and_then(|s| s.as_str()) == Some("error");
                                    (id.clone(), (res.clone(), has_error))
                                })
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

            const MAX_AGGREGATE_RETRIES: usize = 2;

            // Process each phenotype individually for granular tracking
            for phenotype_id in phenotype_ids {
                // Calculate duration from start time
                let phenotype_duration_secs = batch
                    .phenotype_start_times
                    .get(&phenotype_id)
                    .map(|start| start.elapsed().as_secs_f64());

                // Get accumulated CPU core-seconds
                let cpu_core_secs = batch.phenotype_cpu_secs.get(&phenotype_id).copied();

                // Determine if this phenotype failed
                let phenotype_failed = is_global_error
                    || results_map.get(&phenotype_id).map(|(_, err)| *err).unwrap_or(false);

                if phenotype_failed {
                    // Check if we should retry or mark as permanently failed
                    let retries = batch.aggregate_retry_counts.entry(phenotype_id.clone()).or_insert(0);
                    *retries += 1;

                    if *retries > MAX_AGGREGATE_RETRIES {
                        // Permanently failed
                        batch.failed_count += 1;

                        let error_msg = if let Some((res, _)) = results_map.get(&phenotype_id) {
                            res.get("error")
                                .and_then(|e| e.as_str())
                                .map(|s| s.to_string())
                        } else {
                            req.error.clone()
                        };

                        if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                            status.stage = "failed".to_string();
                            status.error = error_msg.clone();
                            status.duration_secs = phenotype_duration_secs;
                            status.cpu_core_secs = cpu_core_secs;
                        }

                        // Clean up tracking data
                        batch.phenotype_start_times.remove(&phenotype_id);
                        batch.phenotype_cpu_secs.remove(&phenotype_id);
                        batch.aggregate_specs.remove(&phenotype_id);
                        batch.aggregate_retry_counts.remove(&phenotype_id);

                        let error_detail = error_msg.as_deref().unwrap_or("unknown error");
                        println!(
                            "Phenotype {} FAILED after {} retries: {}",
                            phenotype_id, MAX_AGGREGATE_RETRIES, error_detail
                        );
                        data.log_event(JobEvent {
                            timestamp_ms: now_ms,
                            event_type: "failed".to_string(),
                            worker_id: Some(req.worker_id.clone()),
                            phenotype_id: Some(phenotype_id.clone()),
                            details: format!("Permanently failed after {} retries: {}", MAX_AGGREGATE_RETRIES, error_detail),
                        });
                    } else {
                        // Requeue for retry if we have the spec
                        if let Some(spec) = batch.aggregate_specs.get(&phenotype_id).cloned() {
                            println!(
                                "Phenotype {} aggregate failed, requeueing for retry ({}/{})",
                                phenotype_id, retries, MAX_AGGREGATE_RETRIES
                            );
                            data.log_event(JobEvent {
                                timestamp_ms: now_ms,
                                event_type: "requeued".to_string(),
                                worker_id: Some(req.worker_id.clone()),
                                phenotype_id: Some(phenotype_id.clone()),
                                details: format!("Aggregate failed, retry {}/{}", retries, MAX_AGGREGATE_RETRIES),
                            });
                            batch.ready_to_aggregate.push((phenotype_id.clone(), spec));

                            // Update status
                            if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                                status.stage = "retry_queued".to_string();
                            }
                        } else {
                            // No spec to retry with, mark as failed
                            batch.failed_count += 1;
                            if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                                status.stage = "failed".to_string();
                                status.error = Some("No aggregate spec available for retry".to_string());
                            }

                            println!(
                                "Phenotype {} FAILED (no aggregate spec for retry)",
                                phenotype_id
                            );
                            data.log_event(JobEvent {
                                timestamp_ms: now_ms,
                                event_type: "failed".to_string(),
                                worker_id: Some(req.worker_id.clone()),
                                phenotype_id: Some(phenotype_id.clone()),
                                details: "Failed: no aggregate spec available for retry".to_string(),
                            });
                        }
                    }
                } else {
                    // Success
                    batch.completed_count += 1;

                    // Update status
                    if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                        status.stage = "completed".to_string();
                        status.duration_secs = phenotype_duration_secs;
                        status.cpu_core_secs = cpu_core_secs;
                        if let Some((res, _)) = results_map.get(&phenotype_id) {
                            status.result = Some(res.clone());
                        }
                    }

                    // Clean up tracking data
                    batch.phenotype_start_times.remove(&phenotype_id);
                    batch.phenotype_cpu_secs.remove(&phenotype_id);
                    batch.aggregate_specs.remove(&phenotype_id);
                    batch.aggregate_retry_counts.remove(&phenotype_id);

                    let duration_str = phenotype_duration_secs
                        .map(|d| format!("{:.1}s", d))
                        .unwrap_or_else(|| "--".to_string());
                    println!(
                        "Phenotype {} complete ({}/{}) [{}]",
                        phenotype_id, batch.completed_count, batch.total_phenotypes, duration_str
                    );
                    data.log_event(JobEvent {
                        timestamp_ms: now_ms,
                        event_type: "completed".to_string(),
                        worker_id: Some(req.worker_id.clone()),
                        phenotype_id: Some(phenotype_id.clone()),
                        details: format!("Completed [{}/{}] in {}", batch.completed_count, batch.total_phenotypes, duration_str),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::coordinator::{
        CoordinatorConfig, ManhattanPipelineState, JobExecutionState,
    };
    use crate::distributed::message::{ExecutionMode, ManhattanSpec, PhenotypeStatus, ManhattanAggregateSpec};
    use crate::distributed::metrics_db::MetricsDb;
    use std::collections::{HashSet, VecDeque};
    use std::time::Instant;
    use uuid::Uuid;

    /// Create a minimal BatchState for testing.
    fn create_test_batch_state() -> BatchState {
        BatchState {
            mode: ExecutionMode::Full,
            phenotype_statuses: HashMap::new(),
            phenotype_start_times: HashMap::new(),
            phenotype_cpu_secs: HashMap::new(),
            pending_queue: VecDeque::new(),
            active_phenotypes: HashMap::new(),
            ready_to_aggregate: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            total_phenotypes: 1,
            aggregate_retry_counts: HashMap::new(),
            aggregate_specs: HashMap::new(),
            scan_round_robin: 0,
        }
    }

    /// Create a minimal ManhattanPipelineState for testing scan completions.
    fn create_test_pipeline_state(partition_ids: &[usize], source: ManhattanSource) -> ManhattanPipelineState {
        let mut exome_processing = HashMap::new();
        let mut genome_processing = HashMap::new();

        for &p in partition_ids {
            let entry = ("test-worker".to_string(), Instant::now());
            match source {
                ManhattanSource::Exome => { exome_processing.insert(p, entry); }
                ManhattanSource::Genome => { genome_processing.insert(p, entry); }
            }
        }

        let (exome_total, genome_total) = match source {
            ManhattanSource::Exome => (partition_ids.len(), 0),
            ManhattanSource::Genome => (0, partition_ids.len()),
        };

        ManhattanPipelineState {
            mode: ExecutionMode::Full,
            phase: crate::distributed::coordinator::ManhattanPhase::Scan,
            original_spec: ManhattanSpec {
                phenotype: Some("test-phenotype".to_string()),
                ancestry: Some("EUR".to_string()),
                exome: None,
                exome_annotations: None,
                genome: None,
                genome_annotations: None,
                gene_burden: None,
                genes: None,
                exome_exp_p: None,
                genome_exp_p: None,
                threshold: 5e-8,
                gene_threshold: 2.5e-6,
                locus_threshold: 0.01,
                locus_window: 1_000_000,
                locus_plots: false,
                min_variants_per_locus: 1,
                width: 1200,
                height: 400,
                y_field: "Pvalue".to_string(),
                output_path: "gs://test/output".to_string(),
                layout: None,
                y_scale: None,
                contig_lengths: None,
                skip_composite: false,
                exome_partitions: None,
                genome_partitions: None,
                styling: Default::default(),
            },
            layout: None,
            y_scale: None,
            contig_lengths: HashMap::new(),
            exome_total_tasks: exome_total,
            exome_pending: VecDeque::new(),
            exome_processing,
            exome_completed: HashSet::new(),
            genome_total_tasks: genome_total,
            genome_pending: VecDeque::new(),
            genome_processing,
            genome_completed: HashSet::new(),
            aggregate_dispatched: false,
            aggregate_complete: false,
        }
    }

    /// Regression test for task ID matching between coordinator and worker.
    ///
    /// This test verifies the fix for the "task X not found in active_tasks" bug
    /// where task IDs (UUIDs) didn't match between assignment and completion.
    #[test]
    fn test_batch_scan_completion_with_uuid_task_id() {
        // Setup: Create a task with UUID as the key in active_tasks
        let task_id = Uuid::new_v4().to_string();
        let phenotype_id = "test-phenotype".to_string();
        let partition_ids = vec![768, 769, 770]; // Batch of 3 partitions

        // Create a minimal CoordinatorData with the active task
        let metrics_db = MetricsDb::in_memory().unwrap();

        let mut data = CoordinatorData {
            pending_partitions: VecDeque::new(),
            processing_partitions: HashMap::new(),
            completed_tasks: HashSet::new(),
            config: CoordinatorConfig::default(),
            total_rows: 0,
            scan_cpu_secs: 0.0,
            aggregate_cpu_secs: 0.0,
            wasted_cpu_secs: 0.0,
            retry_counts: HashMap::new(),
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
            current_job_id: Some("test-job".to_string()),
            session_id: "test-session".to_string(),
            catalog: None,
            ingested_phenotypes: HashSet::new(),
            completed_phenotypes: HashSet::new(),
        };

        // Insert the active task with UUID as key and all partition_ids
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::Scan {
                phenotype_id: phenotype_id.clone(),
                partition_ids: partition_ids.clone(),
                source: ManhattanSource::Genome,
                started_at_ms: CoordinatorData::now_ms() - 1000, // Started 1 second ago
            },
        );

        // Create batch state with the phenotype's pipeline state
        let mut batch = create_test_batch_state();
        batch.active_phenotypes.insert(
            phenotype_id.clone(),
            create_test_pipeline_state(&partition_ids, ManhattanSource::Genome),
        );

        // Simulate a completion request from the worker
        // The first task ID is the UUID (as set by the coordinator)
        // The remaining are partition indices (for backwards compatibility)
        let req = CompleteRequest {
            worker_id: "test-worker".to_string(),
            tasks: vec![task_id.clone(), "769".to_string(), "770".to_string()],
            items_processed: 1000,
            result_json: None,
            error: None,
            session_id: Some("test-session".to_string()),
        };

        // Execute the completion handler
        complete_batch_work(&mut data, &mut batch, &req);

        // Verify: Task should be removed from active_tasks
        assert!(
            !data.active_tasks.contains_key(&task_id),
            "Task should be removed from active_tasks after completion"
        );

        // Verify: When all partitions complete, the phenotype moves to ready_to_aggregate
        // and is removed from active_phenotypes (for Full execution mode)
        assert!(
            !batch.active_phenotypes.contains_key(&phenotype_id),
            "Phenotype should be removed from active_phenotypes after all partitions complete"
        );
        assert_eq!(
            batch.ready_to_aggregate.len(),
            1,
            "Phenotype should be in ready_to_aggregate queue"
        );
        assert_eq!(
            batch.ready_to_aggregate[0].0,
            phenotype_id,
            "Correct phenotype should be in ready_to_aggregate"
        );

        // Verify: CPU time should be tracked
        assert!(
            data.scan_cpu_secs > 0.0,
            "scan_cpu_secs should be incremented"
        );
    }

    /// Test that completion with non-existent task ID logs a warning but doesn't panic.
    #[test]
    fn test_batch_completion_with_unknown_task_id() {
        let metrics_db = MetricsDb::in_memory().unwrap();

        let mut data = CoordinatorData {
            pending_partitions: VecDeque::new(),
            processing_partitions: HashMap::new(),
            completed_tasks: HashSet::new(),
            config: CoordinatorConfig::default(),
            total_rows: 0,
            scan_cpu_secs: 0.0,
            aggregate_cpu_secs: 0.0,
            wasted_cpu_secs: 0.0,
            retry_counts: HashMap::new(),
            failed_partitions: HashSet::new(),
            worker_registry: HashMap::new(),
            job_start_time: Instant::now(),
            last_progress_time: Instant::now(),
            idle: false,
            metrics_db,
            aggregated_results: Vec::new(),
            job_state: JobExecutionState::Standard,
            active_tasks: HashMap::new(), // Empty - no active tasks
            last_error: None,
            events: VecDeque::new(),
            failures: VecDeque::new(),
            events_since_backup: 0,
            last_backup_at: None,
            update_fleet_url: None,
            updated_workers: HashSet::new(),
            current_job_id: Some("test-job".to_string()),
            session_id: "test-session".to_string(),
            catalog: None,
            ingested_phenotypes: HashSet::new(),
            completed_phenotypes: HashSet::new(),
        };

        let mut batch = create_test_batch_state();

        // Completion with an unknown task ID (simulates duplicate completion or stale task)
        let req = CompleteRequest {
            worker_id: "test-worker".to_string(),
            tasks: vec!["unknown-task-id".to_string()],
            items_processed: 100,
            result_json: None,
            error: None,
            session_id: Some("test-session".to_string()),
        };

        // Should not panic - just return early after logging warning
        complete_batch_work(&mut data, &mut batch, &req);

        // State should be unchanged
        assert!(data.active_tasks.is_empty());
        assert_eq!(data.scan_cpu_secs, 0.0);
    }

    /// Regression test: aggregate batch completion must use UUID task_id (not phenotype ID).
    ///
    /// Previously, the coordinator assigned aggregate batch tasks with phenotype IDs as
    /// descriptor IDs. Workers echo back descriptor IDs in the completion request, so
    /// `req.tasks[0]` was a phenotype ID — but `active_tasks` was keyed by UUID.
    /// This caused a lookup miss: the completion handler returned early with a warning,
    /// never transitioning phenotypes to "completed" or incrementing `completed_count`.
    #[test]
    fn test_aggregate_batch_completion_with_uuid_task_id() {
        let task_id = Uuid::new_v4().to_string();
        let phenotype_ids = vec![
            "pheno_A".to_string(),
            "pheno_B".to_string(),
            "pheno_C".to_string(),
        ];

        let metrics_db = MetricsDb::in_memory().unwrap();

        let mut data = CoordinatorData {
            pending_partitions: VecDeque::new(),
            processing_partitions: HashMap::new(),
            completed_tasks: HashSet::new(),
            config: CoordinatorConfig::default(),
            total_rows: 0,
            scan_cpu_secs: 0.0,
            aggregate_cpu_secs: 0.0,
            wasted_cpu_secs: 0.0,
            retry_counts: HashMap::new(),
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
            current_job_id: Some("test-job".to_string()),
            session_id: "test-session".to_string(),
            catalog: None,
            ingested_phenotypes: HashSet::new(),
            completed_phenotypes: HashSet::new(),
        };

        // Insert aggregate batch task keyed by UUID (as coordinator does)
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::AggregateBatch {
                phenotype_ids: phenotype_ids.clone(),
                started_at_ms: CoordinatorData::now_ms() - 5000,
            },
        );

        // Set up batch state with phenotype statuses in "aggregating" stage
        let mut batch = create_test_batch_state();
        batch.total_phenotypes = 3;
        for pid in &phenotype_ids {
            batch.phenotype_statuses.insert(
                pid.clone(),
                PhenotypeStatus {
                    id: pid.clone(),
                    stage: "aggregating".to_string(),
                    partitions_done: 10,
                    partitions_total: 10,
                    result: None,
                    error: None,
                    duration_secs: None,
                    cpu_core_secs: None,
                },
            );
            batch.aggregate_specs.insert(pid.clone(), ManhattanAggregateSpec {
                output_path: format!("gs://test/{}", pid),
                phenotype_id: Some(pid.clone()),
                ancestry: None,
                exome_results: None,
                genome_results: None,
                gene_burden: None,
                exome_exp_p: None,
                genome_exp_p: None,
                exome_annotations: None,
                genome_annotations: None,
                genes: None,
                threshold: 5e-8,
                gene_threshold: 2.5e-6,
                locus_threshold: 0.01,
                locus_window: 1_000_000,
                locus_plots: false,
                min_variants_per_locus: 1,
                width: 1200,
                height: 400,
                layout: Default::default(),
                y_scale: Default::default(),
                cleanup: false,
                styling: Default::default(),
            });
        }

        // Simulate completion: first task ID is the UUID (as fixed in assignment.rs),
        // remaining are phenotype IDs
        let req = CompleteRequest {
            worker_id: "test-worker".to_string(),
            tasks: vec![task_id.clone(), "pheno_B".to_string(), "pheno_C".to_string()],
            items_processed: 0, // Aggregate tasks report 0 rows
            result_json: None,
            error: None,
            session_id: Some("test-session".to_string()),
        };

        complete_batch_work(&mut data, &mut batch, &req);

        // Verify: Task should be removed from active_tasks
        assert!(
            !data.active_tasks.contains_key(&task_id),
            "Task should be removed from active_tasks after completion"
        );

        // Verify: All 3 phenotypes should be marked as completed
        assert_eq!(
            batch.completed_count, 3,
            "All phenotypes in the batch should be counted as completed"
        );

        for pid in &phenotype_ids {
            let status = batch.phenotype_statuses.get(pid).expect("status should exist");
            assert_eq!(
                status.stage, "completed",
                "Phenotype {} should be in 'completed' stage, got '{}'",
                pid, status.stage
            );
        }

        // Verify: Aggregate specs should be cleaned up
        assert!(
            batch.aggregate_specs.is_empty(),
            "Aggregate specs should be cleaned up after completion"
        );

        // Verify: CPU time should be tracked
        assert!(
            data.aggregate_cpu_secs > 0.0,
            "aggregate_cpu_secs should be incremented"
        );
    }
}
