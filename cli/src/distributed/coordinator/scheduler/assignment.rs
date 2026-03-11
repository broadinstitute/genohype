//! Work assignment logic for different job types.
//!
//! This module contains functions for assigning work to workers,
//! including phenotype activation and task distribution for
//! batch, Manhattan pipeline, and ingestion jobs.

use crate::distributed::coordinator::{
    ActiveTask, BatchState, CoordinatorData, IngestionState, ManhattanPhase,
    ManhattanPipelineState, WorkerStatus, AGGREGATE_BATCH_SIZE, BATCH_ACTIVE_LIMIT,
};
use crate::distributed::coordinator::scheduler::determine_batch_size;
use crate::distributed::message::{
    ActiveTaskInfo, JobSpec, ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource,
    PartitionOp, PhenotypeOp, TaskDescriptor, TaskType, WorkResponse,
};
use crate::manhattan::config::PlotType;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use uuid::Uuid;

/// Activate phenotypes from the pending queue up to the active limit.
///
/// This implements lazy loading - phenotypes are only initialized when
/// there's capacity to process them, rather than all at once at job submission.
pub(crate) fn activate_next_phenotypes(batch: &mut BatchState) {
    while batch.active_phenotypes.len() < BATCH_ACTIVE_LIMIT && !batch.pending_queue.is_empty() {
        let spec = batch.pending_queue.pop_front().unwrap();

        // Generate a unique ID for this phenotype
        // Use the output_path as the ID since it should be unique per phenotype
        let phenotype_id = spec.output_path.clone();

        // Get partition counts from the spec (set by CLI/pool submit).
        // If not set, lazily count by reading Hail table metadata.
        let exome_partitions = spec.exome_partitions.unwrap_or_else(|| {
            count_partitions_if_present(spec.exome.as_deref(), "exome", &phenotype_id)
        });
        let genome_partitions = spec.genome_partitions.unwrap_or_else(|| {
            count_partitions_if_present(spec.genome.as_deref(), "genome", &phenotype_id)
        });

        if exome_partitions == 0 && genome_partitions == 0 {
            let error_msg = if spec.exome.is_some() || spec.genome.is_some() {
                format!(
                    "Failed to read partition counts from tables (exome={:?}, genome={:?})",
                    spec.exome, spec.genome
                )
            } else {
                "No exome or genome table paths specified".to_string()
            };
            println!(
                "Warning: Phenotype {} has no partitions: {}",
                phenotype_id, error_msg
            );
            batch.failed_count += 1;
            if let Some(status) = batch.phenotype_statuses.get_mut(&phenotype_id) {
                status.stage = "failed".to_string();
                status.error = Some(error_msg);
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

/// Count partitions for a Hail table path, returning 0 on failure.
/// Uses block_in_place since QueryEngine::open_path does blocking GCS I/O
/// and this is called from tokio worker threads (async handlers).
fn count_partitions_if_present(path: Option<&str>, source: &str, phenotype_id: &str) -> usize {
    let path = match path {
        Some(p) => p,
        None => return 0,
    };
    let path = path.to_string();
    let source = source.to_string();
    let phenotype_id = phenotype_id.to_string();
    tokio::task::block_in_place(move || {
        match genohype_core::query::QueryEngine::open_path(&path) {
            Ok(engine) => {
                let n = engine.num_partitions();
                println!("  Counted {} {} partitions for {}", n, source, phenotype_id);
                n
            }
            Err(e) => {
                println!(
                    "Warning: Failed to count {} partitions for {} ({}): {}",
                    source, phenotype_id, path, e
                );
                0
            }
        }
    })
}

/// Get work for an ingestion job.
pub(crate) fn get_ingestion_work(
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
            session_id: Some(data.session_id.clone()),
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

/// Get work for a batch Manhattan job (multi-phenotype scheduling).
pub(crate) fn get_batch_work(
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

        // Update worker status and task info for visibility
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
            // Set ActiveTaskInfo for aggregate batch - use first phenotype as primary context
            let first_phenotype = phenotype_ids.first().cloned();
            w.current_task = Some(ActiveTaskInfo {
                task_id: task_id.clone(),
                phenotype_id: first_phenotype,
                phase: "aggregate".to_string(),
                source: None, // Aggregate phase doesn't have a specific source
                tasks: phenotype_ids.clone(),
                started_at_ms: CoordinatorData::now_ms(),
            });
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
        // IMPORTANT: The first descriptor uses the coordinator's task_id (UUID) so it can
        // be matched when the worker reports completion. Remaining descriptors use phenotype
        // IDs for progress tracking. This mirrors the scan task pattern.
        let tasks: Vec<TaskDescriptor> = phenotype_ids
            .iter()
            .enumerate()
            .map(|(i, pid)| {
                // First task uses coordinator's task_id for active_tasks lookup
                let descriptor_id = if i == 0 {
                    task_id.clone()
                } else {
                    pid.clone()
                };
                TaskType::Phenotype {
                    phenotype_id: pid.clone(),
                    ancestry: None,
                    operation: PhenotypeOp::ManhattanAggregate,
                }
                .into_descriptor(
                    descriptor_id,
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
            session_id: Some(data.session_id.clone()),
        });
    }

    // Step 3: Priority 2 - Find scan work from active phenotypes
    // Use round-robin to distribute work across phenotypes instead of always
    // hitting the same one (HashMap iteration order is stable but arbitrary).
    let mut phenotype_keys: Vec<String> = batch.active_phenotypes.keys().cloned().collect();
    phenotype_keys.sort(); // Deterministic order

    let num_phenotypes = phenotype_keys.len();
    let start_idx = if num_phenotypes > 0 {
        batch.scan_round_robin % num_phenotypes
    } else {
        0
    };

    // Rotate the list so we start from the round-robin position
    let ordered_keys: Vec<String> = phenotype_keys
        .iter()
        .cycle()
        .skip(start_idx)
        .take(num_phenotypes)
        .cloned()
        .collect();

    for phenotype_id in &ordered_keys {
        let state = match batch.active_phenotypes.get_mut(phenotype_id) {
            Some(s) => s,
            None => continue,
        };

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

        // Advance round-robin for next assignment
        batch.scan_round_robin += 1;

        let task_id = Uuid::new_v4().to_string();

        // Track this task with all partition IDs for completion handling
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::Scan {
                phenotype_id: phenotype_id.clone(),
                partition_ids: partitions.clone(),
                source,
                started_at_ms: CoordinatorData::now_ms(),
            },
        );

        let source_name = match source {
            ManhattanSource::Exome => "exome",
            ManhattanSource::Genome => "genome",
        };

        // Create TaskDescriptors for each partition
        // IMPORTANT: The first descriptor uses the coordinator's task_id (UUID) so it can
        // be matched when the worker reports completion. Remaining descriptors use partition
        // indices for progress tracking.
        let total_tasks = state.exome_total_tasks + state.genome_total_tasks;
        let tasks: Vec<TaskDescriptor> = partitions
            .iter()
            .enumerate()
            .map(|(idx, &i)| {
                // First task uses coordinator's task_id for active_tasks lookup
                let descriptor_id = if idx == 0 {
                    task_id.clone()
                } else {
                    i.to_string()
                };
                TaskType::Partition {
                    table_path: table_path.clone(),
                    partition_index: i,
                    operation: PartitionOp::ManhattanScan {
                        phenotype_id: phenotype_id.clone(),
                        source: source_name.to_string(),
                    },
                }
                .into_descriptor(
                    descriptor_id,
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
            session_id: Some(data.session_id.clone()),
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

/// Get work for a Manhattan pipeline job (two-phase execution).
pub(crate) fn get_manhattan_work(
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
                session_id: Some(data.session_id.clone()),
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

            let phenotype_id = manhattan.original_spec.phenotype.clone().unwrap_or_default();
            let ancestry = manhattan.original_spec.ancestry.clone();

            // Update worker status and task info for visibility
            if let Some(w) = data.worker_registry.get_mut(worker_id) {
                w.status = WorkerStatus::Active;
                w.current_task = Some(ActiveTaskInfo {
                    task_id: task_id.clone(),
                    phenotype_id: Some(phenotype_id.clone()),
                    phase: "aggregate".to_string(),
                    source: ancestry.clone(), // Use ancestry as source context for aggregate
                    tasks: vec![phenotype_id.clone()],
                    started_at_ms: CoordinatorData::now_ms(),
                });
            }

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
                session_id: Some(data.session_id.clone()),
            })
        }

        ManhattanPhase::Complete => {
            // Return Wait so worker stays alive for next job
            axum::Json(WorkResponse::Wait)
        }
    }
}
