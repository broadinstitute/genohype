//! Centralized job management service.
//!
//! Extracts the boilerplate state reset, DB insertion, and state-machine
//! initialization from the HTTP handlers into reusable functions. This ensures
//! all job submission paths (CLI `/api/job` and UI `/api/catalog/process`)
//! converge into the same state machine transition.

use crate::distributed::coordinator::state::{
    CoordinatorData, JobExecutionState, ManhattanPhase, ManhattanPipelineState,
};
use crate::distributed::message::{JobEvent, JobRecord, JobSpec, ManhattanSpec, PhenotypeStatus};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Centralized setup for starting a new distributed job.
///
/// Resets queues, generates a job ID, persists to the metrics DB, and
/// initializes the correct `JobExecutionState`. For `ManhattanBatch` jobs,
/// specs are enriched with default layouts before initialization.
pub(crate) fn start_new_job(
    data: &mut CoordinatorData,
    mut job_spec: JobSpec,
    input_path: String,
    total_tasks: usize,
    batch_size: Option<usize>,
    memory_weight_mb: Option<u64>,
    filters: Vec<String>,
    intervals: Vec<String>,
) -> Result<(), String> {
    // Enrich specs if it's a batch job
    if let JobSpec::ManhattanBatch {
        ref mut specs, ..
    } = job_spec
    {
        super::batch_init::enrich_specs(specs);
    }

    // Configure the job
    data.config.input_path = input_path.clone();
    data.config.job_spec = Some(job_spec.clone());
    data.config.total_tasks = total_tasks;
    data.config.filters = filters;
    data.config.intervals = intervals;
    data.config.memory_weight_mb = memory_weight_mb;
    if let Some(bs) = batch_size {
        data.config.batch_size = bs;
    }

    // Reset job state tracking
    data.pending_partitions = (0..total_tasks).collect();
    data.completed_tasks.clear();
    data.processing_partitions.clear();
    data.failed_partitions.clear();
    data.retry_counts.clear();
    data.total_rows = 0;
    data.job_start_time = Instant::now();
    data.last_progress_time = Instant::now();
    data.aggregated_results.clear();
    data.job_state = JobExecutionState::Standard;
    data.active_tasks.clear();
    data.last_completed_batch = None;
    data.events.clear();
    data.failures.clear();

    // Reset learned capacity limits for all workers on new job submission.
    for worker in data.worker_registry.values_mut() {
        worker.max_batch_capacity = None;
        worker.current_batch_size = None;
    }

    // Generate a unique job ID and persist to database
    let job_id = uuid::Uuid::new_v4().to_string();
    data.current_job_id = Some(job_id.clone());

    let job_record = JobRecord {
        job_id: job_id.clone(),
        status: "running".to_string(),
        start_time_ms: CoordinatorData::now_ms(),
        end_time_ms: None,
        job_spec_json: serde_json::to_value(&job_spec).ok(),
        input_path,
        total_tasks,
        job_type: Some(job_spec.description().to_string()),
    };

    if let Err(e) = data.metrics_db.insert_job(&job_record) {
        eprintln!("Warning: failed to persist job to DB: {}", e);
    }

    // Mark as no longer idle
    data.idle = false;

    // Initialize specific execution states
    match &job_spec {
        JobSpec::ManhattanBatch { specs, mode, .. } => {
            data.pending_partitions.clear();
            let batch_state = super::init_batch_state(specs, *mode);
            data.job_state = JobExecutionState::Batch(batch_state);
        }
        JobSpec::Manhattan { spec, mode } => {
            data.pending_partitions.clear();
            let exome_partitions = spec.exome_partitions.unwrap_or_else(|| {
                if spec.exome.is_some() {
                    total_tasks
                } else {
                    0
                }
            });
            let genome_partitions = spec.genome_partitions.unwrap_or_else(|| {
                if spec.genome.is_some() && spec.exome.is_none() {
                    total_tasks
                } else {
                    0
                }
            });
            let initial_phase =
                if *mode == crate::distributed::message::ExecutionMode::AggregateOnly {
                    ManhattanPhase::Aggregate
                } else {
                    ManhattanPhase::Scan
                };

            data.job_state = JobExecutionState::Manhattan(ManhattanPipelineState {
                mode: *mode,
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
            });
        }
        _ => {
            // Standard, IngestManhattan, Stress, etc. — keep JobExecutionState::Standard
            // (IngestManhattan overrides this after discovery in the handler)
        }
    }

    Ok(())
}

/// Appends new phenotypes dynamically to an already-running Batch job.
///
/// Specs are enriched with default layouts before being queued.
pub(crate) fn append_to_batch(
    data: &mut CoordinatorData,
    mut specs: Vec<ManhattanSpec>,
) -> Result<(), String> {
    super::batch_init::enrich_specs(&mut specs);
    let mut appended = Vec::new();

    if let JobExecutionState::Batch(ref mut batch) = data.job_state {
        for spec in specs {
            let id = spec.output_path.clone();
            if !batch.phenotype_statuses.contains_key(&id) {
                batch.phenotype_statuses.insert(
                    id.clone(),
                    PhenotypeStatus {
                        id: id.clone(),
                        stage: "queued".to_string(),
                        partitions_done: 0,
                        partitions_total: 0,
                        result: None,
                        error: None,
                        duration_secs: None,
                        cpu_core_secs: None,
                    },
                );
                appended.push(spec.phenotype.clone().unwrap_or_default());
                batch.pending_queue.push_back(spec);
                batch.total_phenotypes += 1;
            }
        }
        crate::distributed::coordinator::scheduler::assignment::activate_next_phenotypes(batch);
    } else {
        return Err("Cannot append: no batch job is running".to_string());
    }

    if !appended.is_empty() {
        data.log_event(JobEvent {
            timestamp_ms: CoordinatorData::now_ms(),
            event_type: "submitted".to_string(),
            worker_id: None,
            phenotype_id: None,
            details: format!(
                "Appended {} phenotypes to batch: {}",
                appended.len(),
                appended.join(", ")
            ),
        });
    }
    Ok(())
}
