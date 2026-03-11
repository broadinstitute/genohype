//! Initialization services for batch jobs.
//!
//! Contains logic for initializing BatchState for ManhattanBatch jobs,
//! extracted from the job submission handler.

use crate::distributed::coordinator::state::BatchState;
use crate::distributed::message::{
    ExecutionMode, ManhattanAggregateSpec, ManhattanSpec, PhenotypeStatus,
};
use std::collections::{HashMap, VecDeque};

/// Initialize batch state from Manhattan specs.
///
/// Creates a BatchState with all phenotypes queued and status tracking initialized.
/// Behavior varies based on execution mode:
/// - ScanOnly/Full: Phenotypes are queued in pending_queue for scanning
/// - AggregateOnly: Phenotypes go directly to ready_to_aggregate
pub fn init_batch_state(specs: &[ManhattanSpec], mode: ExecutionMode) -> BatchState {
    let total_phenotypes = specs.len();

    // Initialize status map for all phenotypes
    let mut phenotype_statuses = HashMap::new();
    for spec in specs {
        let id = spec.output_path.clone();
        phenotype_statuses.insert(
            id.clone(),
            PhenotypeStatus {
                id,
                stage: if mode == ExecutionMode::AggregateOnly {
                    "aggregating".to_string()
                } else {
                    "queued".to_string()
                },
                partitions_done: 0,
                partitions_total: 0, // Updated when activated
                result: None,
                error: None,
                duration_secs: None,
                cpu_core_secs: None,
            },
        );
    }

    let mut batch_state = BatchState {
        mode,
        phenotype_statuses,
        phenotype_start_times: HashMap::new(),
        phenotype_cpu_secs: HashMap::new(),
        pending_queue: VecDeque::new(),
        active_phenotypes: HashMap::new(),
        ready_to_aggregate: Vec::new(),
        completed_count: 0,
        failed_count: 0,
        total_phenotypes,
        aggregate_retry_counts: HashMap::new(),
        aggregate_specs: HashMap::new(),
        scan_round_robin: 0,
    };

    if mode == ExecutionMode::AggregateOnly {
        for spec in specs {
            let id = spec.output_path.clone();
            let aggregate_spec = create_aggregate_spec_from_manhattan_spec(spec);
            batch_state
                .aggregate_specs
                .insert(id.clone(), aggregate_spec.clone());
            batch_state.ready_to_aggregate.push((id, aggregate_spec));
        }
    } else {
        batch_state.pending_queue = specs.iter().cloned().collect();
    }

    batch_state
}

/// Create a ManhattanAggregateSpec from a ManhattanSpec.
///
/// This converts the scan-phase spec into the aggregation-phase spec format.
pub fn create_aggregate_spec_from_manhattan_spec(spec: &ManhattanSpec) -> ManhattanAggregateSpec {
    ManhattanAggregateSpec {
        output_path: spec.output_path.clone(),
        phenotype_id: spec.phenotype.clone(),
        ancestry: spec.ancestry.clone(),
        exome_results: spec.exome.clone(),
        genome_results: spec.genome.clone(),
        gene_burden: spec.gene_burden.clone(),
        exome_exp_p: spec.exome_exp_p.clone(),
        genome_exp_p: spec.genome_exp_p.clone(),
        exome_annotations: spec.exome_annotations.clone(),
        genome_annotations: spec.genome_annotations.clone(),
        genes: spec.genes.clone(),
        threshold: spec.threshold,
        gene_threshold: spec.gene_threshold,
        locus_threshold: spec.locus_threshold,
        locus_window: spec.locus_window,
        locus_plots: spec.locus_plots,
        min_variants_per_locus: spec.min_variants_per_locus,
        width: spec.width,
        height: spec.height,
        layout: spec.layout.clone().unwrap_or_default(),
        y_scale: spec.y_scale.clone().unwrap_or_default(),
        cleanup: false,
        styling: spec.styling.clone(),
    }
}
