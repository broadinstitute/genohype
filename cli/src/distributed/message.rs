//! Message types for Coordinator/Worker communication.
//!
//! These messages are exchanged over HTTP as JSON.

use crate::manhattan::config::{ManhattanConfig, ResolvedStyle};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use genohype_pool::distributed::{
    CompleteRequest, CompleteResponse, CoreTaskInfo, HardwareSpec, HeartbeatRequest,
    HeartbeatResponse, StatusResponse, TaskDescriptor, TelemetrySnapshot, WorkRequest,
    WorkResponse,
};

// ============================================================================
// Domain-Specific Task Types
// ============================================================================

/// Describes what kind of work a task represents.
///
/// This is the domain-specific payload that gets serialized into `TaskDescriptor.payload`.
/// Workers deserialize this to understand what operation to perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskType {
    /// Scanning a partition of a Hail table
    Partition {
        table_path: String,
        partition_index: usize,
        operation: PartitionOp,
    },

    /// Processing a phenotype (Manhattan aggregate, ingest, etc.)
    Phenotype {
        phenotype_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ancestry: Option<String>,
        operation: PhenotypeOp,
    },

    /// Synthetic stress test iteration
    Stress { iteration: usize },

    /// Locus plot generation
    Locus {
        phenotype_id: String,
        chromosome: String,
        position: u32,
    },

    /// Generic/custom task type
    Custom {
        name: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,
    },
}

/// Operation to perform on a Hail table partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PartitionOp {
    /// Scan and export to Parquet
    ExportParquet { output_path: String },

    /// Scan and export to JSON
    ExportJson {
        output_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        group_by: Option<String>,
    },

    /// Scan and upload to ClickHouse
    ExportClickhouse {
        clickhouse_url: String,
        table_name: String,
    },

    /// Scan and compute statistics
    Summary,

    /// Scan for Manhattan plot points
    ManhattanScan {
        /// Which phenotype this scan is for
        phenotype_id: String,
        /// "exome" or "genome"
        source: String,
    },
}

/// Operation to perform on a phenotype.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PhenotypeOp {
    /// Aggregate Manhattan scan results into final plots
    ManhattanAggregate,

    /// Ingest phenotype data into ClickHouse
    Ingest {
        clickhouse_url: String,
        database: String,
    },
}

impl TaskType {
    /// Convert this task type into a TaskDescriptor for network transmission.
    pub fn into_descriptor(
        self,
        id: String,
        label: Option<String>,
        index: Option<usize>,
        total: Option<usize>,
    ) -> TaskDescriptor {
        let type_name = match &self {
            TaskType::Partition { .. } => "partition",
            TaskType::Phenotype { .. } => "phenotype",
            TaskType::Stress { .. } => "stress",
            TaskType::Locus { .. } => "locus",
            TaskType::Custom { .. } => "custom",
        }
        .to_string();

        TaskDescriptor {
            id,
            task_type: type_name,
            label,
            index,
            total,
            payload: serde_json::to_value(self).unwrap_or_default(),
        }
    }
}

/// Table initialization strategy for ingestion.
///
/// Controls how tables are created/managed at the start of ingestion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum InitStrategy {
    /// Create tables if they don't exist (default)
    #[default]
    Create,
    /// Drop existing tables and recreate them (replaces all data)
    Replace,
    /// Do nothing - assume tables already exist
    Append,
}

/// Execution mode for a pipeline job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExecutionMode {
    #[default]
    Full,
    ScanOnly,
    AggregateOnly,
}

/// Specification for a distributed job operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JobSpec {
    /// Export to Parquet files
    ExportParquet {
        output_path: String,
    },
    /// Export to JSON files (NDJSON)
    ExportJson {
        output_path: String,
        /// Optional field to group by (creates separate files/directories)
        #[serde(default)]
        group_by: Option<String>,
    },
    /// Compute summary statistics
    Summary,
    /// Validate against schema
    Validate {
        schema_path: String,
        #[serde(default)]
        fail_fast: bool,
    },
    /// Generate Manhattan plot (high-level submission - coordinator splits into phases)
    Manhattan { spec: ManhattanSpec, mode: ExecutionMode },
    /// Submit a batch of Manhattan plots (coordinator queues them)
    ManhattanBatch { specs: Vec<ManhattanSpec>, mode: ExecutionMode },
    /// Phase 1: Worker scans partitions, outputs partial PNGs + sig.parquet
    ManhattanScan(ManhattanScanSpec),
    /// Phase 2: Single worker aggregates results, joins annotations, generates locus plots
    ManhattanAggregate(ManhattanAggregateSpec),
    /// Phase 2 (Batch): Worker executes multiple aggregation tasks in parallel
    ManhattanAggregateBatch { specs: Vec<ManhattanAggregateSpec> },
    /// Generate locus plots from existing Manhattan output
    Loci(LociSpec),
    /// Export to ClickHouse (requires clickhouse feature)
    ExportClickhouse {
        clickhouse_url: String,
        table_name: String,
    },
    /// Ingest Manhattan results into ClickHouse (Coordinator job)
    /// Coordinator scans input_dir, creates one task per phenotype
    IngestManhattan {
        input_dir: String,
        clickhouse_url: String,
        database: String,
        /// Table initialization strategy (create, replace, append)
        #[serde(default)]
        init_strategy: InitStrategy,
    },
    /// Ingest a specific phenotype into ClickHouse (Worker task)
    IngestManhattanTask {
        phenotype_id: String,
        ancestry: String,
        /// Base path for this phenotype (e.g., gs://bucket/manhattans/meta/pheno_id/)
        base_path: String,
        clickhouse_url: String,
        database: String,
    },
    /// Synthetic workload for testing cluster telemetry and autoscaling
    Stress(StressSpec),
}

/// Parameters for a synthetic stress test workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressSpec {
    /// Total number of tasks to queue
    pub partitions: usize,
    /// Seconds of pure CPU math to spin per partition
    pub cpu_secs: f64,
    /// Megabytes of RAM to allocate and hold per partition
    pub memory_mb: usize,
    /// A file to stream into memory to generate Network RX
    pub read_path: Option<String>,
    /// A directory to pump random bytes into to generate Network TX
    pub write_dir: Option<String>,
    /// Generate temporary read data (writes to write_dir first, then reads back)
    #[serde(default)]
    pub generate_read_data: bool,
    /// Size in MB of generated read data per partition (default: 32)
    #[serde(default = "default_read_data_size_mb")]
    pub read_data_size_mb: usize,
}

fn default_read_data_size_mb() -> usize {
    32
}

/// Progress stats for batch Manhattan jobs (multi-phenotype mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardBatchProgress {
    pub total: usize,
    pub queued: usize,
    pub active_scan: usize,
    pub active_aggregate: usize,
    pub completed: usize,
    pub failed: usize,
}

/// Status of a single phenotype in a batch job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhenotypeStatus {
    pub id: String,
    /// Stage: "queued", "scanning", "aggregating", "completed", "failed"
    pub stage: String,
    pub partitions_done: usize,
    pub partitions_total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    /// Total duration in seconds (from scan start to completion)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<f64>,
    /// Accumulated CPU core-seconds for this phenotype
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_core_secs: Option<f64>,
}

/// Response containing status of all phenotypes in a batch.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchStatusResponse {
    pub phenotypes: Vec<PhenotypeStatus>,
}

/// Configuration for distributed locus plot generation (coordinator-level).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LociSpec {
    /// Path to Manhattan output directory (contains *_significant.parquet)
    pub output_dir: String,
    /// Path to exome results Hail table (for reading variants)
    #[serde(default)]
    pub exome_results: Option<String>,
    /// Path to genome results Hail table (for reading variants)
    #[serde(default)]
    pub genome_results: Option<String>,
    /// Path to gene burden results Hail table (for seeding locus regions)
    #[serde(default)]
    pub gene_burden: Option<String>,
    /// Window size around significant hits
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,
    /// Significance threshold for variants
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    /// Significance threshold for gene burden
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,
    /// Generate locus-zoom style plots for significant regions
    #[serde(default)]
    pub locus_plots: bool,
    /// Minimum number of significant variants required to form a locus
    #[serde(default = "default_min_variants_per_locus")]
    pub min_variants_per_locus: usize,
}

/// A locus region to generate a plot for.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocusRegion {
    pub contig: String,
    pub start: i32,
    pub end: i32,
    /// Lead variant position (for manifest)
    pub lead_position: i32,
    /// Lead variant p-value
    pub lead_pvalue: f64,
    /// Source of lead variant (exome/genome)
    pub lead_source: String,
}

/// Worker-level locus generation job (assigned subset of regions).
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LociWorkerSpec {
    /// Path to Manhattan output directory
    pub output_dir: String,
    /// Path to exome results Hail table
    #[serde(default)]
    pub exome_results: Option<String>,
    /// Path to genome results Hail table
    #[serde(default)]
    pub genome_results: Option<String>,
    /// Assigned locus regions to process
    pub regions: Vec<LocusRegion>,
    /// Significance threshold
    pub threshold: f64,
}

fn default_threshold() -> f64 {
    5e-8
}

/// Configuration for a distributed Manhattan plot job.
///
/// Matches the fields in `PipelineConfig` from `src/manhattan/pipeline.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManhattanSpec {
    // Identity metadata (for ClickHouse partitioning)
    /// Phenotype identifier (e.g., analysis ID)
    #[serde(default)]
    pub phenotype: Option<String>,
    /// Ancestry group (e.g., "meta", "EUR", "AFR")
    #[serde(default)]
    pub ancestry: Option<String>,

    // Data inputs
    /// Path to Exome results table
    #[serde(default)]
    pub exome: Option<String>,
    /// Path to Exome annotations table (for merge-join)
    #[serde(default)]
    pub exome_annotations: Option<String>,
    /// Path to Genome results table
    #[serde(default)]
    pub genome: Option<String>,
    /// Path to Genome annotations table (for merge-join)
    #[serde(default)]
    pub genome_annotations: Option<String>,
    /// Path to gene burden results table
    #[serde(default)]
    pub gene_burden: Option<String>,
    /// Path to gnomAD genes table
    #[serde(default)]
    pub genes: Option<String>,
    /// Path to exome expected p-values table (for QQ plots)
    #[serde(default)]
    pub exome_exp_p: Option<String>,
    /// Path to genome expected p-values table (for QQ plots)
    #[serde(default)]
    pub genome_exp_p: Option<String>,

    // Thresholds
    /// P-value threshold for significant variants (default: 5e-8)
    pub threshold: f64,
    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,
    /// P-value threshold to buffer variants for locus plots (default: 0.01)
    #[serde(default = "default_locus_threshold")]
    pub locus_threshold: f64,
    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,
    /// Generate locus-zoom style plots for significant regions
    #[serde(default)]
    pub locus_plots: bool,
    /// Minimum number of significant variants required to form a locus
    #[serde(default = "default_min_variants_per_locus")]
    pub min_variants_per_locus: usize,

    // Output
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,
    /// Field name for P-value (Y-axis)
    pub y_field: String,
    /// Output path (directory)
    pub output_path: String,

    // Pre-computed layout (set by coordinator for distributed rendering)
    /// Chromosome layout for mapping genomic positions to pixel X coordinates
    #[serde(default)]
    pub layout: Option<crate::manhattan::layout::ChromosomeLayout>,
    /// Y-axis scale for mapping -log10(p) to pixel Y coordinates
    #[serde(default)]
    pub y_scale: Option<crate::manhattan::layout::YScale>,

    /// Contig lengths (name -> bp) for per-chromosome layout generation
    #[serde(default)]
    pub contig_lengths: Option<HashMap<String, u32>>,

    /// Skip automatic composite step (run manually later with --from-shards)
    #[serde(default)]
    pub skip_composite: bool,

    // Partition counts (set by pool submit for multi-table jobs)
    /// Number of partitions in exome results table
    #[serde(default)]
    pub exome_partitions: Option<usize>,
    /// Number of partitions in genome results table
    #[serde(default)]
    pub genome_partitions: Option<usize>,

    /// Styling configuration for all plots
    #[serde(default)]
    pub styling: ManhattanConfig,
}

fn default_gene_threshold() -> f64 {
    2.5e-6
}

fn default_locus_threshold() -> f64 {
    0.01
}

fn default_locus_window() -> i32 {
    1_000_000
}

fn default_min_variants_per_locus() -> usize {
    1
}

impl ManhattanSpec {
    /// Get the primary input path for partition counting.
    ///
    /// Returns the first available of: exome, genome, or gene_burden.
    pub fn primary_input_path(&self) -> Option<&str> {
        self.exome
            .as_deref()
            .or(self.genome.as_deref())
            .or(self.gene_burden.as_deref())
    }
}

/// Data source indicator for Manhattan scan partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManhattanSource {
    Exome,
    Genome,
}

/// Phase 1: Scan job specification for a single data source.
///
/// Workers receive this spec to scan partitions and output:
/// - Partial PNG (transparent background, for later compositing)
/// - Significant hits Parquet (variants below threshold)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManhattanScanSpec {
    // Identity metadata (for significant hits Parquet)
    /// Phenotype identifier
    pub phenotype: String,
    /// Ancestry group
    pub ancestry: String,

    /// Which data source this scan is for
    pub source: ManhattanSource,
    /// Path to the results table (exome or genome)
    pub table_path: String,
    /// Output directory (will create {source}/part-*.png and {source}/part-*-sig.parquet)
    pub output_path: String,
    /// P-value threshold for significant hits (default: 5e-8)
    pub threshold: f64,
    /// Field name for P-value (Y-axis)
    pub y_field: String,

    // Pre-computed layout from coordinator
    /// Chromosome layout for mapping genomic positions to pixel X coordinates
    pub layout: crate::manhattan::layout::ChromosomeLayout,
    /// Y-axis scale for mapping -log10(p) to pixel Y coordinates
    pub y_scale: crate::manhattan::layout::YScale,
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,

    /// Contig lengths for per-chromosome layout generation
    #[serde(default)]
    pub contig_lengths: HashMap<String, u32>,

    /// Resolved styling for this scan (background, colors, point size)
    #[serde(default)]
    pub style: ResolvedStyle,
}

/// Phase 2: Aggregate job specification.
///
/// A single worker receives this to:
/// 1. Composite partial PNGs into final Manhattan plots
/// 2. Process gene burden table
/// 3. Merge significant hits from scan phase
/// 4. Generate locus plots for significant regions
/// 5. Write manifest.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManhattanAggregateSpec {
    /// Output directory (contains partial PNGs/parquets from scan phase)
    pub output_path: String,

    /// Phenotype identifier (for telemetry and tracking)
    #[serde(default)]
    pub phenotype_id: Option<String>,
    /// Ancestry group (for telemetry and tracking)
    #[serde(default)]
    pub ancestry: Option<String>,

    // Input tables for targeted reads (locus plots)
    /// Path to Exome results Hail table
    #[serde(default)]
    pub exome_results: Option<String>,
    /// Path to Genome results Hail table
    #[serde(default)]
    pub genome_results: Option<String>,
    /// Path to gene burden results Hail table
    #[serde(default)]
    pub gene_burden: Option<String>,
    /// Path to exome expected p-values table (for QQ plots)
    #[serde(default)]
    pub exome_exp_p: Option<String>,
    /// Path to genome expected p-values table (for QQ plots)
    #[serde(default)]
    pub genome_exp_p: Option<String>,

    // Annotation tables (original Hail tables - for locus plot annotation)
    /// Path to exome annotations Hail table
    #[serde(default)]
    pub exome_annotations: Option<String>,
    /// Path to genome annotations Hail table
    #[serde(default)]
    pub genome_annotations: Option<String>,
    /// Path to gnomAD genes Hail table
    #[serde(default)]
    pub genes: Option<String>,

    // Thresholds
    /// P-value threshold for significant variants (for reference)
    pub threshold: f64,
    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,
    /// P-value threshold for locus plot regions (default: 0.01)
    #[serde(default = "default_locus_threshold")]
    pub locus_threshold: f64,
    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,
    /// Generate locus-zoom style plots for significant regions
    #[serde(default)]
    pub locus_plots: bool,
    /// Minimum number of significant variants required to form a locus
    #[serde(default = "default_min_variants_per_locus")]
    pub min_variants_per_locus: usize,

    // Rendering config
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,

    // Layout (same as scan phase)
    /// Chromosome layout for rendering gene burden manhattan
    pub layout: crate::manhattan::layout::ChromosomeLayout,
    /// Y-axis scale for rendering gene burden manhattan
    pub y_scale: crate::manhattan::layout::YScale,

    /// Delete intermediate shards after aggregation
    #[serde(default)]
    pub cleanup: bool,

    /// Styling configuration for all plots (gene burden, locus, etc.)
    #[serde(default)]
    pub styling: ManhattanConfig,
}

impl JobSpec {
    /// Get a human-readable description of the job type.
    pub fn description(&self) -> &'static str {
        match self {
            JobSpec::ExportParquet { .. } => "export parquet",
            JobSpec::ExportJson { .. } => "export json",
            JobSpec::Summary => "summary",
            JobSpec::Validate { .. } => "validate",
            JobSpec::Manhattan { .. } => "manhattan plot",
            JobSpec::ManhattanBatch { .. } => "manhattan batch",
            JobSpec::ManhattanScan(_) => "manhattan scan",
            JobSpec::ManhattanAggregate(_) => "manhattan aggregate",
            JobSpec::ManhattanAggregateBatch { .. } => "manhattan aggregate batch",
            JobSpec::Loci(_) => "loci plots",
            JobSpec::ExportClickhouse { .. } => "export clickhouse",
            JobSpec::IngestManhattan { .. } => "ingest manhattan",
            JobSpec::IngestManhattanTask { .. } => "ingest manhattan task",
            JobSpec::Stress(_) => "synthetic stress test",
        }
    }

    /// Get the output path if this job produces output.
    pub fn output_path(&self) -> Option<&str> {
        match self {
            JobSpec::ExportParquet { output_path } => Some(output_path),
            JobSpec::ExportJson { output_path, .. } => Some(output_path),
            JobSpec::Summary => None,
            JobSpec::Validate { .. } => None,
            JobSpec::Manhattan { spec, .. } => Some(&spec.output_path),
            JobSpec::ManhattanBatch { specs, .. } => specs.first().map(|s| s.output_path.as_str()),
            JobSpec::ManhattanScan(spec) => Some(&spec.output_path),
            JobSpec::ManhattanAggregate(spec) => Some(&spec.output_path),
            JobSpec::ManhattanAggregateBatch { specs } => specs.first().map(|s| s.output_path.as_str()),
            JobSpec::Loci(spec) => Some(&spec.output_dir),
            JobSpec::ExportClickhouse { table_name, .. } => Some(table_name),
            JobSpec::IngestManhattan { input_dir, .. } => Some(input_dir),
            JobSpec::IngestManhattanTask { base_path, .. } => Some(base_path),
            JobSpec::Stress(spec) => spec.write_dir.as_deref(),
        }
    }

    /// Generate task descriptors for this job.
    ///
    /// Converts a job specification into discrete work units that can be
    /// dispatched to workers. Each task includes full context for logging,
    /// dashboard display, and type-safe dispatch.
    ///
    /// # Arguments
    /// * `input_path` - Path to the input data (Hail table, etc.)
    /// * `total_tasks` - Total number of tasks to generate (usually partition count)
    pub fn generate_tasks(&self, input_path: &str, total_tasks: usize) -> Vec<TaskDescriptor> {
        match self {
            JobSpec::ExportParquet { output_path } => (0..total_tasks)
                .map(|i| {
                    TaskType::Partition {
                        table_path: input_path.to_string(),
                        partition_index: i,
                        operation: PartitionOp::ExportParquet {
                            output_path: output_path.clone(),
                        },
                    }
                    .into_descriptor(
                        i.to_string(),
                        Some(format!("Partition {} → Parquet", i + 1)),
                        Some(i),
                        Some(total_tasks),
                    )
                })
                .collect(),

            JobSpec::ExportJson {
                output_path,
                group_by,
            } => (0..total_tasks)
                .map(|i| {
                    TaskType::Partition {
                        table_path: input_path.to_string(),
                        partition_index: i,
                        operation: PartitionOp::ExportJson {
                            output_path: output_path.clone(),
                            group_by: group_by.clone(),
                        },
                    }
                    .into_descriptor(
                        i.to_string(),
                        Some(format!("Partition {} → JSON", i + 1)),
                        Some(i),
                        Some(total_tasks),
                    )
                })
                .collect(),

            JobSpec::ExportClickhouse {
                clickhouse_url,
                table_name,
            } => (0..total_tasks)
                .map(|i| {
                    TaskType::Partition {
                        table_path: input_path.to_string(),
                        partition_index: i,
                        operation: PartitionOp::ExportClickhouse {
                            clickhouse_url: clickhouse_url.clone(),
                            table_name: table_name.clone(),
                        },
                    }
                    .into_descriptor(
                        i.to_string(),
                        Some(format!("Partition {} → ClickHouse", i + 1)),
                        Some(i),
                        Some(total_tasks),
                    )
                })
                .collect(),

            JobSpec::Summary => (0..total_tasks)
                .map(|i| {
                    TaskType::Partition {
                        table_path: input_path.to_string(),
                        partition_index: i,
                        operation: PartitionOp::Summary,
                    }
                    .into_descriptor(
                        i.to_string(),
                        Some(format!("Partition {} → Stats", i + 1)),
                        Some(i),
                        Some(total_tasks),
                    )
                })
                .collect(),

            JobSpec::Stress(spec) => (0..spec.partitions)
                .map(|i| {
                    TaskType::Stress { iteration: i }.into_descriptor(
                        format!("stress_{}", i),
                        Some(format!("Stress {}", i + 1)),
                        Some(i),
                        Some(spec.partitions),
                    )
                })
                .collect(),

            JobSpec::ManhattanBatch { specs, .. } => specs
                .iter()
                .enumerate()
                .map(|(i, spec)| {
                    let pheno = spec.phenotype.clone().unwrap_or_default();
                    let ancestry = spec.ancestry.clone();
                    let label = ancestry
                        .as_ref()
                        .map(|a| format!("{} ({})", pheno, a))
                        .unwrap_or_else(|| pheno.clone());

                    TaskType::Phenotype {
                        phenotype_id: pheno.clone(),
                        ancestry,
                        operation: PhenotypeOp::ManhattanAggregate,
                    }
                    .into_descriptor(
                        pheno,
                        Some(format!("{} → Aggregate", label)),
                        Some(i),
                        Some(specs.len()),
                    )
                })
                .collect(),

            // These job types have specialized runtime-managed queues or are single tasks
            JobSpec::Validate { .. }
            | JobSpec::Manhattan { .. }
            | JobSpec::ManhattanScan(_)
            | JobSpec::ManhattanAggregate(_)
            | JobSpec::ManhattanAggregateBatch { .. }
            | JobSpec::Loci(_)
            | JobSpec::IngestManhattan { .. }
            | JobSpec::IngestManhattanTask { .. } => vec![],
        }
    }
}




/// Dashboard summary for the overall job.
#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardSummary {
    /// Job progress percentage (0-100)
    pub progress_percent: f64,
    /// Total tasks in the job
    pub total_tasks: usize,
    /// Number of tasks assigned per work request
    #[serde(default)]
    pub batch_size: usize,
    /// Tasks completed
    pub completed_tasks: usize,
    /// Tasks currently processing
    pub processing_tasks: usize,
    /// Tasks pending
    pub pending_tasks: usize,
    /// Tasks permanently failed
    pub failed_tasks: usize,
    /// Total items processed across all workers
    pub total_items: usize,
    /// Aggregate items per second across all workers
    pub cluster_items_per_sec: f64,
    /// Job elapsed time in seconds
    pub elapsed_secs: f64,
    /// Estimated time remaining in seconds, if calculable
    pub eta_secs: Option<f64>,
    /// Whether the job is complete
    pub is_complete: bool,
    /// Input path being processed
    pub input_path: String,
    /// Job specification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_spec: Option<JobSpec>,
    /// Whether the coordinator is idle (waiting for job submission)
    #[serde(default)]
    pub idle: bool,
    /// Last error message (if any task failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Batch progress stats (for batch Manhattan jobs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_progress: Option<DashboardBatchProgress>,
    /// Build version (git commit hash)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
    /// Cumulative CPU-seconds spent in scan phase
    #[serde(default)]
    pub scan_cpu_secs: f64,
    /// Cumulative CPU-seconds spent in aggregate phase
    #[serde(default)]
    pub aggregate_cpu_secs: f64,
    /// Cumulative CPU-seconds wasted due to failures/preemption
    #[serde(default)]
    pub wasted_cpu_secs: f64,
}

/// Request to submit a new job to an idle coordinator.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobConfigRequest {
    /// Path to input Hail table
    pub input_path: String,
    /// Job specification
    pub job_spec: JobSpec,
    /// Total number of tasks to process
    pub total_tasks: usize,
    /// Number of tasks per work request (optional, defaults to coordinator's batch_size)
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// Force submission even if a job is already running (supersede)
    #[serde(default)]
    pub force: bool,
    /// Filter conditions (where clauses)
    #[serde(default)]
    pub filters: Vec<String>,
    /// Interval filters
    #[serde(default)]
    pub intervals: Vec<String>,
    /// Hint for memory required per task in MB (overrides default heuristics)
    #[serde(default)]
    pub memory_weight_mb: Option<u64>,
}

/// Response to a job submission request.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobConfigResponse {
    /// Whether the job was accepted
    pub acknowledged: bool,
    /// Error message if job was rejected
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to cancel the current job.
#[derive(Debug, Serialize, Deserialize)]
pub struct CancelRequest {
    /// Optional reason for cancellation
    pub reason: Option<String>,
}

/// Response to cancellation request.
#[derive(Debug, Serialize, Deserialize)]
pub struct CancelResponse {
    /// Whether cancellation was successful
    pub success: bool,
    /// Status message
    pub message: String,
}

/// Request to export metrics database to GCS.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportMetricsRequest {
    /// GCS path to upload the metrics database (e.g., gs://bucket/path/metrics.db)
    pub destination: String,
}

/// Response to metrics export request.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportMetricsResponse {
    /// Whether the export was successful
    pub success: bool,
    /// Path where metrics were uploaded
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Error message if export failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Dashboard info about a single worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardWorker {
    /// Worker identifier
    pub worker_id: String,
    /// Current status
    pub status: String,
    /// Current dynamically adjusted batch size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_batch_size: Option<usize>,
    /// Seconds since last heartbeat
    pub last_seen_secs: f64,
    /// Latest telemetry snapshot
    pub telemetry: Option<TelemetrySnapshot>,
    /// Total items reported by this worker
    pub total_items: usize,
    /// Total tasks completed by this worker
    pub tasks_completed: usize,
    /// Currently assigned task, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_task: Option<ActiveTaskInfo>,
}

/// Time-series metrics data for charts.
#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardMetrics {
    /// Per-worker time-series data
    pub workers: Vec<WorkerMetricsSeries>,
}

/// Time-series data for a single worker.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerMetricsSeries {
    /// Worker identifier
    pub worker_id: String,
    /// Telemetry snapshots (most recent last)
    pub snapshots: Vec<TelemetrySnapshot>,
}

/// Response containing aggregated job results.
///
/// Used by `GET /api/result` to retrieve final stats for summary/validate jobs.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobResultResponse {
    /// Whether a result is available
    pub available: bool,
    /// The aggregated result (structure depends on job type)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Error message if result retrieval failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Information about a task currently being processed by a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveTaskInfo {
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phenotype_id: Option<String>,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Task IDs being processed (replaces partition indices)
    pub tasks: Vec<String>,
    pub started_at_ms: u64,
}

/// A historical event in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub timestamp_ms: u64,
    pub event_type: String, // "assigned", "completed", "failed", "requeued"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phenotype_id: Option<String>,
    pub details: String,
}

/// A record of a failed task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureRecord {
    pub timestamp_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phenotype_id: Option<String>,
    /// Task IDs that failed (replaces partition indices)
    pub tasks: Vec<String>,
    pub worker_id: String,
    pub error: String,
    pub retry_count: usize,
    /// Time wasted on this failed task (milliseconds)
    #[serde(default)]
    pub wasted_duration_ms: u64,
}

/// Response for GET /api/events endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct EventsResponse {
    pub events: Vec<JobEvent>,
}

/// Response for GET /api/failures endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct FailuresResponse {
    pub failures: Vec<FailureRecord>,
}

/// Response for GET /api/dashboard/bottlenecks endpoint.
///
/// Provides real-time bottleneck analysis based on aggregated worker telemetry.
#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardBottleneck {
    /// Bottleneck identifier: "CPU", "Memory", "Network RX", "Network TX", "I/O Wait", "Mixed", "Idle"
    pub bottleneck: String,
    /// Human-readable description of the bottleneck state
    pub description: String,
    /// Average CPU utilization across active workers (0-100%)
    pub avg_cpu_percent: f32,
    /// Average memory utilization across active workers (0-100%)
    pub avg_mem_percent: f32,
    /// Average network download rate across active workers (MB/s)
    pub avg_network_rx_mb: f64,
    /// Average network upload rate across active workers (MB/s)
    pub avg_network_tx_mb: f64,
}
