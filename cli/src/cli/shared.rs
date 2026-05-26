//! Shared argument types used across multiple commands.

use clap::Args;

/// Arguments for distributed processing (partition slicing).
/// Include these in any command that should support distributed execution.
#[derive(Args, Clone, Copy, Debug)]
pub struct PartitioningArgs {
    /// Worker ID (0-based) for distributed processing
    #[arg(long, default_value = "0")]
    pub worker_id: usize,

    /// Total number of workers in the pool
    #[arg(long, default_value = "1")]
    pub total_workers: usize,
}

impl PartitioningArgs {
    /// Returns true if this is a distributed job (more than one worker).
    pub fn is_distributed(&self) -> bool {
        self.total_workers > 1
    }
}

/// Common arguments shared by all export commands.
/// Use `#[command(flatten)]` to include these in export arg structs,
/// then implement `HasCommonExportArgs` for compile-time enforcement.
#[derive(Args)]
pub struct CommonExportArgs {
    /// Path to the Hail table
    pub input: String,

    /// Filter conditions (field=value, field>value, field>=value, etc.)
    #[arg(long = "where")]
    pub where_clauses: Vec<String>,

    /// Limit number of rows to export
    #[arg(long)]
    pub limit: Option<usize>,

    /// Genomic interval (chr:start-end format, can be specified multiple times)
    #[arg(long)]
    pub interval: Vec<String>,

    /// Path to interval file (.bed, .json, or text with chr:start-end lines)
    #[arg(long)]
    pub intervals_file: Option<String>,

    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,

    /// Output progress as JSON lines (for distributed job coordination)
    #[arg(long, hide = true)]
    pub progress_json: bool,
}

/// Trait that all export argument structs must implement.
/// This enforces at compile time that all export targets have common args.
pub trait HasCommonExportArgs {
    fn common(&self) -> &CommonExportArgs;
}

/// VEP annotation arguments, flattened into export commands.
///
/// When `--vep-gff3` is provided, the QueryEngine wraps its data source
/// in AnnotatingDataSource, adding a `vep` field to every row.
#[cfg(feature = "vep")]
#[derive(Args, Clone)]
pub struct VepArgs {
    /// Path to GFF3 transcript annotation file (enables VEP annotation)
    #[arg(long)]
    pub vep_gff3: Option<String>,

    /// Path to reference FASTA file (enables HGVS notations in VEP output)
    #[arg(long)]
    pub vep_fasta: Option<String>,

    /// Path to supplementary annotation directory (ClinVar, gnomAD, etc.)
    #[arg(long)]
    pub vep_sa_dir: Option<String>,

    /// Maximum distance (bp) from transcript for upstream/downstream annotation
    #[arg(long, default_value = "5000")]
    pub vep_distance: u64,

    /// Select one consequence per variant (canonical transcript preferred)
    #[arg(long)]
    pub vep_pick: bool,
}

#[cfg(feature = "vep")]
impl VepArgs {
    /// Convert to VepInitOptions if VEP annotation is requested.
    pub fn to_init_options(&self) -> Option<genohype_core::datasource::annotating::VepInitOptions> {
        self.vep_gff3.as_ref().map(|gff3| {
            genohype_core::datasource::annotating::VepInitOptions {
                gff3: gff3.clone(),
                fasta: self.vep_fasta.clone(),
                sa_dir: self.vep_sa_dir.clone(),
                distance: self.vep_distance,
                pick: self.vep_pick,
            }
        })
    }
}
