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
