//! Miscellaneous command argument definitions.

use clap::Args;

#[cfg(feature = "clickhouse")]
use super::shared::PartitioningArgs;

#[cfg(any(feature = "validation", feature = "clickhouse"))]
use clap::Subcommand;

#[derive(Args)]
pub struct QueryArgs {
    /// Path to the Hail table or VCF file
    pub table: String,

    /// Point lookup (field=value)
    #[arg(long)]
    pub key: Option<String>,

    /// Filter conditions (field=value, field>value, field>=value, etc.)
    #[arg(long = "where")]
    pub where_clauses: Vec<String>,

    /// Limit number of results
    #[arg(long)]
    pub limit: Option<usize>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Genomic interval (chr:start-end format, can be specified multiple times)
    #[arg(long)]
    pub interval: Vec<String>,

    /// Path to interval file (.bed, .json, or text with chr:start-end lines)
    #[arg(long)]
    pub intervals_file: Option<String>,

    /// Select specific fields (comma-separated, e.g., locus,alleles,freq.AF)
    #[arg(long, conflicts_with = "exclude")]
    pub fields: Option<String>,

    /// Exclude top-level fields (comma-separated, e.g., vep,vep115,histograms)
    #[arg(long, conflicts_with = "fields")]
    pub exclude: Option<String>,

    /// Output structured performance metrics as JSON instead of row data
    #[arg(long)]
    pub stats_json: bool,
}

#[cfg(feature = "validation")]
#[derive(Subcommand)]
pub enum SchemaSubcommands {
    /// Validate table against JSON schema
    Validate(ValidateArgs),

    /// Generate JSON schema from table
    Generate(GenerateSchemaArgs),
}

#[cfg(feature = "validation")]
#[derive(Args)]
pub struct GenerateSchemaArgs {
    /// Path to the Hail table
    pub table: String,
    /// Output JSON schema file (stdout if not specified)
    pub output: Option<String>,
}

#[cfg(feature = "validation")]
#[derive(Args)]
pub struct ValidateArgs {
    /// Path to the Hail table
    pub table: String,

    /// Path to the JSON schema file
    pub schema: String,

    /// Validate first N rows (sequential)
    #[arg(long)]
    pub limit: Option<usize>,

    /// Validate N randomly sampled rows
    #[arg(long)]
    pub sample: Option<usize>,

    /// Stop on first validation error
    #[arg(long)]
    pub fail_fast: bool,

    /// Show each row ID and validation result in real-time (sequential)
    #[arg(long, short)]
    pub verbose: bool,
}

/// Arguments for synthetic stress test workload.
#[derive(Args, Debug)]
pub struct StressArgs {
    /// Total number of tasks to queue
    #[arg(long, visible_alias = "tasks", default_value = "100")]
    pub partitions: usize,

    /// Seconds of pure CPU math to spin per partition
    #[arg(long, default_value = "0.0")]
    pub cpu_secs: f64,

    /// Megabytes of RAM to allocate and hold per partition
    #[arg(long, default_value = "0")]
    pub memory_mb: usize,

    /// A file to stream into memory to generate Network RX (e.g. gs://bucket/file.json)
    #[arg(long)]
    pub read_path: Option<String>,

    /// A directory to pump random bytes into to generate Network TX
    #[arg(long)]
    pub write_dir: Option<String>,

    /// Generate temporary read data (writes to write_dir first, then reads back).
    /// Requires --write-dir to be set.
    #[arg(long)]
    pub generate_read_data: bool,

    /// Size in MB of generated read data per partition (default: 32)
    #[arg(long, default_value = "32")]
    pub read_data_size_mb: usize,

    /// Memory to allocate gradually during CPU work (bypasses pre-flight memory check)
    #[arg(long)]
    pub leak_memory_mb: Option<usize>,

    /// Skip worker-side pre-flight memory check (forces execution even if OOM is likely)
    #[arg(long)]
    pub skip_memory_check: bool,

    /// Random jitter percentage to apply to memory per task (e.g. 50 = +/- 50%)
    #[arg(long)]
    pub memory_jitter_pct: Option<u8>,
}

/// Subcommands for ingesting data into external systems.
#[cfg(feature = "clickhouse")]
#[derive(Subcommand)]
pub enum IngestCommands {
    /// Ingest Manhattan outputs into ClickHouse
    Manhattan(IngestManhattanArgs),
}

/// Arguments for ingesting Manhattan pipeline outputs into ClickHouse.
#[cfg(feature = "clickhouse")]
#[derive(Args, Debug)]
pub struct IngestManhattanArgs {
    /// GCS path containing phenotype directories (e.g., gs://bucket/manhattans/)
    #[arg(long)]
    pub input_dir: String,

    /// ClickHouse URL (e.g., http://clickhouse:8123)
    #[arg(long)]
    pub clickhouse_url: String,

    /// Target database (default: default)
    #[arg(long, default_value = "default")]
    pub database: String,

    /// Table initialization strategy: create (default), replace, or append
    #[arg(long, value_enum, default_value = "create")]
    pub init_strategy: super::pool::InitStrategy,

    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,
}
