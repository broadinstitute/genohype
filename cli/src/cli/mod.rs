//! CLI argument definitions using clap derive API.

pub mod cluster;
pub mod export;
pub mod manhattan;
pub mod misc;
pub mod pool;
pub mod shared;
pub mod vcf;

#[cfg(feature = "vep")]
pub mod annotate;

// Re-export everything so `main.rs` doesn't break
pub use cluster::*;
pub use export::*;
pub use manhattan::*;
pub use misc::*;
pub use pool::*;
pub use shared::*;
pub use vcf::*;

#[cfg(feature = "vep")]
pub use annotate::*;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "genohype",
    version,
    about = "Hail Table Decoder and Converter",
    long_about = None
)]
pub struct Cli {
    /// Path to configuration file (default: ~/.config/genohype/config.toml)
    #[arg(long, global = true)]
    pub config: Option<String>,

    /// Write a Chrome trace profile to the given path (open in https://ui.perfetto.dev)
    #[arg(long, global = true, value_name = "PATH")]
    pub profile: Option<String>,

    /// Bypass metadata cache (always download fresh, but still populate cache)
    #[arg(long, global = true)]
    pub no_cache: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show table metadata, keys, partition layout, and schema (fast)
    Info {
        /// Path to the Hail table or VCF file
        path: String,
        /// Output as JSON
        #[arg(long)]
        json: bool,
        /// Compute total row count (slow for remote tables without partition_counts in metadata)
        #[arg(long)]
        count: bool,
        /// Dump table globals as JSON (e.g., freq_meta population labels)
        #[arg(long)]
        globals: bool,
    },

    /// Scan full dataset to calculate row counts and field statistics (slow)
    Summary {
        /// Path to the Hail table
        path: String,
    },

    /// Stream rows with optional filtering (lazy)
    Query(QueryArgs),

    /// Export data to other formats
    Export {
        #[command(subcommand)]
        command: ExportCommands,
    },

    /// Schema operations (validate, generate)
    #[cfg(feature = "validation")]
    Schema {
        #[command(subcommand)]
        command: SchemaSubcommands,
    },

    /// Generate Manhattan plots (PNG + JSON sidecar)
    Manhattan(ManhattanArgs),

    /// Generate a batch of Manhattan plots from assets JSON
    ManhattanBatch(ManhattanBatchArgs),

    /// Generate locus plots from existing Manhattan output directory
    Loci(LociArgs),

    /// Render a LocusZoom-style scatter plot for a specific region
    Locus(LocusArgs),

    /// Manage a distributed worker pool for parallel processing
    Pool {
        #[command(subcommand)]
        command: PoolCommands,
    },

    /// Manage cluster configurations for multi-environment deployments (legacy)
    Cluster {
        #[command(subcommand)]
        command: ClusterCommands,
    },

    /// Manage ClickHouse database instances
    Clickhouse {
        #[command(subcommand)]
        command: ClickHouseCommands,
    },

    /// Manage environment configuration (.genohype-env)
    Env {
        #[command(subcommand)]
        command: EnvCommands,
    },

    /// Run distributed service components (coordinator or worker)
    Service {
        #[command(subcommand)]
        command: ServiceCommands,
    },

    /// Ingest data into external systems
    #[cfg(feature = "clickhouse")]
    Ingest {
        #[command(subcommand)]
        command: IngestCommands,
    },

    /// Manage the local metadata cache
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },

    /// Annotate variants with VEP consequence predictions
    #[cfg(feature = "vep")]
    Annotate(AnnotateArgs),

    /// VCF utilities (indexing, etc.)
    Vcf {
        #[command(subcommand)]
        command: VcfCommands,
    },

    /// Synthetic workload for testing cluster telemetry
    Stress(StressArgs),
}

#[derive(Subcommand)]
pub enum CacheCommands {
    /// Remove all cached metadata files
    Clear,
}
