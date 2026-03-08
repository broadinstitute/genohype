//! CLI argument definitions using clap derive API.

pub mod cluster;
pub mod export;
pub mod manhattan;
pub mod misc;
pub mod pool;
pub mod shared;

// Re-export everything so `main.rs` doesn't break
pub use cluster::*;
pub use export::*;
pub use manhattan::*;
pub use misc::*;
pub use pool::*;
pub use shared::*;

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

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show table metadata, keys, partition layout, and schema (fast)
    Info {
        /// Path to the Hail table or VCF file
        path: String,
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

    /// Synthetic workload for testing cluster telemetry
    Stress(StressArgs),
}
