//! CLI argument definitions using clap derive API.

use clap::{Args, Parser, Subcommand};

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

#[derive(Subcommand)]
pub enum ExportCommands {
    /// Convert to Parquet file
    Parquet(ExportParquetArgs),

    /// Export to JSON file (NDJSON)
    Json(ExportJsonArgs),

    /// Export to VCF file
    Vcf(ExportVcfArgs),

    /// Export to Hail Table format
    Hail(ExportHailArgs),

    /// Export to ClickHouse
    #[cfg(feature = "clickhouse")]
    Clickhouse(ExportClickhouseArgs),

    /// Export to BigQuery
    #[cfg(feature = "bigquery")]
    Bigquery(ExportBigqueryArgs),
}

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

#[derive(Args)]
pub struct ExportParquetArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output Parquet file path (or directory if --per-partition or --shard-count is used)
    pub output: String,

    /// Write each partition to a separate file in the output directory
    #[arg(long)]
    pub per_partition: bool,

    /// Write output as a directory of N Parquet files (groups partitions)
    #[arg(long, conflicts_with = "per_partition")]
    pub shard_count: Option<usize>,

    /// Collect and display system metrics during export (CPU, memory, I/O)
    #[arg(long)]
    pub benchmark: bool,
}

impl HasCommonExportArgs for ExportParquetArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[derive(Args)]
pub struct ExportJsonArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output JSON file path (or directory if --per-partition or --shard-count is used)
    pub output: String,

    /// Write each partition to a separate file in the output directory
    #[arg(long)]
    pub per_partition: bool,

    /// Write output as a directory of N JSON files (groups partitions)
    #[arg(long, conflicts_with = "per_partition")]
    pub shard_count: Option<usize>,

    /// Group rows by field value and write to separate files (not yet implemented)
    #[arg(long)]
    pub group_by: Option<String>,
}

impl HasCommonExportArgs for ExportJsonArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[derive(Args)]
pub struct ExportVcfArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output VCF file path
    pub output: String,

    /// Compress output with BGZF
    #[arg(long)]
    pub bgzip: bool,
}

impl HasCommonExportArgs for ExportVcfArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[derive(Args)]
pub struct ExportHailArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output Hail table directory path
    pub output: String,
}

impl HasCommonExportArgs for ExportHailArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
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
}

#[cfg(feature = "clickhouse")]
#[derive(Args)]
pub struct ExportClickhouseArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// ClickHouse URL (e.g., http://localhost:8123)
    pub url: String,

    /// Target table name in ClickHouse
    pub table: String,
}

#[cfg(feature = "clickhouse")]
impl HasCommonExportArgs for ExportClickhouseArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[cfg(feature = "bigquery")]
#[derive(Args)]
pub struct ExportBigqueryArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// BigQuery destination (project:dataset.table)
    pub destination: String,

    /// GCS bucket for staging parquet file
    #[arg(long)]
    pub bucket: String,

    /// Directory for temporary parquet file
    #[arg(long, default_value = "/tmp")]
    pub temp_dir: String,
}

#[cfg(feature = "bigquery")]
impl HasCommonExportArgs for ExportBigqueryArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
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

#[derive(Args, Debug)]
pub struct ManhattanArgs {
    // -- Data Inputs --
    /// Path to Exome results Hail Table
    #[arg(long)]
    pub exome: Option<String>,

    /// Path to Exome annotations Hail Table (for merge-join with exome results)
    #[arg(long)]
    pub exome_annotations: Option<String>,

    /// Path to Genome results Hail Table
    #[arg(long)]
    pub genome: Option<String>,

    /// Path to Genome annotations Hail Table (for merge-join with genome results)
    #[arg(long)]
    pub genome_annotations: Option<String>,

    /// Path to gene burden results Hail Table
    #[arg(long)]
    pub gene_burden: Option<String>,

    /// Path to gnomAD genes table (for gene bounds lookup and locus gene tracks)
    #[arg(long)]
    pub genes: Option<String>,

    // -- Legacy/Single Table Mode --
    /// Path to the variant results Hail table (legacy single-table mode)
    #[arg(long)]
    pub table: Option<String>,

    /// Path to annotation table for enriching significant hits (legacy mode)
    #[arg(long)]
    pub annotate: Option<String>,

    /// Fields to extract from annotation table (default: all value fields)
    #[arg(long, value_delimiter = ',')]
    pub annotate_fields: Vec<String>,

    // -- Thresholds & Config --
    /// Chromosomes to include (e.g. '1', '1,6,17', 'all' for genome-wide)
    #[arg(long, default_value = "all")]
    pub chrom: String,

    /// Field name for P-value (Y-axis, -log10 applied automatically)
    #[arg(long, default_value = "Pvalue")]
    pub y_field: String,

    /// P-value threshold for significant variants (default: 5e-8)
    #[arg(long, visible_alias = "variant-threshold", default_value = "5e-8")]
    pub threshold: f64,

    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[arg(long, default_value = "2.5e-6")]
    pub gene_threshold: f64,

    /// Filter gene burden results to specific max_MAF value (e.g., 0.001).
    /// If not specified, all MAF levels are exported.
    #[arg(long)]
    pub gene_maf_filter: Option<f64>,

    /// P-value threshold to buffer variants for locus plots (default: 0.01)
    #[arg(long, default_value = "0.01")]
    pub locus_threshold: f64,

    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[arg(long, default_value = "1000000")]
    pub locus_window: i32,

    /// Generate locus-zoom style plots for significant regions
    #[arg(long)]
    pub locus_plots: bool,

    /// Minimum number of significant variants required to form a locus
    #[arg(long, default_value = "1")]
    pub min_variants_per_locus: usize,

    /// Run only the highly-parallel scan phase (outputs partial PNGs and sig.parquet)
    #[arg(long, conflicts_with = "aggregate_only")]
    pub scan_only: bool,

    /// Run only the memory-intensive aggregate phase (composites PNGs, generates locus plots)
    #[arg(long, conflicts_with = "scan_only")]
    pub aggregate_only: bool,

    // -- Distributed Aggregation --
    /// Path to directory containing distributed scan shards (part-*.json files).
    /// When specified, aggregates shards and renders final PNG instead of scanning tables.
    #[arg(long)]
    pub from_shards: Option<String>,

    // -- Output Options --
    /// Limit number of rows to process (for testing)
    #[arg(long)]
    pub limit: Option<usize>,

    /// Image width in pixels
    #[arg(long, default_value = "3000")]
    pub width: u32,

    /// Image height in pixels
    #[arg(long, default_value = "800")]
    pub height: u32,

    /// Output filename prefix or directory (produces {prefix}.png + {prefix}.json)
    #[arg(long)]
    pub output: Option<String>,

    /// Color scheme (classic = alternating gray/blue per chromosome)
    #[arg(long, default_value = "classic")]
    pub colors: String,

    // -- Distributed Processing --
    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,

    /// Output progress as JSON lines (for distributed job coordination)
    #[arg(long, hide = true)]
    pub progress_json: bool,
}

/// Arguments for generating a batch of Manhattan plots from assets JSON.
///
/// This command reads an assets JSON file (from axaou-server query-assets) and
/// submits a batch of Manhattan plot jobs to the coordinator for parallel processing.
///
/// Settings can be provided via CLI arguments or a TOML config file (--config).
/// CLI arguments override config file settings.
#[derive(Args, Debug)]
pub struct ManhattanBatchArgs {
    /// Path to TOML configuration file (contains all settings)
    ///
    /// When provided, most other arguments become optional as they can be
    /// specified in the config file. CLI arguments override config values.
    #[arg(long)]
    pub config: Option<String>,

    /// Path to assets JSON file (from axaou-server query-assets)
    #[arg(long)]
    pub assets_json: Option<String>,

    /// Base output directory (e.g., gs://bucket/manhattans)
    #[arg(long)]
    pub output_dir: Option<String>,

    /// Filter to specific analysis IDs (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub analysis_ids: Option<Vec<String>>,

    /// Filter to specific ancestry groups (comma-separated, e.g., "meta,eur,afr")
    #[arg(long, value_delimiter = ',')]
    pub ancestries: Option<Vec<String>>,

    /// Sample a fraction of phenotypes (0.0-1.0, e.g., 0.1 for 10%)
    #[arg(long)]
    pub sample: Option<f64>,

    /// Limit number of phenotypes to process
    #[arg(long)]
    pub limit: Option<usize>,

    // Common Manhattan Options (Global overrides)

    /// P-value threshold for significant variants (default: 5e-8)
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[arg(long, default_value = "2.5e-6")]
    pub gene_threshold: f64,

    /// P-value threshold to buffer variants for locus plots (default: 0.01)
    #[arg(long, default_value = "0.01")]
    pub locus_threshold: f64,

    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[arg(long, default_value = "1000000")]
    pub locus_window: i32,

    /// Generate locus-zoom style plots for significant regions
    #[arg(long)]
    pub locus_plots: bool,

    /// Minimum number of significant variants required to form a locus
    #[arg(long, default_value = "1")]
    pub min_variants_per_locus: usize,

    /// Run only the highly-parallel scan phase
    #[arg(long, conflicts_with = "aggregate_only")]
    pub scan_only: bool,

    /// Run only the memory-intensive aggregate phase
    #[arg(long, conflicts_with = "scan_only")]
    pub aggregate_only: bool,

    /// Image width in pixels
    #[arg(long, default_value = "3000")]
    pub width: u32,

    /// Image height in pixels
    #[arg(long, default_value = "800")]
    pub height: u32,

    /// Path to gnomAD genes table
    #[arg(long)]
    pub genes: Option<String>,

    /// Path to Exome annotations Hail Table
    #[arg(long)]
    pub exome_annotations: Option<String>,

    /// Path to Genome annotations Hail Table
    #[arg(long)]
    pub genome_annotations: Option<String>,

    // Distributed Processing

    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,

    /// Output progress as JSON lines
    #[arg(long, hide = true)]
    pub progress_json: bool,

    // Styling Options

    /// Path to TOML configuration file for styling
    #[arg(long)]
    pub style_config: Option<String>,

    /// Override point radius for all plot types (pixels, default: 2.5)
    #[arg(long)]
    pub point_radius: Option<f32>,

    /// Override background style: "transparent", "white", or hex color (default: white)
    #[arg(long)]
    pub background: Option<String>,

    /// Override chromosome colors (comma-separated hex, e.g., "#404040,#4682B4")
    #[arg(long, value_delimiter = ',')]
    pub chrom_colors: Option<Vec<String>>,
}

/// Arguments for generating locus plots from existing Manhattan output
#[derive(Args, Debug)]
pub struct LociArgs {
    /// Path to Manhattan output directory (contains *_significant.parquet files)
    #[arg(long)]
    pub dir: String,

    /// Path to Exome results Hail Table (for reading variants in locus regions)
    #[arg(long)]
    pub exome: Option<String>,

    /// Path to Genome results Hail Table (for reading variants in locus regions)
    #[arg(long)]
    pub genome: Option<String>,

    /// Path to gene burden results Hail Table (for seeding locus regions from significant genes)
    #[arg(long)]
    pub gene_burden: Option<String>,

    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[arg(long, default_value = "1000000")]
    pub locus_window: i32,

    /// P-value threshold for significant variants (default: 5e-8)
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[arg(long, default_value = "2.5e-6")]
    pub gene_threshold: f64,

    /// P-value field name in source tables
    #[arg(long, default_value = "Pvalue")]
    pub y_field: String,

    /// Number of parallel threads (default: 8)
    #[arg(long, default_value = "8")]
    pub threads: usize,

    /// Generate locus-zoom style plots for significant regions
    #[arg(long)]
    pub locus_plots: bool,

    /// Minimum number of significant variants required to form a locus
    #[arg(long, default_value = "1")]
    pub min_variants_per_locus: usize,
}

#[derive(Args, Debug)]
pub struct LocusArgs {
    /// Path to Exome results Hail Table
    #[arg(long)]
    pub exome: Option<String>,

    /// Path to Genome results Hail Table
    #[arg(long)]
    pub genome: Option<String>,

    /// Region to plot (format: chr:start-end)
    #[arg(long)]
    pub region: String,

    /// Output PNG path
    #[arg(long)]
    pub output: String,

    /// P-value field name
    #[arg(long, default_value = "Pvalue")]
    pub y_field: String,

    /// Significance threshold
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Image width
    #[arg(long, default_value = "800")]
    pub width: u32,

    /// Image height
    #[arg(long, default_value = "400")]
    pub height: u32,

    /// Max Y-axis value (-log10 p)
    #[arg(long, default_value = "30.0")]
    pub y_max: f64,
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
}

/// Subcommands for managing cluster configurations (legacy).
#[derive(Subcommand)]
pub enum ClusterCommands {
    /// List all configured clusters
    List {
        /// Filter by status (active, standby, deprecated)
        #[arg(long)]
        status: Option<String>,
    },

    /// Show details of a specific cluster
    Show {
        /// Cluster name
        name: String,
    },

    /// Verify cluster connectivity and configuration
    Verify {
        /// Cluster name
        name: String,
    },

    /// Deploy Cloud Run services to a cluster
    Deploy {
        /// Cluster name
        name: String,

        /// Docker image tag (default: latest)
        #[arg(long, default_value = "latest")]
        tag: String,

        /// Only deploy backend (skip frontend)
        #[arg(long)]
        backend_only: bool,
    },
}

/// Subcommands for managing ClickHouse instances.
#[derive(Subcommand)]
pub enum ClickHouseCommands {
    /// Create a new ClickHouse instance
    Create {
        /// Instance name (used for VM name and identification)
        name: String,

        /// Profile name from config file (e.g., "standard", "large")
        #[arg(long)]
        profile: Option<String>,

        /// GCP machine type (overrides profile)
        #[arg(long)]
        machine_type: Option<String>,

        /// Boot disk size in GB (overrides profile)
        #[arg(long)]
        disk_size_gb: Option<u32>,

        /// GCP zone (overrides profile/defaults)
        #[arg(long)]
        zone: Option<String>,
    },

    /// List ClickHouse instances
    List,

    /// Show details of a ClickHouse instance
    Show {
        /// Instance name
        name: String,
    },

    /// Destroy a ClickHouse instance
    Destroy {
        /// Instance name
        name: String,

        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// Get the internal IP of a ClickHouse instance
    Ip {
        /// Instance name
        name: String,
    },

    /// SSH into a ClickHouse instance
    Ssh {
        /// Instance name
        name: String,

        /// Command to run (optional, opens shell if not provided)
        #[arg(last = true)]
        command: Vec<String>,
    },

    /// Create an SSH tunnel to ClickHouse (port forward 8123 to localhost)
    Tunnel {
        /// Instance name
        name: String,

        /// Local port to bind (default: 8123)
        #[arg(long, default_value = "8123")]
        port: u16,
    },
}

/// Subcommands for managing environment configuration.
#[derive(Subcommand)]
pub enum EnvCommands {
    /// Initialize a new .genohype-env file
    Init {
        /// Environment name (e.g., "20260303", "dev")
        name: String,

        /// Storage path (e.g., "gs://bucket/prefix")
        #[arg(long)]
        storage: Option<String>,

        /// ClickHouse instance name or URL
        #[arg(long)]
        clickhouse: Option<String>,
    },

    /// Show current environment configuration
    Show,

    /// Verify environment connectivity (storage + ClickHouse)
    Verify,
}

/// Subcommands for managing distributed worker pools.
#[derive(Subcommand)]
pub enum PoolCommands {
    /// Create a new worker pool of GCP VMs
    ///
    /// If a pool profile with the same name exists in the config file,
    /// its settings will be used as defaults. CLI arguments override config.
    Create {
        /// Name of the pool (used for tagging and identification)
        /// If a profile with this name exists in config, its settings are used as defaults
        name: String,

        /// Number of worker VMs to create (default: 4, or from config profile)
        #[arg(long)]
        workers: Option<usize>,

        /// GCP machine type (default: c3-highcpu-22, or from config profile)
        #[arg(long)]
        machine_type: Option<String>,

        /// GCP zone for the VMs (default: us-central1-a, or from config)
        #[arg(long)]
        zone: Option<String>,

        /// Use spot/preemptible instances for cost savings
        #[arg(long)]
        spot: Option<bool>,

        /// GCP project ID (defaults to gcloud config or config file)
        #[arg(long)]
        project: Option<String>,

        /// VPC network name (defaults to "default" or config file)
        #[arg(long)]
        network: Option<String>,

        /// Subnet name (required if network is specified and not using default)
        #[arg(long)]
        subnet: Option<String>,

        /// Wait for VMs to be ready (startup script complete)
        #[arg(long)]
        wait: bool,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,

        /// Create a dedicated coordinator node for distributed processing
        #[arg(long)]
        with_coordinator: bool,
    },

    /// Submit a job to run on the worker pool
    Submit {
        /// Name of the pool to submit to
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Target cluster for this job (overrides job config with cluster's ClickHouse URL and output path)
        #[arg(long)]
        cluster: Option<String>,

        /// Path to the Linux-compiled binary (defaults to target/x86_64-unknown-linux-gnu/release/genohype)
        #[arg(long)]
        binary: Option<String>,

        /// Automatically stop VMs after job completion to save costs
        #[arg(long)]
        auto_stop: bool,

        /// Force binary redeployment even if coordinator is already running
        #[arg(long)]
        redeploy_binary: bool,

        /// Automatically scale workers up for this job and down to 0 afterwards
        #[arg(long)]
        autoscale: bool,

        /// Force submission even if a job is already running (supersedes it)
        #[arg(long)]
        force: bool,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,

        /// Number of partitions per worker batch (higher = more parallelism per worker)
        #[arg(long)]
        batch_size: Option<usize>,

        /// Hint for memory required per partition in MB (overrides default heuristics)
        #[arg(long)]
        memory_weight_mb: Option<u64>,

        /// The command to run on workers (everything after --)
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },

    /// Scale the number of workers in a pool
    Scale {
        /// Name of the pool
        name: String,

        /// Target number of workers
        #[arg(long)]
        workers: usize,

        /// GCP zone (defaults to us-central1-a)
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Path to the Linux-compiled binary (optional)
        #[arg(long)]
        binary: Option<String>,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,
    },

    /// Destroy a worker pool and delete all VMs
    Destroy {
        /// Name of the pool to destroy
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// GCS bucket path to export metrics database before destruction (e.g., gs://my-bucket/metrics/)
        #[arg(long)]
        metrics_bucket: Option<String>,
    },

    /// List instances in a worker pool
    List {
        /// Name of the pool
        name: String,
    },

    /// Check status of a distributed job running on the pool
    Status {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,
    },

    /// Update the binary on a running pool (upload to coordinator, workers pull)
    UpdateBinary {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Path to the Linux-compiled binary (defaults to target/x86_64-unknown-linux-gnu/release/genohype)
        #[arg(long)]
        binary: Option<String>,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,
    },

    /// Cancel a running job on the pool
    Cancel {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,
    },

    /// Show real-time worker activity
    Workers {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,
    },

    /// Tail the event log
    Events {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Follow the event stream (like tail -f)
        #[arg(short, long)]
        follow: bool,
    },

    /// Show recent task failures
    Failures {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,
    },

    /// Show tail of a specific worker's logs
    Logs {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Worker ID to query logs for
        #[arg(long)]
        worker: String,
    },
}

/// Subcommands for running distributed service components.
#[derive(Subcommand)]
pub enum ServiceCommands {
    /// Start the coordinator server (manages work distribution)
    StartCoordinator {
        /// Port to listen on
        #[arg(long, default_value = "3000")]
        port: u16,

        /// Local path where the active SQLite database will run
        #[arg(long, default_value = "/var/lib/genohype/ops.db")]
        db_path: String,

        /// GCS path to backup the database to (and restore from on startup)
        #[arg(long)]
        backup_path: Option<String>,

        /// Path to input Hail table (optional, can be set later via POST /api/job)
        #[arg(long)]
        input: Option<String>,

        /// Path to output directory (optional, can be set later via POST /api/job)
        #[arg(long)]
        output: Option<String>,

        /// Total number of partitions to process (optional, can be set later via POST /api/job)
        #[arg(long)]
        total_partitions: Option<usize>,

        /// Number of partitions to assign per work request
        #[arg(long, default_value = "10")]
        batch_size: usize,

        /// Timeout in seconds before rescheduling stale work
        #[arg(long, default_value = "600")]
        timeout: u64,
    },

    /// Start a worker process (connects to coordinator for work)
    StartWorker {
        /// Coordinator URL (e.g., http://10.0.0.5:3000)
        #[arg(long)]
        url: String,

        /// Unique worker ID
        #[arg(long)]
        worker_id: String,

        /// Poll interval in milliseconds when waiting for work
        #[arg(long, default_value = "2000")]
        poll_interval: u64,
    },
}

/// Re-export InitStrategy from distributed module for CLI usage.
/// This allows the CLI to use the same enum as the library code.
#[cfg(feature = "clickhouse")]
pub use crate::distributed::message::InitStrategy;

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
    pub init_strategy: InitStrategy,

    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,
}
