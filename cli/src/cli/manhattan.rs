//! Manhattan plot command argument definitions.

use clap::Args;

use super::shared::PartitioningArgs;

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
