//! Export command argument definitions.

use clap::{Args, Subcommand};

use super::shared::{CommonExportArgs, HasCommonExportArgs};

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

#[cfg(feature = "clickhouse")]
#[derive(Args)]
pub struct ExportClickhouseArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// ClickHouse URL (e.g., http://localhost:8123)
    pub url: String,

    /// Target table name in ClickHouse
    pub table: String,

    /// Glob pattern to match multiple input files (e.g., "*.bed.gz")
    /// When specified, input is treated as a directory and files matching the glob are processed
    #[arg(long)]
    pub glob: Option<String>,

    /// Add a column with the filename stem (without extension) as its value
    /// Useful for injecting sample_id from filenames like HG002.model.pbmm2.combined.bed.gz
    #[arg(long)]
    pub filename_column: Option<String>,
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
