//! Export command argument definitions.

use clap::{Args, Subcommand};

use super::shared::{CommonExportArgs, HasCommonExportArgs};
#[cfg(feature = "vep")]
use super::shared::VepArgs;

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

    /// Export to Elasticsearch (prod-shaped variant index via _bulk)
    #[cfg(feature = "elasticsearch")]
    Elasticsearch(ExportElasticsearchArgs),

    /// Export to Postgres (partitioned JSONB wide table via COPY)
    #[cfg(feature = "postgres")]
    Postgres(ExportPostgresArgs),

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

    #[cfg(feature = "vep")]
    #[command(flatten)]
    pub vep: VepArgs,
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

    #[cfg(feature = "vep")]
    #[command(flatten)]
    pub vep: VepArgs,
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

    #[cfg(feature = "vep")]
    #[command(flatten)]
    pub vep: VepArgs,
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

    #[cfg(feature = "vep")]
    #[command(flatten)]
    pub vep: VepArgs,
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

/// Default index (hoisted + indexed) fields for the gnomAD v4 variant index.
///
/// Mirrors the prod `index_fields` for `gnomad_v4_variants`
/// (`data-pipeline/.../export_to_elasticsearch.py`). These are exactly the fields
/// the prod query DSL filters/sorts on: `locus` (region range + sort),
/// `transcript_consequences.gene_id` (gene term), `variant_id`/`rsids`/`caid`/
/// `vrs.alt.allele_id`→`allele_id` (the variant-by-id `chooseIdField` paths).
///
/// Index fields absent from the source schema are skipped (schema-tolerant), so
/// `vrs.alt.allele_id` is hoisted as `allele_id` when the table carries VRS and
/// quietly omitted otherwise. Prod's computed `document_id` (a compressed variant
/// id added by the pipeline) is intentionally not here — the raw sites HT lacks
/// it; the unique `variant_id` serves as the document `_id` instead.
#[cfg(feature = "elasticsearch")]
pub const DEFAULT_ES_INDEX_FIELDS: &str =
    "variant_id,rsids,caid,locus,transcript_consequences.gene_id,transcript_consequences.transcript_id,vrs.alt.allele_id";

#[cfg(feature = "elasticsearch")]
#[derive(Args)]
pub struct ExportElasticsearchArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Elasticsearch URL (e.g., http://localhost:9200, or http://user:pass@host:9200)
    pub url: String,

    /// Target index name
    pub index: String,

    /// Number of primary shards. For a single benchmark VM this should match the
    /// VM's vCPU count (one Lucene shard per core) — NOT prod's 48, which assumes
    /// a multi-node cluster and would handicap a single VM with context switching.
    #[arg(long, default_value = "1")]
    pub shards: usize,

    /// Schema-width preset: `full` (decode everything, default) or
    /// `browser-minimal` (strict allowlist of only the fields the gnomAD browser
    /// API returns). Cannot be combined with --fields/--exclude (none here, so
    /// it only narrows the document `_source`).
    #[arg(long)]
    pub width: Option<String>,

    /// Comma-separated dotted paths to hoist to the top level and index. The
    /// top-level key is the last path segment. Defaults to the gnomAD v4 variant
    /// index fields.
    #[arg(long, default_value = DEFAULT_ES_INDEX_FIELDS)]
    pub index_fields: String,

    /// Field used as the document `_id` (stable id → idempotent re-loads).
    #[arg(long, default_value = "variant_id")]
    pub id_field: String,

    /// Number of documents per `_bulk` request.
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Delete and recreate the index if it already exists. Without this, an
    /// existing index is reused (re-loads stay idempotent via stable `_id`).
    #[arg(long)]
    pub recreate: bool,

    /// Force-merge the index after loading (compacts segments, like prod).
    #[arg(long)]
    pub forcemerge: bool,
}

#[cfg(feature = "elasticsearch")]
impl HasCommonExportArgs for ExportElasticsearchArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[cfg(feature = "postgres")]
#[derive(Args)]
pub struct ExportPostgresArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Postgres connection URL (e.g. postgres://user:pass@localhost:5432/gnomad)
    pub url: String,

    /// Target table name (created as a partitioned JSONB wide table)
    #[arg(default_value = "variants")]
    pub table: String,

    /// Schema-width preset: `full` (decode everything, default) or
    /// `browser-minimal` (only the fields the gnomAD browser API returns). Narrows
    /// the `data` JSONB payload; the hoisted columns are always extracted.
    #[arg(long)]
    pub width: Option<String>,

    /// Rows per COPY+upsert batch.
    #[arg(long, default_value = "5000")]
    pub batch_size: usize,

    /// Drop and recreate the table if it already exists. Without this, an existing
    /// table is reused (re-loads stay idempotent via ON CONFLICT upsert).
    #[arg(long)]
    pub recreate: bool,

    /// Skip creating the secondary `(contig,pos)` / `variant_id` indexes after
    /// load (e.g. to add them manually once the full dataset is staged).
    #[arg(long)]
    pub no_indexes: bool,
}

#[cfg(feature = "postgres")]
impl HasCommonExportArgs for ExportPostgresArgs {
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
