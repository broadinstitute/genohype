//! Export functionality for Hail tables
//!
//! This module provides export capabilities to various external systems.
//!
//! # Modules
//! - `hail`: Export to Hail Table (.ht) format
//! - `json`: Export to JSON (NDJSON) format
//! - `clickhouse`: Export to ClickHouse using Parquet as intermediate format (requires `clickhouse` feature)
//! - `bigquery`: Export to Google BigQuery using Parquet and GCS staging (requires `bigquery` feature)

pub mod cache_builder;
pub mod hail;
pub mod json;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
#[cfg(feature = "postgres")]
pub mod postgres;

pub use cache_builder::{
    build_cache, build_gene_blob, extract_gene, extract_variant, CacheBuildStats, CacheGene,
    CacheGeneVariantsResponse, CacheVariant,
};
pub use json::{hail_to_json_sharded_full, JsonWriter};

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQueryClient, BigQueryError};
#[cfg(feature = "clickhouse")]
pub use clickhouse::{generate_create_table, ClickHouseClient};
#[cfg(feature = "elasticsearch")]
pub use elasticsearch::{
    build_document, build_gene_document, build_genes_request_body, build_request_body,
    hail_type_to_es_mapping, parse_index_fields, BulkInserter, ElasticsearchClient,
    ElasticsearchError, IndexField,
};
#[cfg(feature = "postgres")]
pub use postgres::{
    extract_columns, extract_gene_columns, generate_create_genes_table,
    generate_create_table as generate_pg_create_table, generate_genes_indexes, CopyInserter,
    GeneColumns, GenesCopyInserter, PostgresClient, PostgresError, VariantColumns,
};
