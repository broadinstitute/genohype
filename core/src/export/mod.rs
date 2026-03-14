//! Export functionality for Hail tables
//!
//! This module provides export capabilities to various external systems.
//!
//! # Modules
//! - `hail`: Export to Hail Table (.ht) format
//! - `json`: Export to JSON (NDJSON) format
//! - `clickhouse`: Export to ClickHouse using Parquet as intermediate format (requires `clickhouse` feature)
//! - `bigquery`: Export to Google BigQuery using Parquet and GCS staging (requires `bigquery` feature)

pub mod hail;
pub mod json;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;

pub use json::{hail_to_json_sharded_full, JsonWriter};

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQueryClient, BigQueryError};
#[cfg(feature = "clickhouse")]
pub use clickhouse::{generate_create_table, ClickHouseClient};
