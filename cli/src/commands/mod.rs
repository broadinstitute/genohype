//! CLI command handlers extracted from main.rs for better organization.

#[cfg(feature = "vep")]
pub mod annotate;
pub mod export;
pub mod info;
#[cfg(feature = "clickhouse")]
pub mod ingest;
pub mod manhattan;
pub mod pool;
pub mod query;
#[cfg(feature = "validation")]
pub mod schema;
pub mod service;
pub mod summary;
pub mod utils;
pub mod vcf;
