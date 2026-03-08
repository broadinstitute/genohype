//! Service layer for coordinator business logic.
//!
//! This module extracts complex business logic from HTTP handlers,
//! making the code more testable and maintainable.

pub mod batch_init;
pub mod ingest_init;

pub use batch_init::{create_aggregate_spec_from_manhattan_spec, init_batch_state};
pub use ingest_init::discover_phenotypes_for_ingestion;

#[cfg(feature = "clickhouse")]
pub use ingest_init::{create_ingestion_state, init_clickhouse_tables};
