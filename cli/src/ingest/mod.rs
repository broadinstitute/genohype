//! Ingestion modules for loading data into external systems.
//!
//! This module provides functionality to ingest processed Manhattan pipeline
//! outputs (manifest.json, significant hits, plots) into ClickHouse for
//! downstream querying and visualization.

pub mod manhattan;
pub mod schema;

pub use schema::get_manhattan_schemas;
