//! # Genohype CLI
//!
//! Command-line interface and application logic for the Hail table decoder.
//!
//! This crate provides:
//! - CLI commands (convert, query, export, etc.)
//! - Cloud orchestration (GCP pool management)
//! - Distributed processing coordination
//! - Domain-specific tools (Manhattan plots, BigQuery/ClickHouse export)
//!
//! The core data engine is provided by `genohype-core`.

pub mod cli;
pub mod cloud;
pub mod cluster;
pub mod config;
pub mod distributed;
pub mod env;
pub mod manhattan;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

#[cfg(feature = "clickhouse")]
pub mod ingest;

pub use genohype_core::export;

#[cfg(feature = "genomic")]
pub use genohype_core::genomic;

pub mod benchmark;

// Re-export core types for convenience
pub use genohype_core::{HailError, Result};

// Re-export core modules for backwards compatibility with hail-decoder consumers
pub use genohype_core::codec;
pub use genohype_core::query;
