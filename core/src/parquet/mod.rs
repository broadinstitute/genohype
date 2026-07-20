//! Parquet conversion utilities
//!
//! This module provides functionality to convert Hail tables to Parquet format.
//!
//! # Modules
//! - `schema`: Convert Hail types to Arrow schema
//! - `builder`: Column builders for row-to-columnar conversion
//! - `writer`: High-level Parquet writer
//! - `converter`: High-level conversion functions

pub mod builder;
pub mod converter;
pub mod schema;
pub mod writer;

pub use converter::{
    hail_to_parquet, hail_to_parquet_sharded, hail_to_parquet_sharded_full,
    hail_to_parquet_sharded_with_metrics, hail_to_parquet_with_options,
    hail_to_parquet_with_progress, ConversionMetadata, ConversionOptions,
};
pub use writer::{build_record_batch, InMemoryParquetWriter, ParquetWriter};
