//! # Genohype Core
//!
//! Core data engine for Hail table format decoding and Parquet conversion.
//!
//! This library provides the low-level building blocks for:
//! - Reading Hail table schemas from metadata.json.gz
//! - Streaming and decompressing Hail binary data blocks
//! - Decoding typed data using Hail's codec system
//! - Converting Hail tables to Apache Parquet format
//! - Streaming from cloud storage (GCS, S3)
//!
//! ## Architecture
//!
//! The decoder implements Hail's 4-layer buffer stack:
//! 1. Stream Block Buffer - reads block-framed data
//! 2. Zstd Decompression - decompresses each block
//! 3. Blocking Buffer - provides fixed-size buffering
//! 4. LEB128 Buffer - decodes variable-length integers

pub mod bed;
pub mod buffer;
pub mod codec;
pub mod datasource;
pub mod error;
pub mod hail_adapter;
pub mod index;
pub mod io;
pub mod metadata;
pub mod parquet;
pub mod partitioning;
pub mod projection;
pub mod progress;
pub mod query;
pub mod schema;
pub mod summary;
pub mod vcf;

#[cfg(feature = "validation")]
pub mod validation;

pub mod export;

#[cfg(feature = "genomic")]
pub mod genomic;

#[cfg(feature = "vep")]
pub mod vep;

pub use error::{HailError, Result};

/// Version of the Hail format this decoder supports
pub const SUPPORTED_FORMAT_VERSION: u32 = 67328;

/// Hail version this decoder was tested against
pub const TESTED_HAIL_VERSION: &str = "0.2.134-952ae203dbbe";
