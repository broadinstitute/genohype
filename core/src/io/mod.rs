//! Cloud storage IO abstraction for Hail tables
//!
//! This module provides a unified interface for reading and writing data from both local
//! files and cloud storage (GCS, S3).
//!
//! ## High-Performance Streaming
//!
//! The module implements a high-performance streaming architecture:
//!
//! - **Reading**: `PrefetchingCloudReader` spawns a background task that fetches data
//!   ahead of the read position, hiding network latency from the CPU.
//!
//! - **Writing**: `CloudWriter` buffers data and uploads to cloud storage on `finish()`,
//!   enabling diskless pipelines.
//!
//! - **Local Files**: Uses memory-mapping (`MmapReader`) for optimal NVMe performance.
//!
//! ## Architecture
//!
//! ```text
//! Input:  GCS/S3 -> [Prefetch Task] -> [Channel] -> [CPU Decode]
//! Output: [CPU Encode] -> [Buffer] -> [Upload] -> GCS/S3
//! ```
//!
//! This enables processing 12TB datasets without local staging.

pub mod adapter;
pub mod resolver;
pub mod writer;

pub use adapter::{
    get_file_size, get_reader, is_cloud_path, join_path, range_read, read_single_block,
    BoxedReader, CloudReader, MmapReader, PrefetchingCloudReader,
};
#[cfg(feature = "gcp")]
pub use adapter::get_gcs_client;
pub use resolver::{resolve_url, resolve_url_for_write, ResolvedStore};
pub use writer::{CloudWriter, OutputWriter, StreamingCloudWriter};
