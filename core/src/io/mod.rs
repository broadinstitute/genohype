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
pub mod inventory;
pub mod manifest;
pub mod publication;
pub mod resolver;
pub mod writer;

#[cfg(feature = "gcp")]
pub use adapter::get_gcs_client;
pub use adapter::{
    get_file_size, get_file_size_with_snapshot, get_prefetch_depth, get_reader,
    get_reader_with_snapshot, is_cloud_path, join_path, range_read, range_read_with_snapshot,
    read_single_block, set_prefetch_depth, BoxedReader, CloudReader, MmapReader,
    PrefetchingCloudReader,
};
#[cfg(feature = "gcp")]
pub use inventory::GcsInventoryBackend;
pub use inventory::{
    inventory_hail_table, inventory_hail_table_path, IgnoredCounts, InventoryAccess,
    InventoryOptions, InventoryResult, InventoryRole, LocalInventoryBackend, ObservedListing,
    ObservedObject, RelativeObjectPath, SourceInventoryBackend, DEFAULT_MAX_OBJECTS,
    MAX_METADATA_BYTES,
};
pub use manifest::{SourceManifest, SourceObject, SourceSnapshot, SOURCE_MANIFEST_SCHEMA_VERSION};
pub use publication::{
    build_attempt_staging_location, content_addressed_path, lease_identity_sha256,
    publish_content_addressed, put_immutable, AttemptStagingLocation, PublicationDisposition,
    PublishedObject,
};
pub use resolver::{
    resolve_object, resolve_url, resolve_url_for_write, ResolvedObject, ResolvedStore,
};
pub use writer::{CloudWriter, OutputWriter, StreamingCloudWriter, StreamingWriteResult};
