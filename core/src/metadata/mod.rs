//! Hail table metadata parsing and structures
//!
//! This module handles parsing of Hail table metadata including:
//! - Table specifications (TableMetadata)
//! - Partition boundaries for pruning (_jRangeBounds)
//! - Index specifications for B-tree indexes (_indexSpec)
//! - Codec specifications for encoding/decoding

pub mod cache;
mod structures;

pub use cache::{CacheOptions, MetadataCache};
pub use structures::*;
