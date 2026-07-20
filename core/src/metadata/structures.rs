//! Metadata structures for Hail tables

use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::debug;

/// Top-level table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(rename = "fileVersion")]
    pub file_version: u32,

    #[serde(rename = "hailVersion")]
    pub hail_version: String,

    #[serde(rename = "references_rel_path")]
    pub references_rel_path: String,

    #[serde(flatten)]
    pub extra: Value,
}

/// Row component specification with partitioning and index info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RVDComponentSpec {
    pub name: String,

    #[serde(rename = "_key")]
    pub key: Vec<String>,

    #[serde(rename = "_codecSpec")]
    pub codec_spec: CodecSpec,

    #[serde(rename = "_indexSpec")]
    pub index_spec: Option<IndexSpec>,

    #[serde(rename = "_partFiles")]
    pub part_files: Vec<String>,

    #[serde(rename = "_jRangeBounds")]
    pub range_bounds: Vec<Interval>,

    #[serde(rename = "_attrs")]
    pub attrs: Value,
}

/// Index specification for B-tree indexes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSpec {
    pub name: String,

    #[serde(rename = "_relPath")]
    pub rel_path: String,

    #[serde(rename = "_leafCodec")]
    pub leaf_codec: CodecSpec,

    #[serde(rename = "_internalNodeCodec")]
    pub internal_node_codec: CodecSpec,

    #[serde(rename = "_keyType")]
    pub key_type: String,

    #[serde(rename = "_annotationType")]
    pub annotation_type: String,
}

/// Codec specification with encoding type and buffer spec
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodecSpec {
    pub name: String,

    #[serde(rename = "_eType")]
    pub e_type: String,

    #[serde(rename = "_vType")]
    pub v_type: String,

    #[serde(rename = "_bufferSpec")]
    pub buffer_spec: BufferSpec,
}

/// Buffer specification describing the layered buffer stack
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum BufferSpec {
    #[serde(rename = "StreamBlockBufferSpec")]
    StreamBlock,

    #[serde(rename = "ZstdBlockBufferSpec")]
    ZstdBlock {
        #[serde(rename = "blockSize")]
        block_size: usize,
        child: Box<BufferSpec>,
    },

    #[serde(rename = "BlockingBufferSpec")]
    Blocking {
        #[serde(rename = "blockSize")]
        block_size: usize,
        child: Box<BufferSpec>,
    },

    #[serde(rename = "LEB128BufferSpec")]
    LEB128 { child: Box<BufferSpec> },
}

/// Partition boundary interval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interval {
    pub start: Value,
    pub end: Value,

    #[serde(rename = "includeStart")]
    pub include_start: bool,

    #[serde(rename = "includeEnd")]
    pub include_end: bool,
}

impl TableMetadata {
    /// Parse metadata from JSON bytes
    pub fn from_json(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    /// Parse metadata from gzipped JSON
    pub fn from_gzipped_json(data: &[u8]) -> Result<Self> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut json_data = Vec::new();
        decoder.read_to_end(&mut json_data)?;
        Self::from_json(&json_data)
    }

    /// Load metadata from a local file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Try gzipped first, fall back to plain JSON
        Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
    }

    /// Load metadata from a path string (local or cloud URL)
    ///
    /// # Supported URL schemes
    /// - `gs://bucket/path` - Google Cloud Storage
    /// - `s3://bucket/path` - Amazon S3
    /// - `http://` or `https://` - HTTP(S) URLs
    /// - Local file path - Regular file system access
    pub fn from_path(path: &str) -> Result<Self> {
        let start = std::time::Instant::now();
        let mut reader = crate::io::get_reader(path)?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        debug!(
            "TableMetadata::from_path read {} bytes in {:?}",
            data.len(),
            start.elapsed()
        );

        // Try gzipped first, fall back to plain JSON
        Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
    }

    /// Load metadata with optional caching support.
    ///
    /// When cache and opts are provided and the path is a cloud URL,
    /// metadata is read from/written to the local cache.
    pub fn from_path_cached(
        path: &str,
        table_url: &str,
        cache: Option<&super::cache::MetadataCache>,
        opts: Option<&super::cache::CacheOptions>,
    ) -> Result<Self> {
        match (cache, opts) {
            (Some(c), Some(o)) if o.enabled && crate::io::is_cloud_path(path) => {
                let data = c.get_or_fetch(table_url, "table_metadata.json.gz", o, || {
                    let start = std::time::Instant::now();
                    let mut reader = crate::io::get_reader(path)?;
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf)?;
                    debug!(
                        "TableMetadata fetched {} bytes in {:?}",
                        buf.len(),
                        start.elapsed()
                    );
                    Ok(buf)
                })?;
                Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
            }
            _ => Self::from_path(path),
        }
    }

    /// Extract per-partition row counts from the `components.partition_counts.counts` field.
    ///
    /// Returns `None` if the metadata doesn't contain partition counts (e.g., older Hail versions).
    pub fn partition_counts(&self) -> Option<Vec<usize>> {
        self.extra
            .get("components")?
            .get("partition_counts")?
            .get("counts")?
            .as_array()?
            .iter()
            .map(|v| v.as_u64().map(|n| n as usize))
            .collect()
    }
}

impl RVDComponentSpec {
    /// Parse RVD component specification from JSON bytes
    pub fn from_json(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    /// Parse RVD component specification from gzipped JSON
    pub fn from_gzipped_json(data: &[u8]) -> Result<Self> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut json_data = Vec::new();
        decoder.read_to_end(&mut json_data)?;
        Self::from_json(&json_data)
    }

    /// Load RVD component specification from a local file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Try gzipped first, fall back to plain JSON
        Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
    }

    /// Load RVD component specification from a path string (local or cloud URL)
    ///
    /// # Supported URL schemes
    /// - `gs://bucket/path` - Google Cloud Storage
    /// - `s3://bucket/path` - Amazon S3
    /// - `http://` or `https://` - HTTP(S) URLs
    /// - Local file path - Regular file system access
    pub fn from_path(path: &str) -> Result<Self> {
        let start = std::time::Instant::now();
        let mut reader = crate::io::get_reader(path)?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        debug!(
            "RVDComponentSpec::from_path read {} bytes in {:?}",
            data.len(),
            start.elapsed()
        );

        let parse_start = std::time::Instant::now();
        let result = Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data));
        debug!(
            "RVDComponentSpec::from_path parsed JSON in {:?}",
            parse_start.elapsed()
        );
        result
    }

    /// Load RVD spec with optional caching support.
    pub fn from_path_cached(
        path: &str,
        table_url: &str,
        cache: Option<&super::cache::MetadataCache>,
        opts: Option<&super::cache::CacheOptions>,
    ) -> Result<Self> {
        match (cache, opts) {
            (Some(c), Some(o)) if o.enabled && crate::io::is_cloud_path(path) => {
                let data = c.get_or_fetch(table_url, "rows_metadata.json.gz", o, || {
                    let start = std::time::Instant::now();
                    let mut reader = crate::io::get_reader(path)?;
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf)?;
                    debug!(
                        "RVDComponentSpec fetched {} bytes in {:?}",
                        buf.len(),
                        start.elapsed()
                    );
                    Ok(buf)
                })?;
                Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
            }
            _ => Self::from_path(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gene_models_rows_metadata() {
        let metadata = RVDComponentSpec::from_file(
            "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz",
        )
        .expect("Failed to parse metadata");

        // Check key fields
        assert_eq!(metadata.key, vec!["gene_id", "chrom", "start"]);

        // Check index spec exists
        assert!(metadata.index_spec.is_some());
        let index_spec = metadata.index_spec.unwrap();
        assert_eq!(index_spec.rel_path, "../index");

        // Check partition bounds
        assert_eq!(metadata.range_bounds.len(), 1);

        // Check part files
        assert_eq!(metadata.part_files.len(), 1);
    }
}
