//! Metadata structures for Hail tables

use crate::{HailError, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::debug;

const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];

fn parse_json_at<T: DeserializeOwned>(data: &[u8], source: &str) -> Result<T> {
    let mut deserializer = serde_json::Deserializer::from_slice(data);
    serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
        let field_path = error.path().to_string();
        let field_path = if field_path.is_empty() {
            "<root>"
        } else {
            field_path.as_str()
        };
        HailError::InvalidFormat(format!(
            "failed to deserialize metadata at {source}, JSON field {field_path}: {}",
            error.inner()
        ))
    })
}

fn parse_gzipped_json_at<T: DeserializeOwned>(data: &[u8], source: &str) -> Result<T> {
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut json_data = Vec::new();
    decoder.read_to_end(&mut json_data).map_err(|error| {
        HailError::InvalidFormat(format!(
            "failed to decompress gzip metadata at {source}: {error}"
        ))
    })?;
    parse_json_at(&json_data, source)
}

/// Parse metadata according to its framing. Gzip bytes are decompressed exactly once;
/// deserialization failures from a valid gzip member are never replaced by an attempt
/// to interpret the compressed bytes as plain JSON.
pub(crate) fn parse_metadata_at<T: DeserializeOwned>(data: &[u8], source: &str) -> Result<T> {
    if data.starts_with(GZIP_MAGIC) {
        parse_gzipped_json_at(data, source)
    } else {
        parse_json_at(data, source)
    }
}

/// Top-level table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(alias = "fileVersion")]
    pub file_version: u32,

    #[serde(alias = "hailVersion")]
    pub hail_version: String,

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

    /// Historical Hail raw-block LZ4 codec. `HC` affects encoding only.
    #[serde(rename = "LZ4HCBlockBufferSpec")]
    LZ4HCBlock {
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

impl BufferSpec {
    /// Return a recognized codec that cannot be decoded. All currently
    /// represented layers are supported; unknown layer names fail metadata
    /// deserialization before a `BufferSpec` can be constructed.
    pub fn unsupported_decode_codec(&self) -> Option<&'static str> {
        None
    }

    pub fn data_decode_supported(&self) -> bool {
        self.unsupported_decode_codec().is_none()
    }

    /// Whether this stack contains the historical LZ4HC block codec.
    pub fn uses_lz4hc(&self) -> bool {
        match self {
            Self::LZ4HCBlock { .. } => true,
            Self::ZstdBlock { child, .. }
            | Self::Blocking { child, .. }
            | Self::LEB128 { child } => child.uses_lz4hc(),
            Self::StreamBlock => false,
        }
    }
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
        parse_json_at(data, "in-memory table metadata")
    }

    /// Parse metadata from gzipped JSON
    pub fn from_gzipped_json(data: &[u8]) -> Result<Self> {
        parse_gzipped_json_at(data, "in-memory table metadata")
    }

    /// Load metadata from a local file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(&path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let source = path.as_ref().to_string_lossy();
        parse_metadata_at(&data, &source)
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

        parse_metadata_at(&data, path)
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
                parse_metadata_at(&data, path)
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
        parse_json_at(data, "in-memory RVD metadata")
    }

    /// Parse RVD component specification from gzipped JSON
    pub fn from_gzipped_json(data: &[u8]) -> Result<Self> {
        parse_gzipped_json_at(data, "in-memory RVD metadata")
    }

    /// Load RVD component specification from a local file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(&path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let source = path.as_ref().to_string_lossy();
        parse_metadata_at(&data, &source)
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
        let result = parse_metadata_at(&data, path);
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
                parse_metadata_at(&data, path)
            }
            _ => Self::from_path(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn rvd_json(codec_name: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "name": "RVDComponentSpec",
            "_key": ["locus"],
            "_codecSpec": {
                "name": "TypedCodecSpec",
                "_eType": "EBaseStruct{}",
                "_vType": "Struct{}",
                "_bufferSpec": {
                    "name": "LEB128BufferSpec",
                    "child": {
                        "name": "BlockingBufferSpec",
                        "blockSize": 32768,
                        "child": {
                            "name": codec_name,
                            "blockSize": 32768,
                            "child": {"name": "StreamBlockBufferSpec"}
                        }
                    }
                }
            },
            "_indexSpec": null,
            "_partFiles": ["part-0"],
            "_jRangeBounds": [],
            "_attrs": {}
        }))
        .unwrap()
    }

    #[test]
    fn table_metadata_accepts_snake_case_versions_and_camel_case_aliases() {
        for file_version in [66816, 67072, 67328] {
            let snake = serde_json::json!({
                "file_version": file_version,
                "hail_version": "0.2.128-test",
                "references_rel_path": "references",
                "components": {"partition_counts": {"counts": [2, 3]}}
            });
            let metadata = TableMetadata::from_json(&serde_json::to_vec(&snake).unwrap()).unwrap();
            assert_eq!(metadata.file_version, file_version);
            assert_eq!(metadata.partition_counts(), Some(vec![2, 3]));
        }

        let camel = br#"{
            "fileVersion": 67328,
            "hailVersion": "0.2.128-test",
            "references_rel_path": "references"
        }"#;
        let metadata = TableMetadata::from_json(camel).unwrap();
        assert_eq!(metadata.file_version, 67328);
        assert_eq!(metadata.hail_version, "0.2.128-test");
    }

    #[test]
    fn framing_uses_plain_json_or_one_gzip_decode() {
        let data = rvd_json("LZ4HCBlockBufferSpec");
        let plain: RVDComponentSpec = parse_metadata_at(&data, "plain.json").unwrap();
        let compressed: RVDComponentSpec =
            parse_metadata_at(&gzip(&data), "rows/metadata.json.gz").unwrap();

        assert_eq!(plain.key, vec!["locus"]);
        assert_eq!(compressed.key, vec!["locus"]);
        assert_eq!(compressed.codec_spec.e_type, "EBaseStruct{}");
        assert_eq!(compressed.codec_spec.v_type, "Struct{}");
        assert!(compressed.codec_spec.buffer_spec.data_decode_supported());
        assert_eq!(
            compressed.codec_spec.buffer_spec.unsupported_decode_codec(),
            None
        );
        assert!(compressed.codec_spec.buffer_spec.uses_lz4hc());
    }

    #[test]
    fn gzip_deserialization_error_preserves_codec_and_paths() {
        let data = gzip(&rvd_json("UnknownBlockBufferSpec"));
        let error = parse_metadata_at::<RVDComponentSpec>(
            &data,
            "gs://bucket/table.ht/rows/metadata.json.gz",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("UnknownBlockBufferSpec"), "{error}");
        assert!(
            error.contains("gs://bucket/table.ht/rows/metadata.json.gz"),
            "{error}"
        );
        assert!(error.contains("_bufferSpec"), "{error}");
        assert!(
            !error.contains("expected value at line 1 column 1"),
            "{error}"
        );
    }

    #[test]
    fn malformed_gzip_is_not_retried_as_plain_json() {
        let error = parse_metadata_at::<TableMetadata>(&[0x1f, 0x8b, 0, 1], "broken.json.gz")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("decompress gzip metadata at broken.json.gz"),
            "{error}"
        );
        assert!(!error.contains("expected value"), "{error}");
    }

    #[test]
    fn test_parse_keyed_rows_metadata() {
        let fixture =
            std::path::PathBuf::from("tests/fixtures/tiny-keyed.ht/rows/metadata.json.gz");
        let metadata =
            RVDComponentSpec::from_file(fixture).expect("failed to parse fixture rows metadata");

        assert_eq!(metadata.key, vec!["gene_id", "chrom", "start"]);

        let index_spec = metadata.index_spec.expect("fixture should have an index");
        assert_eq!(index_spec.rel_path, "../index");
        assert_eq!(metadata.range_bounds.len(), 1);
        assert_eq!(metadata.part_files, vec!["part-0"]);
    }

    #[test]
    fn fixture_top_metadata_exposes_versions_and_fast_count() {
        let fixture = std::path::PathBuf::from("tests/fixtures/tiny-keyed.ht/metadata.json.gz");
        let metadata = TableMetadata::from_file(fixture).expect("failed to parse table metadata");

        assert_eq!(metadata.file_version, 67328);
        assert_eq!(metadata.hail_version, "0.2.137-733ac4ccd943");
        assert_eq!(metadata.references_rel_path, "references");
        assert_eq!(metadata.partition_counts(), Some(vec![2]));
    }
}
