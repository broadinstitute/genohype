//! Index metadata structures

use crate::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Index metadata from metadata.json.gz in index directory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    #[serde(rename = "fileVersion")]
    pub file_version: u32,

    #[serde(rename = "branchingFactor")]
    pub branching_factor: usize,

    pub height: u32,

    #[serde(rename = "keyType")]
    pub key_type: String,

    #[serde(rename = "annotationType")]
    pub annotation_type: String,

    #[serde(rename = "nKeys")]
    pub n_keys: usize,

    #[serde(rename = "indexPath")]
    pub index_path: String,

    #[serde(rename = "rootOffset")]
    pub root_offset: u64,

    pub attributes: serde_json::Value,
}

impl IndexMetadata {
    /// Parse index metadata from JSON bytes
    pub fn from_json(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    /// Parse index metadata from gzipped JSON
    pub fn from_gzipped_json(data: &[u8]) -> Result<Self> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut json_data = Vec::new();
        decoder.read_to_end(&mut json_data)?;
        Self::from_json(&json_data)
    }

    /// Load index metadata from a local file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Try gzipped first, fall back to plain JSON
        Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
    }

    /// Load index metadata from a path string (local or cloud URL)
    ///
    /// # Supported URL schemes
    /// - `gs://bucket/path` - Google Cloud Storage
    /// - `s3://bucket/path` - Amazon S3
    /// - `http://` or `https://` - HTTP(S) URLs
    /// - Local file path - Regular file system access
    pub fn from_path(path: &str) -> Result<Self> {
        let mut reader = crate::io::get_reader(path)?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;

        // Try gzipped first, fall back to plain JSON
        Self::from_gzipped_json(&data).or_else(|_| Self::from_json(&data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_index_metadata() {
        let fixture = std::path::PathBuf::from(
            "tests/fixtures/tiny-keyed.ht/index/part-0.idx/metadata.json.gz",
        );
        let metadata =
            IndexMetadata::from_file(fixture).expect("failed to parse fixture index metadata");

        assert_eq!(metadata.file_version, 66048);
        assert_eq!(metadata.branching_factor, 4096);
        assert_eq!(metadata.height, 2);
        assert_eq!(metadata.n_keys, 2);
        assert!(metadata.root_offset > 0);
        assert_eq!(metadata.index_path, "index");
        assert_eq!(
            metadata.key_type,
            "Struct{gene_id:String,chrom:String,start:Int32}"
        );
    }
}
