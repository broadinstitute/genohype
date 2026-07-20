//! Hail schema representation and parsing

use crate::Result;
use serde::{Deserialize, Serialize};

/// Hail table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub file_version: u32,
    pub hail_version: String,
    pub references_rel_path: String,

    #[serde(flatten)]
    pub extra: serde_json::Value,
}

/// Hail type system
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum HailType {
    #[serde(rename = "int32")]
    Int32,

    #[serde(rename = "int64")]
    Int64,

    #[serde(rename = "float32")]
    Float32,

    #[serde(rename = "float64")]
    Float64,

    #[serde(rename = "bool")]
    Boolean,

    #[serde(rename = "str")]
    String,

    #[serde(rename = "array")]
    Array {
        #[serde(rename = "elementType")]
        element_type: Box<HailType>,
    },

    #[serde(rename = "struct")]
    Struct { fields: Vec<Field> },

    #[serde(rename = "tuple")]
    Tuple { types: Vec<HailType> },

    #[serde(rename = "locus")]
    Locus {
        #[serde(rename = "rg")]
        reference_genome: String,
    },

    #[serde(rename = "interval")]
    Interval {
        #[serde(rename = "pointType")]
        point_type: Box<HailType>,
    },
}

/// Struct field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: HailType,
}

impl Metadata {
    /// Parse metadata from JSON bytes
    pub fn from_json(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    /// Parse metadata from gzipped JSON
    pub fn from_gzipped_json(data: &[u8]) -> Result<Self> {
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut json_data = Vec::new();
        decoder.read_to_end(&mut json_data)?;
        Self::from_json(&json_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_type() {
        let json = r#"{"name": "int32"}"#;
        let hail_type: HailType = serde_json::from_str(json).unwrap();
        assert!(matches!(hail_type, HailType::Int32));
    }

    #[test]
    fn test_parse_array_type() {
        let json = r#"{"name": "array", "elementType": {"name": "int32"}}"#;
        let hail_type: HailType = serde_json::from_str(json).unwrap();
        match hail_type {
            HailType::Array { element_type } => {
                assert!(matches!(*element_type, HailType::Int32));
            }
            _ => panic!("Expected array type"),
        }
    }
}
