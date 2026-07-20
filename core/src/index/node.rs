//! Index node structures for B-tree indexes

use crate::codec::EncodedValue;
use crate::HailError;
use crate::Result;

/// Index node (either leaf or internal)
#[derive(Debug, Clone)]
pub enum IndexNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

/// Leaf node in the B-tree index
#[derive(Debug, Clone)]
pub struct LeafNode {
    pub first_idx: i64,
    pub keys: Vec<LeafEntry>,
}

/// Entry in a leaf node
#[derive(Debug, Clone)]
pub struct LeafEntry {
    pub key: EncodedValue,
    pub offset: i64,
    pub annotation: EncodedValue,
}

/// Internal node in the B-tree index
#[derive(Debug, Clone)]
pub struct InternalNode {
    pub children: Vec<InternalEntry>,
}

/// Entry in an internal node
#[derive(Debug, Clone)]
pub struct InternalEntry {
    pub index_file_offset: i64,
    pub first_idx: i64,
    pub first_key: EncodedValue,
    pub first_record_offset: i64,
    pub first_annotation: EncodedValue,
}

impl LeafNode {
    /// Convert from an EncodedValue::Struct to a LeafNode
    pub fn from_encoded(value: EncodedValue) -> Result<Self> {
        match value {
            EncodedValue::Struct(fields) => {
                let mut first_idx = None;
                let mut keys = None;

                for (name, val) in fields {
                    match name.as_str() {
                        "first_idx" => {
                            if let EncodedValue::Int64(i) = val {
                                first_idx = Some(i);
                            }
                        }
                        "keys" => {
                            if let EncodedValue::Array(arr) = val {
                                let entries: Result<Vec<_>> =
                                    arr.into_iter().map(LeafEntry::from_encoded).collect();
                                keys = Some(entries?);
                            }
                        }
                        _ => {}
                    }
                }

                Ok(LeafNode {
                    first_idx: first_idx.ok_or_else(|| {
                        HailError::Codec("Missing first_idx in leaf node".to_string())
                    })?,
                    keys: keys
                        .ok_or_else(|| HailError::Codec("Missing keys in leaf node".to_string()))?,
                })
            }
            _ => Err(HailError::Codec(
                "Expected Struct for leaf node".to_string(),
            )),
        }
    }
}

impl LeafEntry {
    /// Convert from an EncodedValue::Struct to a LeafEntry
    pub fn from_encoded(value: EncodedValue) -> Result<Self> {
        match value {
            EncodedValue::Struct(fields) => {
                let mut key = None;
                let mut offset = None;
                let mut annotation = None;

                for (name, val) in fields {
                    match name.as_str() {
                        "key" => key = Some(val),
                        "offset" => {
                            if let EncodedValue::Int64(i) = val {
                                offset = Some(i);
                            }
                        }
                        "annotation" => annotation = Some(val),
                        _ => {}
                    }
                }

                Ok(LeafEntry {
                    key: key
                        .ok_or_else(|| HailError::Codec("Missing key in leaf entry".to_string()))?,
                    offset: offset.ok_or_else(|| {
                        HailError::Codec("Missing offset in leaf entry".to_string())
                    })?,
                    annotation: annotation.ok_or_else(|| {
                        HailError::Codec("Missing annotation in leaf entry".to_string())
                    })?,
                })
            }
            _ => Err(HailError::Codec(
                "Expected Struct for leaf entry".to_string(),
            )),
        }
    }
}

impl InternalNode {
    /// Convert from an EncodedValue::Struct to an InternalNode
    pub fn from_encoded(value: EncodedValue) -> Result<Self> {
        match value {
            EncodedValue::Struct(fields) => {
                let mut children = None;

                for (name, val) in fields {
                    if name == "children" {
                        if let EncodedValue::Array(arr) = val {
                            let entries: Result<Vec<_>> =
                                arr.into_iter().map(InternalEntry::from_encoded).collect();
                            children = Some(entries?);
                        }
                    }
                }

                Ok(InternalNode {
                    children: children.ok_or_else(|| {
                        HailError::Codec("Missing children in internal node".to_string())
                    })?,
                })
            }
            _ => Err(HailError::Codec(
                "Expected Struct for internal node".to_string(),
            )),
        }
    }
}

impl InternalEntry {
    /// Convert from an EncodedValue::Struct to an InternalEntry
    pub fn from_encoded(value: EncodedValue) -> Result<Self> {
        match value {
            EncodedValue::Struct(fields) => {
                let mut index_file_offset = None;
                let mut first_idx = None;
                let mut first_key = None;
                let mut first_record_offset = None;
                let mut first_annotation = None;

                for (name, val) in fields {
                    match name.as_str() {
                        "index_file_offset" => {
                            if let EncodedValue::Int64(i) = val {
                                index_file_offset = Some(i);
                            }
                        }
                        "first_idx" => {
                            if let EncodedValue::Int64(i) = val {
                                first_idx = Some(i);
                            }
                        }
                        "first_key" => first_key = Some(val),
                        "first_record_offset" => {
                            if let EncodedValue::Int64(i) = val {
                                first_record_offset = Some(i);
                            }
                        }
                        "first_annotation" => first_annotation = Some(val),
                        _ => {}
                    }
                }

                Ok(InternalEntry {
                    index_file_offset: index_file_offset
                        .ok_or_else(|| HailError::Codec("Missing index_file_offset".to_string()))?,
                    first_idx: first_idx
                        .ok_or_else(|| HailError::Codec("Missing first_idx".to_string()))?,
                    first_key: first_key
                        .ok_or_else(|| HailError::Codec("Missing first_key".to_string()))?,
                    first_record_offset: first_record_offset.ok_or_else(|| {
                        HailError::Codec("Missing first_record_offset".to_string())
                    })?,
                    first_annotation: first_annotation
                        .ok_or_else(|| HailError::Codec("Missing first_annotation".to_string()))?,
                })
            }
            _ => Err(HailError::Codec(
                "Expected Struct for internal entry".to_string(),
            )),
        }
    }
}
