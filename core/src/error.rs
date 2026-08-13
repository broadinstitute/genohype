//! Error types for the Hail decoder

use thiserror::Error;

pub type Result<T> = std::result::Result<T, HailError>;

#[derive(Error, Debug)]
pub enum HailError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Zstd decompression error")]
    Zstd,

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Unsupported format version: {0}")]
    UnsupportedVersion(u32),

    #[error("Unexpected end of data")]
    UnexpectedEof,

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Index error: {0}")]
    Index(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error(
        "Object store range read failed for {uri} bytes {range_start}..{range_end} after {attempts} attempt(s): {source}"
    )]
    ObjectStoreRangeRead {
        uri: String,
        range_start: usize,
        range_end: usize,
        attempts: u32,
        #[source]
        source: object_store::Error,
    },

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Configuration error: {0}")]
    Config(String),
}
