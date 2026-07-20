//! Hail codec system for type-specific encoding/decoding
//!
//! This module will implement decoders for all Hail types

pub mod decoder;
pub mod encoded_type;
pub mod encoder;
pub mod etype_parser;

pub use decoder::{Decoder, HailDecoder, Value};
pub use encoded_type::{EncodedField, EncodedType, EncodedValue};
pub use encoder::Encoder;
pub use etype_parser::ETypeParser;

use crate::Result;

/// Codec specification
#[derive(Debug, Clone)]
pub struct CodecSpec {
    pub buffer_spec: BufferSpec,
}

/// Buffer specification
#[derive(Debug, Clone)]
pub enum BufferSpec {
    /// Stream block buffer with Zstd compression
    StreamBlockBufferSpec {
        block_size: usize,
        child: Box<BufferSpec>,
    },

    /// LEB128 encoding
    LEB128BufferSpec { child: Box<BufferSpec> },

    /// Blocking buffer
    BlockingBufferSpec {
        block_size: usize,
        child: Box<BufferSpec>,
    },

    /// Base spec (no transformation)
    BlockBufferSpec,
}

impl BufferSpec {
    /// Parse buffer spec from JSON
    pub fn from_json(_data: &serde_json::Value) -> Result<Self> {
        // TODO: Implement parsing from Hail metadata
        // For now, return default spec
        Ok(Self::default_spec())
    }

    /// Default buffer spec used by Hail
    pub fn default_spec() -> Self {
        BufferSpec::StreamBlockBufferSpec {
            block_size: 64 * 1024,
            child: Box::new(BufferSpec::LEB128BufferSpec {
                child: Box::new(BufferSpec::BlockingBufferSpec {
                    block_size: 64 * 1024,
                    child: Box::new(BufferSpec::BlockBufferSpec),
                }),
            }),
        }
    }
}
