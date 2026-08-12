//! Buffer stack builder for constructing the correct Hail buffer chain
//!
//! Hail uses a layered buffer architecture. The correct stack depends on the
//! metadata's `_bufferSpec` field. This module provides builders to construct
//! the appropriate stack.

use crate::buffer::{
    BlockingBuffer, InputBlockBuffer, InputBuffer, LEB128Buffer, Lz4Buffer, StreamBlockBuffer,
    ZstdBuffer,
};
use crate::metadata::BufferSpec;
use crate::{HailError, Result};
use std::fs::File;
use std::path::Path;

/// Builds the standard Hail buffer stack for reading encoded data
///
/// The standard stack (as specified by LEB128BufferSpec in metadata):
/// ```text
/// LEB128Buffer
///   └─ BlockingBuffer (optional, for performance)
///       └─ ZstdBuffer (block-based Zstd decompression)
///           └─ StreamBlockBuffer (reads length-prefixed blocks)
///               └─ File
/// ```
///
/// # Example
/// ```no_run
/// use genohype_core::buffer::BufferBuilder;
///
/// let mut buffer = BufferBuilder::from_file("data.bin")
///     .expect("Failed to open file")
///     .with_leb128()
///     .build();
/// ```
pub struct BufferBuilder<R> {
    reader: R,
    use_blocking: bool,
    block_size: usize,
}

impl BufferBuilder<File> {
    /// Create a buffer builder from a local file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self::from_reader(file))
    }
}

impl BufferBuilder<crate::io::BoxedReader> {
    /// Create a buffer builder from a path string (local or cloud URL)
    ///
    /// This method auto-detects whether the path is a local file or a cloud URL
    /// and creates the appropriate reader.
    ///
    /// # Supported URL schemes
    /// - `gs://bucket/path` - Google Cloud Storage
    /// - `s3://bucket/path` - Amazon S3
    /// - `http://` or `https://` - HTTP(S) URLs
    /// - Local file path - Regular file system access
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::buffer::BufferBuilder;
    ///
    /// // Local file
    /// let buffer = BufferBuilder::from_path("data/table.ht/rows/parts/part-0")
    ///     .expect("Failed to open file")
    ///     .with_leb128()
    ///     .build();
    ///
    /// // Cloud storage
    /// let buffer = BufferBuilder::from_path("gs://my-bucket/data/table.ht/rows/parts/part-0")
    ///     .expect("Failed to open cloud file")
    ///     .with_leb128()
    ///     .build();
    /// ```
    pub fn from_path(path: &str) -> Result<Self> {
        let reader = crate::io::get_reader(path)?;
        Ok(Self::from_reader(reader))
    }
}

impl<R: std::io::Read + Send + 'static> BufferBuilder<R> {
    /// Create a buffer builder from any reader
    pub fn from_reader(reader: R) -> Self {
        Self {
            reader,
            use_blocking: false,
            block_size: 65536,
        }
    }

    /// Enable blocking buffer layer (recommended for large files)
    pub fn with_blocking(mut self, block_size: usize) -> Self {
        self.use_blocking = true;
        self.block_size = block_size;
        self
    }

    /// Build the buffer stack with LEB128 encoding
    ///
    /// This is the STANDARD configuration for Hail data files when the
    /// metadata specifies `LEB128BufferSpec`.
    ///
    /// Use this unless you have a specific reason not to.
    pub fn with_leb128(self) -> LEB128BufferStack<R> {
        LEB128BufferStack { builder: self }
    }

    /// Build the buffer stack WITHOUT LEB128 encoding
    ///
    /// WARNING: This should only be used for:
    /// - Testing specific buffer layers
    /// - Data files that explicitly don't use LEB128BufferSpec
    ///
    /// Most Hail data files REQUIRE LEB128 encoding.
    pub fn without_leb128(self) -> RawBufferStack<R> {
        RawBufferStack { builder: self }
    }

    /// Build the decoder described by an RVD `CodecSpec._bufferSpec`.
    ///
    /// Hail row and globals data observed in supported tables use exactly:
    /// `LEB128 -> Blocking -> (Zstd | LZ4HC) -> StreamBlock`. The ordering is
    /// semantically significant, so known layers in any other arrangement are
    /// rejected rather than silently replaced with the modern Zstd default.
    pub fn build_from_spec(self, spec: &BufferSpec) -> Result<Box<dyn InputBuffer>> {
        let blocking = match spec {
            BufferSpec::LEB128 { child } => match child.as_ref() {
                BufferSpec::Blocking { block_size, child } => (*block_size, child.as_ref()),
                other => {
                    return Err(invalid_layer(
                        "LEB128BufferSpec child",
                        "BlockingBufferSpec",
                        other,
                    ))
                }
            },
            other => {
                return Err(invalid_layer(
                    "buffer stack root",
                    "LEB128BufferSpec",
                    other,
                ))
            }
        };

        let (blocking_size, compression_spec) = blocking;
        validate_block_size("BlockingBufferSpec", blocking_size)?;

        let stream: Box<dyn InputBlockBuffer> = match compression_spec {
            BufferSpec::ZstdBlock { block_size, child } => {
                validate_block_size("ZstdBlockBufferSpec", *block_size)?;
                require_stream_child("ZstdBlockBufferSpec", child)?;
                Box::new(ZstdBuffer::with_max_block_size(
                    StreamBlockBuffer::new(self.reader),
                    *block_size,
                ))
            }
            BufferSpec::LZ4HCBlock { block_size, child } => {
                validate_block_size("LZ4HCBlockBufferSpec", *block_size)?;
                require_stream_child("LZ4HCBlockBufferSpec", child)?;
                Box::new(Lz4Buffer::new(
                    StreamBlockBuffer::new(self.reader),
                    *block_size,
                ))
            }
            other => {
                return Err(invalid_layer(
                    "BlockingBufferSpec child",
                    "ZstdBlockBufferSpec or LZ4HCBlockBufferSpec",
                    other,
                ))
            }
        };

        Ok(Box::new(LEB128Buffer::new(BlockingBuffer::new(
            stream,
            blocking_size,
        ))))
    }
}

fn spec_name(spec: &BufferSpec) -> &'static str {
    match spec {
        BufferSpec::StreamBlock => "StreamBlockBufferSpec",
        BufferSpec::ZstdBlock { .. } => "ZstdBlockBufferSpec",
        BufferSpec::LZ4HCBlock { .. } => "LZ4HCBlockBufferSpec",
        BufferSpec::Blocking { .. } => "BlockingBufferSpec",
        BufferSpec::LEB128 { .. } => "LEB128BufferSpec",
    }
}

fn invalid_layer(context: &str, expected: &str, actual: &BufferSpec) -> HailError {
    HailError::Codec(format!(
        "unsupported Hail buffer stack at {context}: expected {expected}, found {}",
        spec_name(actual)
    ))
}

fn validate_block_size(layer: &str, block_size: usize) -> Result<()> {
    if block_size == 0 {
        return Err(HailError::Codec(format!(
            "invalid Hail buffer stack: {layer} has blockSize 0"
        )));
    }
    Ok(())
}

fn require_stream_child(parent: &str, child: &BufferSpec) -> Result<()> {
    if !matches!(child, BufferSpec::StreamBlock) {
        return Err(invalid_layer(
            &format!("{parent} child"),
            "StreamBlockBufferSpec",
            child,
        ));
    }
    Ok(())
}

/// Buffer stack with LEB128 integer encoding (STANDARD)
pub struct LEB128BufferStack<R> {
    builder: BufferBuilder<R>,
}

impl<R: std::io::Read + Send + 'static> LEB128BufferStack<R> {
    /// Build the complete buffer stack
    ///
    /// Returns LEB128Buffer wrapping the appropriate inner buffers.
    /// The stack is: LEB128Buffer -> BlockingBuffer -> ZstdBuffer -> StreamBlockBuffer
    pub fn build(self) -> Box<dyn InputBuffer> {
        let stream = StreamBlockBuffer::new(self.builder.reader);
        let zstd = ZstdBuffer::new(stream);
        let blocking = BlockingBuffer::new(zstd, self.builder.block_size);
        Box::new(LEB128Buffer::new(blocking))
    }
}

/// Buffer stack without LEB128 encoding (LEGACY/TESTING ONLY)
pub struct RawBufferStack<R> {
    builder: BufferBuilder<R>,
}

impl<R: std::io::Read + Send + 'static> RawBufferStack<R> {
    /// Build the buffer stack without LEB128
    ///
    /// Returns a blocking buffer that provides byte-level access to decompressed data.
    /// The stack is: BlockingBuffer -> ZstdBuffer -> StreamBlockBuffer
    pub fn build(self) -> Box<dyn InputBuffer> {
        let stream = StreamBlockBuffer::new(self.builder.reader);
        let zstd = ZstdBuffer::new(stream);
        Box::new(BlockingBuffer::new(zstd, self.builder.block_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_builder_construction() {
        let data = vec![0u8; 100];
        let cursor = Cursor::new(data);

        // Should compile and construct without panicking
        let _buffer = BufferBuilder::from_reader(cursor).with_leb128().build();
    }

    #[test]
    fn test_builder_with_blocking() {
        let data = vec![0u8; 100];
        let cursor = Cursor::new(data);

        let _buffer = BufferBuilder::from_reader(cursor)
            .with_blocking(32768)
            .with_leb128()
            .build();
    }

    fn canonical(compression: BufferSpec) -> BufferSpec {
        BufferSpec::LEB128 {
            child: Box::new(BufferSpec::Blocking {
                block_size: 32,
                child: Box::new(compression),
            }),
        }
    }

    #[test]
    fn metadata_factory_builds_lz4hc_stack() {
        let golden = [0x07, 0, 0, 0, 0x02, 0, 0, 0, 0x20, 0x01, 0x00];
        let spec = canonical(BufferSpec::LZ4HCBlock {
            block_size: 32,
            child: Box::new(BufferSpec::StreamBlock),
        });
        let mut buffer = BufferBuilder::from_reader(Cursor::new(golden))
            .build_from_spec(&spec)
            .unwrap();
        let mut decoded = [0; 2];
        buffer.read_exact(&mut decoded).unwrap();
        assert_eq!(decoded, [1, 0]);
    }

    #[test]
    fn metadata_factory_builds_zstd_stack() {
        let original = [1, 0];
        let compressed = zstd::encode_all(original.as_slice(), 1).unwrap();
        let payload_len = 4 + compressed.len();
        let mut framed = Vec::new();
        framed.extend_from_slice(&(payload_len as u32).to_le_bytes());
        framed.extend_from_slice(&(original.len() as i32).to_le_bytes());
        framed.extend_from_slice(&compressed);
        let spec = canonical(BufferSpec::ZstdBlock {
            block_size: 32,
            child: Box::new(BufferSpec::StreamBlock),
        });
        let mut buffer = BufferBuilder::from_reader(Cursor::new(framed))
            .build_from_spec(&spec)
            .unwrap();
        let mut decoded = [0; 2];
        buffer.read_exact(&mut decoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn metadata_factory_rejects_invalid_order_and_block_sizes_contextually() {
        let invalid = [
            BufferSpec::StreamBlock,
            BufferSpec::LEB128 {
                child: Box::new(BufferSpec::ZstdBlock {
                    block_size: 32,
                    child: Box::new(BufferSpec::StreamBlock),
                }),
            },
            canonical(BufferSpec::ZstdBlock {
                block_size: 0,
                child: Box::new(BufferSpec::StreamBlock),
            }),
            canonical(BufferSpec::ZstdBlock {
                block_size: 32,
                child: Box::new(BufferSpec::Blocking {
                    block_size: 32,
                    child: Box::new(BufferSpec::StreamBlock),
                }),
            }),
        ];

        for spec in invalid {
            let error = BufferBuilder::from_reader(Cursor::new(Vec::<u8>::new()))
                .build_from_spec(&spec)
                .err()
                .expect("invalid stack should fail")
                .to_string();
            assert!(
                error.contains("buffer stack") || error.contains("blockSize"),
                "{error}"
            );
        }
    }
}
