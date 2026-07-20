//! Buffer stack builder for constructing the correct Hail buffer chain
//!
//! Hail uses a layered buffer architecture. The correct stack depends on the
//! metadata's `_bufferSpec` field. This module provides builders to construct
//! the appropriate stack.

use crate::buffer::{BlockingBuffer, InputBuffer, LEB128Buffer, StreamBlockBuffer, ZstdBuffer};
use crate::Result;
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
}
