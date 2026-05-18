//! Buffer implementations for reading Hail data
//!
//! This module implements the 4-layer buffer stack used by Hail:
//! 1. StreamBlockBuffer - reads length-prefixed blocks
//! 2. ZstdBuffer - decompresses Zstd data
//! 3. BlockingBuffer - provides fixed-size buffering
//! 4. LEB128Buffer - decodes variable-length integers
//!
//! # Usage
//!
//! **For most use cases**, use the [`BufferBuilder`] to construct the correct stack:
//!
//! ```no_run
//! use genohype_core::buffer::BufferBuilder;
//!
//! // Standard configuration (with LEB128 encoding)
//! let mut buffer = BufferBuilder::from_file("data.bin")
//!     .unwrap()
//!     .with_leb128()
//!     .build();
//! ```
//!
//! The builder ensures you set up the correct buffer chain as specified by
//! Hail's metadata `_bufferSpec` field.

pub mod block;
pub mod block_map;
pub mod blocking;
pub mod builder;
pub mod leb128;
pub mod output;
pub mod slice;
pub mod stream_block;
pub mod zstd;

pub use block::InputBlockBuffer;
pub use block_map::BlockMap;
pub use blocking::BlockingBuffer;
pub use builder::BufferBuilder;
pub use leb128::LEB128Buffer;
pub use output::{OutputBuffer, LEB128OutputBuffer, BlockingOutputBuffer, StreamBlockOutputBuffer, ZstdOutputBuffer, OutputBufferBuilder};
pub use slice::SliceBuffer;
pub use stream_block::StreamBlockBuffer;
pub use zstd::ZstdBuffer;

use crate::Result;

/// Trait for all input buffers
///
/// The `Send` bound is required to support parallel partition processing
/// where each worker thread owns its own buffer.
pub trait InputBuffer: Send {
    /// Read exactly `buf.len()` bytes into `buf`
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;

    /// Read a single byte
    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a 32-bit integer (little-endian)
    fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    /// Read a 64-bit integer (little-endian)
    fn read_i64(&mut self) -> Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    /// Read a 32-bit float (little-endian)
    fn read_f32(&mut self) -> Result<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    /// Read a 64-bit float (little-endian)
    fn read_f64(&mut self) -> Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    /// Read a boolean
    fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    /// Skip `len` bytes without allocating or returning them.
    ///
    /// Default implementation reads into a small stack buffer in a loop.
    /// Concrete buffer types can override for better performance.
    fn skip(&mut self, mut len: usize) -> Result<()> {
        let mut buf = [0u8; 8192];
        while len > 0 {
            let chunk = len.min(buf.len());
            self.read_exact(&mut buf[..chunk])?;
            len -= chunk;
        }
        Ok(())
    }
}

// Implement InputBuffer for Box<dyn InputBuffer> to allow dynamic dispatch
impl InputBuffer for Box<dyn InputBuffer> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (**self).read_exact(buf)
    }

    fn read_i32(&mut self) -> Result<i32> {
        (**self).read_i32()
    }

    fn read_i64(&mut self) -> Result<i64> {
        (**self).read_i64()
    }

    fn read_f32(&mut self) -> Result<f32> {
        (**self).read_f32()
    }

    fn read_f64(&mut self) -> Result<f64> {
        (**self).read_f64()
    }

    fn read_u8(&mut self) -> Result<u8> {
        (**self).read_u8()
    }

    fn read_bool(&mut self) -> Result<bool> {
        (**self).read_bool()
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        (**self).skip(len)
    }
}
