//! Block-based input buffer trait
//!
//! This trait represents buffers that operate on complete, framed blocks of data.
//! Unlike `InputBuffer` which provides a byte stream, `InputBlockBuffer` deals
//! with discrete blocks that may be compressed or otherwise encoded.

use crate::Result;

/// Trait for buffers that read complete blocks of data
///
/// A block is a discrete chunk of data with a known boundary. For example:
/// - `StreamBlockBuffer` reads length-prefixed blocks from a file
/// - `ZstdBlockBuffer` reads compressed blocks and decompresses them
///
/// The `BlockingBuffer` then converts these blocks into a byte stream
/// by implementing `InputBuffer`.
///
/// The `Send` bound is required to support parallel partition processing.
pub trait InputBlockBuffer: Send {
    /// Read one complete block into the provided buffer
    ///
    /// # Returns
    /// - `Ok(n)` where n > 0: A block of n bytes was read into `buf`
    /// - `Ok(0)`: EOF reached, no more blocks available
    /// - `Err(e)`: An error occurred while reading
    ///
    /// # Implementation Notes
    /// - The buffer `buf` will be resized as needed to hold the block
    /// - The previous contents of `buf` may be overwritten
    /// - Implementations should handle framing (e.g., length prefixes)
    fn read_block(&mut self, buf: &mut Vec<u8>) -> Result<usize>;
}

impl InputBlockBuffer for Box<dyn InputBlockBuffer> {
    fn read_block(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        (**self).read_block(buf)
    }
}
