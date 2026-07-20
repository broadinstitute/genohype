//! Blocking buffer implementation
//!
//! Provides byte-level access on top of block-based buffers.
//! Converts `InputBlockBuffer` to `InputBuffer`.

use crate::buffer::{InputBlockBuffer, InputBuffer};
use crate::error::{HailError, Result};

/// Fixed-size blocking buffer (typically 64KB)
///
/// This buffer bridges block-based I/O (`InputBlockBuffer`) and byte-level I/O (`InputBuffer`).
/// It reads complete blocks from the underlying buffer and provides them as a byte stream.
pub struct BlockingBuffer<B: InputBlockBuffer> {
    inner: B,
    buffer: Vec<u8>,
    position: usize,
    filled: usize,
}

impl<B: InputBlockBuffer> BlockingBuffer<B> {
    /// Create a new blocking buffer
    ///
    /// The block_size parameter is unused with the new architecture but kept
    /// for API compatibility. The actual buffer size is determined by the
    /// blocks read from the underlying `InputBlockBuffer`.
    pub fn new(inner: B, _block_size: usize) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            position: 0,
            filled: 0,
        }
    }

    /// Create with default size (parameter is unused)
    pub fn with_default_size(inner: B) -> Self {
        Self::new(inner, 64 * 1024)
    }

    fn refill(&mut self) -> Result<()> {
        // Read one complete block from the underlying buffer
        let bytes_read = self.inner.read_block(&mut self.buffer)?;

        if bytes_read == 0 {
            return Err(HailError::UnexpectedEof);
        }

        self.position = 0;
        self.filled = bytes_read;
        Ok(())
    }

    fn available(&self) -> usize {
        self.filled.saturating_sub(self.position)
    }
}

impl<B: InputBlockBuffer> InputBuffer for BlockingBuffer<B> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut offset = 0;

        while offset < buf.len() {
            if self.available() == 0 {
                self.refill()?;
            }

            let to_copy = buf.len() - offset;
            let available = self.available();
            let copy_len = to_copy.min(available);

            buf[offset..offset + copy_len]
                .copy_from_slice(&self.buffer[self.position..self.position + copy_len]);

            self.position += copy_len;
            offset += copy_len;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::stream_block::StreamBlockBuffer;

    #[test]
    fn test_blocking_buffer() {
        let data = vec![
            10, 0, 0, 0, // block length = 10
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let mut buffer = BlockingBuffer::new(stream, 4); // Small block for testing

        let mut result = [0u8; 10];
        buffer.read_exact(&mut result).unwrap();

        assert_eq!(result, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }
}
