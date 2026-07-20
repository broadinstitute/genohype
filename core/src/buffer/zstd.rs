//! Zstd decompression buffer
//!
//! Reads Zstd-compressed blocks formatted as: [4-byte decompressed size][zstd data]
//!
//! This buffer implements `InputBlockBuffer` and expects its underlying buffer
//! to provide complete blocks where each block contains a 4-byte decompressed
//! length followed by raw Zstd compressed data (without magic number).

use crate::buffer::InputBlockBuffer;
use crate::error::{HailError, Result};

/// Zstd decompression buffer
///
/// Reads blocks from the underlying `InputBlockBuffer`, where each block
/// has the format: `[4-byte decompressed_len][zstd_compressed_data]`
pub struct ZstdBuffer<B: InputBlockBuffer> {
    inner: B,
}

impl<B: InputBlockBuffer> ZstdBuffer<B> {
    /// Create a new Zstd buffer
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

impl<B: InputBlockBuffer> InputBlockBuffer for ZstdBuffer<B> {
    fn read_block(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        // Read a compressed block from the underlying buffer
        let mut compressed_block = Vec::new();
        let block_size = self.inner.read_block(&mut compressed_block)?;

        if block_size == 0 {
            return Ok(0); // EOF
        }

        if block_size < 4 {
            return Err(HailError::InvalidFormat(
                "Zstd block too small for decompressed length".to_string(),
            ));
        }

        // Parse the first 4 bytes as the expected decompressed length
        let expected_decompressed_len = i32::from_le_bytes([
            compressed_block[0],
            compressed_block[1],
            compressed_block[2],
            compressed_block[3],
        ]) as usize;

        // The rest is the raw Zstd compressed data (no magic number)
        let compressed_data = &compressed_block[4..];

        // Decompress the data
        let decompressed = zstd::decode_all(compressed_data).map_err(|_| HailError::Zstd)?;

        // Verify the decompressed size matches the expected size
        if decompressed.len() != expected_decompressed_len {
            return Err(HailError::InvalidFormat(format!(
                "Decompressed size mismatch: expected {}, got {}",
                expected_decompressed_len,
                decompressed.len()
            )));
        }

        // Copy decompressed data into output buffer
        buf.clear();
        buf.extend_from_slice(&decompressed);

        Ok(decompressed.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StreamBlockBuffer;

    #[test]
    fn test_decompress_simple_block() {
        // Create a simple test: compress "hello" with zstd
        let original = b"hello";
        let compressed = zstd::encode_all(&original[..], 3).unwrap();

        // Build the block format: [decompressed_len: 4][compressed_data]
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&(original.len() as i32).to_le_bytes());
        block_data.extend_from_slice(&compressed);

        // Wrap in StreamBlockBuffer format: [block_len: 4][block_data]
        let mut stream_data = Vec::new();
        stream_data.extend_from_slice(&(block_data.len() as u32).to_le_bytes());
        stream_data.extend_from_slice(&block_data);

        // Create the buffer stack
        let stream_buffer = StreamBlockBuffer::new(&stream_data[..]);
        let mut zstd_buffer = ZstdBuffer::new(stream_buffer);

        // Read and decompress
        let mut output = Vec::new();
        let len = zstd_buffer.read_block(&mut output).unwrap();

        assert_eq!(len, 5);
        assert_eq!(output, b"hello");
    }
}
