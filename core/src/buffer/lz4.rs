//! Hail LZ4HC raw-block decompression.
//!
//! Hail's `LZ4HCBlockBufferSpec` uses the same on-disk framing as its Zstd
//! block codec: each outer stream block contains a four-byte little-endian
//! signed decompressed length followed by a raw LZ4 block. "HC" only describes
//! the encoder's compression strategy; decoding uses the ordinary LZ4 block
//! format (not an LZ4 frame and not lz4_flex's size-prepended helper).

use crate::buffer::InputBlockBuffer;
use crate::error::{HailError, Result};

/// Decompresses raw LZ4 blocks supplied by an inner block buffer.
pub struct Lz4Buffer<B: InputBlockBuffer> {
    inner: B,
    max_block_size: usize,
}

impl<B: InputBlockBuffer> Lz4Buffer<B> {
    pub fn new(inner: B, max_block_size: usize) -> Self {
        Self {
            inner,
            max_block_size,
        }
    }
}

impl<B: InputBlockBuffer> InputBlockBuffer for Lz4Buffer<B> {
    fn read_block(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let mut compressed_block = Vec::new();
        let block_size = self.inner.read_block(&mut compressed_block)?;
        if block_size == 0 {
            return Ok(0);
        }
        if block_size < 4 {
            return Err(HailError::InvalidFormat(format!(
                "LZ4HC block is truncated: expected a 4-byte decompressed length, got {block_size} bytes"
            )));
        }

        let expected_i32 = i32::from_le_bytes(compressed_block[..4].try_into().unwrap());
        let expected_len = usize::try_from(expected_i32).map_err(|_| {
            HailError::InvalidFormat(format!(
                "LZ4HC block has negative decompressed length {expected_i32}"
            ))
        })?;
        if expected_len > self.max_block_size {
            return Err(HailError::InvalidFormat(format!(
                "LZ4HC decompressed length {expected_len} exceeds recorded blockSize {}",
                self.max_block_size
            )));
        }

        buf.clear();
        buf.resize(expected_len, 0);
        let actual_len =
            lz4_flex::block::decompress_into(&compressed_block[4..], buf).map_err(|error| {
                HailError::InvalidFormat(format!("malformed LZ4HC raw block: {error}"))
            })?;
        if actual_len != expected_len {
            return Err(HailError::InvalidFormat(format!(
                "LZ4HC decompressed size mismatch: expected {expected_len}, got {actual_len}"
            )));
        }
        buf.truncate(actual_len);
        Ok(actual_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StreamBlockBuffer;

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn decodes_retained_hail_lz4hc_golden_block() {
        // Retained bytes from a historical Hail part: outer length 7,
        // decompressed length 2, then the raw LZ4 literal block 20 01 00.
        let golden = [0x07, 0, 0, 0, 0x02, 0, 0, 0, 0x20, 0x01, 0x00];
        let stream = StreamBlockBuffer::new(golden.as_slice());
        let mut lz4 = Lz4Buffer::new(stream, 32);
        let mut output = Vec::new();

        assert_eq!(lz4.read_block(&mut output).unwrap(), 2);
        assert_eq!(output, [0x01, 0x00]);
    }

    #[test]
    fn decodes_raw_lz4_literal_without_frame_or_size_prefix() {
        let payload = [5, 0, 0, 0, 0x50, b'h', b'e', b'l', b'l', b'o'];
        let bytes = framed(&payload);
        let stream = StreamBlockBuffer::new(bytes.as_slice());
        let mut lz4 = Lz4Buffer::new(stream, 32);
        let mut output = Vec::new();

        assert_eq!(lz4.read_block(&mut output).unwrap(), 5);
        assert_eq!(output, b"hello");
    }

    #[test]
    fn rejects_truncated_header_and_malformed_raw_data() {
        for payload in [&[1, 0, 0][..], &[5, 0, 0, 0, 0x00, 0x00][..]] {
            let bytes = framed(payload);
            let stream = StreamBlockBuffer::new(bytes.as_slice());
            let error = Lz4Buffer::new(stream, 32)
                .read_block(&mut Vec::new())
                .unwrap_err()
                .to_string();
            assert!(error.contains("LZ4HC"), "{error}");
        }
    }

    #[test]
    fn rejects_incorrect_negative_and_over_limit_lengths() {
        let cases = [
            (vec![6, 0, 0, 0, 0x50, b'h', b'e', b'l', b'l', b'o'], 32),
            (vec![0xff, 0xff, 0xff, 0xff, 0x00], 32),
            (vec![33, 0, 0, 0, 0x00], 32),
        ];
        for (payload, limit) in cases {
            let bytes = framed(&payload);
            let stream = StreamBlockBuffer::new(bytes.as_slice());
            let error = Lz4Buffer::new(stream, limit)
                .read_block(&mut Vec::new())
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("mismatch")
                    || error.contains("negative")
                    || error.contains("blockSize"),
                "{error}"
            );
        }
    }
}
