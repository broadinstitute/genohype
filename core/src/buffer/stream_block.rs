//! Stream block buffer implementation
//!
//! Reads data formatted as: [4-byte length][data block]...

use crate::buffer::InputBlockBuffer;
use crate::error::{HailError, Result};
use std::io::Read;

/// Stream block buffer that reads length-prefixed blocks
///
/// This buffer reads blocks from a stream where each block is prefixed
/// with a 4-byte little-endian length. It implements `InputBlockBuffer`
/// to provide complete blocks to the next layer in the buffer stack.
pub struct StreamBlockBuffer<R: Read + Send> {
    reader: R,
}

impl<R: Read + Send> StreamBlockBuffer<R> {
    /// Create a new stream block buffer
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: Read + Send> InputBlockBuffer for StreamBlockBuffer<R> {
    fn read_block(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        // A clean EOF is valid only before any byte of the next length prefix.
        // Once framing begins, a short prefix or payload is malformed data.
        let mut len_buf = [0u8; 4];
        loop {
            match self.reader.read(&mut len_buf[..1]) {
                Ok(0) => return Ok(0),
                Ok(1) => break,
                Ok(_) => unreachable!(),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        self.reader.read_exact(&mut len_buf[1..]).map_err(|error| {
            HailError::InvalidFormat(format!("truncated StreamBlock length prefix: {error}"))
        })?;

        let block_len = u32::from_le_bytes(len_buf) as usize;

        buf.resize(block_len, 0);
        self.reader.read_exact(buf).map_err(|error| {
            HailError::InvalidFormat(format!(
                "truncated StreamBlock payload: recorded {block_len} bytes: {error}"
            ))
        })?;

        Ok(block_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_single_block() {
        // Block format: [length: 4][data: 5]
        let data = vec![
            5, 0, 0, 0, // length = 5
            1, 2, 3, 4, 5, // data
        ];

        let mut buffer = StreamBlockBuffer::new(&data[..]);
        let mut block = Vec::new();
        let len = buffer.read_block(&mut block).unwrap();

        assert_eq!(len, 5);
        assert_eq!(block, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_read_multiple_blocks() {
        let data = vec![
            3, 0, 0, 0, // length = 3
            1, 2, 3, // data
            2, 0, 0, 0, // length = 2
            4, 5, // data
        ];

        let mut buffer = StreamBlockBuffer::new(&data[..]);

        // Read first block
        let mut block = Vec::new();
        let len = buffer.read_block(&mut block).unwrap();
        assert_eq!(len, 3);
        assert_eq!(block, vec![1, 2, 3]);

        // Read second block
        let len = buffer.read_block(&mut block).unwrap();
        assert_eq!(len, 2);
        assert_eq!(block, vec![4, 5]);
    }

    #[test]
    fn test_read_eof() {
        let data = vec![
            2, 0, 0, 0, // length = 2
            1, 2, // data
        ];

        let mut buffer = StreamBlockBuffer::new(&data[..]);

        // Read the block
        let mut block = Vec::new();
        let len = buffer.read_block(&mut block).unwrap();
        assert_eq!(len, 2);

        // Try to read past EOF
        let len = buffer.read_block(&mut block).unwrap();
        assert_eq!(len, 0); // EOF
    }

    #[test]
    fn rejects_truncated_length_and_payload() {
        for data in [&[7, 0][..], &[3, 0, 0, 0, 1, 2][..]] {
            let error = StreamBlockBuffer::new(data)
                .read_block(&mut Vec::new())
                .unwrap_err()
                .to_string();
            assert!(error.contains("truncated StreamBlock"), "{error}");
        }
    }
}
