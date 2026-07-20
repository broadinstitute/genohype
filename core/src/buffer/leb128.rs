//! LEB128 (Little Endian Base 128) variable-length integer encoding
//!
//! Used by Hail for efficient encoding of small integers

use crate::buffer::InputBuffer;
use crate::error::{HailError, Result};

/// LEB128 decoding buffer
pub struct LEB128Buffer<B: InputBuffer> {
    inner: B,
}

impl<B: InputBuffer> LEB128Buffer<B> {
    /// Create a new LEB128 buffer
    pub fn new(inner: B) -> Self {
        Self { inner }
    }

    /// Read an unsigned LEB128 integer
    pub fn read_uleb128(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;

        loop {
            let byte = self.inner.read_u8()?;
            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 64 {
                return Err(HailError::InvalidFormat(
                    "LEB128 value too large".to_string(),
                ));
            }
        }

        Ok(result)
    }

    /// Read a signed LEB128 integer
    pub fn read_sleb128(&mut self) -> Result<i64> {
        let mut result: i64 = 0;
        let mut shift = 0;
        let mut byte;

        loop {
            byte = self.inner.read_u8()?;
            result |= ((byte & 0x7F) as i64) << shift;
            shift += 7;

            if byte & 0x80 == 0 {
                break;
            }

            if shift >= 64 {
                return Err(HailError::InvalidFormat(
                    "SLEB128 value too large".to_string(),
                ));
            }
        }

        // Sign extend if necessary
        if shift < 64 && (byte & 0x40) != 0 {
            result |= !0 << shift;
        }

        Ok(result)
    }

    /// Get access to inner buffer
    pub fn inner_mut(&mut self) -> &mut B {
        &mut self.inner
    }
}

impl<B: InputBuffer> InputBuffer for LEB128Buffer<B> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf)
    }

    // Override integer reads to use LEB128 encoding
    // This is required when metadata specifies LEB128BufferSpec
    // IMPORTANT: Hail uses ULEB128 (unsigned), not SLEB128 for Int32/Int64!
    // The values are encoded as unsigned but represent signed integers,
    // so we cast u64 -> i64 -> i32 to preserve the bit pattern
    fn read_i32(&mut self) -> Result<i32> {
        self.read_uleb128().map(|v| v as i64 as i32)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.read_uleb128().map(|v| v as i64)
    }

    // Note: read_u8, read_bool, read_f32, read_f64 use default implementations
    // (read raw bytes via read_exact) because these are not LEB128 encoded in Hail
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BlockingBuffer, StreamBlockBuffer};

    #[test]
    fn test_read_uleb128_single_byte() {
        let data = vec![
            1, 0, 0, 0,    // block length
            0x7F, // 127 in LEB128
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let blocking = BlockingBuffer::with_default_size(stream);
        let mut buffer = LEB128Buffer::new(blocking);

        assert_eq!(buffer.read_uleb128().unwrap(), 127);
    }

    #[test]
    fn test_read_uleb128_multi_byte() {
        let data = vec![
            2, 0, 0, 0, // block length
            0x80, 0x01, // 128 in LEB128
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let blocking = BlockingBuffer::with_default_size(stream);
        let mut buffer = LEB128Buffer::new(blocking);

        assert_eq!(buffer.read_uleb128().unwrap(), 128);
    }

    #[test]
    fn test_read_sleb128_positive() {
        let data = vec![
            2, 0, 0, 0, // block length
            0xFF, 0x00, // 127 in SLEB128 (multi-byte encoding)
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let blocking = BlockingBuffer::with_default_size(stream);
        let mut buffer = LEB128Buffer::new(blocking);

        assert_eq!(buffer.read_sleb128().unwrap(), 127);
    }

    #[test]
    fn test_read_sleb128_negative() {
        let data = vec![
            1, 0, 0, 0,    // block length
            0x7F, // -1 in SLEB128 (bit 6 set, sign extends to -1)
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let blocking = BlockingBuffer::with_default_size(stream);
        let mut buffer = LEB128Buffer::new(blocking);

        assert_eq!(buffer.read_sleb128().unwrap(), -1);
    }
}
