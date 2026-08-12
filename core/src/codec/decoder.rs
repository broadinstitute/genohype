//! Type-specific decoders for Hail data

use crate::buffer::InputBuffer;
use crate::schema::LegacyJsonHailType;
use crate::Result;

/// Trait for decoding Hail types
pub trait Decoder {
    /// Decode a value of the given type from the buffer
    fn decode(&self, buffer: &mut dyn InputBuffer, hail_type: &LegacyJsonHailType)
        -> Result<Value>;
}

/// Decoded value
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(String),
    Array(Vec<Value>),
    Struct(Vec<(String, Value)>),
    Tuple(Vec<Value>),
}

/// Default Hail decoder
pub struct HailDecoder;

impl HailDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HailDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for HailDecoder {
    fn decode(
        &self,
        buffer: &mut dyn InputBuffer,
        hail_type: &LegacyJsonHailType,
    ) -> Result<Value> {
        // Check for null (Hail uses a leading byte for null/present)
        let is_present = buffer.read_bool()?;
        if !is_present {
            return Ok(Value::Null);
        }

        match hail_type {
            LegacyJsonHailType::Int32 => Ok(Value::Int32(buffer.read_i32()?)),
            LegacyJsonHailType::Int64 => Ok(Value::Int64(buffer.read_i64()?)),
            LegacyJsonHailType::Float32 => Ok(Value::Float32(buffer.read_f32()?)),
            LegacyJsonHailType::Float64 => Ok(Value::Float64(buffer.read_f64()?)),
            LegacyJsonHailType::Boolean => Ok(Value::Boolean(buffer.read_bool()?)),
            LegacyJsonHailType::String => {
                // Strings are length-prefixed
                let length = buffer.read_i32()? as usize;
                let mut bytes = vec![0u8; length];
                buffer.read_exact(&mut bytes)?;
                Ok(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
            }
            LegacyJsonHailType::Array { element_type } => {
                let length = buffer.read_i32()? as usize;
                let mut values = Vec::with_capacity(length);
                for _ in 0..length {
                    values.push(self.decode(buffer, element_type)?);
                }
                Ok(Value::Array(values))
            }
            LegacyJsonHailType::Struct { fields } => {
                let mut values = Vec::with_capacity(fields.len());
                for field in fields {
                    let value = self.decode(buffer, &field.field_type)?;
                    values.push((field.name.clone(), value));
                }
                Ok(Value::Struct(values))
            }
            LegacyJsonHailType::Tuple { types } => {
                let mut values = Vec::with_capacity(types.len());
                for typ in types {
                    values.push(self.decode(buffer, typ)?);
                }
                Ok(Value::Tuple(values))
            }
            LegacyJsonHailType::Locus { .. } => {
                // Locus is encoded as (contig: String, position: Int32)
                let contig =
                    if let Value::String(s) = self.decode(buffer, &LegacyJsonHailType::String)? {
                        s
                    } else {
                        return Ok(Value::Null);
                    };
                let position = buffer.read_i32()?;
                Ok(Value::Struct(vec![
                    ("contig".to_string(), Value::String(contig)),
                    ("position".to_string(), Value::Int32(position)),
                ]))
            }
            LegacyJsonHailType::Interval { point_type } => {
                // Interval is (start: point_type, end: point_type, includes_start: bool, includes_end: bool)
                let start = self.decode(buffer, point_type)?;
                let end = self.decode(buffer, point_type)?;
                let includes_start = buffer.read_bool()?;
                let includes_end = buffer.read_bool()?;
                Ok(Value::Struct(vec![
                    ("start".to_string(), start),
                    ("end".to_string(), end),
                    ("includes_start".to_string(), Value::Boolean(includes_start)),
                    ("includes_end".to_string(), Value::Boolean(includes_end)),
                ]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BlockingBuffer, StreamBlockBuffer};

    #[test]
    fn test_decode_int32() {
        let data = vec![
            5, 0, 0, 0, // block length
            1, // present
            42, 0, 0, 0, // value = 42
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let mut buffer = BlockingBuffer::with_default_size(stream);
        let decoder = HailDecoder::new();

        let value = decoder
            .decode(&mut buffer, &LegacyJsonHailType::Int32)
            .unwrap();
        assert_eq!(value, Value::Int32(42));
    }

    #[test]
    fn test_decode_null() {
        let data = vec![
            1, 0, 0, 0, // block length
            0, // not present
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let mut buffer = BlockingBuffer::with_default_size(stream);
        let decoder = HailDecoder::new();

        let value = decoder
            .decode(&mut buffer, &LegacyJsonHailType::Int32)
            .unwrap();
        assert_eq!(value, Value::Null);
    }
}
