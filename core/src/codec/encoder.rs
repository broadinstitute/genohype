//! Encoder for writing EncodedValue to Hail binary format
//!
//! This is the inverse of the decoding logic in `encoded_type.rs`.
//! It writes values using Hail's binary encoding format.

use crate::buffer::OutputBuffer;
use crate::codec::{EncodedType, EncodedValue};
use crate::{HailError, Result};

/// Encoder for writing EncodedValue to an OutputBuffer
pub struct Encoder;

impl Encoder {
    /// Write a value to the buffer according to its type
    ///
    /// This handles the top-level nullable flag if the type is not required.
    pub fn write_value(
        buffer: &mut dyn OutputBuffer,
        ty: &EncodedType,
        value: &EncodedValue,
    ) -> Result<()> {
        // For standalone values, write nullable flag if needed
        if !ty.is_required() {
            let is_present = !matches!(value, EncodedValue::Null);
            buffer.write_bool(is_present)?;
            if !is_present {
                return Ok(());
            }
        }

        // Write the present value
        Self::write_present_value(buffer, ty, value)
    }

    /// Write a value that's known to be present (no nullable flag)
    ///
    /// This is used for struct fields after the bitmap has been written,
    /// and for required types.
    pub fn write_present_value(
        buffer: &mut dyn OutputBuffer,
        ty: &EncodedType,
        value: &EncodedValue,
    ) -> Result<()> {
        match (ty, value) {
            (EncodedType::EBinary { .. }, EncodedValue::Binary(bytes)) => {
                // Write i32 byte-length prefix
                buffer.write_i32(bytes.len() as i32)?;
                buffer.write_bytes(bytes)?;
                Ok(())
            }

            (EncodedType::EInt32 { .. }, EncodedValue::Int32(v)) => {
                buffer.write_i32(*v)?;
                Ok(())
            }

            (EncodedType::EInt64 { .. }, EncodedValue::Int64(v)) => {
                buffer.write_i64(*v)?;
                Ok(())
            }

            (EncodedType::EFloat32 { .. }, EncodedValue::Float32(v)) => {
                buffer.write_f32(*v)?;
                Ok(())
            }

            (EncodedType::EFloat64 { .. }, EncodedValue::Float64(v)) => {
                buffer.write_f64(*v)?;
                Ok(())
            }

            (EncodedType::EBoolean { .. }, EncodedValue::Boolean(v)) => {
                buffer.write_bool(*v)?;
                Ok(())
            }

            (EncodedType::EBaseStruct { fields, .. }, EncodedValue::Struct(field_values)) => {
                Self::write_struct(buffer, fields, field_values)
            }

            (EncodedType::EArray { element, .. }, EncodedValue::Array(elements)) => {
                Self::write_array(buffer, element, elements)
            }

            // Handle type mismatches
            (ty, EncodedValue::Null) => Err(HailError::InvalidFormat(format!(
                "Cannot write null value for required type {:?}",
                ty
            ))),

            (ty, value) => Err(HailError::InvalidFormat(format!(
                "Type mismatch: expected {:?}, got {:?}",
                ty, value
            ))),
        }
    }

    /// Write a struct value
    ///
    /// Struct encoding format:
    /// [missing bitmap][field data for present fields only]
    fn write_struct(
        buffer: &mut dyn OutputBuffer,
        fields: &[crate::codec::EncodedField],
        field_values: &[(String, EncodedValue)],
    ) -> Result<()> {
        // Create a lookup map for field values
        let value_map: std::collections::HashMap<&str, &EncodedValue> = field_values
            .iter()
            .map(|(name, val)| (name.as_str(), val))
            .collect();

        // Count nullable fields
        let n_nullable = fields
            .iter()
            .filter(|f| !f.encoded_type.is_required())
            .count();
        let n_missing_bytes = (n_nullable + 7) / 8;

        // Build the missing bitmap
        let mut missing_bitmap = vec![0u8; n_missing_bytes];
        let mut nullable_idx = 0;

        for field in fields {
            if !field.encoded_type.is_required() {
                let value = value_map.get(field.name.as_str());
                let is_missing = match value {
                    Some(EncodedValue::Null) | None => true,
                    _ => false,
                };

                if is_missing {
                    let byte_idx = nullable_idx / 8;
                    let bit_idx = nullable_idx % 8;
                    missing_bitmap[byte_idx] |= 1 << bit_idx;
                }

                nullable_idx += 1;
            }
        }

        // Write the bitmap
        buffer.write_bytes(&missing_bitmap)?;

        // Write present field values
        nullable_idx = 0;
        for field in fields {
            let value = value_map
                .get(field.name.as_str())
                .cloned()
                .unwrap_or(&EncodedValue::Null);

            if field.encoded_type.is_required() {
                // Required field - always write
                Self::write_present_value(buffer, &field.encoded_type, value)?;
            } else {
                // Nullable field - check bitmap
                let byte_idx = nullable_idx / 8;
                let bit_idx = nullable_idx % 8;
                let is_missing = (missing_bitmap[byte_idx] >> bit_idx) & 1 == 1;

                if !is_missing {
                    // Field is present - write it WITHOUT nullable flag
                    Self::write_present_value(buffer, &field.encoded_type, value)?;
                }

                nullable_idx += 1;
            }
        }

        Ok(())
    }

    /// Write an array value
    ///
    /// Array encoding format:
    /// [i32 length][missing bitmap (if nullable elements)][elements (only present ones)]
    fn write_array(
        buffer: &mut dyn OutputBuffer,
        element_type: &EncodedType,
        elements: &[EncodedValue],
    ) -> Result<()> {
        // Write array length
        buffer.write_i32(elements.len() as i32)?;

        // If elements are nullable, write missing bitmap
        if !element_type.is_required() {
            let n_missing_bytes = (elements.len() + 7) / 8;
            let mut missing_bitmap = vec![0u8; n_missing_bytes];

            for (i, elem) in elements.iter().enumerate() {
                if matches!(elem, EncodedValue::Null) {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    missing_bitmap[byte_idx] |= 1 << bit_idx;
                }
            }

            buffer.write_bytes(&missing_bitmap)?;
        }

        // Write present elements
        for elem in elements.iter() {
            // Check if element is present
            let is_present = if !element_type.is_required() {
                !matches!(elem, EncodedValue::Null)
            } else {
                true
            };

            if is_present {
                Self::write_present_value(buffer, element_type, elem)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BlockingOutputBuffer, LEB128OutputBuffer};
    use crate::codec::EncodedField;

    #[test]
    fn test_encode_int32() {
        let mut buffer = BlockingOutputBuffer::with_default_size();
        let ty = EncodedType::EInt32 { required: true };
        let value = EncodedValue::Int32(42);

        Encoder::write_value(&mut buffer, &ty, &value).unwrap();

        let data = buffer.into_data();
        assert_eq!(data, 42i32.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_int32_leb128() {
        let blocking = BlockingOutputBuffer::with_default_size();
        let mut buffer = LEB128OutputBuffer::new(blocking);
        let ty = EncodedType::EInt32 { required: true };
        let value = EncodedValue::Int32(127);

        Encoder::write_value(&mut buffer, &ty, &value).unwrap();

        let data = buffer.into_inner().into_data();
        // 127 in ULEB128 is 0x7F
        assert_eq!(data, vec![0x7F]);
    }

    #[test]
    fn test_encode_binary() {
        let mut buffer = BlockingOutputBuffer::with_default_size();
        let ty = EncodedType::EBinary { required: true };
        let value = EncodedValue::Binary(b"hello".to_vec());

        Encoder::write_value(&mut buffer, &ty, &value).unwrap();

        let data = buffer.into_data();
        // Length prefix (5) + "hello"
        let expected: Vec<u8> = [5i32.to_le_bytes().as_slice(), b"hello"].concat();
        assert_eq!(data, expected);
    }

    #[test]
    fn test_encode_array() {
        let mut buffer = BlockingOutputBuffer::with_default_size();
        let ty = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EInt32 { required: true }),
        };
        let value = EncodedValue::Array(vec![
            EncodedValue::Int32(1),
            EncodedValue::Int32(2),
            EncodedValue::Int32(3),
        ]);

        Encoder::write_value(&mut buffer, &ty, &value).unwrap();

        let data = buffer.into_data();
        // Length (3) + three i32 values
        let mut expected = Vec::new();
        expected.extend_from_slice(&3i32.to_le_bytes());
        expected.extend_from_slice(&1i32.to_le_bytes());
        expected.extend_from_slice(&2i32.to_le_bytes());
        expected.extend_from_slice(&3i32.to_le_bytes());
        assert_eq!(data, expected);
    }

    #[test]
    fn test_encode_struct() {
        let mut buffer = BlockingOutputBuffer::with_default_size();

        let ty = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "a".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "b".to_string(),
                    encoded_type: EncodedType::EInt32 { required: false },
                    index: 1,
                },
            ],
        };

        let value = EncodedValue::Struct(vec![
            ("a".to_string(), EncodedValue::Int32(10)),
            ("b".to_string(), EncodedValue::Int32(20)),
        ]);

        Encoder::write_value(&mut buffer, &ty, &value).unwrap();

        let data = buffer.into_data();
        // Bitmap (1 byte, 0 = both present) + a (10) + b (20)
        let mut expected = Vec::new();
        expected.push(0x00); // bitmap: field b is present (bit 0 = 0)
        expected.extend_from_slice(&10i32.to_le_bytes());
        expected.extend_from_slice(&20i32.to_le_bytes());
        assert_eq!(data, expected);
    }
}
