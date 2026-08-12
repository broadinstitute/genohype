//! Lossless, typed serialization and canonical digests for decoded Hail values.

use super::EncodedValue;
use crate::{HailError, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CANONICAL_ENCODING: &str = "genohype-encoded-value-v1";
const DIGEST_DOMAIN: &[u8] = b"genohype.encoded-value.sha256.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LosslessValue {
    Null,
    Boolean { value: bool },
    Int32 { value: i32 },
    Int64 { value: i64 },
    Float32 { ieee754_hex: String },
    Float64 { ieee754_hex: String },
    Binary { base64: String, byte_length: u64 },
    Array { values: Vec<LosslessValue> },
    Struct { fields: Vec<LosslessField> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LosslessField {
    pub name: String,
    pub value: LosslessValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueDigest {
    pub canonical_encoding: String,
    pub canonical_bytes: u64,
    pub sha256: String,
}

impl From<&EncodedValue> for LosslessValue {
    fn from(value: &EncodedValue) -> Self {
        match value {
            EncodedValue::Null => Self::Null,
            EncodedValue::Boolean(value) => Self::Boolean { value: *value },
            EncodedValue::Int32(value) => Self::Int32 { value: *value },
            EncodedValue::Int64(value) => Self::Int64 { value: *value },
            EncodedValue::Float32(value) => Self::Float32 {
                ieee754_hex: format!("{:08x}", value.to_bits()),
            },
            EncodedValue::Float64(value) => Self::Float64 {
                ieee754_hex: format!("{:016x}", value.to_bits()),
            },
            EncodedValue::Binary(value) => Self::Binary {
                base64: STANDARD.encode(value),
                byte_length: value.len() as u64,
            },
            EncodedValue::Array(values) => Self::Array {
                values: values.iter().map(Self::from).collect(),
            },
            EncodedValue::Struct(fields) => Self::Struct {
                fields: fields
                    .iter()
                    .map(|(name, value)| LosslessField {
                        name: name.clone(),
                        value: Self::from(value),
                    })
                    .collect(),
            },
        }
    }
}

impl TryFrom<&LosslessValue> for EncodedValue {
    type Error = HailError;

    fn try_from(value: &LosslessValue) -> Result<Self> {
        Ok(match value {
            LosslessValue::Null => Self::Null,
            LosslessValue::Boolean { value } => Self::Boolean(*value),
            LosslessValue::Int32 { value } => Self::Int32(*value),
            LosslessValue::Int64 { value } => Self::Int64(*value),
            LosslessValue::Float32 { ieee754_hex } => {
                Self::Float32(f32::from_bits(parse_hex(ieee754_hex, 8, "float32")? as u32))
            }
            LosslessValue::Float64 { ieee754_hex } => {
                Self::Float64(f64::from_bits(parse_hex(ieee754_hex, 16, "float64")?))
            }
            LosslessValue::Binary {
                base64,
                byte_length,
            } => {
                let bytes = STANDARD.decode(base64).map_err(|error| {
                    HailError::InvalidFormat(format!("invalid lossless binary base64: {error}"))
                })?;
                if bytes.len() as u64 != *byte_length {
                    return Err(HailError::InvalidFormat(format!(
                        "lossless binary byte length mismatch: declared {byte_length}, decoded {}",
                        bytes.len()
                    )));
                }
                Self::Binary(bytes)
            }
            LosslessValue::Array { values } => Self::Array(
                values
                    .iter()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ),
            LosslessValue::Struct { fields } => Self::Struct(
                fields
                    .iter()
                    .map(|field| Ok((field.name.clone(), Self::try_from(&field.value)?)))
                    .collect::<Result<Vec<_>>>()?,
            ),
        })
    }
}

fn parse_hex(value: &str, width: usize, kind: &str) -> Result<u64> {
    if value.len() != width
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HailError::InvalidFormat(format!(
            "{kind} IEEE-754 value must be {width} lowercase hexadecimal characters"
        )));
    }
    u64::from_str_radix(value, 16)
        .map_err(|error| HailError::InvalidFormat(format!("invalid {kind} bits: {error}")))
}

/// Injective canonical bytes: type tags, fixed-width big-endian numbers,
/// explicit lengths, original field order, and exact binary bytes.
pub fn canonical_bytes(value: &EncodedValue) -> Vec<u8> {
    let mut output = Vec::new();
    append_canonical(value, &mut output);
    output
}

fn append_canonical(value: &EncodedValue, output: &mut Vec<u8>) {
    match value {
        EncodedValue::Null => output.push(0),
        EncodedValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        EncodedValue::Int32(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_be_bytes());
        }
        EncodedValue::Int64(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        EncodedValue::Float32(value) => {
            output.push(4);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        EncodedValue::Float64(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        EncodedValue::Binary(value) => {
            output.push(6);
            put_len(value.len(), output);
            output.extend_from_slice(value);
        }
        EncodedValue::Array(values) => {
            output.push(7);
            put_len(values.len(), output);
            for value in values {
                append_canonical(value, output);
            }
        }
        EncodedValue::Struct(fields) => {
            output.push(8);
            put_len(fields.len(), output);
            for (name, value) in fields {
                put_len(name.len(), output);
                output.extend_from_slice(name.as_bytes());
                append_canonical(value, output);
            }
        }
    }
}

fn put_len(length: usize, output: &mut Vec<u8>) {
    output.extend_from_slice(&(length as u64).to_be_bytes());
}

pub fn canonical_digest(value: &EncodedValue) -> ValueDigest {
    let canonical = canonical_bytes(value);
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update((canonical.len() as u64).to_be_bytes());
    hasher.update(&canonical);
    ValueDigest {
        canonical_encoding: CANONICAL_ENCODING.to_string(),
        canonical_bytes: canonical.len() as u64,
        sha256: format!("{:x}", hasher.finalize()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_values_round_trip_without_losing_bits_or_order() {
        let values = [
            EncodedValue::Null,
            EncodedValue::Binary(vec![]),
            EncodedValue::Binary(vec![0, 0xff, 0x80]),
            EncodedValue::Int64(i64::MIN),
            EncodedValue::Int64(i64::MAX),
            EncodedValue::Float32(f32::from_bits(0x7fc0_1234)),
            EncodedValue::Float64(f64::INFINITY),
            EncodedValue::Float64(-0.0),
            EncodedValue::Struct(vec![
                ("z".to_string(), EncodedValue::Int32(1)),
                (
                    "a".to_string(),
                    EncodedValue::Array(vec![EncodedValue::Null]),
                ),
            ]),
        ];
        for value in values {
            let encoded = LosslessValue::from(&value);
            let decoded = EncodedValue::try_from(&encoded).unwrap();
            match (&value, &decoded) {
                (EncodedValue::Float32(a), EncodedValue::Float32(b)) => {
                    assert_eq!(a.to_bits(), b.to_bits())
                }
                (EncodedValue::Float64(a), EncodedValue::Float64(b)) => {
                    assert_eq!(a.to_bits(), b.to_bits())
                }
                _ => assert_eq!(value, decoded),
            }
        }
    }

    #[test]
    fn canonical_digest_is_stable_and_sensitive_to_type_order_and_bytes() {
        let value = EncodedValue::Struct(vec![
            ("a".into(), EncodedValue::Binary(vec![0xff])),
            ("b".into(), EncodedValue::Int32(1)),
        ]);
        assert_eq!(canonical_digest(&value), canonical_digest(&value));
        let reversed = EncodedValue::Struct(vec![
            ("b".into(), EncodedValue::Int32(1)),
            ("a".into(), EncodedValue::Binary(vec![0xff])),
        ]);
        assert_ne!(canonical_digest(&value), canonical_digest(&reversed));
        assert_ne!(
            canonical_digest(&EncodedValue::Int32(1)),
            canonical_digest(&EncodedValue::Int64(1))
        );
    }

    #[test]
    fn malformed_lossless_values_fail_closed() {
        assert!(EncodedValue::try_from(&LosslessValue::Binary {
            base64: "AA==".into(),
            byte_length: 2,
        })
        .is_err());
        assert!(EncodedValue::try_from(&LosslessValue::Float32 {
            ieee754_hex: "0".into(),
        })
        .is_err());
    }
}
