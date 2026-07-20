//! Schema conversion from Hail EncodedType to Arrow Schema
//!
//! This module provides functions to convert Hail's physical type system (EncodedType)
//! to Arrow's type system (DataType and Schema).

use crate::codec::{EncodedField, EncodedType};
use crate::error::{HailError, Result};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Convert a Hail EncodedType to an Arrow DataType
pub fn hail_type_to_arrow(etype: &EncodedType) -> Result<DataType> {
    match etype {
        EncodedType::EInt32 { .. } => Ok(DataType::Int32),
        EncodedType::EInt64 { .. } => Ok(DataType::Int64),
        EncodedType::EFloat32 { .. } => Ok(DataType::Float32),
        EncodedType::EFloat64 { .. } => Ok(DataType::Float64),
        EncodedType::EBoolean { .. } => Ok(DataType::Boolean),
        EncodedType::EBinary { .. } => Ok(DataType::Utf8), // Hail strings are UTF-8
        EncodedType::EArray { element, .. } => {
            let inner_type = hail_type_to_arrow(element)?;
            // Field name "item" is standard for Arrow lists
            let field = Field::new("item", inner_type, !element.is_required());
            Ok(DataType::List(Arc::new(field)))
        }
        EncodedType::EBaseStruct { fields, .. } => {
            let arrow_fields = fields
                .iter()
                .map(|f| hail_field_to_arrow(f))
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(arrow_fields.into()))
        }
    }
}

/// Convert a Hail EncodedField to an Arrow Field
fn hail_field_to_arrow(field: &EncodedField) -> Result<Field> {
    let dtype = hail_type_to_arrow(&field.encoded_type)?;
    Ok(Field::new(
        &field.name,
        dtype,
        !field.encoded_type.is_required(),
    ))
}

/// Create an Arrow Schema from a root EncodedType (must be a Struct)
pub fn create_schema(root_type: &EncodedType) -> Result<Schema> {
    match root_type {
        EncodedType::EBaseStruct { fields, .. } => {
            let arrow_fields = fields
                .iter()
                .map(|f| hail_field_to_arrow(f))
                .collect::<Result<Vec<_>>>()?;
            Ok(Schema::new(arrow_fields))
        }
        _ => Err(HailError::InvalidFormat(
            "Root type must be a struct".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_types() {
        assert_eq!(
            hail_type_to_arrow(&EncodedType::EInt32 { required: true }).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            hail_type_to_arrow(&EncodedType::EInt64 { required: true }).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            hail_type_to_arrow(&EncodedType::EFloat32 { required: true }).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            hail_type_to_arrow(&EncodedType::EFloat64 { required: true }).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            hail_type_to_arrow(&EncodedType::EBoolean { required: true }).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            hail_type_to_arrow(&EncodedType::EBinary { required: true }).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_array_type() {
        let array_type = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EInt32 { required: false }),
        };
        let arrow_type = hail_type_to_arrow(&array_type).unwrap();
        match arrow_type {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Int32);
                assert!(field.is_nullable());
            }
            _ => panic!("Expected List type"),
        }
    }

    #[test]
    fn test_struct_type() {
        let struct_type = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "a".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "b".to_string(),
                    encoded_type: EncodedType::EBinary { required: false },
                    index: 1,
                },
            ],
        };
        let schema = create_schema(&struct_type).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "a");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "b");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());
    }
}
