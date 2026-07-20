//! Column builders for converting Hail row data to Arrow columnar format
//!
//! This module provides builders that accumulate row-oriented Hail data
//! and produce Arrow arrays. Each builder corresponds to a Hail type
//! and wraps the appropriate Arrow array builder.

use crate::codec::{EncodedType, EncodedValue};
use crate::error::{HailError, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, StructArray,
};
use arrow::datatypes::{Field, Fields};
use std::sync::Arc;

/// A builder that can append Hail EncodedValues and produce Arrow Arrays
pub enum ColumnBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    String(StringBuilder),
    /// For arrays/lists, we accumulate the elements and offsets separately
    List {
        inner_type: EncodedType,
        /// Accumulated elements from all lists
        values: Vec<EncodedValue>,
        /// Offsets marking where each list starts
        offsets: Vec<i32>,
        /// Validity bitmap for null lists
        nulls: Vec<bool>,
    },
    Struct {
        field_types: Vec<EncodedType>,
        builders: Vec<ColumnBuilder>,
        fields: Fields,
        /// Validity bitmap for null structs
        nulls: Vec<bool>,
    },
}

impl ColumnBuilder {
    /// Create a builder for the given Hail type
    pub fn new(etype: &EncodedType) -> Result<Self> {
        match etype {
            EncodedType::EInt32 { .. } => Ok(ColumnBuilder::Int32(Int32Builder::new())),
            EncodedType::EInt64 { .. } => Ok(ColumnBuilder::Int64(Int64Builder::new())),
            EncodedType::EFloat32 { .. } => Ok(ColumnBuilder::Float32(Float32Builder::new())),
            EncodedType::EFloat64 { .. } => Ok(ColumnBuilder::Float64(Float64Builder::new())),
            EncodedType::EBoolean { .. } => Ok(ColumnBuilder::Boolean(BooleanBuilder::new())),
            EncodedType::EBinary { .. } => Ok(ColumnBuilder::String(StringBuilder::new())),
            EncodedType::EArray { element, .. } => Ok(ColumnBuilder::List {
                inner_type: (**element).clone(),
                values: Vec::new(),
                offsets: vec![0], // First offset is always 0
                nulls: Vec::new(),
            }),
            EncodedType::EBaseStruct { fields, .. } => {
                let mut builders = Vec::with_capacity(fields.len());
                let mut field_types = Vec::with_capacity(fields.len());
                let mut arrow_fields = Vec::with_capacity(fields.len());

                for f in fields {
                    builders.push(ColumnBuilder::new(&f.encoded_type)?);
                    field_types.push(f.encoded_type.clone());
                    arrow_fields.push(Field::new(
                        &f.name,
                        crate::parquet::schema::hail_type_to_arrow(&f.encoded_type)?,
                        !f.encoded_type.is_required(),
                    ));
                }

                Ok(ColumnBuilder::Struct {
                    field_types,
                    builders,
                    fields: arrow_fields.into(),
                    nulls: Vec::new(),
                })
            }
        }
    }

    /// Append a single value
    pub fn append(&mut self, value: &EncodedValue) -> Result<()> {
        match (self, value) {
            // Int32
            (ColumnBuilder::Int32(b), EncodedValue::Int32(v)) => {
                b.append_value(*v);
            }
            (ColumnBuilder::Int32(b), EncodedValue::Null) => {
                b.append_null();
            }

            // Int64
            (ColumnBuilder::Int64(b), EncodedValue::Int64(v)) => {
                b.append_value(*v);
            }
            (ColumnBuilder::Int64(b), EncodedValue::Null) => {
                b.append_null();
            }

            // Float32
            (ColumnBuilder::Float32(b), EncodedValue::Float32(v)) => {
                b.append_value(*v);
            }
            (ColumnBuilder::Float32(b), EncodedValue::Null) => {
                b.append_null();
            }

            // Float64
            (ColumnBuilder::Float64(b), EncodedValue::Float64(v)) => {
                b.append_value(*v);
            }
            (ColumnBuilder::Float64(b), EncodedValue::Null) => {
                b.append_null();
            }

            // Boolean
            (ColumnBuilder::Boolean(b), EncodedValue::Boolean(v)) => {
                b.append_value(*v);
            }
            (ColumnBuilder::Boolean(b), EncodedValue::Null) => {
                b.append_null();
            }

            // String (from Binary)
            (ColumnBuilder::String(b), EncodedValue::Binary(v)) => {
                let s = std::str::from_utf8(v)
                    .map_err(|e| HailError::ParseError(format!("Invalid UTF-8: {}", e)))?;
                b.append_value(s);
            }
            (ColumnBuilder::String(b), EncodedValue::Null) => {
                b.append_null();
            }

            // List/Array
            (
                ColumnBuilder::List {
                    values,
                    offsets,
                    nulls,
                    ..
                },
                EncodedValue::Array(elements),
            ) => {
                // Add all elements to the values buffer
                for elem in elements {
                    values.push(elem.clone());
                }
                // Record the new offset (cumulative count of elements)
                offsets.push(values.len() as i32);
                nulls.push(true); // List is present
            }
            (ColumnBuilder::List { offsets, nulls, .. }, EncodedValue::Null) => {
                // Null list - offset stays the same, mark as null
                let last_offset = *offsets.last().unwrap();
                offsets.push(last_offset);
                nulls.push(false);
            }

            // Struct
            (
                ColumnBuilder::Struct {
                    builders, nulls, ..
                },
                EncodedValue::Struct(field_values),
            ) => {
                // Append each field value to its corresponding builder
                // The order of fields in EncodedValue::Struct should match the schema
                for (i, builder) in builders.iter_mut().enumerate() {
                    if i < field_values.len() {
                        builder.append(&field_values[i].1)?;
                    } else {
                        // Field missing - append null
                        builder.append_null()?;
                    }
                }
                nulls.push(true); // Struct is present
            }
            (
                ColumnBuilder::Struct {
                    builders, nulls, ..
                },
                EncodedValue::Null,
            ) => {
                // Append nulls to all child builders
                for builder in builders.iter_mut() {
                    builder.append_null()?;
                }
                nulls.push(false); // Struct is null
            }

            // Type mismatch
            (builder, value) => {
                return Err(HailError::TypeMismatch {
                    expected: builder.type_name().to_string(),
                    actual: value.type_name().to_string(),
                });
            }
        }
        Ok(())
    }

    /// Append a null value (recursively for complex types)
    pub fn append_null(&mut self) -> Result<()> {
        match self {
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::String(b) => b.append_null(),
            ColumnBuilder::List { offsets, nulls, .. } => {
                let last_offset = *offsets.last().unwrap();
                offsets.push(last_offset);
                nulls.push(false);
            }
            ColumnBuilder::Struct {
                builders, nulls, ..
            } => {
                for builder in builders.iter_mut() {
                    builder.append_null()?;
                }
                nulls.push(false);
            }
        }
        Ok(())
    }

    /// Finish building and return the Arrow Array
    pub fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()),
            ColumnBuilder::String(b) => Arc::new(b.finish()),
            ColumnBuilder::List {
                inner_type,
                values,
                offsets,
                nulls,
            } => {
                // Build the inner array from accumulated values
                let mut inner_builder =
                    ColumnBuilder::new(inner_type).expect("Inner builder creation should not fail");
                for v in values.drain(..) {
                    inner_builder
                        .append(&v)
                        .expect("Inner append should not fail");
                }
                let inner_array = inner_builder.finish();

                // Create offsets buffer
                let offsets_buffer = arrow::buffer::OffsetBuffer::new(
                    arrow::buffer::ScalarBuffer::from(std::mem::take(offsets)),
                );

                // Create nulls buffer if there are any nulls
                let null_buffer = if nulls.iter().all(|&v| v) {
                    None
                } else {
                    Some(arrow::buffer::NullBuffer::from(std::mem::take(nulls)))
                };

                // Determine the inner field
                let inner_field = Field::new(
                    "item",
                    inner_array.data_type().clone(),
                    !inner_type.is_required(),
                );

                Arc::new(
                    arrow::array::ListArray::try_new(
                        Arc::new(inner_field),
                        offsets_buffer,
                        inner_array,
                        null_buffer,
                    )
                    .expect("ListArray creation should not fail"),
                )
            }
            ColumnBuilder::Struct {
                builders,
                fields,
                nulls,
                ..
            } => {
                let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

                // Create nulls buffer if there are any nulls
                let null_buffer = if nulls.iter().all(|&v| v) {
                    None
                } else {
                    Some(arrow::buffer::NullBuffer::from(std::mem::take(nulls)))
                };

                Arc::new(
                    StructArray::try_new(fields.clone(), arrays, null_buffer)
                        .expect("StructArray creation should not fail"),
                )
            }
        }
    }

    /// Get a string name for this builder's type (for error messages)
    fn type_name(&self) -> &'static str {
        match self {
            ColumnBuilder::Int32(_) => "Int32",
            ColumnBuilder::Int64(_) => "Int64",
            ColumnBuilder::Float32(_) => "Float32",
            ColumnBuilder::Float64(_) => "Float64",
            ColumnBuilder::Boolean(_) => "Boolean",
            ColumnBuilder::String(_) => "String",
            ColumnBuilder::List { .. } => "List",
            ColumnBuilder::Struct { .. } => "Struct",
        }
    }
}

impl EncodedValue {
    /// Get a string name for this value's type (for error messages)
    fn type_name(&self) -> &'static str {
        match self {
            EncodedValue::Null => "Null",
            EncodedValue::Int32(_) => "Int32",
            EncodedValue::Int64(_) => "Int64",
            EncodedValue::Float32(_) => "Float32",
            EncodedValue::Float64(_) => "Float64",
            EncodedValue::Boolean(_) => "Boolean",
            EncodedValue::Binary(_) => "Binary",
            EncodedValue::Array(_) => "Array",
            EncodedValue::Struct(_) => "Struct",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, ListArray, StringArray};

    #[test]
    fn test_int32_builder() {
        let mut builder = ColumnBuilder::new(&EncodedType::EInt32 { required: false }).unwrap();
        builder.append(&EncodedValue::Int32(42)).unwrap();
        builder.append(&EncodedValue::Null).unwrap();
        builder.append(&EncodedValue::Int32(100)).unwrap();

        let array = builder.finish();
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.len(), 3);
        assert_eq!(int_array.value(0), 42);
        assert!(int_array.is_null(1));
        assert_eq!(int_array.value(2), 100);
    }

    #[test]
    fn test_string_builder() {
        let mut builder = ColumnBuilder::new(&EncodedType::EBinary { required: false }).unwrap();
        builder
            .append(&EncodedValue::Binary(b"hello".to_vec()))
            .unwrap();
        builder.append(&EncodedValue::Null).unwrap();
        builder
            .append(&EncodedValue::Binary(b"world".to_vec()))
            .unwrap();

        let array = builder.finish();
        let str_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_array.len(), 3);
        assert_eq!(str_array.value(0), "hello");
        assert!(str_array.is_null(1));
        assert_eq!(str_array.value(2), "world");
    }

    #[test]
    fn test_list_builder() {
        let mut builder = ColumnBuilder::new(&EncodedType::EArray {
            required: false,
            element: Box::new(EncodedType::EInt32 { required: false }),
        })
        .unwrap();

        builder
            .append(&EncodedValue::Array(vec![
                EncodedValue::Int32(1),
                EncodedValue::Int32(2),
                EncodedValue::Int32(3),
            ]))
            .unwrap();
        builder.append(&EncodedValue::Null).unwrap();
        builder
            .append(&EncodedValue::Array(vec![EncodedValue::Int32(4)]))
            .unwrap();

        let array = builder.finish();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 3);

        // First list: [1, 2, 3]
        assert!(!list_array.is_null(0));
        let first_list = list_array.value(0);
        let first_ints = first_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first_ints.len(), 3);
        assert_eq!(first_ints.value(0), 1);
        assert_eq!(first_ints.value(1), 2);
        assert_eq!(first_ints.value(2), 3);

        // Second list: null
        assert!(list_array.is_null(1));

        // Third list: [4]
        assert!(!list_array.is_null(2));
        let third_list = list_array.value(2);
        let third_ints = third_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(third_ints.len(), 1);
        assert_eq!(third_ints.value(0), 4);
    }
}
