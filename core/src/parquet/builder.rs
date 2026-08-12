//! Column builders driven by a retained Arrow type plan.

use crate::codec::EncodedValue;
use crate::error::{HailError, Result};
use crate::schema::{ArrowNode, ArrowTypePlan};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, StructArray,
};
use arrow::datatypes::{Field, Fields};
use std::sync::Arc;

pub enum ColumnBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    String(StringBuilder),
    Binary(BinaryBuilder),
    List {
        element_plan: Box<ArrowTypePlan>,
        values: Vec<EncodedValue>,
        offsets: Vec<i32>,
        nulls: Vec<bool>,
    },
    Struct {
        field_plans: Vec<ArrowTypePlan>,
        builders: Vec<ColumnBuilder>,
        fields: Fields,
        nulls: Vec<bool>,
    },
}

impl ColumnBuilder {
    pub fn new(plan: &ArrowTypePlan) -> Result<Self> {
        Ok(match &plan.node {
            ArrowNode::Int32 => Self::Int32(Int32Builder::new()),
            ArrowNode::Int64 => Self::Int64(Int64Builder::new()),
            ArrowNode::Float32 => Self::Float32(Float32Builder::new()),
            ArrowNode::Float64 => Self::Float64(Float64Builder::new()),
            ArrowNode::Boolean => Self::Boolean(BooleanBuilder::new()),
            ArrowNode::Utf8 => Self::String(StringBuilder::new()),
            ArrowNode::Binary => Self::Binary(BinaryBuilder::new()),
            ArrowNode::List { element } => Self::List {
                element_plan: element.clone(),
                values: Vec::new(),
                offsets: vec![0],
                nulls: Vec::new(),
            },
            ArrowNode::Struct { fields } => Self::Struct {
                field_plans: fields.iter().map(|field| field.plan.clone()).collect(),
                builders: fields
                    .iter()
                    .map(|field| Self::new(&field.plan))
                    .collect::<Result<Vec<_>>>()?,
                fields: fields
                    .iter()
                    .map(|field| field.arrow_field())
                    .collect::<Vec<_>>()
                    .into(),
                nulls: Vec::new(),
            },
        })
    }

    pub fn append(&mut self, value: &EncodedValue) -> Result<()> {
        match (self, value) {
            (Self::Int32(builder), EncodedValue::Int32(value)) => builder.append_value(*value),
            (Self::Int32(builder), EncodedValue::Null) => builder.append_null(),
            (Self::Int64(builder), EncodedValue::Int64(value)) => builder.append_value(*value),
            (Self::Int64(builder), EncodedValue::Null) => builder.append_null(),
            (Self::Float32(builder), EncodedValue::Float32(value)) => builder.append_value(*value),
            (Self::Float32(builder), EncodedValue::Null) => builder.append_null(),
            (Self::Float64(builder), EncodedValue::Float64(value)) => builder.append_value(*value),
            (Self::Float64(builder), EncodedValue::Null) => builder.append_null(),
            (Self::Boolean(builder), EncodedValue::Boolean(value)) => builder.append_value(*value),
            (Self::Boolean(builder), EncodedValue::Null) => builder.append_null(),
            (Self::String(builder), EncodedValue::Binary(value)) => {
                let value = std::str::from_utf8(value).map_err(|error| {
                    HailError::ParseError(format!("invalid UTF-8 for logical String: {error}"))
                })?;
                builder.append_value(value);
            }
            (Self::String(builder), EncodedValue::Null) => builder.append_null(),
            (Self::Binary(builder), EncodedValue::Binary(value)) => builder.append_value(value),
            (Self::Binary(builder), EncodedValue::Null) => builder.append_null(),
            (
                Self::List {
                    values,
                    offsets,
                    nulls,
                    ..
                },
                EncodedValue::Array(elements),
            ) => {
                values.extend(elements.iter().cloned());
                offsets.push(values.len() as i32);
                nulls.push(true);
            }
            (Self::List { offsets, nulls, .. }, EncodedValue::Null) => {
                offsets.push(*offsets.last().expect("list always has initial offset"));
                nulls.push(false);
            }
            (
                Self::Struct {
                    builders, nulls, ..
                },
                EncodedValue::Struct(field_values),
            ) => {
                for (index, builder) in builders.iter_mut().enumerate() {
                    if let Some((_, value)) = field_values.get(index) {
                        builder.append(value)?;
                    } else {
                        builder.append_null()?;
                    }
                }
                nulls.push(true);
            }
            (
                Self::Struct {
                    builders, nulls, ..
                },
                EncodedValue::Null,
            ) => {
                for builder in builders {
                    builder.append_null()?;
                }
                nulls.push(false);
            }
            (builder, value) => {
                return Err(HailError::TypeMismatch {
                    expected: builder.type_name().to_string(),
                    actual: value.type_name().to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<()> {
        match self {
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Boolean(builder) => builder.append_null(),
            Self::String(builder) => builder.append_null(),
            Self::Binary(builder) => builder.append_null(),
            Self::List { offsets, nulls, .. } => {
                offsets.push(*offsets.last().expect("list always has initial offset"));
                nulls.push(false);
            }
            Self::Struct {
                builders, nulls, ..
            } => {
                for builder in builders {
                    builder.append_null()?;
                }
                nulls.push(false);
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::List {
                element_plan,
                values,
                offsets,
                nulls,
            } => {
                let mut inner = Self::new(element_plan).expect("validated element plan");
                for value in values.drain(..) {
                    inner.append(&value).expect("decoded value matched plan");
                }
                let values = inner.finish();
                let offsets = arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(
                    std::mem::take(offsets),
                ));
                let nulls = null_buffer(std::mem::take(nulls));
                Arc::new(
                    arrow::array::ListArray::try_new(
                        Arc::new(Field::new(
                            "item",
                            element_plan.data_type.clone(),
                            element_plan.nullable,
                        )),
                        offsets,
                        values,
                        nulls,
                    )
                    .expect("validated list plan"),
                )
            }
            Self::Struct {
                builders,
                fields,
                nulls,
                ..
            } => {
                let arrays = builders.iter_mut().map(Self::finish).collect();
                Arc::new(
                    StructArray::try_new(
                        fields.clone(),
                        arrays,
                        null_buffer(std::mem::take(nulls)),
                    )
                    .expect("validated struct plan"),
                )
            }
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::Float32(_) => "Float32",
            Self::Float64(_) => "Float64",
            Self::Boolean(_) => "Boolean",
            Self::String(_) => "String",
            Self::Binary(_) => "Binary",
            Self::List { .. } => "List",
            Self::Struct { .. } => "Struct",
        }
    }
}

// Kept separate to make the null semantics easy to audit.
fn null_buffer(valid: Vec<bool>) -> Option<arrow::buffer::NullBuffer> {
    if valid.iter().all(|value| *value) {
        None
    } else {
        Some(arrow::buffer::NullBuffer::from(valid))
    }
}

impl EncodedValue {
    fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "Null",
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::Float32(_) => "Float32",
            Self::Float64(_) => "Float64",
            Self::Boolean(_) => "Boolean",
            Self::Binary(_) => "Binary",
            Self::Array(_) => "Array",
            Self::Struct(_) => "Struct",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ETypeParser;
    use crate::schema::PairedSchema;
    use arrow::array::{Array, BinaryArray, StringArray};

    fn field_plan(vtype: &str) -> ArrowTypePlan {
        let paired = PairedSchema::from_raw_vtype(
            ETypeParser::parse("+EBaseStruct{x:EBinary}").unwrap(),
            &format!("Struct{{x:{vtype}}}"),
        )
        .unwrap();
        paired.arrow_plan().unwrap().fields()[0].plan.clone()
    }

    #[test]
    fn binary_preserves_arbitrary_bytes_and_missingness() {
        let mut builder = ColumnBuilder::new(&field_plan("Binary")).unwrap();
        builder.append(&EncodedValue::Binary(vec![])).unwrap();
        builder
            .append(&EncodedValue::Binary(vec![0, 0xff, 0x80]))
            .unwrap();
        builder.append(&EncodedValue::Null).unwrap();
        let array = builder.finish();
        let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(array.value(0), b"");
        assert_eq!(array.value(1), &[0, 0xff, 0x80]);
        assert!(array.is_null(2));
    }

    #[test]
    fn string_rejects_invalid_utf8() {
        let mut builder = ColumnBuilder::new(&field_plan("String")).unwrap();
        let error = builder
            .append(&EncodedValue::Binary(vec![0xff, 0x80]))
            .unwrap_err();
        assert!(error.to_string().contains("UTF-8"));
        builder
            .append(&EncodedValue::Binary(b"valid".to_vec()))
            .unwrap();
        let array = builder.finish();
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "valid"
        );
    }
}
