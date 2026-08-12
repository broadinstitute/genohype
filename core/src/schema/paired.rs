//! Recursive, fail-closed pairing of Hail physical ETypes and logical VTypes.

use super::{LogicalType, VTypeParser};
use crate::codec::{EncodedField, EncodedType};
use crate::{HailError, Result};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalBinaryEncoding {
    Utf8,
    Binary,
}

/// One recursively paired Arrow node. The `node` retains child plans so builders
/// never need to reverse-engineer an EType from Arrow after a flush.
#[derive(Debug, Clone)]
pub struct ArrowTypePlan {
    pub data_type: DataType,
    pub nullable: bool,
    pub node: ArrowNode,
}

#[derive(Debug, Clone)]
pub enum ArrowNode {
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    Binary,
    List { element: Box<ArrowTypePlan> },
    Struct { fields: Vec<ArrowFieldPlan> },
}

#[derive(Debug, Clone)]
pub struct ArrowFieldPlan {
    pub name: String,
    pub plan: ArrowTypePlan,
    pub metadata: HashMap<String, String>,
}

impl ArrowFieldPlan {
    pub fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.plan.data_type.clone(), self.plan.nullable)
            .with_metadata(self.metadata.clone())
    }
}

/// Complete root plan for one record schema.
#[derive(Debug, Clone)]
pub struct ArrowSchemaPlan {
    root: ArrowTypePlan,
    schema: Arc<Schema>,
}

impl ArrowSchemaPlan {
    fn from_root(root: ArrowTypePlan) -> Result<Self> {
        let ArrowNode::Struct { fields } = &root.node else {
            return Err(HailError::InvalidFormat(
                "root schema must be a struct".to_string(),
            ));
        };
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(ArrowFieldPlan::arrow_field)
                .collect::<Vec<_>>(),
        ));
        Ok(Self { root, schema })
    }

    /// Explicit compatibility plan for sources which have no Hail VType. The
    /// caller, not generic Parquet code, chooses what every EBinary means.
    pub fn physical_only(
        physical: &EncodedType,
        binary_encoding: PhysicalBinaryEncoding,
    ) -> Result<Self> {
        Self::from_root(pair_physical_only(physical, binary_encoding))
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn root(&self) -> &ArrowTypePlan {
        &self.root
    }

    pub fn fields(&self) -> &[ArrowFieldPlan] {
        match &self.root.node {
            ArrowNode::Struct { fields } => fields,
            _ => unreachable!("root checked by constructor"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairedSchema {
    pub physical: EncodedType,
    pub logical: LogicalType,
}

impl PairedSchema {
    pub fn new(physical: EncodedType, logical: LogicalType) -> Result<Self> {
        // Pair once during construction so an invalid schema can never be held.
        pair_at(&physical, &logical, "$")?;
        Ok(Self { physical, logical })
    }

    pub fn from_raw_vtype(physical: EncodedType, raw_vtype: &str) -> Result<Self> {
        Self::new(physical, VTypeParser::parse(raw_vtype)?)
    }

    pub fn arrow_plan(&self) -> Result<ArrowSchemaPlan> {
        ArrowSchemaPlan::from_root(pair_at(&self.physical, &self.logical, "$")?)
    }

    /// Retain the logical meaning while applying a physical field projection.
    /// The target may only remove struct fields recursively; it cannot change
    /// types or invent fields.
    pub fn project_to(&self, target: EncodedType) -> Result<Self> {
        let logical = project_logical(&self.physical, &self.logical, &target, "$")?;
        Self::new(target, logical)
    }
}

fn project_logical(
    source: &EncodedType,
    logical: &LogicalType,
    target: &EncodedType,
    path: &str,
) -> Result<LogicalType> {
    if source == target {
        return Ok(logical.clone());
    }
    match (source, logical, target) {
        (
            EncodedType::EBaseStruct { fields: source_fields, .. },
            LogicalType::Struct { fields: logical_fields },
            EncodedType::EBaseStruct { fields: target_fields, .. },
        ) => {
            let mut projected = Vec::with_capacity(target_fields.len());
            for target_field in target_fields {
                let index = source_fields
                    .iter()
                    .position(|field| field.name == target_field.name)
                    .ok_or_else(|| HailError::InvalidFormat(format!(
                        "projection invents field {path}.{}",
                        target_field.name
                    )))?;
                projected.push(super::LogicalField {
                    name: target_field.name.clone(),
                    logical_type: project_logical(
                        &source_fields[index].encoded_type,
                        &logical_fields[index].logical_type,
                        &target_field.encoded_type,
                        &format!("{path}.{}", target_field.name),
                    )?,
                });
            }
            Ok(LogicalType::Struct { fields: projected })
        }
        (
            EncodedType::EArray { element: source_element, .. },
            LogicalType::Array { element: logical_element },
            EncodedType::EArray { element: target_element, .. },
        ) => Ok(LogicalType::Array {
            element: Box::new(project_logical(
                source_element,
                logical_element,
                target_element,
                &format!("{path}[]"),
            )?),
        }),
        (
            EncodedType::EArray { element: source_element, .. },
            LogicalType::Set { element: logical_element },
            EncodedType::EArray { element: target_element, .. },
        ) => Ok(LogicalType::Set {
            element: Box::new(project_logical(
                source_element,
                logical_element,
                target_element,
                &format!("{path}[]"),
            )?),
        }),
        _ => Err(HailError::InvalidFormat(format!(
            "projection changes physical representation at {path}: source {source:?}, target {target:?}"
        ))),
    }
}

fn mismatch<T>(path: &str, physical: &EncodedType, logical: &LogicalType) -> Result<T> {
    Err(HailError::TypeMismatch {
        expected: format!(
            "{} physical representation at {path}",
            logical.display_name()
        ),
        actual: format!("{physical:?}"),
    })
}

fn pair_at(physical: &EncodedType, logical: &LogicalType, path: &str) -> Result<ArrowTypePlan> {
    let nullable = !physical.is_required();
    let node = match (physical, logical) {
        (EncodedType::EInt32 { .. }, LogicalType::Int32 | LogicalType::Call) => ArrowNode::Int32,
        (EncodedType::EInt64 { .. }, LogicalType::Int64) => ArrowNode::Int64,
        (EncodedType::EFloat32 { .. }, LogicalType::Float32) => ArrowNode::Float32,
        (EncodedType::EFloat64 { .. }, LogicalType::Float64) => ArrowNode::Float64,
        (EncodedType::EBoolean { .. }, LogicalType::Boolean) => ArrowNode::Boolean,
        (EncodedType::EBinary { .. }, LogicalType::String) => ArrowNode::Utf8,
        (EncodedType::EBinary { .. }, LogicalType::Binary) => ArrowNode::Binary,
        (EncodedType::EArray { element, .. }, LogicalType::Array { element: logical })
        | (EncodedType::EArray { element, .. }, LogicalType::Set { element: logical }) => {
            ArrowNode::List {
                element: Box::new(pair_at(element, logical, &format!("{path}[]"))?),
            }
        }
        (
            EncodedType::EArray { element, .. },
            LogicalType::Dict {
                key: logical_key,
                value: logical_value,
            },
        ) => {
            let EncodedType::EBaseStruct { fields, .. } = element.as_ref() else {
                return mismatch(path, physical, logical);
            };
            require_field_names(fields, &["key", "value"], &format!("{path}[]"))?;
            let key = pair_at(
                &fields[0].encoded_type,
                logical_key,
                &format!("{path}[].key"),
            )?;
            let value = pair_at(
                &fields[1].encoded_type,
                logical_value,
                &format!("{path}[].value"),
            )?;
            ArrowNode::List {
                element: Box::new(struct_plan(
                    !element.is_required(),
                    vec![
                        arrow_field("key", key, logical_key),
                        arrow_field("value", value, logical_value),
                    ],
                )),
            }
        }
        (
            EncodedType::EBaseStruct { fields, .. },
            LogicalType::Struct {
                fields: logical_fields,
            },
        ) => {
            if fields.len() != logical_fields.len() {
                return Err(HailError::InvalidFormat(format!(
                    "schema field count mismatch at {path}: physical {}, logical {}",
                    fields.len(),
                    logical_fields.len()
                )));
            }
            let mut plans = Vec::with_capacity(fields.len());
            for (physical_field, logical_field) in fields.iter().zip(logical_fields) {
                if physical_field.name != logical_field.name {
                    return Err(HailError::InvalidFormat(format!(
                        "schema field mismatch at {path}: physical {:?}, logical {:?}",
                        physical_field.name, logical_field.name
                    )));
                }
                let field_path = format!("{path}.{}", physical_field.name);
                plans.push(arrow_field(
                    &physical_field.name,
                    pair_at(
                        &physical_field.encoded_type,
                        &logical_field.logical_type,
                        &field_path,
                    )?,
                    &logical_field.logical_type,
                ));
            }
            ArrowNode::Struct { fields: plans }
        }
        (EncodedType::EBaseStruct { fields, .. }, LogicalType::Tuple { elements }) => {
            if fields.len() != elements.len() {
                return Err(HailError::InvalidFormat(format!(
                    "tuple arity mismatch at {path}: physical {}, logical {}",
                    fields.len(),
                    elements.len()
                )));
            }
            ArrowNode::Struct {
                fields: fields
                    .iter()
                    .zip(elements)
                    .enumerate()
                    .map(|(index, (field, logical))| {
                        Ok(arrow_field(
                            &field.name,
                            pair_at(&field.encoded_type, logical, &format!("{path}[{index}]"))?,
                            logical,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            }
        }
        (EncodedType::EBaseStruct { fields, .. }, LogicalType::Locus { reference_genome }) => {
            require_field_names(fields, &["contig", "position"], path)?;
            let contig = pair_at(
                &fields[0].encoded_type,
                &LogicalType::String,
                &format!("{path}.contig"),
            )?;
            let position = pair_at(
                &fields[1].encoded_type,
                &LogicalType::Int32,
                &format!("{path}.position"),
            )?;
            let mut contig_field = arrow_field("contig", contig, &LogicalType::String);
            contig_field.metadata.insert(
                "hail.reference_genome".to_string(),
                reference_genome.clone(),
            );
            ArrowNode::Struct {
                fields: vec![
                    contig_field,
                    arrow_field("position", position, &LogicalType::Int32),
                ],
            }
        }
        (EncodedType::EBaseStruct { fields, .. }, LogicalType::Interval { point }) => {
            require_field_names(
                fields,
                &["start", "end", "includesStart", "includesEnd"],
                path,
            )?;
            ArrowNode::Struct {
                fields: vec![
                    arrow_field(
                        "start",
                        pair_at(&fields[0].encoded_type, point, &format!("{path}.start"))?,
                        point,
                    ),
                    arrow_field(
                        "end",
                        pair_at(&fields[1].encoded_type, point, &format!("{path}.end"))?,
                        point,
                    ),
                    arrow_field(
                        "includesStart",
                        pair_at(
                            &fields[2].encoded_type,
                            &LogicalType::Boolean,
                            &format!("{path}.includesStart"),
                        )?,
                        &LogicalType::Boolean,
                    ),
                    arrow_field(
                        "includesEnd",
                        pair_at(
                            &fields[3].encoded_type,
                            &LogicalType::Boolean,
                            &format!("{path}.includesEnd"),
                        )?,
                        &LogicalType::Boolean,
                    ),
                ],
            }
        }
        _ => return mismatch(path, physical, logical),
    };
    let data_type = node_data_type(&node);
    Ok(ArrowTypePlan {
        data_type,
        nullable,
        node,
    })
}

fn arrow_field(name: &str, plan: ArrowTypePlan, logical: &LogicalType) -> ArrowFieldPlan {
    let mut metadata = HashMap::new();
    metadata.insert(
        "hail.logical_type".to_string(),
        logical.display_name().to_string(),
    );
    if let LogicalType::Locus { reference_genome } = logical {
        metadata.insert(
            "hail.reference_genome".to_string(),
            reference_genome.clone(),
        );
    }
    ArrowFieldPlan {
        name: name.to_string(),
        plan,
        metadata,
    }
}

fn struct_plan(nullable: bool, fields: Vec<ArrowFieldPlan>) -> ArrowTypePlan {
    let node = ArrowNode::Struct { fields };
    ArrowTypePlan {
        data_type: node_data_type(&node),
        nullable,
        node,
    }
}

fn node_data_type(node: &ArrowNode) -> DataType {
    match node {
        ArrowNode::Int32 => DataType::Int32,
        ArrowNode::Int64 => DataType::Int64,
        ArrowNode::Float32 => DataType::Float32,
        ArrowNode::Float64 => DataType::Float64,
        ArrowNode::Boolean => DataType::Boolean,
        ArrowNode::Utf8 => DataType::Utf8,
        ArrowNode::Binary => DataType::Binary,
        ArrowNode::List { element } => DataType::List(Arc::new(Field::new(
            "item",
            element.data_type.clone(),
            element.nullable,
        ))),
        ArrowNode::Struct { fields } => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(ArrowFieldPlan::arrow_field)
                .collect::<Vec<_>>(),
        )),
    }
}

fn require_field_names(fields: &[EncodedField], names: &[&str], path: &str) -> Result<()> {
    let actual: Vec<_> = fields.iter().map(|field| field.name.as_str()).collect();
    if actual != names {
        return Err(HailError::InvalidFormat(format!(
            "physical struct shape mismatch at {path}: expected {names:?}, got {actual:?}"
        )));
    }
    Ok(())
}

fn pair_physical_only(
    physical: &EncodedType,
    binary_encoding: PhysicalBinaryEncoding,
) -> ArrowTypePlan {
    let nullable = !physical.is_required();
    let node = match physical {
        EncodedType::EInt32 { .. } => ArrowNode::Int32,
        EncodedType::EInt64 { .. } => ArrowNode::Int64,
        EncodedType::EFloat32 { .. } => ArrowNode::Float32,
        EncodedType::EFloat64 { .. } => ArrowNode::Float64,
        EncodedType::EBoolean { .. } => ArrowNode::Boolean,
        EncodedType::EBinary { .. } => match binary_encoding {
            PhysicalBinaryEncoding::Utf8 => ArrowNode::Utf8,
            PhysicalBinaryEncoding::Binary => ArrowNode::Binary,
        },
        EncodedType::EArray { element, .. } => ArrowNode::List {
            element: Box::new(pair_physical_only(element, binary_encoding)),
        },
        EncodedType::EBaseStruct { fields, .. } => ArrowNode::Struct {
            fields: fields
                .iter()
                .map(|field| ArrowFieldPlan {
                    name: field.name.clone(),
                    plan: pair_physical_only(&field.encoded_type, binary_encoding),
                    metadata: HashMap::new(),
                })
                .collect(),
        },
    };
    ArrowTypePlan {
        data_type: node_data_type(&node),
        nullable,
        node,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ETypeParser;

    fn pair(etype: &str, vtype: &str) -> Result<ArrowSchemaPlan> {
        PairedSchema::from_raw_vtype(ETypeParser::parse(etype)?, vtype)?.arrow_plan()
    }

    #[test]
    fn string_and_binary_are_distinct_at_every_nesting_level() {
        let plan = pair(
            "+EBaseStruct{s:+EBinary,b:EBinary,n:+EArray[EBinary],d:+EArray[+EBaseStruct{key:+EBinary,value:EBinary}],t:+EBaseStruct{_0:+EBinary,_1:EBinary}}",
            "Struct{s:String,b:Binary,n:Array[Binary],d:Dict[String,Binary],t:Tuple[String,Binary]}",
        )
        .unwrap();
        assert_eq!(plan.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(plan.schema().field(1).data_type(), &DataType::Binary);
        let DataType::List(item) = plan.schema().field(2).data_type() else {
            panic!("expected list")
        };
        assert_eq!(item.data_type(), &DataType::Binary);
        let DataType::List(entry) = plan.schema().field(3).data_type() else {
            panic!("expected dict list")
        };
        let DataType::Struct(fields) = entry.data_type() else {
            panic!("expected dict entry struct")
        };
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].data_type(), &DataType::Binary);
    }

    #[test]
    fn locus_interval_set_and_call_pair_strictly() {
        pair(
            "+EBaseStruct{l:+EBaseStruct{contig:+EBinary,position:+EInt32},i:EBaseStruct{start:EBaseStruct{contig:+EBinary,position:+EInt32},end:EBaseStruct{contig:+EBinary,position:+EInt32},includesStart:+EBoolean,includesEnd:+EBoolean},s:EArray[+EBinary],c:+EInt32}",
            "Struct{l:Locus(GRCh38),i:Interval[Locus(GRCh38)],s:Set[String],c:Call}",
        )
        .unwrap();
    }

    #[test]
    fn mismatches_fail_with_paths() {
        let error = pair(
            "+EBaseStruct{x:+EArray[+EBinary]}",
            "Struct{x:Array[Int32]}",
        )
        .unwrap_err();
        assert!(error.to_string().contains("$.x[]"), "{error}");

        let error = pair("+EBaseStruct{x:+EBinary}", "Struct{y:String}").unwrap_err();
        assert!(error.to_string().contains("field mismatch at $"), "{error}");
    }
}
