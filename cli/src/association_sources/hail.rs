use super::mapping::{mapping_pairs, LogicalField};
use genohype_association::{
    normalize_significance, AssociationPointV1, AssociationSource, AssociationSourceError,
    AssociationSourceFieldV1, AssociationSourceItem, AssociationSourceKindV1,
    AssociationSourceMetadataV1, AssociationSourceRejectionV1, NormalizedFieldMappingV1,
    PValueError, ValidationError, WarningCode,
};
use genohype_core::codec::{EncodedType, EncodedValue};
use genohype_core::projection::{FieldPath, PathSegment, ProjectionTree};
use genohype_core::query::QueryEngine;
use genohype_core::schema::{LogicalType, PairedSchema};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const MAX_HAIL_INSPECT_FIELDS: usize = 4_096;
const MAX_HAIL_SCHEMA_DEPTH: usize = 64;
const MAX_ERROR_MESSAGE_BYTES: usize = 512;

/// Bounded, path-free Hail schema information for the generalized inspector.
///
/// `fields` contains exact dotted leaf paths. Arrays and other non-struct
/// containers are reported at their containing path because association mappings
/// intentionally do not support bracket navigation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HailInspectionInput {
    pub fields: Vec<AssociationSourceFieldV1>,
    pub reference_genomes: Vec<String>,
}

/// Read only bounded Hail metadata. This function does not scan a row.
pub fn inspect_hail_schema(source: &str) -> Result<HailInspectionInput, AssociationSourceError> {
    require_hail_name(source)?;
    let engine = QueryEngine::open_path(source).map_err(|error| hail_error("open_hail", error))?;
    inspect_hail_engine(&engine)
}

/// Inspect a caller-opened engine without consuming it or scanning rows.
pub fn inspect_hail_engine(
    engine: &QueryEngine,
) -> Result<HailInspectionInput, AssociationSourceError> {
    inspection_from_schema(
        engine
            .row_schema()
            .map_err(|error| hail_error("hail_logical_schema", error))?,
    )
}

/// A deterministic, decode-time-projected Hail association source.
pub struct HailAssociationSource {
    rows: Box<dyn Iterator<Item = genohype_core::Result<EncodedValue>> + Send>,
    paths: BTreeMap<LogicalField, CompiledPath>,
    metadata: AssociationSourceMetadataV1,
    next_ordinal: u64,
    hasher: Option<Sha256>,
    fingerprint: Option<String>,
}

impl HailAssociationSource {
    pub fn open(
        source: &str,
        mapping: NormalizedFieldMappingV1,
        assembly: &str,
    ) -> Result<Self, AssociationSourceError> {
        require_hail_name(source)?;
        validate_assembly(assembly)?;
        mapping
            .validate()
            .map_err(|error| AssociationSourceError::new("invalid_mapping", error.to_string()))?;
        let engine =
            QueryEngine::open_path(source).map_err(|error| hail_error("open_hail", error))?;
        Self::from_engine(engine, mapping, assembly)
    }

    /// Construct from a caller-opened Hail engine. Session orchestration can use
    /// this seam with `QueryEngine::open_path_with_snapshot` while retaining the
    /// snapshot digest in its own private state.
    pub fn from_engine(
        engine: QueryEngine,
        mapping: NormalizedFieldMappingV1,
        assembly: &str,
    ) -> Result<Self, AssociationSourceError> {
        validate_assembly(assembly)?;
        mapping
            .validate()
            .map_err(|error| AssociationSourceError::new("invalid_mapping", error.to_string()))?;
        let schema = engine
            .row_schema()
            .map_err(|error| hail_error("hail_logical_schema", error))?;
        let inspection = inspection_from_schema(schema)?;
        let prepared = prepare_mapping(schema, &mapping, assembly)?;
        let metadata = AssociationSourceMetadataV1 {
            kind: AssociationSourceKindV1::Hail,
            observed_fields: inspection.fields,
            normalized_mapping: mapping,
            assembly: assembly.to_string(),
            display_name: None,
        };
        metadata.validate()?;

        // This is deliberately the deterministic API. The ordinary projected
        // query races partitions and cannot define stable source ordinals.
        let rows = engine
            .query_iter_deterministic_with_projection(
                &[],
                None,
                Some(Arc::new(prepared.projection)),
            )
            .map_err(|error| hail_error("hail_query", error))?;
        let mut hasher = Sha256::new();
        hasher.update(b"genohype.association.hail-semantic-source.v1\0");

        Ok(Self {
            rows: Box::new(rows),
            paths: prepared.paths,
            metadata,
            next_ordinal: 0,
            hasher: Some(hasher),
            fingerprint: None,
        })
    }
}

impl AssociationSource for HailAssociationSource {
    fn metadata(&self) -> &AssociationSourceMetadataV1 {
        &self.metadata
    }

    fn next_item(&mut self) -> Result<Option<AssociationSourceItem>, AssociationSourceError> {
        if self.fingerprint.is_some() {
            return Ok(None);
        }
        let Some(row) = self.rows.next() else {
            let hasher = self.hasher.take().expect("hasher exists until EOF");
            self.fingerprint = Some(format!("sha256:{:x}", hasher.finalize()));
            return Ok(None);
        };
        // A decoder/partition error is fatal. It must never be converted to a
        // rejected association row or silently skipped.
        let row = row.map_err(|error| hail_error("hail_decode", error))?;
        let ordinal = self.next_ordinal;
        self.next_ordinal = self.next_ordinal.checked_add(1).ok_or_else(|| {
            AssociationSourceError::new("ordinal_overflow", "source contains too many rows")
        })?;

        let values = extract_mapped_values(&row, &self.paths)?;
        match decode_mapped_row(&values, ordinal)? {
            Ok((point, warnings)) => {
                hash_accepted_item(
                    self.hasher.as_mut().expect("hasher exists until EOF"),
                    &point,
                    &warnings,
                )?;
                Ok(Some(AssociationSourceItem::Point { point, warnings }))
            }
            Err(rejection) => {
                hash_rejected_item(
                    self.hasher.as_mut().expect("hasher exists until EOF"),
                    ordinal,
                    &rejection,
                    &values,
                )?;
                Ok(Some(AssociationSourceItem::Rejected(rejection)))
            }
        }
    }

    fn finalized_source_fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }
}

#[derive(Debug)]
struct PreparedMapping {
    paths: BTreeMap<LogicalField, CompiledPath>,
    projection: ProjectionTree,
}

#[derive(Debug, Clone)]
struct CompiledPath {
    source: String,
    segments: Vec<String>,
}

fn prepare_mapping(
    schema: &PairedSchema,
    mapping: &NormalizedFieldMappingV1,
    assembly: &str,
) -> Result<PreparedMapping, AssociationSourceError> {
    validate_assembly(assembly)?;
    mapping
        .validate()
        .map_err(|error| AssociationSourceError::new("invalid_mapping", error.to_string()))?;

    let pairs = mapping_pairs(mapping);
    reject_parent_child_paths(&pairs)?;
    let mut paths = BTreeMap::new();
    let mut projections = Vec::with_capacity(pairs.len());
    let mut coordinate_reference_genomes = BTreeSet::new();

    for (logical, source) in pairs {
        let parsed = parse_mapping_path(logical, source)?;
        let resolved = resolve_path(&schema.physical, &schema.logical, &parsed.segments, source)?;
        validate_field_type(logical, &resolved, source)?;
        if matches!(logical, LogicalField::Contig | LogicalField::Position) {
            if let Some(reference_genome) = resolved.reference_genome {
                coordinate_reference_genomes.insert(reference_genome.to_string());
            }
        }
        let segments = parsed
            .segments
            .iter()
            .map(|segment| match segment {
                PathSegment::Field(name) => Ok(name.clone()),
                PathSegment::Index(_) | PathSegment::Slice(_, _) => {
                    Err(AssociationSourceError::new(
                        "unsupported_mapping_path",
                        format!("mapped source path {source:?} cannot use array navigation"),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        paths.insert(
            logical,
            CompiledPath {
                source: source.to_string(),
                segments,
            },
        );
        projections.push(parsed);
    }

    if coordinate_reference_genomes.len() > 1 {
        return Err(AssociationSourceError::new(
            "conflicting_hail_assemblies",
            format!(
                "mapped coordinate paths use different Hail reference genomes: {}",
                coordinate_reference_genomes
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
    if let Some(reference_genome) = coordinate_reference_genomes.first() {
        if *reference_genome != assembly {
            return Err(AssociationSourceError::new(
                "assembly_conflict",
                format!(
                    "requested assembly {assembly} conflicts with mapped Hail locus reference genome {reference_genome}"
                ),
            ));
        }
    }

    // `projections` contains exactly the validated mapped paths: no key fields,
    // parent paths, aliases, or convenience fields are added here.
    Ok(PreparedMapping {
        paths,
        projection: ProjectionTree::from_fields(&projections),
    })
}

fn parse_mapping_path(
    logical: LogicalField,
    source: &str,
) -> Result<FieldPath, AssociationSourceError> {
    if source.contains('[')
        || source.contains(']')
        || source.starts_with('.')
        || source.ends_with('.')
        || source.split('.').any(str::is_empty)
    {
        return Err(AssociationSourceError::new(
            "unsupported_mapping_path",
            format!(
                "mapped source path {source:?} for {logical} must be an exact dotted struct path without brackets"
            ),
        ));
    }
    FieldPath::parse(source).map_err(|error| {
        AssociationSourceError::new(
            "invalid_mapping_path",
            bounded_message(&format!(
                "mapped source path {source:?} for {logical}: {error}"
            )),
        )
    })
}

fn reject_parent_child_paths(pairs: &[(LogicalField, &str)]) -> Result<(), AssociationSourceError> {
    for (index, (_, left)) in pairs.iter().enumerate() {
        for (_, right) in pairs.iter().skip(index + 1) {
            if is_path_parent(left, right) || is_path_parent(right, left) {
                return Err(AssociationSourceError::new(
                    "overlapping_mapping_paths",
                    format!(
                        "mapped source paths {left:?} and {right:?} overlap; map exact leaves only"
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn is_path_parent(parent: &str, child: &str) -> bool {
    child
        .strip_prefix(parent)
        .is_some_and(|suffix| suffix.starts_with('.'))
}

#[derive(Clone, Copy)]
enum ResolvedLogical<'a> {
    Schema(&'a LogicalType),
    LocusContig,
    LocusPosition,
}

struct ResolvedPath<'a> {
    physical: &'a EncodedType,
    logical: ResolvedLogical<'a>,
    reference_genome: Option<&'a str>,
}

impl ResolvedPath<'_> {
    fn type_label(&self) -> String {
        format!(
            "{} ({})",
            logical_type_name(self.logical),
            physical_type_name(self.physical)
        )
    }
}

fn resolve_path<'a>(
    physical: &'a EncodedType,
    logical: &'a LogicalType,
    segments: &[PathSegment],
    source: &str,
) -> Result<ResolvedPath<'a>, AssociationSourceError> {
    let mut physical = physical;
    let mut logical = ResolvedLogical::Schema(logical);
    let mut reference_genome = None;

    for (depth, segment) in segments.iter().enumerate() {
        let PathSegment::Field(name) = segment else {
            return Err(AssociationSourceError::new(
                "unsupported_mapping_path",
                format!("mapped source path {source:?} cannot use array navigation"),
            ));
        };
        let EncodedType::EBaseStruct { fields, .. } = physical else {
            return Err(AssociationSourceError::new(
                "invalid_mapped_path",
                format!("mapped source path {source:?} descends through a non-struct value"),
            ));
        };
        let physical_field = fields
            .iter()
            .find(|field| field.name == *name)
            .ok_or_else(|| {
                AssociationSourceError::new(
                    "missing_mapped_field",
                    format!("mapped source path {source:?} does not exist in the Hail row schema"),
                )
            })?;

        logical = match logical {
            ResolvedLogical::Schema(LogicalType::Struct { fields }) => {
                let field = fields.iter().find(|field| field.name == *name).ok_or_else(|| {
                    AssociationSourceError::new(
                        "invalid_hail_schema",
                        format!(
                            "physical field {name:?} in mapped path {source:?} has no exact logical field"
                        ),
                    )
                })?;
                ResolvedLogical::Schema(&field.logical_type)
            }
            ResolvedLogical::Schema(LogicalType::Locus {
                reference_genome: rg,
            }) => {
                reference_genome = Some(rg.as_str());
                match name.as_str() {
                    "contig" => ResolvedLogical::LocusContig,
                    "position" => ResolvedLogical::LocusPosition,
                    _ => {
                        return Err(AssociationSourceError::new(
                            "invalid_hail_schema",
                            format!(
                                "mapped source path {source:?} names unknown Hail locus member {name:?}"
                            ),
                        ))
                    }
                }
            }
            _ => {
                return Err(AssociationSourceError::new(
                    "invalid_mapped_path",
                    format!("mapped source path {source:?} descends through a non-struct value"),
                ))
            }
        };
        physical = &physical_field.encoded_type;
        if depth + 1 < segments.len()
            && matches!(
                logical,
                ResolvedLogical::LocusContig | ResolvedLogical::LocusPosition
            )
        {
            return Err(AssociationSourceError::new(
                "invalid_mapped_path",
                format!("mapped source path {source:?} descends past a Hail locus leaf"),
            ));
        }
    }

    Ok(ResolvedPath {
        physical,
        logical,
        reference_genome,
    })
}

fn validate_field_type(
    field: LogicalField,
    resolved: &ResolvedPath<'_>,
    source: &str,
) -> Result<(), AssociationSourceError> {
    let valid = match field {
        LogicalField::Contig
        | LogicalField::Ref
        | LogicalField::Alt
        | LogicalField::Gene
        | LogicalField::Consequence
        | LogicalField::Series => is_string_type(resolved),
        LogicalField::Position => is_integer_type(resolved),
        LogicalField::Pvalue
        | LogicalField::NegLog10P
        | LogicalField::Beta
        | LogicalField::Se
        | LogicalField::Af
        | LogicalField::Ac => is_number_type(resolved),
    };
    if valid {
        Ok(())
    } else {
        Err(AssociationSourceError::new(
            "incompatible_mapped_type",
            format!(
                "mapped source path {source:?} for {field} has incompatible type {}",
                resolved.type_label()
            ),
        ))
    }
}

fn is_string_type(resolved: &ResolvedPath<'_>) -> bool {
    matches!(resolved.physical, EncodedType::EBinary { .. })
        && matches!(
            resolved.logical,
            ResolvedLogical::Schema(LogicalType::String) | ResolvedLogical::LocusContig
        )
}

fn is_integer_type(resolved: &ResolvedPath<'_>) -> bool {
    matches!(
        (resolved.physical, resolved.logical),
        (
            EncodedType::EInt32 { .. },
            ResolvedLogical::Schema(LogicalType::Int32) | ResolvedLogical::LocusPosition
        ) | (
            EncodedType::EInt64 { .. },
            ResolvedLogical::Schema(LogicalType::Int64)
        )
    )
}

fn is_number_type(resolved: &ResolvedPath<'_>) -> bool {
    matches!(
        (resolved.physical, resolved.logical),
        (
            EncodedType::EInt32 { .. },
            ResolvedLogical::Schema(LogicalType::Int32)
        ) | (
            EncodedType::EInt64 { .. },
            ResolvedLogical::Schema(LogicalType::Int64)
        ) | (
            EncodedType::EFloat32 { .. },
            ResolvedLogical::Schema(LogicalType::Float32)
        ) | (
            EncodedType::EFloat64 { .. },
            ResolvedLogical::Schema(LogicalType::Float64)
        )
    )
}

fn logical_type_name(logical: ResolvedLogical<'_>) -> &'static str {
    match logical {
        ResolvedLogical::LocusContig => "String",
        ResolvedLogical::LocusPosition => "Int32",
        ResolvedLogical::Schema(logical) => logical.display_name(),
    }
}

fn physical_type_name(physical: &EncodedType) -> &'static str {
    match physical {
        EncodedType::EBinary { .. } => "EBinary",
        EncodedType::EBaseStruct { .. } => "EBaseStruct",
        EncodedType::EInt32 { .. } => "EInt32",
        EncodedType::EInt64 { .. } => "EInt64",
        EncodedType::EFloat32 { .. } => "EFloat32",
        EncodedType::EFloat64 { .. } => "EFloat64",
        EncodedType::EBoolean { .. } => "EBoolean",
        EncodedType::EArray { .. } => "EArray",
    }
}

fn extract_mapped_values<'a>(
    row: &'a EncodedValue,
    paths: &BTreeMap<LogicalField, CompiledPath>,
) -> Result<BTreeMap<LogicalField, &'a EncodedValue>, AssociationSourceError> {
    let mut values = BTreeMap::new();
    for (logical, path) in paths {
        let mut value = row;
        for segment in &path.segments {
            let EncodedValue::Struct(fields) = value else {
                return Err(AssociationSourceError::new(
                    "projected_decode_shape",
                    format!(
                        "projected Hail row is not a struct while reading exact path {:?}",
                        path.source
                    ),
                ));
            };
            value = fields
                .iter()
                .find(|(name, _)| name == segment)
                .map(|(_, value)| value)
                .ok_or_else(|| {
                    AssociationSourceError::new(
                        "projected_decode_shape",
                        format!(
                            "projected Hail row is missing exact mapped path {:?}",
                            path.source
                        ),
                    )
                })?;
        }
        values.insert(*logical, value);
    }
    Ok(values)
}

fn decode_mapped_row(
    values: &BTreeMap<LogicalField, &EncodedValue>,
    ordinal: u64,
) -> Result<
    Result<(AssociationPointV1, Vec<WarningCode>), AssociationSourceRejectionV1>,
    AssociationSourceError,
> {
    // Validate every projected scalar before applying recoverable domain rules.
    // A corrupt value in a later mapped field must not be hidden by an earlier
    // row rejection.
    validate_runtime_values(values, ordinal)?;

    let contig_value = required_value(values, LogicalField::Contig)?;
    let contig = match text_value(contig_value, LogicalField::Contig, ordinal)? {
        Some(value) if !value.is_empty() => value,
        _ => {
            return Ok(Err(rejection(
                ordinal,
                "missing_contig",
                LogicalField::Contig,
                contig_value,
            )))
        }
    };

    let position_value = required_value(values, LogicalField::Position)?;
    let position = match position_value {
        EncodedValue::Int32(value) => i64::from(*value),
        EncodedValue::Int64(value) => *value,
        EncodedValue::Null => {
            return Ok(Err(rejection(
                ordinal,
                "invalid_position",
                LogicalField::Position,
                position_value,
            )))
        }
        _ => return Err(runtime_type_error(LogicalField::Position)),
    };

    let pvalue = match optional_number(values, LogicalField::Pvalue, ordinal, "invalid_pvalue")? {
        Ok(value) => value,
        Err(rejection) => return Ok(Err(rejection)),
    };
    let neg_log10_p = match optional_number(
        values,
        LogicalField::NegLog10P,
        ordinal,
        "invalid_neg_log10_p",
    )? {
        Ok(value) => value,
        Err(rejection) => return Ok(Err(rejection)),
    };
    let significance = match normalize_significance(pvalue, neg_log10_p) {
        Ok(value) => value,
        Err(error) => {
            let (code, field) = match error {
                PValueError::MissingSignificance => {
                    if values.contains_key(&LogicalField::Pvalue) {
                        ("missing_significance", LogicalField::Pvalue)
                    } else {
                        ("missing_significance", LogicalField::NegLog10P)
                    }
                }
                PValueError::InvalidPvalue => ("invalid_pvalue", LogicalField::Pvalue),
                PValueError::InvalidNegLog10P => ("invalid_neg_log10_p", LogicalField::NegLog10P),
            };
            return Ok(Err(rejection(
                ordinal,
                code,
                field,
                values.get(&field).copied().unwrap_or(&EncodedValue::Null),
            )));
        }
    };

    macro_rules! number {
        ($field:expr, $code:literal) => {
            match optional_number(values, $field, ordinal, $code)? {
                Ok(value) => value,
                Err(rejection) => return Ok(Err(rejection)),
            }
        };
    }
    let point = AssociationPointV1 {
        contig,
        position,
        ref_allele: optional_text(values, LogicalField::Ref, ordinal)?,
        alt_allele: optional_text(values, LogicalField::Alt, ordinal)?,
        pvalue: significance.pvalue,
        neg_log10_p: significance.neg_log10_p,
        beta: number!(LogicalField::Beta, "invalid_beta"),
        standard_error: number!(LogicalField::Se, "invalid_se"),
        allele_frequency: number!(LogicalField::Af, "invalid_af"),
        allele_count: number!(LogicalField::Ac, "invalid_ac"),
        gene_symbol: optional_text(values, LogicalField::Gene, ordinal)?,
        consequence: optional_text(values, LogicalField::Consequence, ordinal)?,
        series: optional_text(values, LogicalField::Series, ordinal)?,
        source_ordinal: ordinal,
    };
    match point.normalize() {
        Ok(result) => Ok(Ok(result)),
        Err(error) => {
            let (code, field) = validation_reason(error);
            Ok(Err(rejection(
                ordinal,
                code,
                field,
                values.get(&field).copied().unwrap_or(&EncodedValue::Null),
            )))
        }
    }
}

fn validate_runtime_values(
    values: &BTreeMap<LogicalField, &EncodedValue>,
    ordinal: u64,
) -> Result<(), AssociationSourceError> {
    for (field, value) in values {
        if matches!(value, EncodedValue::Null) {
            continue;
        }
        match field {
            LogicalField::Contig
            | LogicalField::Ref
            | LogicalField::Alt
            | LogicalField::Gene
            | LogicalField::Consequence
            | LogicalField::Series => match value {
                EncodedValue::Binary(bytes) => {
                    std::str::from_utf8(bytes).map_err(|_| {
                        AssociationSourceError::new(
                            "invalid_utf8",
                            format!("mapped field {field} contains invalid UTF-8 at row {ordinal}"),
                        )
                    })?;
                }
                _ => return Err(runtime_type_error(*field)),
            },
            LogicalField::Position => {
                if !matches!(value, EncodedValue::Int32(_) | EncodedValue::Int64(_)) {
                    return Err(runtime_type_error(*field));
                }
            }
            LogicalField::Pvalue
            | LogicalField::NegLog10P
            | LogicalField::Beta
            | LogicalField::Se
            | LogicalField::Af
            | LogicalField::Ac => {
                if !matches!(
                    value,
                    EncodedValue::Int32(_)
                        | EncodedValue::Int64(_)
                        | EncodedValue::Float32(_)
                        | EncodedValue::Float64(_)
                ) {
                    return Err(runtime_type_error(*field));
                }
            }
        }
    }
    Ok(())
}

fn required_value<'a>(
    values: &'a BTreeMap<LogicalField, &EncodedValue>,
    field: LogicalField,
) -> Result<&'a EncodedValue, AssociationSourceError> {
    values
        .get(&field)
        .copied()
        .ok_or_else(|| AssociationSourceError::new("invalid_mapping", format!("missing {field}")))
}

fn text_value(
    value: &EncodedValue,
    field: LogicalField,
    ordinal: u64,
) -> Result<Option<String>, AssociationSourceError> {
    match value {
        EncodedValue::Null => Ok(None),
        EncodedValue::Binary(bytes) => std::str::from_utf8(bytes)
            .map(|value| Some(value.to_string()))
            .map_err(|_| {
                AssociationSourceError::new(
                    "invalid_utf8",
                    format!("mapped field {field} contains invalid UTF-8 at row {ordinal}"),
                )
            }),
        _ => Err(runtime_type_error(field)),
    }
}

fn optional_text(
    values: &BTreeMap<LogicalField, &EncodedValue>,
    field: LogicalField,
    ordinal: u64,
) -> Result<Option<String>, AssociationSourceError> {
    let Some(value) = values.get(&field).copied() else {
        return Ok(None);
    };
    text_value(value, field, ordinal)
}

fn optional_number(
    values: &BTreeMap<LogicalField, &EncodedValue>,
    field: LogicalField,
    ordinal: u64,
    code: &'static str,
) -> Result<Result<Option<f64>, AssociationSourceRejectionV1>, AssociationSourceError> {
    let Some(value) = values.get(&field).copied() else {
        return Ok(Ok(None));
    };
    let number = match value {
        EncodedValue::Null => return Ok(Ok(None)),
        EncodedValue::Int32(value) => f64::from(*value),
        EncodedValue::Int64(value) => *value as f64,
        EncodedValue::Float32(value) => f64::from(*value),
        EncodedValue::Float64(value) => *value,
        _ => return Err(runtime_type_error(field)),
    };
    if !number.is_finite() {
        Ok(Err(rejection(ordinal, code, field, value)))
    } else {
        Ok(Ok(Some(number)))
    }
}

fn runtime_type_error(field: LogicalField) -> AssociationSourceError {
    AssociationSourceError::new(
        "projected_decode_type",
        format!("projected Hail value for {field} does not match its prevalidated type"),
    )
}

fn validation_reason(error: ValidationError) -> (&'static str, LogicalField) {
    match error {
        ValidationError::InvalidContig => ("invalid_contig", LogicalField::Contig),
        ValidationError::InvalidPosition | ValidationError::BrowserUnsafeInteger("position") => {
            ("invalid_position", LogicalField::Position)
        }
        ValidationError::BrowserUnsafeInteger(_) => ("invalid_ordinal", LogicalField::Position),
        ValidationError::InvalidAlleleFrequency => ("invalid_af", LogicalField::Af),
        ValidationError::InvalidAlleleCount => ("invalid_ac", LogicalField::Ac),
        ValidationError::NonFiniteOptionalStatistic => {
            ("invalid_optional_number", LogicalField::Beta)
        }
        ValidationError::TextFieldTooLong | ValidationError::PointTextTooLarge => {
            ("point_text_too_large", LogicalField::Contig)
        }
        ValidationError::InvalidPvalue(PValueError::InvalidPvalue) => {
            ("invalid_pvalue", LogicalField::Pvalue)
        }
        ValidationError::InvalidPvalue(PValueError::InvalidNegLog10P) => {
            ("invalid_neg_log10_p", LogicalField::NegLog10P)
        }
        ValidationError::InvalidPvalue(PValueError::MissingSignificance) => {
            ("missing_significance", LogicalField::Pvalue)
        }
    }
}

fn rejection(
    ordinal: u64,
    code: &'static str,
    field: LogicalField,
    value: &EncodedValue,
) -> AssociationSourceRejectionV1 {
    let example = value_example(value);
    AssociationSourceRejectionV1::new(
        ordinal,
        code,
        Some(field.to_string()),
        Some(example.as_str()),
    )
}

fn value_example(value: &EncodedValue) -> String {
    match value {
        EncodedValue::Null => String::new(),
        EncodedValue::Binary(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        EncodedValue::Int32(value) => value.to_string(),
        EncodedValue::Int64(value) => value.to_string(),
        EncodedValue::Float32(value) => value.to_string(),
        EncodedValue::Float64(value) => value.to_string(),
        EncodedValue::Boolean(value) => value.to_string(),
        EncodedValue::Array(_) => "<array>".to_string(),
        EncodedValue::Struct(_) => "<struct>".to_string(),
    }
}

fn hash_accepted_item(
    hasher: &mut Sha256,
    point: &AssociationPointV1,
    warnings: &[WarningCode],
) -> Result<(), AssociationSourceError> {
    hasher.update(b"accepted\0");
    let point = serde_json::to_vec(point).map_err(|_| {
        AssociationSourceError::new(
            "fingerprint_serialization",
            "could not serialize a normalized association point",
        )
    })?;
    hash_bytes(hasher, &point);
    hasher.update((warnings.len() as u64).to_be_bytes());
    for warning in warnings {
        hash_bytes(hasher, warning.as_str().as_bytes());
    }
    Ok(())
}

fn hash_rejected_item(
    hasher: &mut Sha256,
    ordinal: u64,
    rejection: &AssociationSourceRejectionV1,
    values: &BTreeMap<LogicalField, &EncodedValue>,
) -> Result<(), AssociationSourceError> {
    hasher.update(b"rejected\0");
    hasher.update(ordinal.to_be_bytes());
    hash_bytes(hasher, rejection.reason_code.as_bytes());
    hasher.update((values.len() as u64).to_be_bytes());
    for (logical, value) in values {
        hash_bytes(hasher, logical.as_str().as_bytes());
        hash_encoded_leaf(hasher, value)?;
    }
    Ok(())
}

fn hash_encoded_leaf(
    hasher: &mut Sha256,
    value: &EncodedValue,
) -> Result<(), AssociationSourceError> {
    match value {
        EncodedValue::Null => hasher.update([0]),
        EncodedValue::Binary(value) => {
            hasher.update([1]);
            hash_bytes(hasher, value);
        }
        EncodedValue::Int32(value) => {
            hasher.update([2]);
            hasher.update(value.to_be_bytes());
        }
        EncodedValue::Int64(value) => {
            hasher.update([3]);
            hasher.update(value.to_be_bytes());
        }
        EncodedValue::Float32(value) => {
            hasher.update([4]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        EncodedValue::Float64(value) => {
            hasher.update([5]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        EncodedValue::Boolean(value) => {
            hasher.update([6, u8::from(*value)]);
        }
        EncodedValue::Array(_) | EncodedValue::Struct(_) => {
            return Err(AssociationSourceError::new(
                "projected_decode_type",
                "mapped Hail fingerprint value is not a prevalidated scalar",
            ))
        }
    }
    Ok(())
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn inspection_from_schema(
    schema: &PairedSchema,
) -> Result<HailInspectionInput, AssociationSourceError> {
    let mut fields = Vec::new();
    flatten_schema(&schema.physical, &schema.logical, "", 0, &mut fields)?;
    let mut reference_genomes = BTreeSet::new();
    collect_reference_genomes(&schema.logical, 0, &mut reference_genomes)?;
    Ok(HailInspectionInput {
        fields,
        reference_genomes: reference_genomes.into_iter().map(str::to_string).collect(),
    })
}

fn flatten_schema(
    physical: &EncodedType,
    logical: &LogicalType,
    prefix: &str,
    depth: usize,
    output: &mut Vec<AssociationSourceFieldV1>,
) -> Result<(), AssociationSourceError> {
    if depth > MAX_HAIL_SCHEMA_DEPTH {
        return Err(AssociationSourceError::new(
            "hail_schema_too_deep",
            format!("Hail schema exceeds {MAX_HAIL_SCHEMA_DEPTH} nested levels"),
        ));
    }
    match (physical, logical) {
        (
            EncodedType::EBaseStruct {
                fields: physical_fields,
                ..
            },
            LogicalType::Struct {
                fields: logical_fields,
            },
        ) => {
            for physical_field in physical_fields {
                let logical_field = logical_fields
                    .iter()
                    .find(|field| field.name == physical_field.name)
                    .ok_or_else(|| {
                        AssociationSourceError::new(
                            "invalid_hail_schema",
                            "physical and logical Hail struct fields do not match",
                        )
                    })?;
                let path = join_field_path(prefix, &physical_field.name)?;
                flatten_schema(
                    &physical_field.encoded_type,
                    &logical_field.logical_type,
                    &path,
                    depth + 1,
                    output,
                )?;
            }
        }
        (
            EncodedType::EBaseStruct {
                fields: physical_fields,
                ..
            },
            LogicalType::Locus { .. },
        ) => {
            for (name, logical_name) in [("contig", "String"), ("position", "Int32")] {
                let physical_field = physical_fields
                    .iter()
                    .find(|field| field.name == name)
                    .ok_or_else(|| {
                        AssociationSourceError::new(
                            "invalid_hail_schema",
                            format!("Hail locus is missing physical member {name:?}"),
                        )
                    })?;
                push_inspection_field(
                    output,
                    join_field_path(prefix, name)?,
                    format!(
                        "{logical_name} ({})",
                        physical_type_name(&physical_field.encoded_type)
                    ),
                )?;
            }
        }
        _ => push_inspection_field(
            output,
            prefix.to_string(),
            format!(
                "{} ({})",
                logical.display_name(),
                physical_type_name(physical)
            ),
        )?,
    }
    Ok(())
}

fn push_inspection_field(
    output: &mut Vec<AssociationSourceFieldV1>,
    name: String,
    data_type: String,
) -> Result<(), AssociationSourceError> {
    if name.is_empty() || name.len() > 256 || data_type.is_empty() || data_type.len() > 128 {
        return Err(AssociationSourceError::new(
            "hail_schema_field_too_large",
            "Hail schema contains a field path or type outside bounded inspection limits",
        ));
    }
    if output.len() >= MAX_HAIL_INSPECT_FIELDS {
        return Err(AssociationSourceError::new(
            "too_many_hail_fields",
            format!("Hail schema exceeds {MAX_HAIL_INSPECT_FIELDS} inspectable fields"),
        ));
    }
    output.push(AssociationSourceFieldV1 {
        name,
        data_type: Some(data_type),
    });
    Ok(())
}

fn join_field_path(prefix: &str, name: &str) -> Result<String, AssociationSourceError> {
    if name.is_empty() || name.contains('.') || name.contains('[') || name.contains(']') {
        return Err(AssociationSourceError::new(
            "unsupported_hail_field_name",
            format!(
                "Hail field name {name:?} cannot be represented as an exact association mapping path"
            ),
        ));
    }
    Ok(if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    })
}

fn collect_reference_genomes<'a>(
    logical: &'a LogicalType,
    depth: usize,
    output: &mut BTreeSet<&'a str>,
) -> Result<(), AssociationSourceError> {
    if depth > MAX_HAIL_SCHEMA_DEPTH {
        return Err(AssociationSourceError::new(
            "hail_schema_too_deep",
            format!("Hail schema exceeds {MAX_HAIL_SCHEMA_DEPTH} nested levels"),
        ));
    }
    match logical {
        LogicalType::Locus { reference_genome } => {
            if reference_genome.is_empty() || reference_genome.len() > 64 {
                return Err(AssociationSourceError::new(
                    "hail_reference_genome_too_large",
                    "Hail reference genome is outside bounded inspection limits",
                ));
            }
            output.insert(reference_genome);
        }
        LogicalType::Array { element } | LogicalType::Set { element } => {
            collect_reference_genomes(element, depth + 1, output)?
        }
        LogicalType::Dict { key, value } => {
            collect_reference_genomes(key, depth + 1, output)?;
            collect_reference_genomes(value, depth + 1, output)?;
        }
        LogicalType::Tuple { elements } => {
            for element in elements {
                collect_reference_genomes(element, depth + 1, output)?;
            }
        }
        LogicalType::Struct { fields } => {
            for field in fields {
                collect_reference_genomes(&field.logical_type, depth + 1, output)?;
            }
        }
        LogicalType::Interval { point } => collect_reference_genomes(point, depth + 1, output)?,
        LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::Float32
        | LogicalType::Float64
        | LogicalType::Boolean
        | LogicalType::String
        | LogicalType::Binary
        | LogicalType::Call => {}
    }
    Ok(())
}

fn require_hail_name(source: &str) -> Result<(), AssociationSourceError> {
    let normalized = source.trim_end_matches('/').to_ascii_lowercase();
    if normalized.ends_with(".ht") {
        Ok(())
    } else {
        Err(AssociationSourceError::new(
            "source_kind_mismatch",
            "Hail association source requires an explicit .ht path",
        ))
    }
}

fn validate_assembly(assembly: &str) -> Result<(), AssociationSourceError> {
    if matches!(assembly, "GRCh37" | "GRCh38") {
        Ok(())
    } else {
        Err(AssociationSourceError::new(
            "invalid_assembly",
            "association assembly must be exactly GRCh37 or GRCh38",
        ))
    }
}

fn hail_error(operation: &'static str, error: genohype_core::HailError) -> AssociationSourceError {
    AssociationSourceError::new(operation, bounded_message(&error.to_string()))
}

fn bounded_message(message: &str) -> String {
    if message.len() <= MAX_ERROR_MESSAGE_BYTES {
        return message.to_string();
    }
    let mut end = MAX_ERROR_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use genohype_core::codec::EncodedField;
    use genohype_core::schema::LogicalField as HailLogicalField;

    fn field(name: &str, encoded_type: EncodedType, index: usize) -> EncodedField {
        EncodedField {
            name: name.to_string(),
            encoded_type,
            index,
        }
    }

    fn logical_field(name: &str, logical_type: LogicalType) -> HailLogicalField {
        HailLogicalField {
            name: name.to_string(),
            logical_type,
        }
    }

    fn schema() -> PairedSchema {
        PairedSchema::new(
            EncodedType::EBaseStruct {
                required: true,
                fields: vec![
                    field(
                        "locus",
                        EncodedType::EBaseStruct {
                            required: true,
                            fields: vec![
                                field("contig", EncodedType::EBinary { required: true }, 0),
                                field("position", EncodedType::EInt32 { required: true }, 1),
                            ],
                        },
                        0,
                    ),
                    field("Pvalue", EncodedType::EFloat64 { required: false }, 1),
                    field("Pvalue_log10", EncodedType::EFloat64 { required: false }, 2),
                    field("BETA", EncodedType::EFloat64 { required: false }, 3),
                    field("unused", EncodedType::EBinary { required: false }, 4),
                ],
            },
            LogicalType::Struct {
                fields: vec![
                    logical_field(
                        "locus",
                        LogicalType::Locus {
                            reference_genome: "GRCh38".to_string(),
                        },
                    ),
                    logical_field("Pvalue", LogicalType::Float64),
                    logical_field("Pvalue_log10", LogicalType::Float64),
                    logical_field("BETA", LogicalType::Float64),
                    logical_field("unused", LogicalType::String),
                ],
            },
        )
        .unwrap()
    }

    fn mapping() -> NormalizedFieldMappingV1 {
        NormalizedFieldMappingV1 {
            contig: "locus.contig".into(),
            position: "locus.position".into(),
            ref_allele: None,
            alt_allele: None,
            pvalue: Some("Pvalue".into()),
            neg_log10_p: Some("Pvalue_log10".into()),
            beta: Some("BETA".into()),
            se: None,
            af: None,
            ac: None,
            gene: None,
            consequence: None,
            series: None,
        }
    }

    fn row(pvalue: EncodedValue) -> EncodedValue {
        EncodedValue::Struct(vec![
            (
                "locus".into(),
                EncodedValue::Struct(vec![
                    ("contig".into(), EncodedValue::Binary(b"chr22".to_vec())),
                    ("position".into(), EncodedValue::Int32(16_050_075)),
                ]),
            ),
            ("Pvalue".into(), pvalue),
            ("Pvalue_log10".into(), EncodedValue::Float64(20.0)),
            ("BETA".into(), EncodedValue::Float64(-0.25)),
        ])
    }

    #[test]
    fn projection_contains_exact_mapped_paths_only() {
        let prepared = prepare_mapping(&schema(), &mapping(), "GRCh38").unwrap();
        assert!(prepared.projection.get_child("locus").is_some());
        assert!(prepared.projection.get_child("Pvalue").is_some());
        assert!(prepared.projection.get_child("Pvalue_log10").is_some());
        assert!(prepared.projection.get_child("BETA").is_some());
        assert!(prepared.projection.get_child("unused").is_none());
        let locus = prepared.projection.get_child("locus").unwrap();
        assert!(locus.get_child("contig").is_some());
        assert!(locus.get_child("position").is_some());
        assert!(!locus.is_select_all());
    }

    #[test]
    fn missing_type_brackets_and_assembly_conflicts_fail_before_iteration() {
        let mut invalid = mapping();
        invalid.pvalue = Some("missing".into());
        assert_eq!(
            prepare_mapping(&schema(), &invalid, "GRCh38")
                .unwrap_err()
                .code,
            "missing_mapped_field"
        );

        let mut invalid = mapping();
        invalid.pvalue = Some("unused".into());
        assert_eq!(
            prepare_mapping(&schema(), &invalid, "GRCh38")
                .unwrap_err()
                .code,
            "incompatible_mapped_type"
        );

        let mut invalid = mapping();
        invalid.pvalue = Some("values[0]".into());
        assert_eq!(
            prepare_mapping(&schema(), &invalid, "GRCh38")
                .unwrap_err()
                .code,
            "unsupported_mapping_path"
        );

        assert_eq!(
            prepare_mapping(&schema(), &mapping(), "GRCh37")
                .unwrap_err()
                .code,
            "assembly_conflict"
        );
    }

    #[test]
    fn exact_name_decode_normalizes_with_stable_ordinal() {
        let prepared = prepare_mapping(&schema(), &mapping(), "GRCh38").unwrap();
        let row = row(EncodedValue::Float64(1e-20));
        let values = extract_mapped_values(&row, &prepared.paths).unwrap();
        let (point, warnings) = decode_mapped_row(&values, 7).unwrap().unwrap();
        assert_eq!(point.contig, "22");
        assert_eq!(point.position, 16_050_075);
        assert_eq!(point.pvalue, 1e-20);
        assert_eq!(point.neg_log10_p, 20.0);
        assert_eq!(point.beta, Some(-0.25));
        assert_eq!(point.source_ordinal, 7);
        assert!(warnings.is_empty());
    }

    #[test]
    fn bad_row_is_rejected_but_projected_shape_error_is_fatal() {
        let prepared = prepare_mapping(&schema(), &mapping(), "GRCh38").unwrap();
        let row = row(EncodedValue::Float64(2.0));
        let values = extract_mapped_values(&row, &prepared.paths).unwrap();
        assert_eq!(
            decode_mapped_row(&values, 0)
                .unwrap()
                .unwrap_err()
                .reason_code,
            "invalid_pvalue"
        );

        let missing_exact_name = EncodedValue::Struct(vec![]);
        assert_eq!(
            extract_mapped_values(&missing_exact_name, &prepared.paths)
                .unwrap_err()
                .code,
            "projected_decode_shape"
        );
    }

    #[test]
    fn semantic_rejection_hash_uses_mapped_values_not_unmapped_fields() {
        let prepared = prepare_mapping(&schema(), &mapping(), "GRCh38").unwrap();
        let first = row(EncodedValue::Float64(2.0));
        let mut second = first.clone();
        let EncodedValue::Struct(fields) = &mut second else {
            unreachable!()
        };
        fields.push(("unused".into(), EncodedValue::Binary(b"changed".to_vec())));
        let first_values = extract_mapped_values(&first, &prepared.paths).unwrap();
        let second_values = extract_mapped_values(&second, &prepared.paths).unwrap();
        let rejection = decode_mapped_row(&first_values, 0).unwrap().unwrap_err();
        let mut first_hash = Sha256::new();
        let mut second_hash = Sha256::new();
        hash_rejected_item(&mut first_hash, 0, &rejection, &first_values).unwrap();
        hash_rejected_item(&mut second_hash, 0, &rejection, &second_values).unwrap();
        assert_eq!(first_hash.finalize(), second_hash.finalize());
    }

    #[test]
    fn inspection_is_exact_and_reports_logical_reference_genome() {
        let inspection = inspection_from_schema(&schema()).unwrap();
        assert_eq!(inspection.reference_genomes, ["GRCh38"]);
        assert!(inspection
            .fields
            .iter()
            .any(|field| field.name == "locus.contig"));
        assert!(inspection
            .fields
            .iter()
            .any(|field| field.name == "locus.position"));
        assert!(inspection.fields.iter().any(|field| field.name == "Pvalue"));
    }

    #[test]
    fn canonical_fixture_matches_legacy_adapter_for_the_exact_mapped_semantics() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../data/test/association/single-variant.ht");
        let path = path.to_str().unwrap();
        let mut mapping = mapping();
        mapping.se = Some("SE".into());
        mapping.af = Some("AF_Allele2".into());
        mapping.ac = Some("AC_Allele2".into());
        let mut source = HailAssociationSource::open(path, mapping, "GRCh38").unwrap();
        let engine = QueryEngine::open_path(path).unwrap();
        let rows = engine
            .query_iter_deterministic_with_projection(&[], None, None)
            .unwrap();

        for (ordinal, row) in rows.enumerate() {
            let row = row.unwrap();
            let generalized = source.next_item().unwrap().unwrap();
            let legacy =
                crate::manhattan::data::extract_association_point(&row, "Pvalue", ordinal as u64);
            match generalized {
                AssociationSourceItem::Point { point, warnings } => {
                    let (mut legacy_point, legacy_warnings) = legacy.unwrap();
                    // Array-index mapping is deliberately unsupported by the
                    // exact dotted-path projector, so compare every mapped field.
                    legacy_point.ref_allele = None;
                    legacy_point.alt_allele = None;
                    assert_eq!(point, legacy_point);
                    assert_eq!(warnings, legacy_warnings);
                }
                AssociationSourceItem::Rejected(_) => assert!(legacy.is_err()),
            }
        }
        assert!(source.next_item().unwrap().is_none());
    }

    #[test]
    fn iterator_decode_error_is_fatal_and_never_becomes_a_rejection() {
        let prepared = prepare_mapping(&schema(), &mapping(), "GRCh38").unwrap();
        let metadata = AssociationSourceMetadataV1 {
            kind: AssociationSourceKindV1::Hail,
            observed_fields: Vec::new(),
            normalized_mapping: mapping(),
            assembly: "GRCh38".into(),
            display_name: None,
        };
        let mut hasher = Sha256::new();
        hasher.update(b"genohype.association.hail-semantic-source.v1\0");
        let mut source = HailAssociationSource {
            rows: Box::new(
                vec![Err(genohype_core::HailError::InvalidFormat(
                    "synthetic decode failure".into(),
                ))]
                .into_iter(),
            ),
            paths: prepared.paths,
            metadata,
            next_ordinal: 0,
            hasher: Some(hasher),
            fingerprint: None,
        };
        let error = source.next_item().unwrap_err();
        assert_eq!(error.code, "hail_decode");
        assert_eq!(source.next_ordinal, 0);
        assert!(source.finalized_source_fingerprint().is_none());
    }

    #[test]
    fn real_hail_fixture_streams_once_and_finalizes_fingerprint() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../data/test/association/single-variant.ht");
        let path = path.to_str().unwrap();
        let mut mapping = mapping();
        mapping.se = Some("SE".into());
        mapping.af = Some("AF_Allele2".into());
        mapping.ac = Some("AC_Allele2".into());
        let mut source = HailAssociationSource::open(path, mapping.clone(), "GRCh38").unwrap();
        assert_eq!(source.metadata().kind, AssociationSourceKindV1::Hail);
        assert!(source.metadata().display_name.is_none());
        assert!(source.finalized_source_fingerprint().is_none());

        let mut ordinals = Vec::new();
        while let Some(item) = source.next_item().unwrap() {
            ordinals.push(match item {
                AssociationSourceItem::Point { point, .. } => point.source_ordinal,
                AssociationSourceItem::Rejected(rejection) => rejection.source_ordinal,
            });
        }
        assert_eq!(ordinals, (0..12).collect::<Vec<_>>());
        let fingerprint = source.finalized_source_fingerprint().unwrap().to_string();
        assert!(fingerprint.starts_with("sha256:"));
        assert_eq!(fingerprint.len(), 71);
        assert!(source.next_item().unwrap().is_none());

        let mut repeated = HailAssociationSource::open(path, mapping, "GRCh38").unwrap();
        while repeated.next_item().unwrap().is_some() {}
        assert_eq!(
            repeated.finalized_source_fingerprint(),
            Some(fingerprint.as_str())
        );
    }
}
