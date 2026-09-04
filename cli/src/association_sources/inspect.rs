use super::delimited::{
    decode_record, mapped_indexes, MAX_DELIMITED_HEADER_BYTES, MAX_DELIMITED_HEADER_FIELDS,
    MAX_DELIMITED_RECORD_BYTES,
};
use super::hail::{inspect_hail_schema, HailAssociationSource};
use super::mapping::{mapping_pairs, LogicalField, MappingInput, LOGICAL_FIELDS};
use super::DetectedAssociationSource;
use csv::ByteRecord;
use flate2::read::MultiGzDecoder;
use genohype_association::{
    AssociationSource, AssociationSourceError, AssociationSourceItem, NormalizedFieldMappingV1,
    RejectionDiagnostics,
};
use genohype_core::codec::EncodedValue;
use genohype_core::projection::{FieldPath, ProjectionTree};
use genohype_core::query::QueryEngine;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

pub const MAX_INSPECT_RECORDS: u64 = 100;
pub const MAX_INSPECT_DECODED_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InspectFieldReport {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InspectReasonReport {
    pub count: u64,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AssociationInspectReport {
    pub source_kind: String,
    pub inspected_records: u64,
    pub decoded_sample_bytes: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_logical_assemblies: Vec<String>,
    pub fields: Vec<InspectFieldReport>,
    pub explicit_mappings: BTreeMap<String, String>,
    pub inferred_mappings: BTreeMap<String, String>,
    pub mappings: BTreeMap<String, String>,
    pub ambiguities: BTreeMap<String, Vec<String>>,
    pub unresolved: Vec<String>,
    pub invalid_reasons: BTreeMap<String, InspectReasonReport>,
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runnable_view_command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolution_commands: Vec<String>,
}

pub fn inspect_delimited(
    source: &str,
    detected: DetectedAssociationSource,
    explicit: &MappingInput,
    sample_rows: u64,
) -> Result<AssociationInspectReport, String> {
    if sample_rows == 0 || sample_rows > MAX_INSPECT_RECORDS {
        return Err(format!("sample rows must be in 1..={MAX_INSPECT_RECORDS}"));
    }
    let (headers, samples, decoded_sample_bytes) =
        read_bounded_delimited_samples(Path::new(source), detected, sample_rows as usize)?;
    validate_explicit_headers(&headers, explicit)?;

    let types = infer_types(&headers, &samples);
    let mut resolved = explicit.clone();
    let mut ambiguities = BTreeMap::new();
    infer_mappings(&headers, &types, &mut resolved, &mut ambiguities);
    resolved
        .validate_partial()
        .map_err(|error| error.to_string())?;

    let unresolved = resolved.unresolved_required();
    let normalized = if unresolved.is_empty() && ambiguities.is_empty() {
        Some(resolved.complete().map_err(|error| error.to_string())?)
    } else {
        None
    };
    let invalid_reasons = if let Some(mapping) = &normalized {
        inspect_invalid_rows(&headers, mapping, &samples)?
    } else {
        BTreeMap::new()
    };
    let ready = normalized.is_some();
    let runnable_view_command = normalized.as_ref().map(|mapping| {
        build_command(
            "view",
            source,
            resolved.assembly,
            mapping_pairs(mapping)
                .into_iter()
                .map(|(logical, source)| (logical, source.to_string()))
                .collect(),
        )
    });
    let resolution_commands = ambiguity_commands(source, &resolved, &ambiguities);

    let explicit_mappings = display_mappings(&explicit.fields);
    let inferred_mappings = resolved
        .fields
        .iter()
        .filter(|(logical, _)| !explicit.fields.contains_key(logical))
        .map(|(logical, source)| (logical.to_string(), source.clone()))
        .collect();
    Ok(AssociationInspectReport {
        source_kind: detected.label().to_string(),
        inspected_records: samples.len() as u64,
        decoded_sample_bytes,
        declared_logical_assemblies: Vec::new(),
        fields: headers
            .iter()
            .zip(types)
            .map(|(name, data_type)| InspectFieldReport {
                name: name.clone(),
                data_type,
            })
            .collect(),
        explicit_mappings,
        inferred_mappings,
        mappings: display_mappings(&resolved.fields),
        ambiguities,
        unresolved,
        invalid_reasons,
        ready,
        runnable_view_command,
        resolution_commands,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InspectQuoteState {
    FieldStart,
    Unquoted,
    Quoted,
    QuoteClosed,
}

/// Read only complete logical records that fit in the aggregate decoded sample
/// budget. The in-progress record shares that budget with retained records, so
/// inspection never allocates or validates the remainder of an over-budget
/// record. Raw decoded input is independently capped before each allocation.
fn read_bounded_delimited_samples(
    path: &Path,
    detected: DetectedAssociationSource,
    sample_rows: usize,
) -> Result<(Vec<String>, Vec<ByteRecord>, usize), String> {
    let DetectedAssociationSource::Delimited { delimiter, gzip } = detected else {
        return Err("delimited inspection requires a CSV or TSV source".into());
    };
    let file =
        File::open(path).map_err(|error| format!("could not open delimited source: {error}"))?;
    let input: Box<dyn Read> = if gzip {
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut input = BufReader::new(input);
    let mut byte = [0_u8; 1];
    let mut state = InspectQuoteState::FieldStart;
    let mut previous_was_record_cr = false;
    let mut record_index = 0_u64;
    let mut raw_record_bytes = 0_usize;
    let mut logical_record_bytes = 0_usize;
    let mut field = Vec::new();
    let mut record = ByteRecord::new();
    let mut headers: Option<Vec<String>> = None;
    let mut samples = Vec::new();
    let mut decoded_sample_bytes = 0_usize;

    loop {
        let read = input
            .read(&mut byte)
            .map_err(|error| format!("could not decode delimited source: {error}"))?;
        if read == 0 {
            if state == InspectQuoteState::Quoted {
                return Err(format!(
                    "invalid_csv_framing: unterminated quoted field at EOF in record {record_index}"
                ));
            }
            if raw_record_bytes != 0 {
                finish_inspect_record(
                    &mut record,
                    &mut field,
                    record_index,
                    logical_record_bytes,
                    &mut headers,
                    &mut samples,
                    &mut decoded_sample_bytes,
                )?;
            }
            break;
        }
        let byte = byte[0];
        if previous_was_record_cr && byte == b'\n' {
            previous_was_record_cr = false;
            continue;
        }
        previous_was_record_cr = false;

        raw_record_bytes = raw_record_bytes
            .checked_add(1)
            .ok_or_else(|| "delimited record size overflow".to_string())?;
        let raw_limit = if record_index == 0 {
            MAX_DELIMITED_HEADER_BYTES
        } else {
            MAX_DELIMITED_RECORD_BYTES
        };
        if raw_record_bytes > raw_limit {
            let kind = if record_index == 0 {
                "header"
            } else {
                "record"
            };
            return Err(format!(
                "{kind}_too_large: record {record_index} exceeds {raw_limit} decoded bytes"
            ));
        }

        let remaining = MAX_INSPECT_DECODED_BYTES - decoded_sample_bytes;
        let can_add_decoded_byte = record_index == 0 || logical_record_bytes < remaining;
        let mut finish = false;
        let mut finish_with_cr = false;
        match state {
            InspectQuoteState::FieldStart => match byte {
                b'"' => state = InspectQuoteState::Quoted,
                value if value == delimiter => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    push_inspect_field(&mut record, &mut field, headers.as_deref())?;
                }
                b'\n' => finish = true,
                b'\r' => {
                    finish = true;
                    finish_with_cr = true;
                }
                _ => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    field.push(byte);
                    state = InspectQuoteState::Unquoted;
                }
            },
            InspectQuoteState::Unquoted => match byte {
                b'\n' => finish = true,
                b'\r' => {
                    finish = true;
                    finish_with_cr = true;
                }
                value if value == delimiter => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    push_inspect_field(&mut record, &mut field, headers.as_deref())?;
                    state = InspectQuoteState::FieldStart;
                }
                b'"' if !can_add_decoded_byte => break,
                b'"' => {
                    return Err(format!(
                        "invalid_csv_framing: quote inside an unquoted field in record {record_index}"
                    ));
                }
                _ => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    field.push(byte);
                }
            },
            InspectQuoteState::Quoted => match byte {
                b'"' => state = InspectQuoteState::QuoteClosed,
                _ => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    field.push(byte);
                }
            },
            InspectQuoteState::QuoteClosed => match byte {
                b'"' => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    field.push(b'"');
                    state = InspectQuoteState::Quoted;
                }
                value if value == delimiter => {
                    if !can_add_decoded_byte {
                        break;
                    }
                    logical_record_bytes += 1;
                    push_inspect_field(&mut record, &mut field, headers.as_deref())?;
                    state = InspectQuoteState::FieldStart;
                }
                b'\n' => finish = true,
                b'\r' => {
                    finish = true;
                    finish_with_cr = true;
                }
                _ if !can_add_decoded_byte => break,
                _ => {
                    return Err(format!(
                        "invalid_csv_framing: characters after a closing quote in record {record_index}"
                    ));
                }
            },
        }

        if finish {
            finish_inspect_record(
                &mut record,
                &mut field,
                record_index,
                logical_record_bytes,
                &mut headers,
                &mut samples,
                &mut decoded_sample_bytes,
            )?;
            record_index = record_index
                .checked_add(1)
                .ok_or_else(|| "delimited source contains too many records".to_string())?;
            raw_record_bytes = 0;
            logical_record_bytes = 0;
            state = InspectQuoteState::FieldStart;
            previous_was_record_cr = finish_with_cr;
            if samples.len() >= sample_rows {
                break;
            }
        }
    }

    let headers = headers.ok_or_else(|| "missing_header: delimited source is empty".to_string())?;
    Ok((headers, samples, decoded_sample_bytes))
}

fn push_inspect_field(
    record: &mut ByteRecord,
    field: &mut Vec<u8>,
    headers: Option<&[String]>,
) -> Result<(), String> {
    if let Some(headers) = headers {
        if record.len() >= headers.len() {
            return Err(
                "invalid_record_width: delimited record width does not match the header".into(),
            );
        }
    }
    record.push_field(field);
    *field = Vec::new();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_inspect_record(
    record: &mut ByteRecord,
    field: &mut Vec<u8>,
    record_index: u64,
    logical_record_bytes: usize,
    headers: &mut Option<Vec<String>>,
    samples: &mut Vec<ByteRecord>,
    decoded_sample_bytes: &mut usize,
) -> Result<(), String> {
    push_inspect_field(record, field, headers.as_deref())?;
    if record_index == 0 {
        *headers = Some(validate_inspect_headers(record)?);
        *record = ByteRecord::new();
    } else {
        let expected = headers
            .as_ref()
            .expect("header precedes data records")
            .len();
        if record.len() != expected {
            return Err(
                "invalid_record_width: delimited record width does not match the header".into(),
            );
        }
        *decoded_sample_bytes = decoded_sample_bytes
            .checked_add(logical_record_bytes)
            .ok_or_else(|| "decoded sample size overflow".to_string())?;
        debug_assert!(*decoded_sample_bytes <= MAX_INSPECT_DECODED_BYTES);
        samples.push(std::mem::take(record));
    }
    Ok(())
}

fn validate_inspect_headers(record: &ByteRecord) -> Result<Vec<String>, String> {
    const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";
    if record.is_empty() {
        return Err("missing_header: delimited source header is empty".into());
    }
    if record.len() > MAX_DELIMITED_HEADER_FIELDS {
        return Err(format!(
            "too_many_header_fields: header exceeds {MAX_DELIMITED_HEADER_FIELDS} fields"
        ));
    }
    let mut headers = Vec::with_capacity(record.len());
    let mut unique = BTreeSet::new();
    for (index, raw) in record.iter().enumerate() {
        let raw = if index == 0 {
            raw.strip_prefix(UTF8_BOM).unwrap_or(raw)
        } else {
            raw
        };
        let header = std::str::from_utf8(raw)
            .map_err(|_| "invalid_header_utf8: header must be valid UTF-8".to_string())?;
        if header.is_empty() {
            return Err(format!("empty_header: header field {} is empty", index + 1));
        }
        if header.len() > 256 {
            return Err(format!(
                "header_field_too_long: header field {} exceeds 256 UTF-8 bytes",
                index + 1
            ));
        }
        if header
            .chars()
            .any(|character| matches!(character, '\0' | '\n' | '\r'))
        {
            return Err(format!(
                "invalid_header: header field {} contains a control character",
                index + 1
            ));
        }
        if !unique.insert(header.to_string()) {
            return Err(format!(
                "duplicate_header: header field {header:?} is repeated"
            ));
        }
        headers.push(header.to_string());
    }
    Ok(headers)
}

pub fn inspect_hail(
    source: &str,
    explicit: &MappingInput,
    sample_rows: u64,
) -> Result<AssociationInspectReport, String> {
    if sample_rows == 0 || sample_rows > MAX_INSPECT_RECORDS {
        return Err(format!("sample rows must be in 1..={MAX_INSPECT_RECORDS}"));
    }

    let inspection = inspect_hail_schema(source).map_err(inspect_source_error)?;
    let headers: Vec<String> = inspection
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect();
    let types: Vec<String> = inspection
        .fields
        .iter()
        .map(|field| field.data_type.clone().unwrap_or_else(|| "unknown".into()))
        .collect();
    validate_explicit_headers(&headers, explicit)?;

    let mut resolved = explicit.clone();
    let mut ambiguities = BTreeMap::new();
    infer_hail_mappings(&headers, &types, &mut resolved, &mut ambiguities);
    resolved
        .validate_partial()
        .map_err(|error| error.to_string())?;

    // Logical locus metadata is informative only. It never fills --build, but
    // an explicit conflict must fail before any row is scanned.
    if let Some(build) = resolved.assembly {
        let uses_logical_locus = [LogicalField::Contig, LogicalField::Position]
            .into_iter()
            .filter_map(|field| resolved.fields.get(&field))
            .any(|path| path.starts_with("locus."));
        if uses_logical_locus
            && inspection.reference_genomes.len() == 1
            && inspection.reference_genomes[0] != build.as_str()
        {
            return Err(format!(
                "--build {build} conflicts with mapped Hail locus assembly {}",
                inspection.reference_genomes[0]
            ));
        }
    }

    let unresolved = resolved.unresolved_required();
    let normalized = if unresolved.is_empty() && ambiguities.is_empty() {
        Some(resolved.complete().map_err(|error| error.to_string())?)
    } else {
        None
    };

    let mut inspected_records = 0_u64;
    let mut decoded_sample_bytes = 0_usize;
    let mut diagnostics = RejectionDiagnostics::default();
    if let Some(mapping) = &normalized {
        let assembly = resolved
            .assembly
            .expect("a complete inspection mapping has an assembly");
        let mut hail = HailAssociationSource::open(source, mapping.clone(), assembly.as_str())
            .map_err(inspect_source_error)?;
        // Size the exact decode-time projection before normalization can shrink
        // a mapped value into a bounded rejection diagnostic.
        let projected_sizes = bounded_hail_projected_sizes(source, mapping, sample_rows as usize)?;
        for projected_size in projected_sizes {
            let item = hail
                .next_item()
                .map_err(inspect_source_error)?
                .ok_or_else(|| "Hail source changed while it was being inspected".to_string())?;
            decoded_sample_bytes = decoded_sample_bytes
                .checked_add(projected_size)
                .ok_or_else(|| "decoded Hail sample size overflow".to_string())?;
            inspected_records += 1;
            if let AssociationSourceItem::Rejected(rejection) = item {
                diagnostics.record(&rejection);
            }
        }
    }

    let runnable_view_command = normalized.as_ref().map(|mapping| {
        build_command(
            "view",
            source,
            resolved.assembly,
            mapping_pairs(mapping)
                .into_iter()
                .map(|(logical, source)| (logical, source.to_string()))
                .collect(),
        )
    });
    let resolution_commands = ambiguity_commands(source, &resolved, &ambiguities);
    let explicit_mappings = display_mappings(&explicit.fields);
    let inferred_mappings = resolved
        .fields
        .iter()
        .filter(|(logical, _)| !explicit.fields.contains_key(logical))
        .map(|(logical, source)| (logical.to_string(), source.clone()))
        .collect();

    Ok(AssociationInspectReport {
        source_kind: "hail".into(),
        inspected_records,
        decoded_sample_bytes,
        declared_logical_assemblies: inspection.reference_genomes,
        fields: inspection
            .fields
            .into_iter()
            .map(|field| InspectFieldReport {
                name: field.name,
                data_type: field.data_type.unwrap_or_else(|| "unknown".into()),
            })
            .collect(),
        explicit_mappings,
        inferred_mappings,
        mappings: display_mappings(&resolved.fields),
        ambiguities,
        unresolved,
        invalid_reasons: diagnostics
            .reasons
            .into_iter()
            .map(|(reason, details)| {
                (
                    reason,
                    InspectReasonReport {
                        count: details.count,
                        examples: details.examples,
                    },
                )
            })
            .collect(),
        ready: normalized.is_some(),
        runnable_view_command,
        resolution_commands,
    })
}

fn bounded_hail_projected_sizes(
    source: &str,
    mapping: &NormalizedFieldMappingV1,
    sample_rows: usize,
) -> Result<Vec<usize>, String> {
    let projections = mapping_pairs(mapping)
        .into_iter()
        .map(|(logical, source)| {
            FieldPath::parse(source).map_err(|error| {
                format!("invalid mapped Hail path {source:?} for {logical}: {error}")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let engine = QueryEngine::open_path(source).map_err(|error| format!("open_hail: {error}"))?;
    let rows = engine
        .query_iter_deterministic_with_projection(
            &[],
            None,
            Some(Arc::new(ProjectionTree::from_fields(&projections))),
        )
        .map_err(|error| format!("hail_query: {error}"))?
        .map(|row| row.map_err(|error| format!("hail_decode: {error}")));
    collect_bounded_hail_projected_sizes(rows, sample_rows)
}

fn collect_bounded_hail_projected_sizes(
    rows: impl Iterator<Item = Result<EncodedValue, String>>,
    sample_rows: usize,
) -> Result<Vec<usize>, String> {
    let mut sizes = Vec::new();
    let mut decoded_bytes = 0_usize;
    for row in rows.take(sample_rows) {
        let size = hail_projected_value_bytes(&row?)?;
        if size > MAX_INSPECT_DECODED_BYTES - decoded_bytes {
            break;
        }
        decoded_bytes = decoded_bytes
            .checked_add(size)
            .ok_or_else(|| "decoded Hail sample size overflow".to_string())?;
        sizes.push(size);
    }
    Ok(sizes)
}

fn hail_projected_value_bytes(value: &EncodedValue) -> Result<usize, String> {
    match value {
        EncodedValue::Null => Ok(0),
        EncodedValue::Binary(bytes) => Ok(bytes.len()),
        EncodedValue::Int32(_) | EncodedValue::Float32(_) => Ok(4),
        EncodedValue::Int64(_) | EncodedValue::Float64(_) => Ok(8),
        EncodedValue::Boolean(_) => Ok(1),
        EncodedValue::Struct(fields) => {
            sum_hail_projected_value_bytes(fields.iter().map(|(_, value)| value))
        }
        EncodedValue::Array(values) => sum_hail_projected_value_bytes(values.iter()),
    }
}

fn sum_hail_projected_value_bytes<'a>(
    values: impl Iterator<Item = &'a EncodedValue>,
) -> Result<usize, String> {
    values.fold(Ok(0_usize), |total, value| {
        total?
            .checked_add(hail_projected_value_bytes(value)?)
            .ok_or_else(|| "decoded Hail projected value size overflow".to_string())
    })
}

pub fn print_human_report(report: &AssociationInspectReport) {
    println!("Association source: {}", report.source_kind);
    if !report.declared_logical_assemblies.is_empty() {
        println!(
            "Declared logical assembly: {} (reported only; never inferred)",
            report.declared_logical_assemblies.join(", ")
        );
    }
    println!(
        "Inspected: {} records, {} decoded bytes (limits: {} records, {} bytes)",
        report.inspected_records,
        report.decoded_sample_bytes,
        MAX_INSPECT_RECORDS,
        MAX_INSPECT_DECODED_BYTES
    );
    println!("Fields:");
    for field in &report.fields {
        println!("  {} ({})", field.name, field.data_type);
    }
    print_mapping_section("Explicit mappings", &report.explicit_mappings);
    print_mapping_section("Inferred mappings", &report.inferred_mappings);
    if !report.ambiguities.is_empty() {
        println!("Ambiguities:");
        for (logical, candidates) in &report.ambiguities {
            println!("  {logical}: {}", candidates.join(", "));
        }
    }
    if !report.unresolved.is_empty() {
        println!("Unresolved: {}", report.unresolved.join(", "));
    }
    println!("Invalid sampled rows:");
    if report.invalid_reasons.is_empty() {
        println!("  (none)");
    } else {
        for (reason, details) in &report.invalid_reasons {
            if details.examples.is_empty() {
                println!("  {reason}: {}", details.count);
            } else {
                println!(
                    "  {reason}: {} (examples: {})",
                    details.count,
                    details.examples.join(", ")
                );
            }
        }
    }
    println!("Ready: {}", if report.ready { "yes" } else { "no" });
    if let Some(command) = &report.runnable_view_command {
        println!("Runnable command:");
        println!("  {command}");
    }
    if !report.resolution_commands.is_empty() {
        println!("Resolve ambiguity with one of:");
        for command in &report.resolution_commands {
            println!("  {command}");
        }
    }
}

fn inspect_source_error(error: AssociationSourceError) -> String {
    format!("{}: {}", error.code(), error.internal_message())
}

fn print_mapping_section(label: &str, mappings: &BTreeMap<String, String>) {
    println!("{label}:");
    if mappings.is_empty() {
        println!("  (none)");
    } else {
        for (logical, source) in mappings {
            println!("  {logical} = {source}");
        }
    }
}

fn display_mappings(mappings: &BTreeMap<LogicalField, String>) -> BTreeMap<String, String> {
    mappings
        .iter()
        .map(|(logical, source)| (logical.to_string(), source.clone()))
        .collect()
}

fn validate_explicit_headers(headers: &[String], explicit: &MappingInput) -> Result<(), String> {
    let available: BTreeSet<&str> = headers.iter().map(String::as_str).collect();
    for (logical, source) in &explicit.fields {
        if !available.contains(source.as_str()) {
            return Err(format!(
                "mapped source field {source:?} for {logical} is not in the header"
            ));
        }
    }
    Ok(())
}

fn infer_types(headers: &[String], samples: &[ByteRecord]) -> Vec<String> {
    (0..headers.len())
        .map(|index| {
            let values: Vec<&[u8]> = samples
                .iter()
                .filter_map(|record| record.get(index))
                .filter(|value| !value.is_empty())
                .collect();
            if values.is_empty() {
                "empty".to_string()
            } else if values.iter().all(|value| {
                std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .is_some()
            }) {
                "integer".to_string()
            } else if values.iter().all(|value| {
                std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some()
            }) {
                "number".to_string()
            } else if values.iter().any(|value| {
                std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some()
            }) {
                "mixed".to_string()
            } else {
                "text".to_string()
            }
        })
        .collect()
}

fn infer_mappings(
    headers: &[String],
    types: &[String],
    mapping: &mut MappingInput,
    ambiguities: &mut BTreeMap<String, Vec<String>>,
) {
    let mut already_used: BTreeSet<String> = mapping.fields.values().cloned().collect();
    for logical in LOGICAL_FIELDS {
        if mapping.fields.contains_key(&logical) {
            continue;
        }
        let aliases = aliases(logical);
        let mut candidates: Vec<String> = headers
            .iter()
            .zip(types)
            .filter(|(header, data_type)| {
                !already_used.contains(header.as_str())
                    && aliases.contains(&normalize_name(header))
                    && type_compatible(logical, data_type)
            })
            .map(|(header, _)| header.clone())
            .collect();
        candidates.sort();
        candidates.dedup();
        match candidates.len() {
            0 => {}
            1 => {
                let source = candidates.remove(0);
                already_used.insert(source.clone());
                mapping.fields.insert(logical, source);
            }
            _ => {
                ambiguities.insert(logical.to_string(), candidates);
            }
        }
    }
}

fn type_compatible(logical: LogicalField, data_type: &str) -> bool {
    match logical {
        LogicalField::Position => matches!(data_type, "integer" | "mixed"),
        LogicalField::Pvalue
        | LogicalField::NegLog10P
        | LogicalField::Beta
        | LogicalField::Se
        | LogicalField::Af
        | LogicalField::Ac => matches!(data_type, "integer" | "number" | "mixed"),
        _ => data_type != "empty",
    }
}

/// Hail reports exact dotted leaf paths and schema types rather than the
/// sampled flat-file type labels. Keep this inference separate so adding Hail
/// inspection cannot change existing delimited inference semantics.
fn infer_hail_mappings(
    paths: &[String],
    types: &[String],
    mapping: &mut MappingInput,
    ambiguities: &mut BTreeMap<String, Vec<String>>,
) {
    let mut already_used: BTreeSet<String> = mapping.fields.values().cloned().collect();
    for logical in LOGICAL_FIELDS {
        if mapping.fields.contains_key(&logical) {
            continue;
        }
        let aliases = aliases(logical);
        let mut candidates: Vec<String> = paths
            .iter()
            .zip(types)
            .filter(|(path, data_type)| {
                let leaf = path.rsplit('.').next().unwrap_or(path);
                !already_used.contains(path.as_str())
                    && aliases.contains(&normalize_name(leaf))
                    && hail_type_compatible(logical, data_type)
            })
            .map(|(path, _)| path.clone())
            .collect();
        candidates.sort();
        candidates.dedup();
        match candidates.len() {
            0 => {}
            1 => {
                let source = candidates.remove(0);
                already_used.insert(source.clone());
                mapping.fields.insert(logical, source);
            }
            _ => {
                ambiguities.insert(logical.to_string(), candidates);
            }
        }
    }
}

fn hail_type_compatible(logical: LogicalField, data_type: &str) -> bool {
    let logical_type = data_type
        .split_once(" (")
        .map_or(data_type, |(name, _)| name);
    match logical {
        LogicalField::Position => matches!(logical_type, "Int32" | "Int64"),
        LogicalField::Pvalue
        | LogicalField::NegLog10P
        | LogicalField::Beta
        | LogicalField::Se
        | LogicalField::Af
        | LogicalField::Ac => {
            matches!(logical_type, "Int32" | "Int64" | "Float32" | "Float64")
        }
        _ => matches!(logical_type, "String" | "Binary"),
    }
}

fn aliases(logical: LogicalField) -> BTreeSet<String> {
    let values: &[&str] = match logical {
        LogicalField::Contig => &["contig", "chrom", "chromosome", "chr"],
        LogicalField::Position => &["position", "pos", "bp", "base_pair_location"],
        LogicalField::Ref => &["ref", "reference", "reference_allele", "other_allele"],
        LogicalField::Alt => &["alt", "alternate", "alternate_allele", "effect_allele"],
        LogicalField::Pvalue => &["p", "pvalue", "p_value", "pval", "p_value_nominal"],
        LogicalField::NegLog10P => &[
            "neg_log10_p",
            "minus_log10_p",
            "mlog10p",
            "pvalue_log10",
            "p_log10",
        ],
        LogicalField::Beta => &["beta", "effect", "effect_size", "estimate"],
        LogicalField::Se => &["se", "standard_error", "stderr"],
        LogicalField::Af => &["af", "allele_frequency", "effect_allele_frequency", "eaf"],
        LogicalField::Ac => &["ac", "allele_count", "effect_allele_count"],
        LogicalField::Gene => &["gene", "gene_symbol", "symbol"],
        LogicalField::Consequence => &["consequence", "annotation", "most_severe_consequence"],
        LogicalField::Series => &["series", "phenotype", "trait"],
    };
    values.iter().map(|value| normalize_name(value)).collect()
}

fn normalize_name(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn inspect_invalid_rows(
    headers: &[String],
    mapping: &NormalizedFieldMappingV1,
    samples: &[ByteRecord],
) -> Result<BTreeMap<String, InspectReasonReport>, String> {
    let indexes = mapped_indexes(headers, mapping).map_err(|error| error.to_string())?;
    let mut diagnostics = RejectionDiagnostics::default();
    for (ordinal, record) in samples.iter().enumerate() {
        match decode_record(&indexes, record, ordinal as u64).map_err(|error| error.to_string())? {
            Ok(_) => {}
            Err(rejection) => diagnostics.record(&rejection),
        }
    }
    Ok(diagnostics
        .reasons
        .into_iter()
        .map(|(reason, details)| {
            (
                reason,
                InspectReasonReport {
                    count: details.count,
                    examples: details
                        .examples
                        .into_iter()
                        .map(|example| bounded_visible(&example))
                        .collect(),
                },
            )
        })
        .collect())
}

fn bounded_visible(value: &str) -> String {
    const MAX_VISIBLE: usize = 80;
    let escaped: String = value.chars().flat_map(char::escape_default).collect();
    if escaped.len() <= MAX_VISIBLE {
        return escaped;
    }
    let mut end = MAX_VISIBLE;
    while !escaped.is_char_boundary(end) {
        end -= 1;
    }
    escaped[..end].to_string()
}

fn ambiguity_commands(
    source: &str,
    mapping: &MappingInput,
    ambiguities: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let explicit: Vec<(LogicalField, String)> = mapping
        .fields
        .iter()
        .map(|(logical, source)| (*logical, source.clone()))
        .collect();
    let mut commands = Vec::new();
    for (logical, candidates) in ambiguities {
        let Ok(logical) = logical.parse::<LogicalField>() else {
            continue;
        };
        for candidate in candidates {
            let mut fields = explicit.clone();
            fields.retain(|(field, _)| *field != logical);
            fields.push((logical, candidate.clone()));
            commands.push(build_command("inspect", source, mapping.assembly, fields));
        }
    }
    commands
}

fn build_command(
    action: &str,
    source: &str,
    assembly: Option<crate::cli::AssociationBuildArg>,
    mut fields: Vec<(LogicalField, String)>,
) -> String {
    fields.sort_by_key(|(logical, _)| *logical);
    let mut arguments = vec![
        "genohype".to_string(),
        "association".to_string(),
        action.to_string(),
        source.to_string(),
    ];
    if let Some(assembly) = assembly {
        arguments.push("--build".to_string());
        arguments.push(assembly.to_string());
    }

    // Clap comma-splits --map values and the mapping parser reserves the first
    // '=' as the logical/source separator. Use ordinary repeated arguments when
    // they are exact, and a strict inline TOML mapping on stdin otherwise.
    let needs_mapping_file = fields
        .iter()
        .any(|(_, source)| source.contains(',') || source.contains('='));
    let mapping_file = needs_mapping_file.then(|| inline_mapping_file(&fields));
    if needs_mapping_file {
        arguments.push("--mapping-file".to_string());
        arguments.push("/dev/stdin".to_string());
    } else {
        for (logical, source) in fields {
            arguments.push("--map".to_string());
            arguments.push(format!("{logical}={source}"));
        }
    }
    let command = arguments
        .into_iter()
        .map(|argument| shell_quote(&argument))
        .collect::<Vec<_>>()
        .join(" ");
    match mapping_file {
        Some(mapping_file) => format!("printf %s {} | {command}", shell_quote(&mapping_file)),
        None => command,
    }
}

#[derive(Serialize)]
struct InlineMappingFile {
    schema_version: &'static str,
    fields: BTreeMap<String, String>,
}

fn inline_mapping_file(fields: &[(LogicalField, String)]) -> String {
    toml::to_string(&InlineMappingFile {
        schema_version: "1.0",
        fields: fields
            .iter()
            .map(|(logical, source)| (logical.to_string(), source.clone()))
            .collect(),
    })
    .expect("string-only inline association mapping is TOML serializable")
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':' | b'=')
        })
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::AssociationBuildArg;
    use tempfile::tempdir;

    fn inspect(contents: &[u8], input: MappingInput) -> AssociationInspectReport {
        let temp = tempdir().unwrap();
        let path = temp.path().join("results.tsv");
        std::fs::write(&path, contents).unwrap();
        inspect_delimited(
            path.to_str().unwrap(),
            DetectedAssociationSource::Delimited {
                delimiter: b'\t',
                gzip: false,
            },
            &input,
            100,
        )
        .unwrap()
    }

    #[test]
    fn bounded_hail_inspect_reuses_report_contract_without_inferring_assembly() {
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../data/test/association/single-variant.ht");
        let source = source.to_str().unwrap();

        let report = inspect_hail(source, &MappingInput::default(), 100).unwrap();
        assert_eq!(report.source_kind, "hail");
        assert_eq!(report.declared_logical_assemblies, ["GRCh38"]);
        assert!(report.unresolved.contains(&"assembly".to_string()));
        assert_eq!(report.inspected_records, 0);
        assert!(!report.ready);

        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);
        let report = inspect_hail(source, &input, 100).unwrap();
        assert!(report.ready);
        assert_eq!(report.inspected_records, 12);
        assert_eq!(report.invalid_reasons["invalid_pvalue"].count, 2);
        assert!(report
            .runnable_view_command
            .as_deref()
            .unwrap()
            .contains("contig=locus.contig"));

        input.assembly = Some(AssociationBuildArg::Grch37);
        assert!(inspect_hail(source, &input, 100)
            .unwrap_err()
            .contains("conflicts"));
    }

    #[test]
    fn hail_budget_charges_large_projected_strings_before_diagnostic_bounding() {
        let first_size = 700_000;
        let second_size = MAX_INSPECT_DECODED_BYTES - first_size + 1;
        let rows = vec![
            Ok(EncodedValue::Struct(vec![
                (
                    "mapped_large_string".into(),
                    EncodedValue::Binary(vec![b'x'; first_size]),
                ),
                (
                    "unmapped_names_are_not_charged".repeat(100),
                    EncodedValue::Null,
                ),
            ])),
            Ok(EncodedValue::Struct(vec![(
                "mapped_large_string".into(),
                EncodedValue::Binary(vec![b'y'; second_size]),
            )])),
        ];
        let sizes = collect_bounded_hail_projected_sizes(rows.into_iter(), 100).unwrap();
        assert_eq!(sizes, [first_size]);
        assert_eq!(sizes.iter().sum::<usize>(), first_size);

        let exact = collect_bounded_hail_projected_sizes(
            vec![Ok(EncodedValue::Binary(vec![
                b'z';
                MAX_INSPECT_DECODED_BYTES
            ]))]
            .into_iter(),
            100,
        )
        .unwrap();
        assert_eq!(exact, [MAX_INSPECT_DECODED_BYTES]);
    }

    #[test]
    fn inference_retains_exact_spelling_and_never_infers_assembly() {
        let report = inspect(
            b"Chromosome\tBase-Pair Location\tP_VALUE\tBETA\nchr1\t1\t0.5\t1.0\n",
            MappingInput::default(),
        );
        assert_eq!(report.mappings["contig"], "Chromosome");
        assert_eq!(report.mappings["position"], "Base-Pair Location");
        assert_eq!(report.mappings["pvalue"], "P_VALUE");
        assert!(report.explicit_mappings.is_empty());
        assert_eq!(report.inferred_mappings, report.mappings);
        assert!(report.unresolved.contains(&"assembly".to_string()));
        assert!(!report.ready);
    }

    #[test]
    fn pvalue_ambiguity_is_reported_without_preference() {
        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);
        let report = inspect(b"CHR\tPOS\tP\tPVAL\n1\t1\t.5\t.4\n", input);
        assert_eq!(report.ambiguities["pvalue"], ["P", "PVAL"]);
        assert!(!report.ready);
        assert_eq!(report.resolution_commands.len(), 2);
        assert!(report.runnable_view_command.is_none());
    }

    #[test]
    fn ready_report_has_deterministic_runnable_command_and_invalid_examples() {
        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);
        let report = inspect(
            b"CHR\tPOS\tP\tBETA\nchr1\t1\t0.5\t1\nchr1\tbad\t0.4\t2\nchr1\t3\t2\t3\n",
            input,
        );
        assert!(report.ready);
        assert_eq!(report.invalid_reasons["invalid_position"].count, 1);
        assert_eq!(report.invalid_reasons["invalid_pvalue"].count, 1);
        let command = report.runnable_view_command.unwrap();
        assert!(command.starts_with("genohype association view "));
        assert!(command.contains("--build GRCh38"));
        assert!(command.contains("--map contig=CHR"));
        assert!(command.contains("--map position=POS"));
        assert!(command.contains("--map pvalue=P"));
        assert!(command.contains("--map beta=BETA"));
    }

    #[test]
    fn invalid_examples_are_bounded_per_reason_and_by_visible_bytes() {
        let mut rows = b"CHR\tPOS\tP\n".to_vec();
        for ordinal in 0..10 {
            rows.extend_from_slice(
                format!("chr1\tbad-position-{ordinal}-{}\t0.5\n", "x".repeat(100)).as_bytes(),
            );
        }
        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);
        input.fields.insert(LogicalField::Position, "POS".into());
        let report = inspect(&rows, input);
        assert_eq!(report.explicit_mappings["position"], "POS");
        assert!(!report.inferred_mappings.contains_key("position"));
        let reason = &report.invalid_reasons["invalid_position"];
        assert_eq!(reason.count, 10);
        assert_eq!(reason.examples.len(), 5);
        assert!(reason.examples.iter().all(|example| example.len() <= 80));
    }

    #[test]
    fn sample_records_and_decoded_bytes_are_hard_bounded() {
        let mut rows = b"CHR\tPOS\tP\n".to_vec();
        for ordinal in 0..150 {
            rows.extend_from_slice(format!("chr1\t{}\t0.5\n", ordinal + 1).as_bytes());
        }
        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);
        let report = inspect(&rows, input);
        assert_eq!(report.inspected_records, 100);
        assert!(report.decoded_sample_bytes <= MAX_INSPECT_DECODED_BYTES);
    }

    #[test]
    fn decoded_sample_budget_is_exact_and_does_not_observe_the_next_record() {
        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);

        let first_extra = 700_000;
        let first_size = 11 + first_extra;
        let remaining = MAX_INSPECT_DECODED_BYTES - first_size;
        let mut rows = b"CHR\tPOS\tP\tEXTRA\nchr1\t1\t0.5\t".to_vec();
        rows.extend(std::iter::repeat_n(b'x', first_extra));
        rows.extend_from_slice(b"\nchr2\t2\t0.4\t");
        rows.extend(std::iter::repeat_n(b'y', remaining - 11));
        // This quote is a framing error if the second record is decoded. It is
        // the first byte beyond the remaining aggregate sample budget.
        rows.extend_from_slice(b"\"after-budget\n");
        let report = inspect(&rows, input);
        assert_eq!(report.inspected_records, 1);
        assert_eq!(report.decoded_sample_bytes, first_size);

        let first_extra = 500_000;
        let second_extra = MAX_INSPECT_DECODED_BYTES - (11 + first_extra) - 11;
        let mut exact = b"CHR\tPOS\tP\tEXTRA\nchr1\t1\t0.5\t".to_vec();
        exact.extend(std::iter::repeat_n(b'x', first_extra));
        exact.extend_from_slice(b"\nchr2\t2\t0.4\t");
        exact.extend(std::iter::repeat_n(b'y', second_extra));
        let report = inspect(&exact, MappingInput::default());
        assert_eq!(report.inspected_records, 2);
        assert_eq!(report.decoded_sample_bytes, MAX_INSPECT_DECODED_BYTES);
    }

    #[test]
    fn delimited_inspect_preserves_the_independent_record_cap() {
        let mut rows = b"CHR\tPOS\tP\tEXTRA\nchr1\t1\t0.5\t".to_vec();
        rows.extend(std::iter::repeat_n(b'x', MAX_DELIMITED_RECORD_BYTES));
        let temp = tempdir().unwrap();
        let path = temp.path().join("oversized.tsv");
        std::fs::write(&path, rows).unwrap();
        let error = inspect_delimited(
            path.to_str().unwrap(),
            DetectedAssociationSource::Delimited {
                delimiter: b'\t',
                gzip: false,
            },
            &MappingInput::default(),
            100,
        )
        .unwrap_err();
        assert!(error.contains("record_too_large"));
    }

    #[cfg(unix)]
    #[test]
    fn emitted_command_round_trips_punctuation_through_shell_clap_and_mapping_load() {
        use crate::association_sources::mapping::load_mapping;
        use crate::cli::{AssociationCommands, Cli, Commands};
        use clap::Parser;
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;

        let fields = vec![
            (LogicalField::Contig, "CHR".to_string()),
            (LogicalField::Position, "POS=raw".to_string()),
            (LogicalField::Pvalue, "P,VAL".to_string()),
            (LogicalField::Beta, "effect size".to_string()),
            (LogicalField::Gene, "Gene's \"label\"".to_string()),
        ];
        let temp = tempdir().unwrap();
        let source = temp.path().join("source file's.csv");
        std::fs::write(
            &source,
            b"CHR,\"POS=raw\",\"P,VAL\",\"effect size\",\"Gene's \"\"label\"\"\"\nchr1,1,0.5,1,G\n",
        )
        .unwrap();
        let mut input = MappingInput::default();
        input.assembly = Some(AssociationBuildArg::Grch38);
        input
            .fields
            .insert(LogicalField::Position, "POS=raw".into());
        input
            .fields
            .insert(LogicalField::Gene, "Gene's \"label\"".into());
        let report = inspect_delimited(
            source.to_str().unwrap(),
            DetectedAssociationSource::Delimited {
                delimiter: b',',
                gzip: false,
            },
            &input,
            100,
        )
        .unwrap();
        assert!(report.ready);
        let command = report.runnable_view_command.unwrap();
        assert!(command.contains("--mapping-file /dev/stdin"));

        let executable = temp.path().join("genohype");
        let captured_args = temp.path().join("args");
        let captured_stdin = temp.path().join("stdin");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CAPTURE_ARGS\"\ncat > \"$CAPTURE_STDIN\"\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let path = format!(
            "{}:{}",
            temp.path().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let status = Command::new("sh")
            .arg("-c")
            .arg(&command)
            .env("PATH", path)
            .env("CAPTURE_ARGS", &captured_args)
            .env("CAPTURE_STDIN", &captured_stdin)
            .status()
            .unwrap();
        assert!(status.success());

        let arguments = std::fs::read_to_string(&captured_args).unwrap();
        let cli =
            Cli::try_parse_from(std::iter::once("genohype").chain(arguments.lines())).unwrap();
        let Commands::Association { command } = cli.command else {
            panic!("expected association command");
        };
        let AssociationCommands::View(mut args) = command else {
            panic!("expected view command");
        };
        assert_eq!(args.source, source.to_str().unwrap());
        assert_eq!(
            args.mapping.mapping_file.as_deref(),
            Some(Path::new("/dev/stdin"))
        );
        args.mapping.mapping_file = Some(captured_stdin);
        let loaded = load_mapping(&args.mapping, Some(args.build)).unwrap();
        for (logical, source) in fields {
            assert_eq!(loaded.fields[&logical], source);
        }
    }

    #[test]
    fn shell_quoting_handles_spaces_and_single_quotes() {
        assert_eq!(shell_quote("simple.tsv"), "simple.tsv");
        assert_eq!(shell_quote("a b.tsv"), "'a b.tsv'");
        assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
    }
}
