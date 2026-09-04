use super::delimited::{decode_record, mapped_indexes, DelimitedRecordReader};
use super::hail::{inspect_hail_schema, HailAssociationSource};
use super::mapping::{mapping_pairs, LogicalField, MappingInput, LOGICAL_FIELDS};
use super::DetectedAssociationSource;
use csv::ByteRecord;
use genohype_association::{
    AssociationSource, AssociationSourceError, AssociationSourceItem, NormalizedFieldMappingV1,
    RejectionDiagnostics,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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
    let mut reader = DelimitedRecordReader::open(Path::new(source), detected)
        .map_err(|error| error.to_string())?;
    validate_explicit_headers(reader.headers(), explicit)?;

    let mut samples = Vec::new();
    let mut decoded_sample_bytes = 0_usize;
    let mut record = ByteRecord::new();
    while samples.len() < sample_rows as usize {
        if !reader
            .next_record(&mut record)
            .map_err(|error| error.to_string())?
        {
            break;
        }
        let record_bytes = decoded_record_size(&record);
        if record_bytes > MAX_INSPECT_DECODED_BYTES.saturating_sub(decoded_sample_bytes) {
            break;
        }
        decoded_sample_bytes += record_bytes;
        samples.push(record.clone());
    }

    let types = infer_types(reader.headers(), &samples);
    let mut resolved = explicit.clone();
    let mut ambiguities = BTreeMap::new();
    infer_mappings(reader.headers(), &types, &mut resolved, &mut ambiguities);
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
        inspect_invalid_rows(reader.headers(), mapping, &samples)?
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
        fields: reader
            .headers()
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
        while inspected_records < sample_rows {
            let Some(item) = hail.next_item().map_err(inspect_source_error)? else {
                break;
            };
            let size = match &item {
                AssociationSourceItem::Point { point, warnings } => {
                    serde_json::to_vec(&(point, warnings))
                }
                AssociationSourceItem::Rejected(rejection) => serde_json::to_vec(rejection),
            }
            .map_err(|error| error.to_string())?
            .len();
            if size > MAX_INSPECT_DECODED_BYTES.saturating_sub(decoded_sample_bytes) {
                break;
            }
            decoded_sample_bytes += size;
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

fn decoded_record_size(record: &ByteRecord) -> usize {
    record
        .iter()
        .map(|field| field.len())
        .sum::<usize>()
        .saturating_add(record.len().saturating_sub(1))
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
    if !fields.is_empty() {
        arguments.push("--map".to_string());
        arguments.push(
            fields
                .into_iter()
                .map(|(logical, source)| format!("{logical}={source}"))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    arguments
        .into_iter()
        .map(|argument| shell_quote(&argument))
        .collect::<Vec<_>>()
        .join(" ")
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
        assert!(command.contains("contig=CHR,position=POS,pvalue=P,beta=BETA"));
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
    fn shell_quoting_handles_spaces_and_single_quotes() {
        assert_eq!(shell_quote("simple.tsv"), "simple.tsv");
        assert_eq!(shell_quote("a b.tsv"), "'a b.tsv'");
        assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
    }
}
