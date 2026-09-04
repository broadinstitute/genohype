use super::inspect::{
    AssociationInspectReport, InspectFieldReport, InspectReasonReport, MAX_INSPECT_DECODED_BYTES,
    MAX_INSPECT_RECORDS,
};
use super::mapping::{mapping_pairs, LogicalField, MappingInput};
use super::DetectedAssociationSource;
use genohype_association::{
    normalize_significance, AssociationPointV1, AssociationSource, AssociationSourceError,
    AssociationSourceFieldV1, AssociationSourceItem, AssociationSourceMetadataV1,
    AssociationSourceRejectionV1, NormalizedFieldMappingV1, PValueError, RejectionDiagnostics,
    ValidationError,
};
use noodles::vcf;
use noodles::vcf::header::record::value::map::info::{Number, Type};
use noodles::vcf::variant::record::AlternateBases as _;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

/// Maximum decompressed VCF header bytes accepted before noodles allocates them.
pub const MAX_VCF_HEADER_BYTES: usize = 1024 * 1024;
/// Maximum decompressed bytes in one VCF record, including FORMAT/sample columns.
pub const MAX_VCF_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_VCF_OBSERVED_FIELDS: usize = 4_096;

const HEADER_BOUND_PREFIX: &str = "association_vcf_header_bound:";
const RECORD_BOUND_PREFIX: &str = "association_vcf_record_bound:";

type Input = BufReader<BoundedVcfReader<Box<dyn Read>>>;

#[derive(Debug, Default)]
struct RawStructuralTokens {
    chrom: String,
    position: String,
    reference: String,
    alternate: String,
}

/// Sequential lazy-record reader. The bounder sits below both noodles header and
/// record parsing, so an oversized header or FORMAT-heavy record is rejected
/// before noodles can grow its String past the corresponding limit. The raw line
/// buffer is likewise bounded and is retained only long enough to preserve the
/// exact four mapped structural tokens; INFO/FORMAT and the full row are dropped.
pub struct VcfRecordReader {
    reader: vcf::io::Reader<Input>,
    header: vcf::Header,
    raw_record: Vec<u8>,
    raw_structural: RawStructuralTokens,
}

impl VcfRecordReader {
    pub fn open(
        path: &Path,
        detected: DetectedAssociationSource,
    ) -> Result<Self, AssociationSourceError> {
        if detected != DetectedAssociationSource::Vcf {
            return Err(AssociationSourceError::new(
                "source_kind_mismatch",
                "VCF reader requires a .vcf, .vcf.gz, or .vcf.bgz source",
            ));
        }

        let file = File::open(path).map_err(|error| source_io("open", error))?;
        let name = path.to_string_lossy().to_ascii_lowercase();
        let input: Box<dyn Read> = if name.ends_with(".vcf.gz") || name.ends_with(".vcf.bgz") {
            // This is deliberately BGZF, not a generic gzip decoder. Ordinary
            // gzip streams fail while reading instead of being silently accepted.
            Box::new(noodles::bgzf::Reader::new(file))
        } else if name.ends_with(".vcf") {
            Box::new(BufReader::new(file))
        } else {
            return Err(AssociationSourceError::new(
                "unsupported_vcf_compression",
                "VCF input must end in .vcf, .vcf.gz, or .vcf.bgz",
            ));
        };

        let bounded = BoundedVcfReader::new(input, MAX_VCF_HEADER_BYTES, MAX_VCF_RECORD_BYTES);
        let mut reader = vcf::io::Reader::new(BufReader::new(bounded));
        let header = reader
            .read_header()
            .map_err(|error| vcf_io_error("header", error))?;
        validate_observed_fields(&header)?;
        Ok(Self {
            reader,
            header,
            raw_record: Vec::new(),
            raw_structural: RawStructuralTokens::default(),
        })
    }

    pub fn header(&self) -> &vcf::Header {
        &self.header
    }

    /// Reads one lazy record and returns its decompressed line size. FORMAT and
    /// sample values remain raw text in `vcf::Record` and are never decoded.
    pub fn next_record(
        &mut self,
        record: &mut vcf::Record,
    ) -> Result<Option<usize>, AssociationSourceError> {
        self.raw_record.clear();
        let size = self
            .reader
            .get_mut()
            .read_until(b'\n', &mut self.raw_record)
            .map_err(|error| vcf_io_error("record", error))?;
        if size == 0 {
            return Ok(None);
        }

        *record = vcf::Record::try_from(self.raw_record.as_slice())
            .map_err(|error| vcf_io_error("record", error))?;
        self.raw_structural = raw_structural_tokens(&self.raw_record)?;
        self.raw_record.clear();
        Ok(Some(size))
    }

    fn raw_structural(&self) -> &RawStructuralTokens {
        &self.raw_structural
    }
}

fn raw_structural_tokens(raw_record: &[u8]) -> Result<RawStructuralTokens, AssociationSourceError> {
    let line = raw_record.strip_suffix(b"\n").unwrap_or(raw_record);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let mut fields = line.split(|byte| *byte == b'\t');
    let mut next = || {
        fields.next().ok_or_else(|| {
            AssociationSourceError::new(
                "invalid_vcf_record",
                "VCF record does not contain all required structural fields",
            )
        })
    };
    let chrom = next()?;
    let position = next()?;
    let _id = next()?;
    let reference = next()?;
    let alternate = next()?;
    let decode = |value: &[u8]| {
        std::str::from_utf8(value).map(str::to_string).map_err(|_| {
            AssociationSourceError::new(
                "invalid_vcf_record",
                "VCF structural fields must contain valid UTF-8",
            )
        })
    };

    Ok(RawStructuralTokens {
        chrom: decode(chrom)?,
        position: decode(position)?,
        reference: decode(reference)?,
        alternate: decode(alternate)?,
    })
}

/// One-pass association source for plain and BGZF VCF. Source ordinals are a
/// stable, unique, zero-based sequence over emitted record/ALT items. A valid
/// multiallelic record consumes one ordinal per ALT; a record-level rejection
/// consumes one ordinal.
pub struct VcfAssociationSource {
    records: VcfRecordReader,
    prepared: PreparedMapping,
    metadata: AssociationSourceMetadataV1,
    record: vcf::Record,
    pending: VecDeque<AssociationSourceItem>,
    next_record_ordinal: u64,
    next_source_ordinal: u64,
    hasher: Option<Sha256>,
    fingerprint: Option<String>,
}

impl VcfAssociationSource {
    pub fn open(
        path: &Path,
        detected: DetectedAssociationSource,
        mapping: NormalizedFieldMappingV1,
        assembly: &str,
    ) -> Result<Self, AssociationSourceError> {
        if !matches!(assembly, "GRCh37" | "GRCh38") {
            return Err(AssociationSourceError::new(
                "invalid_assembly",
                "association assembly must be exactly GRCh37 or GRCh38",
            ));
        }
        let records = VcfRecordReader::open(path, detected)?;
        let (mapping, prepared) = prepare_mapping(mapping, records.header())?;
        let metadata = AssociationSourceMetadataV1 {
            kind: detected.source_kind(),
            observed_fields: observed_fields(records.header())?,
            normalized_mapping: mapping,
            assembly: assembly.to_string(),
            display_name: None,
        };
        metadata.validate()?;

        let mut hasher = Sha256::new();
        hasher.update(b"genohype.association.vcf-semantic-source.v1\0");
        Ok(Self {
            records,
            prepared,
            metadata,
            record: vcf::Record::default(),
            pending: VecDeque::new(),
            next_record_ordinal: 0,
            next_source_ordinal: 0,
            hasher: Some(hasher),
            fingerprint: None,
        })
    }
}

impl AssociationSource for VcfAssociationSource {
    fn metadata(&self) -> &AssociationSourceMetadataV1 {
        &self.metadata
    }

    fn next_item(&mut self) -> Result<Option<AssociationSourceItem>, AssociationSourceError> {
        if let Some(item) = self.pending.pop_front() {
            return Ok(Some(item));
        }
        if self.fingerprint.is_some() {
            return Ok(None);
        }

        loop {
            let Some(_) = self.records.next_record(&mut self.record)? else {
                let hasher = self.hasher.take().expect("hasher exists until EOF");
                self.fingerprint = Some(format!("sha256:{:x}", hasher.finalize()));
                return Ok(None);
            };
            let record_ordinal = self.next_record_ordinal;
            self.next_record_ordinal =
                self.next_record_ordinal.checked_add(1).ok_or_else(|| {
                    AssociationSourceError::new("ordinal_overflow", "VCF contains too many records")
                })?;

            let decoded = decode_record(
                &self.record,
                self.records.raw_structural(),
                &self.prepared,
                record_ordinal,
                self.next_source_ordinal,
            )?;
            self.next_source_ordinal = self
                .next_source_ordinal
                .checked_add(decoded.items.len() as u64)
                .ok_or_else(|| {
                    AssociationSourceError::new(
                        "ordinal_overflow",
                        "VCF contains too many record/ALT items",
                    )
                })?;
            for item in &decoded.items {
                hash_item(
                    self.hasher.as_mut().expect("hasher exists until EOF"),
                    item,
                    &decoded.record_semantic_digest,
                )?;
            }
            self.pending.extend(decoded.items);
            if let Some(item) = self.pending.pop_front() {
                return Ok(Some(item));
            }
        }
    }

    fn finalized_source_fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }
}

/// Bounded VCF inspection. Structural CHROM/POS/REF/ALT mappings are inferred;
/// INFO mappings are never inferred and must be explicit. Only the requested
/// number of logical records and decoded-byte budget are sampled.
pub fn inspect_vcf(
    source: &str,
    detected: DetectedAssociationSource,
    explicit: &MappingInput,
    sample_rows: u64,
) -> Result<AssociationInspectReport, String> {
    if sample_rows == 0 || sample_rows > MAX_INSPECT_RECORDS {
        return Err(format!("sample rows must be in 1..={MAX_INSPECT_RECORDS}"));
    }

    explicit
        .validate_partial()
        .map_err(|error| error.to_string())?;
    let mut reader =
        VcfRecordReader::open(Path::new(source), detected).map_err(inspect_source_error)?;
    validate_partial_mapping(explicit, reader.header()).map_err(inspect_source_error)?;

    let mut resolved = explicit.clone();
    let structural = [
        (LogicalField::Contig, "CHROM"),
        (LogicalField::Position, "POS"),
        (LogicalField::Ref, "REF"),
        (LogicalField::Alt, "ALT"),
    ];
    let mut inferred_mappings = BTreeMap::new();
    for (logical, source_name) in structural {
        if !resolved.fields.contains_key(&logical) {
            resolved.fields.insert(logical, source_name.to_string());
            inferred_mappings.insert(logical.to_string(), source_name.to_string());
        }
    }
    resolved
        .validate_partial()
        .map_err(|error| error.to_string())?;

    // VCF INFO significance is advisory even when there is only one plausible
    // field. Inspection reports exact `info.<ID>` choices; it never executes one.
    let mut ambiguities = BTreeMap::new();
    if !resolved.fields.contains_key(&LogicalField::Pvalue)
        && !resolved.fields.contains_key(&LogicalField::NegLog10P)
    {
        for logical in [LogicalField::Pvalue, LogicalField::NegLog10P] {
            let candidates = vcf_info_candidates(reader.header(), logical);
            if !candidates.is_empty() {
                ambiguities.insert(logical.to_string(), candidates);
            }
        }
    }

    let unresolved = resolved.unresolved_required();
    let prepared = if unresolved.is_empty() {
        let mapping = resolved.complete().map_err(|error| error.to_string())?;
        let (_, prepared) =
            prepare_mapping(mapping, reader.header()).map_err(inspect_source_error)?;
        Some(prepared)
    } else {
        None
    };

    let mut inspected_records = 0_u64;
    let mut decoded_sample_bytes = 0_usize;
    let mut next_source_ordinal = 0_u64;
    let mut diagnostics = RejectionDiagnostics::default();
    let mut record = vcf::Record::default();
    while inspected_records < sample_rows {
        let Some(size) = reader
            .next_record(&mut record)
            .map_err(inspect_source_error)?
        else {
            break;
        };
        if size > MAX_INSPECT_DECODED_BYTES.saturating_sub(decoded_sample_bytes) {
            break;
        }
        decoded_sample_bytes += size;
        if let Some(prepared) = &prepared {
            let decoded = decode_record(
                &record,
                reader.raw_structural(),
                prepared,
                inspected_records,
                next_source_ordinal,
            )
            .map_err(inspect_source_error)?;
            next_source_ordinal = next_source_ordinal
                .checked_add(decoded.items.len() as u64)
                .ok_or_else(|| "VCF contains too many record/ALT items".to_string())?;
            for item in decoded.items {
                if let AssociationSourceItem::Rejected(rejection) = item {
                    diagnostics.record(&rejection);
                }
            }
        }
        inspected_records += 1;
    }

    let normalized = if unresolved.is_empty() {
        Some(resolved.complete().map_err(|error| error.to_string())?)
    } else {
        None
    };
    let runnable_view_command = normalized
        .as_ref()
        .map(|mapping| build_view_command(source, resolved.assembly, mapping));
    let resolution_commands = vcf_resolution_commands(source, &resolved, &ambiguities);

    Ok(AssociationInspectReport {
        source_kind: "vcf".to_string(),
        inspected_records,
        decoded_sample_bytes,
        declared_logical_assemblies: Vec::new(),
        fields: inspect_fields(reader.header()).map_err(inspect_source_error)?,
        explicit_mappings: display_mappings(&explicit.fields),
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
                        examples: details
                            .examples
                            .into_iter()
                            .map(|example| bounded_visible(&example))
                            .collect(),
                    },
                )
            })
            .collect(),
        ready: normalized.is_some(),
        runnable_view_command,
        resolution_commands,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cardinality {
    Scalar,
    AlternateBases,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericKind {
    Integer,
    Float,
}

#[derive(Debug, Clone)]
struct PreparedInfoField {
    logical: LogicalField,
    /// Exact public mapping spelling (`info.<ID>`).
    source: String,
    /// Raw VCF INFO ID used only for exact token/header lookup.
    key: String,
    kind: NumericKind,
    cardinality: Cardinality,
}

#[derive(Debug, Clone)]
struct PreparedMapping {
    fields: BTreeMap<LogicalField, PreparedInfoField>,
}

fn prepare_mapping(
    mut mapping: NormalizedFieldMappingV1,
    header: &vcf::Header,
) -> Result<(NormalizedFieldMappingV1, PreparedMapping), AssociationSourceError> {
    canonicalize_structural_mapping(&mut mapping)?;
    mapping
        .validate()
        .map_err(|error| AssociationSourceError::new("invalid_mapping", error.to_string()))?;
    let fields = validate_info_mappings(mapping_pairs(&mapping), header)?;
    Ok((mapping, PreparedMapping { fields }))
}

fn canonicalize_structural_mapping(
    mapping: &mut NormalizedFieldMappingV1,
) -> Result<(), AssociationSourceError> {
    validate_structural_name(LogicalField::Contig, &mapping.contig)?;
    validate_structural_name(LogicalField::Position, &mapping.position)?;
    if let Some(source) = &mapping.ref_allele {
        validate_structural_name(LogicalField::Ref, source)?;
    }
    if let Some(source) = &mapping.alt_allele {
        validate_structural_name(LogicalField::Alt, source)?;
    }
    mapping.contig = "CHROM".to_string();
    mapping.position = "POS".to_string();
    mapping.ref_allele = Some("REF".to_string());
    mapping.alt_allele = Some("ALT".to_string());
    Ok(())
}

fn validate_partial_mapping(
    mapping: &MappingInput,
    header: &vcf::Header,
) -> Result<(), AssociationSourceError> {
    for (logical, source) in &mapping.fields {
        if is_structural(*logical) {
            validate_structural_name(*logical, source)?;
        }
    }
    validate_info_mappings(
        mapping
            .fields
            .iter()
            .map(|(logical, source)| (*logical, source.as_str())),
        header,
    )?;
    Ok(())
}

fn validate_info_mappings<'a>(
    mappings: impl IntoIterator<Item = (LogicalField, &'a str)>,
    header: &vcf::Header,
) -> Result<BTreeMap<LogicalField, PreparedInfoField>, AssociationSourceError> {
    let mut result = BTreeMap::new();
    for (logical, source) in mappings {
        if is_structural(logical) {
            continue;
        }
        if matches!(
            logical,
            LogicalField::Gene | LogicalField::Consequence | LogicalField::Series
        ) {
            return Err(AssociationSourceError::new(
                "unsupported_vcf_text_mapping",
                format!(
                    "VCF mapping for {logical} is unsupported; mapped INFO values must be numeric"
                ),
            ));
        }
        let key = info_key(source, logical)?;
        let definition = header.infos().get(key).ok_or_else(|| {
            AssociationSourceError::new(
                "missing_mapped_info",
                format!("mapped INFO field {source:?} for {logical} is not declared in the header"),
            )
        })?;
        let kind = match definition.ty() {
            Type::Integer => NumericKind::Integer,
            Type::Float => NumericKind::Float,
            actual => {
                return Err(AssociationSourceError::new(
                    "non_numeric_mapped_info",
                    format!(
                        "mapped INFO field {source:?} for {logical} has Type={actual}; expected Integer or Float"
                    ),
                ))
            }
        };
        let cardinality = match definition.number() {
            Number::Count(1) => Cardinality::Scalar,
            Number::AlternateBases => Cardinality::AlternateBases,
            Number::Count(_)
            | Number::ReferenceAlternateBases
            | Number::Samples
            | Number::Unknown => Cardinality::Unsupported,
        };
        result.insert(
            logical,
            PreparedInfoField {
                logical,
                source: source.to_string(),
                key: key.to_string(),
                kind,
                cardinality,
            },
        );
    }
    Ok(result)
}

fn info_key<'a>(source: &'a str, logical: LogicalField) -> Result<&'a str, AssociationSourceError> {
    let Some(key) = source.strip_prefix("info.") else {
        return Err(AssociationSourceError::new(
            "invalid_vcf_info_mapping",
            format!("VCF {logical} must use an explicit info.<ID> selector, not {source:?}"),
        ));
    };
    if key.is_empty() || key.contains('.') {
        return Err(AssociationSourceError::new(
            "invalid_vcf_info_mapping",
            format!("VCF {logical} has invalid INFO selector {source:?}"),
        ));
    }
    Ok(key)
}

fn is_structural(logical: LogicalField) -> bool {
    matches!(
        logical,
        LogicalField::Contig | LogicalField::Position | LogicalField::Ref | LogicalField::Alt
    )
}

fn validate_structural_name(
    logical: LogicalField,
    actual: &str,
) -> Result<(), AssociationSourceError> {
    let expected = match logical {
        LogicalField::Contig => "CHROM",
        LogicalField::Position => "POS",
        LogicalField::Ref => "REF",
        LogicalField::Alt => "ALT",
        _ => return Ok(()),
    };
    let accepted = actual == expected || (logical == LogicalField::Contig && actual == "#CHROM");
    if accepted {
        Ok(())
    } else {
        Err(AssociationSourceError::new(
            "invalid_vcf_structural_mapping",
            format!("VCF {logical} is structural and must map to {expected}, not {actual:?}"),
        ))
    }
}

#[derive(Debug, Clone)]
enum NumericState {
    Absent,
    Missing,
    Scalar(f64),
    Alternate(Vec<Option<f64>>),
    Malformed(String),
    Duplicate,
}

struct DecodedRecord {
    items: Vec<AssociationSourceItem>,
    record_semantic_digest: [u8; 32],
}

fn decode_record(
    record: &vcf::Record,
    raw_structural: &RawStructuralTokens,
    prepared: &PreparedMapping,
    record_ordinal: u64,
    first_source_ordinal: u64,
) -> Result<DecodedRecord, AssociationSourceError> {
    let contig = record.reference_sequence_name().to_string();
    let reference = record.reference_bases().to_string();
    let position = match record.variant_start() {
        None => {
            return Ok(single_rejection(
                record,
                raw_structural,
                prepared,
                first_source_ordinal,
                "missing_position",
                LogicalField::Position,
                Some(&raw_structural.position),
            ))
        }
        Some(Err(_)) => {
            return Ok(single_rejection(
                record,
                raw_structural,
                prepared,
                first_source_ordinal,
                "malformed_position",
                LogicalField::Position,
                Some(&raw_structural.position),
            ))
        }
        Some(Ok(value)) => i64::try_from(usize::from(value)).map_err(|_| {
            AssociationSourceError::new(
                "position_overflow",
                format!("record {record_ordinal} position does not fit i64"),
            )
        })?,
    };

    let alternates = match record
        .alternate_bases()
        .iter()
        .map(|result| result.map(str::to_string))
        .collect::<io::Result<Vec<_>>>()
    {
        Ok(values) => values,
        Err(_) => {
            return Ok(single_rejection(
                record,
                raw_structural,
                prepared,
                first_source_ordinal,
                "malformed_alt",
                LogicalField::Alt,
                Some(&raw_structural.alternate),
            ))
        }
    };
    if contig.is_empty() {
        return Ok(single_rejection(
            record,
            raw_structural,
            prepared,
            first_source_ordinal,
            "missing_contig",
            LogicalField::Contig,
            Some(&raw_structural.chrom),
        ));
    }
    if reference.is_empty() || reference == "." {
        return Ok(single_rejection(
            record,
            raw_structural,
            prepared,
            first_source_ordinal,
            "invalid_ref",
            LogicalField::Ref,
            Some(&reference),
        ));
    }
    if alternates.is_empty()
        || alternates
            .iter()
            .any(|value| value.is_empty() || value == ".")
    {
        return Ok(single_rejection(
            record,
            raw_structural,
            prepared,
            first_source_ordinal,
            "missing_alt",
            LogicalField::Alt,
            Some(&raw_structural.alternate),
        ));
    }

    let states = parse_mapped_info(record.info().as_ref(), prepared);
    let digest = semantic_record_digest(
        &contig,
        position,
        &reference,
        &alternates,
        prepared,
        &states,
    );

    for field in prepared.fields.values() {
        if field.cardinality == Cardinality::Unsupported {
            return Ok(DecodedRecord {
                items: vec![AssociationSourceItem::Rejected(rejection(
                    first_source_ordinal,
                    "unsupported_mapped_info_number",
                    field.logical,
                    Some(&field.source),
                ))],
                record_semantic_digest: digest,
            });
        }
        if field.cardinality == Cardinality::Scalar && alternates.len() != 1 {
            return Ok(DecodedRecord {
                items: vec![AssociationSourceItem::Rejected(rejection(
                    first_source_ordinal,
                    "scalar_info_on_multiallelic_record",
                    field.logical,
                    Some(&field.source),
                ))],
                record_semantic_digest: digest,
            });
        }
        let state = states
            .get(&field.logical)
            .expect("all mapped fields have state");
        let (code, example) = match state {
            NumericState::Malformed(raw) => (Some("malformed_mapped_info"), Some(raw.as_str())),
            NumericState::Duplicate => (Some("duplicate_mapped_info"), Some(field.source.as_str())),
            NumericState::Alternate(values) if values.len() != alternates.len() => {
                (Some("info_alt_count_mismatch"), Some(field.source.as_str()))
            }
            _ => (None, None),
        };
        if let Some(code) = code {
            return Ok(DecodedRecord {
                items: vec![AssociationSourceItem::Rejected(rejection(
                    first_source_ordinal,
                    code,
                    field.logical,
                    example,
                ))],
                record_semantic_digest: digest,
            });
        }
    }

    let mut items = Vec::with_capacity(alternates.len());
    for (alt_index, alternate) in alternates.iter().enumerate() {
        let source_ordinal = first_source_ordinal
            .checked_add(alt_index as u64)
            .ok_or_else(|| {
                AssociationSourceError::new(
                    "ordinal_overflow",
                    "VCF contains too many record/ALT items",
                )
            })?;
        // A literal `.` inside a Number=A array is distinct from an absent or
        // wholly missing optional INFO field. Reject only the affected ALT;
        // never silently reinterpret an explicit missing array element as an
        // optional scalar absence.
        if let Some(field) = prepared.fields.values().find(|field| {
            matches!(
                states.get(&field.logical),
                Some(NumericState::Alternate(values))
                    if values.get(alt_index).is_some_and(Option::is_none)
            )
        }) {
            items.push(AssociationSourceItem::Rejected(rejection(
                source_ordinal,
                "missing_mapped_info_element",
                field.logical,
                Some("."),
            )));
            continue;
        }

        let pvalue = value_for(&states, LogicalField::Pvalue, alt_index);
        let neg_log10_p = value_for(&states, LogicalField::NegLog10P, alt_index);
        let significance = match normalize_significance(pvalue, neg_log10_p) {
            Ok(value) => value,
            Err(error) => {
                let (code, field) = match error {
                    PValueError::MissingSignificance => {
                        let all_absent = [LogicalField::Pvalue, LogicalField::NegLog10P]
                            .into_iter()
                            .filter_map(|logical| states.get(&logical))
                            .all(|state| matches!(state, NumericState::Absent));
                        (
                            if all_absent {
                                "absent_significance"
                            } else {
                                "missing_significance"
                            },
                            if prepared.fields.contains_key(&LogicalField::Pvalue) {
                                LogicalField::Pvalue
                            } else {
                                LogicalField::NegLog10P
                            },
                        )
                    }
                    PValueError::InvalidPvalue => ("invalid_pvalue", LogicalField::Pvalue),
                    PValueError::InvalidNegLog10P => {
                        ("invalid_neg_log10_p", LogicalField::NegLog10P)
                    }
                };
                items.push(AssociationSourceItem::Rejected(rejection(
                    source_ordinal,
                    code,
                    field,
                    state_example(&states, field),
                )));
                continue;
            }
        };

        let optional = [
            (LogicalField::Beta, "invalid_beta"),
            (LogicalField::Se, "invalid_se"),
            (LogicalField::Af, "invalid_af"),
            (LogicalField::Ac, "invalid_ac"),
        ];
        let mut invalid_optional = None;
        for (field, code) in optional {
            if value_for(&states, field, alt_index).is_some_and(|value| !value.is_finite()) {
                invalid_optional = Some((field, code));
                break;
            }
        }
        if let Some((field, code)) = invalid_optional {
            items.push(AssociationSourceItem::Rejected(rejection(
                source_ordinal,
                code,
                field,
                state_example(&states, field),
            )));
            continue;
        }

        let point = AssociationPointV1 {
            contig: contig.clone(),
            position,
            ref_allele: Some(reference.clone()),
            alt_allele: Some(alternate.clone()),
            pvalue: significance.pvalue,
            neg_log10_p: significance.neg_log10_p,
            beta: value_for(&states, LogicalField::Beta, alt_index),
            standard_error: value_for(&states, LogicalField::Se, alt_index),
            allele_frequency: value_for(&states, LogicalField::Af, alt_index),
            allele_count: value_for(&states, LogicalField::Ac, alt_index),
            gene_symbol: None,
            consequence: None,
            series: None,
            source_ordinal,
        };
        match point.normalize() {
            Ok((point, warnings)) => {
                items.push(AssociationSourceItem::Point { point, warnings });
            }
            Err(error) => {
                let (code, field) = validation_reason(error);
                items.push(AssociationSourceItem::Rejected(rejection(
                    source_ordinal,
                    code,
                    field,
                    state_example(&states, field),
                )));
            }
        }
    }

    Ok(DecodedRecord {
        items,
        record_semantic_digest: digest,
    })
}

fn single_rejection(
    record: &vcf::Record,
    raw_structural: &RawStructuralTokens,
    prepared: &PreparedMapping,
    source_ordinal: u64,
    code: &'static str,
    field: LogicalField,
    example: Option<&str>,
) -> DecodedRecord {
    let states = parse_mapped_info(record.info().as_ref(), prepared);
    let digest = rejected_record_digest(raw_structural, prepared, &states);
    DecodedRecord {
        items: vec![AssociationSourceItem::Rejected(rejection(
            source_ordinal,
            code,
            field,
            example,
        ))],
        record_semantic_digest: digest,
    }
}

fn parse_mapped_info(
    raw_info: &str,
    prepared: &PreparedMapping,
) -> BTreeMap<LogicalField, NumericState> {
    let by_source: BTreeMap<&str, &PreparedInfoField> = prepared
        .fields
        .values()
        .map(|field| (field.key.as_str(), field))
        .collect();
    let mut states: BTreeMap<LogicalField, NumericState> = prepared
        .fields
        .keys()
        .map(|logical| (*logical, NumericState::Absent))
        .collect();

    if raw_info.is_empty() {
        return states;
    }
    for token in raw_info.split(';') {
        let key = token.split_once('=').map_or(token, |(key, _)| key);
        let Some(field) = by_source.get(key) else {
            // Deliberate policy: malformed unmapped INFO is irrelevant and is
            // neither parsed nor included in the semantic fingerprint.
            continue;
        };
        let slot = states
            .get_mut(&field.logical)
            .expect("all mapped fields have state");
        if !matches!(slot, NumericState::Absent) {
            *slot = NumericState::Duplicate;
            continue;
        }
        let Some((_, raw)) = token.split_once('=') else {
            *slot = NumericState::Malformed(token.to_string());
            continue;
        };
        if raw == "." {
            *slot = NumericState::Missing;
            continue;
        }
        *slot = match field.cardinality {
            Cardinality::Scalar => parse_number(raw, field.kind)
                .map(NumericState::Scalar)
                .unwrap_or_else(|| NumericState::Malformed(raw.to_string())),
            Cardinality::AlternateBases => {
                let mut values = Vec::new();
                let mut malformed = false;
                for value in raw.split(',') {
                    if value == "." {
                        values.push(None);
                    } else if let Some(value) = parse_number(value, field.kind) {
                        values.push(Some(value));
                    } else {
                        malformed = true;
                        break;
                    }
                }
                if malformed {
                    NumericState::Malformed(raw.to_string())
                } else {
                    NumericState::Alternate(values)
                }
            }
            Cardinality::Unsupported => NumericState::Malformed(raw.to_string()),
        };
    }
    states
}

fn parse_number(raw: &str, kind: NumericKind) -> Option<f64> {
    match kind {
        NumericKind::Integer => raw.parse::<i32>().ok().map(f64::from),
        NumericKind::Float => raw.parse::<f32>().ok().map(f64::from),
    }
}

fn value_for(
    states: &BTreeMap<LogicalField, NumericState>,
    logical: LogicalField,
    alt_index: usize,
) -> Option<f64> {
    match states.get(&logical)? {
        NumericState::Scalar(value) => Some(*value),
        NumericState::Alternate(values) => values.get(alt_index).copied().flatten(),
        NumericState::Absent
        | NumericState::Missing
        | NumericState::Malformed(_)
        | NumericState::Duplicate => None,
    }
}

fn state_example(
    states: &BTreeMap<LogicalField, NumericState>,
    logical: LogicalField,
) -> Option<&str> {
    match states.get(&logical) {
        Some(NumericState::Absent) => Some("absent"),
        Some(NumericState::Missing) => Some("."),
        Some(NumericState::Malformed(raw)) => Some(raw),
        Some(NumericState::Duplicate) => Some("duplicate"),
        _ => None,
    }
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
    example: Option<&str>,
) -> AssociationSourceRejectionV1 {
    AssociationSourceRejectionV1::new(ordinal, code, Some(field.to_string()), example)
}

fn semantic_record_digest(
    contig: &str,
    position: i64,
    reference: &str,
    alternates: &[String],
    prepared: &PreparedMapping,
    states: &BTreeMap<LogicalField, NumericState>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"genohype.association.vcf-record-semantic.v1\0");
    hash_bytes(&mut hasher, contig.as_bytes());
    hasher.update(position.to_be_bytes());
    hash_bytes(&mut hasher, reference.as_bytes());
    hasher.update((alternates.len() as u64).to_be_bytes());
    for alternate in alternates {
        hash_bytes(&mut hasher, alternate.as_bytes());
    }
    for field in prepared.fields.values() {
        hash_bytes(&mut hasher, field.logical.as_str().as_bytes());
        hash_bytes(&mut hasher, field.source.as_bytes());
        hash_numeric_state(
            &mut hasher,
            states.get(&field.logical).expect("mapped state exists"),
        );
    }
    hasher.finalize().into()
}

fn rejected_record_digest(
    raw_structural: &RawStructuralTokens,
    prepared: &PreparedMapping,
    states: &BTreeMap<LogicalField, NumericState>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"genohype.association.vcf-rejected-record-semantic.v1\0");
    hash_bytes(&mut hasher, raw_structural.chrom.as_bytes());
    hash_bytes(&mut hasher, raw_structural.position.as_bytes());
    hash_bytes(&mut hasher, raw_structural.reference.as_bytes());
    hash_bytes(&mut hasher, raw_structural.alternate.as_bytes());
    for field in prepared.fields.values() {
        hash_bytes(&mut hasher, field.logical.as_str().as_bytes());
        hash_bytes(&mut hasher, field.source.as_bytes());
        hash_numeric_state(
            &mut hasher,
            states.get(&field.logical).expect("mapped state exists"),
        );
    }
    hasher.finalize().into()
}

fn hash_numeric_state(hasher: &mut Sha256, state: &NumericState) {
    match state {
        NumericState::Absent => hasher.update([0]),
        NumericState::Missing => hasher.update([1]),
        NumericState::Scalar(value) => {
            hasher.update([2]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        NumericState::Alternate(values) => {
            hasher.update([3]);
            hasher.update((values.len() as u64).to_be_bytes());
            for value in values {
                match value {
                    None => hasher.update([0]),
                    Some(value) => {
                        hasher.update([1]);
                        hasher.update(value.to_bits().to_be_bytes());
                    }
                }
            }
        }
        NumericState::Malformed(raw) => {
            hasher.update([4]);
            hash_bytes(hasher, raw.as_bytes());
        }
        NumericState::Duplicate => hasher.update([5]),
    }
}

fn hash_item(
    hasher: &mut Sha256,
    item: &AssociationSourceItem,
    record_digest: &[u8; 32],
) -> Result<(), AssociationSourceError> {
    match item {
        AssociationSourceItem::Point { point, warnings } => {
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
        }
        AssociationSourceItem::Rejected(rejection) => {
            hasher.update(b"rejected\0");
            hasher.update(rejection.source_ordinal.to_be_bytes());
            hash_bytes(hasher, rejection.reason_code.as_bytes());
            hasher.update(record_digest);
        }
    }
    Ok(())
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_observed_fields(header: &vcf::Header) -> Result<(), AssociationSourceError> {
    if header.infos().len().saturating_add(4) > MAX_VCF_OBSERVED_FIELDS {
        return Err(AssociationSourceError::new(
            "too_many_vcf_fields",
            format!("VCF exposes more than {MAX_VCF_OBSERVED_FIELDS} structural and INFO fields"),
        ));
    }
    for name in header.infos().keys() {
        if name.is_empty() || name.len() > 256 {
            return Err(AssociationSourceError::new(
                "invalid_vcf_info_name",
                "VCF INFO names must be nonempty and at most 256 UTF-8 bytes",
            ));
        }
    }
    Ok(())
}

fn observed_fields(
    header: &vcf::Header,
) -> Result<Vec<AssociationSourceFieldV1>, AssociationSourceError> {
    validate_observed_fields(header)?;
    let mut fields = vec![
        source_field("CHROM", Some("String")),
        source_field("POS", Some("Integer")),
        source_field("REF", Some("String")),
        source_field("ALT", Some("String[A]")),
    ];
    fields.extend(
        header
            .infos()
            .iter()
            .map(|(name, definition)| AssociationSourceFieldV1 {
                name: format!("info.{name}"),
                data_type: Some(format!(
                    "INFO Number={} Type={}",
                    display_number(definition.number()),
                    definition.ty()
                )),
            }),
    );
    Ok(fields)
}

fn inspect_fields(header: &vcf::Header) -> Result<Vec<InspectFieldReport>, AssociationSourceError> {
    Ok(observed_fields(header)?
        .into_iter()
        .map(|field| InspectFieldReport {
            name: field.name,
            data_type: field.data_type.unwrap_or_else(|| "unknown".to_string()),
        })
        .collect())
}

fn source_field(name: &str, data_type: Option<&str>) -> AssociationSourceFieldV1 {
    AssociationSourceFieldV1 {
        name: name.to_string(),
        data_type: data_type.map(str::to_string),
    }
}

fn display_number(number: Number) -> String {
    match number {
        Number::Count(count) => count.to_string(),
        Number::AlternateBases => "A".to_string(),
        Number::ReferenceAlternateBases => "R".to_string(),
        Number::Samples => "G".to_string(),
        Number::Unknown => ".".to_string(),
    }
}

fn display_mappings(mappings: &BTreeMap<LogicalField, String>) -> BTreeMap<String, String> {
    mappings
        .iter()
        .map(|(logical, source)| (logical.to_string(), source.clone()))
        .collect()
}

fn vcf_info_candidates(header: &vcf::Header, logical: LogicalField) -> Vec<String> {
    let aliases: &[&str] = match logical {
        LogicalField::Pvalue => &["p", "pvalue", "pval", "p_value"],
        LogicalField::NegLog10P => &[
            "neg_log10_p",
            "minus_log10_p",
            "mlog10p",
            "pvalue_log10",
            "p_log10",
        ],
        _ => &[],
    };
    let normalized_aliases: BTreeSet<String> = aliases
        .iter()
        .map(|value| normalize_candidate_name(value))
        .collect();
    let mut candidates: Vec<String> = header
        .infos()
        .iter()
        .filter(|(name, definition)| {
            matches!(definition.ty(), Type::Integer | Type::Float)
                && matches!(
                    definition.number(),
                    Number::Count(1) | Number::AlternateBases
                )
                && normalized_aliases.contains(&normalize_candidate_name(name))
        })
        .map(|(name, _)| format!("info.{name}"))
        .collect();
    candidates.sort();
    candidates.dedup();
    candidates
}

fn normalize_candidate_name(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn vcf_resolution_commands(
    source: &str,
    mapping: &MappingInput,
    ambiguities: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut commands = Vec::new();
    for (logical, candidates) in ambiguities {
        let Ok(logical) = logical.parse::<LogicalField>() else {
            continue;
        };
        for candidate in candidates {
            let mut fields = mapping.fields.clone();
            fields.insert(logical, candidate.clone());
            let mut command = format!("genohype association inspect {}", shell_quote(source));
            if let Some(assembly) = mapping.assembly {
                command.push_str(" --build ");
                command.push_str(assembly.as_str());
            }
            for (field, source) in fields {
                command.push_str(" --map ");
                command.push_str(&shell_quote(&format!("{field}={source}")));
            }
            commands.push(command);
        }
    }
    commands
}

fn build_view_command(
    source: &str,
    assembly: Option<crate::cli::AssociationBuildArg>,
    mapping: &NormalizedFieldMappingV1,
) -> String {
    let mut command = format!("genohype association view {}", shell_quote(source));
    if let Some(assembly) = assembly {
        command.push_str(" --build ");
        command.push_str(assembly.as_str());
    }
    for (logical, source) in mapping_pairs(mapping) {
        command.push_str(" --map ");
        command.push_str(&shell_quote(&format!("{}={source}", logical.as_str())));
    }
    command
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_./:@%+=,-".contains(&byte))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn inspect_source_error(error: AssociationSourceError) -> String {
    format!("{}: {}", error.code(), error.internal_message())
}

fn source_io(operation: &str, error: io::Error) -> AssociationSourceError {
    AssociationSourceError::new(
        "source_io",
        format!("could not {operation} VCF source: {error}"),
    )
}

fn vcf_io_error(stage: &str, error: io::Error) -> AssociationSourceError {
    let message = bounded_error_message(&error.to_string());
    if message.contains(HEADER_BOUND_PREFIX) {
        AssociationSourceError::new("vcf_header_too_large", message)
    } else if message.contains(RECORD_BOUND_PREFIX) {
        AssociationSourceError::new("vcf_record_too_large", message)
    } else {
        AssociationSourceError::new(format!("invalid_vcf_{stage}"), message)
    }
}

fn bounded_error_message(message: &str) -> String {
    const MAX: usize = 512;
    if message.len() <= MAX {
        return message.to_string();
    }
    let mut end = MAX;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputPhase {
    Header,
    Records,
}

/// A line-aware read adapter that never emits bytes beyond a header or record
/// boundary. This is intentionally below `BufReader`/noodles: post-parse size
/// checks would be too late to prevent an oversized allocation.
struct BoundedVcfReader<R> {
    inner: R,
    phase: InputPhase,
    header_limit: usize,
    record_limit: usize,
    header_bytes: usize,
    record_bytes: usize,
    header_line_prefix: [u8; 7],
    prefix_len: usize,
    buffer: [u8; 8192],
    buffer_start: usize,
    buffer_end: usize,
    pending_error: Option<io::Error>,
}

impl<R> BoundedVcfReader<R> {
    fn new(inner: R, header_limit: usize, record_limit: usize) -> Self {
        Self {
            inner,
            phase: InputPhase::Header,
            header_limit,
            record_limit,
            header_bytes: 0,
            record_bytes: 0,
            header_line_prefix: [0; 7],
            prefix_len: 0,
            buffer: [0; 8192],
            buffer_start: 0,
            buffer_end: 0,
            pending_error: None,
        }
    }

    fn accept_byte(&mut self, byte: u8) -> io::Result<()> {
        match self.phase {
            InputPhase::Header => {
                if self.header_bytes == self.header_limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{HEADER_BOUND_PREFIX} header exceeds {} bytes",
                            self.header_limit
                        ),
                    ));
                }
                self.header_bytes += 1;
                if self.prefix_len < self.header_line_prefix.len() {
                    self.header_line_prefix[self.prefix_len] = byte;
                    self.prefix_len += 1;
                }
                if byte == b'\n' {
                    if self.prefix_len == self.header_line_prefix.len()
                        && self.header_line_prefix == *b"#CHROM\t"
                    {
                        self.phase = InputPhase::Records;
                        self.record_bytes = 0;
                    }
                    self.header_line_prefix = [0; 7];
                    self.prefix_len = 0;
                }
            }
            InputPhase::Records => {
                if self.record_bytes == self.record_limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{RECORD_BOUND_PREFIX} record exceeds {} bytes",
                            self.record_limit
                        ),
                    ));
                }
                self.record_bytes += 1;
                if byte == b'\n' {
                    self.record_bytes = 0;
                }
            }
        }
        Ok(())
    }
}

impl<R: Read> Read for BoundedVcfReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }

        let mut written = 0;
        while written < output.len() {
            if self.buffer_start == self.buffer_end {
                self.buffer_end = self.inner.read(&mut self.buffer)?;
                self.buffer_start = 0;
                if self.buffer_end == 0 {
                    break;
                }
            }
            let byte = self.buffer[self.buffer_start];
            match self.accept_byte(byte) {
                Ok(()) => {
                    self.buffer_start += 1;
                    output[written] = byte;
                    written += 1;
                }
                Err(error) if written > 0 => {
                    self.pending_error = Some(error);
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use genohype_association::AssociationSourceItem;
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    fn mapping(pvalue: &str) -> NormalizedFieldMappingV1 {
        NormalizedFieldMappingV1 {
            contig: "CHROM".into(),
            position: "POS".into(),
            ref_allele: Some("REF".into()),
            alt_allele: Some("ALT".into()),
            pvalue: Some(format!("info.{pvalue}")),
            neg_log10_p: None,
            beta: None,
            se: None,
            af: None,
            ac: None,
            gene: None,
            consequence: None,
            series: None,
        }
    }

    fn fixture(info_header: &str, records: &str) -> String {
        format!(
            "##fileformat=VCFv4.3\n{info_header}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n{records}"
        )
    }

    fn open(contents: &str, mapping: NormalizedFieldMappingV1) -> VcfAssociationSource {
        let temp = tempdir().unwrap();
        let path = temp.path().join("input.vcf");
        fs::write(&path, contents).unwrap();
        // Keep the temporary directory alive for the source's open file handle.
        let source =
            VcfAssociationSource::open(&path, DetectedAssociationSource::Vcf, mapping, "GRCh38")
                .unwrap();
        drop(temp);
        source
    }

    fn collect(source: &mut VcfAssociationSource) -> Vec<AssociationSourceItem> {
        let mut result = Vec::new();
        while let Some(item) = source.next_item().unwrap() {
            result.push(item);
        }
        result
    }

    #[test]
    fn scalar_is_biallelic_and_a_values_align_to_unique_alt_ordinals() {
        let scalar = fixture(
            "##INFO=<ID=P,Number=1,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC\t.\tPASS\tP=0.25\tGT\tnot-a-genotype\n",
        );
        let mut source = open(&scalar, mapping("P"));
        assert!(source.metadata().display_name.is_none());
        assert!(source
            .metadata()
            .observed_fields
            .iter()
            .all(|field| field.name != "FORMAT" && field.name != "S1"));
        let items = collect(&mut source);
        let AssociationSourceItem::Point { point, .. } = &items[0] else {
            panic!("expected point")
        };
        assert_eq!(point.source_ordinal, 0);
        assert_eq!(point.alt_allele.as_deref(), Some("C"));
        assert_eq!(point.pvalue, 0.25);

        let per_alt = fixture(
            "##INFO=<ID=P,Number=A,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC,G\t.\tPASS\tP=0.25,0.5\tGT\tdefinitely-invalid\n",
        );
        let mut source = open(&per_alt, mapping("P"));
        let items = collect(&mut source);
        assert_eq!(items.len(), 2);
        for (ordinal, expected_alt) in ["C", "G"].into_iter().enumerate() {
            let AssociationSourceItem::Point { point, .. } = &items[ordinal] else {
                panic!("expected point")
            };
            assert_eq!(point.source_ordinal, ordinal as u64);
            assert_eq!(point.alt_allele.as_deref(), Some(expected_alt));
        }
    }

    #[test]
    fn number_a_significance_and_effects_align_to_each_alt() {
        let contents = fixture(
            "##INFO=<ID=P,Number=A,Type=Float,Description=\"p\">\n##INFO=<ID=BETA,Number=A,Type=Float,Description=\"effect\">\n",
            "1\t10\t.\tA\tC,G\t.\tPASS\tP=0.25,0.5;BETA=-1.5,2.25\tGT\tnot-decoded\n",
        );
        let mut mapped = mapping("P");
        mapped.beta = Some("info.BETA".into());
        let mut source = open(&contents, mapped);
        let items = collect(&mut source);
        let values: Vec<(&str, f64)> = items
            .iter()
            .map(|item| match item {
                AssociationSourceItem::Point { point, .. } => {
                    (point.alt_allele.as_deref().unwrap(), point.beta.unwrap())
                }
                AssociationSourceItem::Rejected(rejection) => {
                    panic!("unexpected rejection: {rejection:?}")
                }
            })
            .collect();
        assert_eq!(values, [("C", -1.5), ("G", 2.25)]);
    }

    #[test]
    fn cardinality_and_missing_array_elements_have_named_rejections() {
        let scalar_multi = fixture(
            "##INFO=<ID=P,Number=1,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC,G\t.\tPASS\tP=0.25\t.\t.\n",
        );
        let mut source = open(&scalar_multi, mapping("P"));
        let AssociationSourceItem::Rejected(rejection) = source.next_item().unwrap().unwrap()
        else {
            panic!("expected rejection")
        };
        assert_eq!(rejection.reason_code, "scalar_info_on_multiallelic_record");

        let missing = fixture(
            "##INFO=<ID=P,Number=A,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC,G\t.\tPASS\tP=.,0.5\t.\t.\n",
        );
        let mut source = open(&missing, mapping("P"));
        let items = collect(&mut source);
        assert!(matches!(
            &items[0],
            AssociationSourceItem::Rejected(r)
                if r.reason_code == "missing_mapped_info_element"
        ));
        assert!(matches!(&items[1], AssociationSourceItem::Point { .. }));

        let absent = fixture(
            "##INFO=<ID=P,Number=A,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC\t.\tPASS\t.\t.\t.\n",
        );
        let mut source = open(&absent, mapping("P"));
        assert!(matches!(
            source.next_item().unwrap().unwrap(),
            AssociationSourceItem::Rejected(r) if r.reason_code == "absent_significance"
        ));

        let missing_optional = fixture(
            "##INFO=<ID=P,Number=A,Type=Float,Description=\"p\">\n##INFO=<ID=BETA,Number=A,Type=Float,Description=\"effect\">\n",
            "1\t10\t.\tA\tC,G\t.\tPASS\tP=0.25,0.5;BETA=.,2.0\t.\t.\n",
        );
        let mut mapped = mapping("P");
        mapped.beta = Some("info.BETA".into());
        let mut source = open(&missing_optional, mapped);
        let items = collect(&mut source);
        assert!(matches!(
            &items[0],
            AssociationSourceItem::Rejected(r)
                if r.reason_code == "missing_mapped_info_element"
        ));
        assert!(matches!(
            &items[1],
            AssociationSourceItem::Point { point, .. } if point.beta == Some(2.0)
        ));

        let wrong_length = fixture(
            "##INFO=<ID=P,Number=A,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC,G\t.\tPASS\tP=0.5\t.\t.\n",
        );
        let mut source = open(&wrong_length, mapping("P"));
        assert!(matches!(
            source.next_item().unwrap().unwrap(),
            AssociationSourceItem::Rejected(r) if r.reason_code == "info_alt_count_mismatch"
        ));
    }

    #[test]
    fn malformed_mapped_is_rejected_but_malformed_unmapped_is_ignored_and_not_hashed() {
        let header = "##INFO=<ID=BAD,Number=1,Type=Integer,Description=\"unused\">\n##INFO=<ID=P,Number=1,Type=Float,Description=\"p\">\n";
        let malformed_unmapped =
            fixture(header, "1\t10\t.\tA\tC\t.\tPASS\tBAD=nope;P=0.25\t.\t.\n");
        let clean_unmapped = fixture(header, "1\t10\t.\tA\tC\t.\tPASS\tBAD=1;P=0.25\t.\t.\n");
        let mut first = open(&malformed_unmapped, mapping("P"));
        assert!(matches!(
            first.next_item().unwrap().unwrap(),
            AssociationSourceItem::Point { .. }
        ));
        assert!(first.next_item().unwrap().is_none());
        let first_fingerprint = first.finalized_source_fingerprint().unwrap().to_string();
        let mut second = open(&clean_unmapped, mapping("P"));
        collect(&mut second);
        assert_eq!(
            first_fingerprint,
            second.finalized_source_fingerprint().unwrap()
        );

        let malformed_mapped = fixture(header, "1\t10\t.\tA\tC\t.\tPASS\tP=nope\t.\t.\n");
        let mut source = open(&malformed_mapped, mapping("P"));
        assert!(matches!(
            source.next_item().unwrap().unwrap(),
            AssociationSourceItem::Rejected(r) if r.reason_code == "malformed_mapped_info"
        ));
        assert!(source.next_item().unwrap().is_none());
        let malformed_fingerprint = source.finalized_source_fingerprint().unwrap().to_string();

        let differently_malformed =
            fixture(header, "1\t10\t.\tA\tC\t.\tPASS\tP=still-nope\t.\t.\n");
        let mut source = open(&differently_malformed, mapping("P"));
        collect(&mut source);
        assert_ne!(
            source.finalized_source_fingerprint(),
            Some(malformed_fingerprint.as_str())
        );
    }

    #[test]
    fn rejected_fingerprints_preserve_raw_structural_tokens_but_ignore_unmapped_columns() {
        let header = "##INFO=<ID=BAD,Number=1,Type=String,Description=\"unused\">\n##INFO=<ID=P,Number=1,Type=Float,Description=\"p\">\n";

        let malformed_position_x = fixture(
            header,
            "1\tx\t.\tA\tC\t.\tPASS\tBAD=first;P=0.25\tGT\t0/1\n",
        );
        let mut source = open(&malformed_position_x, mapping("P"));
        let items = collect(&mut source);
        assert!(matches!(
            &items[0],
            AssociationSourceItem::Rejected(r)
                if r.reason_code == "malformed_position"
                    && r.bounded_example.as_deref() == Some("x")
        ));
        let position_x_fingerprint = source.finalized_source_fingerprint().unwrap().to_string();

        let malformed_position_y = fixture(
            header,
            "1\ty\t.\tA\tC\t.\tPASS\tBAD=first;P=0.25\tGT\t0/1\n",
        );
        let mut source = open(&malformed_position_y, mapping("P"));
        collect(&mut source);
        assert_ne!(
            source.finalized_source_fingerprint(),
            Some(position_x_fingerprint.as_str())
        );

        // The lazy noodles record maps both a missing `.` ALT and an empty,
        // malformed ALT token to no alternates. Rejected identity must retain
        // the exact raw mapped token rather than collapsing the two.
        let missing_alt = fixture(
            header,
            "1\t10\t.\tA\t.\t.\tPASS\tBAD=first;P=0.25\tGT\t0/1\n",
        );
        let mut source = open(&missing_alt, mapping("P"));
        let items = collect(&mut source);
        assert!(matches!(
            &items[0],
            AssociationSourceItem::Rejected(r) if r.reason_code == "missing_alt"
        ));
        let missing_alt_fingerprint = source.finalized_source_fingerprint().unwrap().to_string();

        let malformed_alt = fixture(
            header,
            "1\t10\t.\tA\t\t.\tPASS\tBAD=first;P=0.25\tGT\t0/1\n",
        );
        let mut source = open(&malformed_alt, mapping("P"));
        let items = collect(&mut source);
        assert!(matches!(
            &items[0],
            AssociationSourceItem::Rejected(r) if r.reason_code == "missing_alt"
        ));
        assert_ne!(
            source.finalized_source_fingerprint(),
            Some(missing_alt_fingerprint.as_str())
        );

        let unmapped_changes = fixture(
            header,
            "1\tx\t.\tA\tC\t.\tPASS\tBAD=second;P=0.25\tGT\t1/1\n",
        );
        let mut source = open(&unmapped_changes, mapping("P"));
        collect(&mut source);
        assert_eq!(
            source.finalized_source_fingerprint(),
            Some(position_x_fingerprint.as_str())
        );
    }

    #[test]
    fn inspect_reports_exact_info_candidates_without_choosing_them() {
        let contents = fixture(
            "##INFO=<ID=P,Number=1,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC\t.\tPASS\tP=0.25\tGT\tnot-decoded\n",
        );
        let temp = tempdir().unwrap();
        let path = temp.path().join("inspect.vcf");
        fs::write(&path, contents).unwrap();
        let mut input = MappingInput::default();
        input.assembly = Some(crate::cli::AssociationBuildArg::Grch38);
        let report = inspect_vcf(
            path.to_str().unwrap(),
            DetectedAssociationSource::Vcf,
            &input,
            100,
        )
        .unwrap();
        assert!(!report.ready);
        assert_eq!(report.ambiguities["pvalue"], ["info.P"]);
        assert!(report.fields.iter().any(|field| {
            field.name == "info.P" && field.data_type.contains("Number=1 Type=Float")
        }));
        assert!(report
            .resolution_commands
            .iter()
            .any(|command| command.contains("pvalue=info.P")));

        input.fields.insert(LogicalField::Pvalue, "info.P".into());
        let report = inspect_vcf(
            path.to_str().unwrap(),
            DetectedAssociationSource::Vcf,
            &input,
            100,
        )
        .unwrap();
        assert!(report.ready);
        assert_eq!(report.inspected_records, 1);
        assert!(report.ambiguities.is_empty());

        input.fields.insert(LogicalField::Pvalue, "P".into());
        let error = inspect_vcf(
            path.to_str().unwrap(),
            DetectedAssociationSource::Vcf,
            &input,
            100,
        )
        .unwrap_err();
        assert!(error.contains("invalid_vcf_info_mapping"), "{error}");
    }

    #[test]
    fn bgzf_vcf_uses_the_same_sequential_lazy_path_and_generic_gzip_is_rejected() {
        let contents = fixture(
            "##INFO=<ID=P,Number=1,Type=Float,Description=\"p\">\n",
            "1\t10\t.\tA\tC\t.\tPASS\tP=0.25\tGT\tstill-not-decoded\n",
        );
        let temp = tempdir().unwrap();
        let path = temp.path().join("input.vcf.gz");
        let mut writer = noodles::bgzf::Writer::new(Vec::new());
        writer.write_all(contents.as_bytes()).unwrap();
        fs::write(&path, writer.finish().unwrap()).unwrap();

        let mut source = VcfAssociationSource::open(
            &path,
            DetectedAssociationSource::Vcf,
            mapping("P"),
            "GRCh38",
        )
        .unwrap();
        assert!(matches!(
            source.next_item().unwrap().unwrap(),
            AssociationSourceItem::Point { .. }
        ));
        assert!(source.next_item().unwrap().is_none());

        let path = temp.path().join("ordinary-gzip.vcf.gz");
        let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        writer.write_all(contents.as_bytes()).unwrap();
        fs::write(&path, writer.finish().unwrap()).unwrap();
        assert!(VcfAssociationSource::open(
            &path,
            DetectedAssociationSource::Vcf,
            mapping("P"),
            "GRCh38",
        )
        .is_err());
    }

    #[test]
    fn non_numeric_type_fails_before_records_and_unsupported_number_rejects_record() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("non-numeric.vcf");
        fs::write(
            &path,
            fixture(
                "##INFO=<ID=P,Number=1,Type=String,Description=\"bad\">\n",
                "1\t1\t.\tA\tC\t.\tPASS\tP=1\t.\t.\n",
            ),
        )
        .unwrap();
        let error = match VcfAssociationSource::open(
            &path,
            DetectedAssociationSource::Vcf,
            mapping("P"),
            "GRCh38",
        ) {
            Ok(_) => panic!("non-numeric header unexpectedly opened"),
            Err(error) => error,
        };
        assert_eq!(error.code, "non_numeric_mapped_info");

        let path = temp.path().join("unsupported-number.vcf");
        fs::write(
            &path,
            fixture(
                "##INFO=<ID=P,Number=R,Type=Float,Description=\"bad\">\n",
                "1\t1\t.\tA\tC\t.\tPASS\tP=1,0.5\t.\t.\n",
            ),
        )
        .unwrap();
        let mut source = VcfAssociationSource::open(
            &path,
            DetectedAssociationSource::Vcf,
            mapping("P"),
            "GRCh38",
        )
        .unwrap();
        assert!(matches!(
            source.next_item().unwrap().unwrap(),
            AssociationSourceItem::Rejected(rejection)
                if rejection.reason_code == "unsupported_mapped_info_number"
        ));
    }

    #[test]
    fn bounds_are_enforced_below_noodles() {
        let input = format!(
            "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n1\t1\t.\tA\tC\t.\tPASS\t{}\n",
            "X".repeat(32)
        );
        let mut bounded = BoundedVcfReader::new(input.as_bytes(), 128, 16);
        let mut output = Vec::new();
        let error = bounded.read_to_end(&mut output).unwrap_err();
        assert!(error.to_string().contains(RECORD_BOUND_PREFIX));
        assert!(output.len() <= input.find("1\t1").unwrap() + 16);

        let oversized_header = format!("##{}\n#CHROM\tPOS\n", "X".repeat(32));
        let mut bounded = BoundedVcfReader::new(oversized_header.as_bytes(), 16, 128);
        let mut output = Vec::new();
        let error = bounded.read_to_end(&mut output).unwrap_err();
        assert!(error.to_string().contains(HEADER_BOUND_PREFIX));
        assert_eq!(output.len(), 16);
    }
}
