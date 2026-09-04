use super::mapping::{mapping_pairs, LogicalField};
use super::DetectedAssociationSource;
use csv::{ByteRecord, ErrorKind, Reader, ReaderBuilder};
use flate2::read::MultiGzDecoder;
use genohype_association::{
    normalize_significance, AssociationPointV1, AssociationSource, AssociationSourceError,
    AssociationSourceFieldV1, AssociationSourceItem, AssociationSourceMetadataV1,
    AssociationSourceRejectionV1, NormalizedFieldMappingV1, PValueError, ValidationError,
    WarningCode,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

pub const MAX_DELIMITED_HEADER_BYTES: usize = 64 * 1024;
pub const MAX_DELIMITED_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_DELIMITED_HEADER_FIELDS: usize = 4_096;
const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";
const FRAMING_PREFIX: &str = "association_csv_framing:";
const HEADER_BOUND_PREFIX: &str = "association_csv_header_bound:";
const RECORD_BOUND_PREFIX: &str = "association_csv_record_bound:";

type Input = StrictBoundedReader<Box<dyn Read>>;

/// Streaming CSV/TSV reader with strict quote validation and pre-allocation
/// header/record bounds enforced below the `csv` parser.
pub struct DelimitedRecordReader {
    reader: Reader<Input>,
    headers: Vec<String>,
}

impl DelimitedRecordReader {
    pub fn open(
        path: &Path,
        detected: DetectedAssociationSource,
    ) -> Result<Self, AssociationSourceError> {
        let DetectedAssociationSource::Delimited { delimiter, gzip } = detected else {
            return Err(AssociationSourceError::new(
                "source_kind_mismatch",
                "delimited reader requires a CSV or TSV source",
            ));
        };
        let file = File::open(path).map_err(|error| source_io("open", error))?;
        let input: Box<dyn Read> = if gzip {
            Box::new(MultiGzDecoder::new(file))
        } else {
            Box::new(file)
        };
        let bounded = StrictBoundedReader::new(
            input,
            delimiter,
            MAX_DELIMITED_HEADER_BYTES,
            MAX_DELIMITED_RECORD_BYTES,
        );
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(false)
            .flexible(false)
            .double_quote(true)
            .quoting(true)
            .from_reader(bounded);
        let mut header = ByteRecord::new();
        if !reader.read_byte_record(&mut header).map_err(csv_error)? {
            return Err(AssociationSourceError::new(
                "missing_header",
                "delimited source is empty",
            ));
        }
        let headers = validate_headers(&header)?;
        Ok(Self { reader, headers })
    }

    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    pub fn next_record(&mut self, record: &mut ByteRecord) -> Result<bool, AssociationSourceError> {
        self.reader.read_byte_record(record).map_err(csv_error)
    }
}

/// A normalized one-pass CSV/TSV association source.
pub struct DelimitedAssociationSource {
    records: DelimitedRecordReader,
    indexes: BTreeMap<LogicalField, usize>,
    metadata: AssociationSourceMetadataV1,
    next_ordinal: u64,
    hasher: Option<Sha256>,
    fingerprint: Option<String>,
    record: ByteRecord,
}

impl DelimitedAssociationSource {
    pub fn open(
        path: &Path,
        detected: DetectedAssociationSource,
        mapping: NormalizedFieldMappingV1,
        assembly: &str,
    ) -> Result<Self, AssociationSourceError> {
        mapping
            .validate()
            .map_err(|error| AssociationSourceError::new("invalid_mapping", error.to_string()))?;
        let records = DelimitedRecordReader::open(path, detected)?;
        let indexes = mapped_indexes(records.headers(), &mapping)?;
        let metadata = AssociationSourceMetadataV1 {
            kind: detected.source_kind(),
            observed_fields: records
                .headers()
                .iter()
                .map(|name| AssociationSourceFieldV1 {
                    name: name.clone(),
                    data_type: None,
                })
                .collect(),
            normalized_mapping: mapping,
            assembly: assembly.to_string(),
            display_name: None,
        };
        metadata.validate()?;
        let mut hasher = Sha256::new();
        hasher.update(b"genohype.association.delimited-semantic-source.v1\0");
        Ok(Self {
            records,
            indexes,
            metadata,
            next_ordinal: 0,
            hasher: Some(hasher),
            fingerprint: None,
            record: ByteRecord::new(),
        })
    }
}

impl AssociationSource for DelimitedAssociationSource {
    fn metadata(&self) -> &AssociationSourceMetadataV1 {
        &self.metadata
    }

    fn next_item(&mut self) -> Result<Option<AssociationSourceItem>, AssociationSourceError> {
        if self.fingerprint.is_some() {
            return Ok(None);
        }
        if !self.records.next_record(&mut self.record)? {
            let hasher = self.hasher.take().expect("hasher exists until EOF");
            self.fingerprint = Some(format!("sha256:{:x}", hasher.finalize()));
            return Ok(None);
        }
        let ordinal = self.next_ordinal;
        self.next_ordinal = self.next_ordinal.checked_add(1).ok_or_else(|| {
            AssociationSourceError::new("ordinal_overflow", "source contains too many records")
        })?;
        match decode_record(&self.indexes, &self.record, ordinal)? {
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
                    &self.indexes,
                    &self.record,
                );
                Ok(Some(AssociationSourceItem::Rejected(rejection)))
            }
        }
    }

    fn finalized_source_fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }
}

pub(crate) fn mapped_indexes(
    headers: &[String],
    mapping: &NormalizedFieldMappingV1,
) -> Result<BTreeMap<LogicalField, usize>, AssociationSourceError> {
    let header_indexes: BTreeMap<&str, usize> = headers
        .iter()
        .enumerate()
        .map(|(index, header)| (header.as_str(), index))
        .collect();
    let mut indexes = BTreeMap::new();
    for (logical, source) in mapping_pairs(mapping) {
        let Some(index) = header_indexes.get(source) else {
            return Err(AssociationSourceError::new(
                "missing_mapped_field",
                format!("mapped source field {source:?} for {logical} is not in the header"),
            ));
        };
        indexes.insert(logical, *index);
    }
    Ok(indexes)
}

pub(crate) fn decode_record(
    indexes: &BTreeMap<LogicalField, usize>,
    record: &ByteRecord,
    ordinal: u64,
) -> Result<
    Result<(AssociationPointV1, Vec<WarningCode>), AssociationSourceRejectionV1>,
    AssociationSourceError,
> {
    let mut cells = BTreeMap::new();
    for (logical, index) in indexes {
        let bytes = record.get(*index).ok_or_else(|| {
            AssociationSourceError::new(
                "invalid_record_width",
                "record width does not match its validated header",
            )
        })?;
        let value = std::str::from_utf8(bytes).map_err(|_| {
            AssociationSourceError::new(
                "invalid_utf8",
                format!("mapped field {logical} contains invalid UTF-8 at record {ordinal}"),
            )
        })?;
        cells.insert(*logical, value);
    }

    let required = |logical: LogicalField| -> Result<&str, AssociationSourceRejectionV1> {
        let value = cells.get(&logical).copied().unwrap_or_default();
        if value.is_empty() {
            Err(rejection(
                ordinal,
                match logical {
                    LogicalField::Contig => "missing_contig",
                    LogicalField::Position => "invalid_position",
                    _ => "missing_required_value",
                },
                logical,
                value,
            ))
        } else {
            Ok(value)
        }
    };

    let contig = match required(LogicalField::Contig) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let position_text = match required(LogicalField::Position) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let position = match position_text.parse::<i64>() {
        Ok(value) => value,
        Err(_) => {
            return Ok(Err(rejection(
                ordinal,
                "invalid_position",
                LogicalField::Position,
                position_text,
            )))
        }
    };

    let pvalue = match optional_number(&cells, LogicalField::Pvalue, ordinal, "invalid_pvalue") {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let neg_log10_p = match optional_number(
        &cells,
        LogicalField::NegLog10P,
        ordinal,
        "invalid_neg_log10_p",
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let significance = match normalize_significance(pvalue, neg_log10_p) {
        Ok(value) => value,
        Err(error) => {
            let (code, field) = match error {
                PValueError::MissingSignificance => ("missing_significance", LogicalField::Pvalue),
                PValueError::InvalidPvalue => ("invalid_pvalue", LogicalField::Pvalue),
                PValueError::InvalidNegLog10P => ("invalid_neg_log10_p", LogicalField::NegLog10P),
            };
            return Ok(Err(rejection(
                ordinal,
                code,
                field,
                cells.get(&field).copied().unwrap_or_default(),
            )));
        }
    };

    macro_rules! number {
        ($field:expr, $code:literal) => {
            match optional_number(&cells, $field, ordinal, $code) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            }
        };
    }
    let point = AssociationPointV1 {
        contig: contig.to_string(),
        position,
        ref_allele: optional_text(&cells, LogicalField::Ref),
        alt_allele: optional_text(&cells, LogicalField::Alt),
        pvalue: significance.pvalue,
        neg_log10_p: significance.neg_log10_p,
        beta: number!(LogicalField::Beta, "invalid_beta"),
        standard_error: number!(LogicalField::Se, "invalid_se"),
        allele_frequency: number!(LogicalField::Af, "invalid_af"),
        allele_count: number!(LogicalField::Ac, "invalid_ac"),
        gene_symbol: optional_text(&cells, LogicalField::Gene),
        consequence: optional_text(&cells, LogicalField::Consequence),
        series: optional_text(&cells, LogicalField::Series),
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
                cells.get(&field).copied().unwrap_or_default(),
            )))
        }
    }
}

fn optional_number(
    cells: &BTreeMap<LogicalField, &str>,
    field: LogicalField,
    ordinal: u64,
    code: &'static str,
) -> Result<Option<f64>, AssociationSourceRejectionV1> {
    let Some(value) = cells.get(&field).copied() else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    let parsed = value
        .parse::<f64>()
        .map_err(|_| rejection(ordinal, code, field, value))?;
    if !parsed.is_finite() {
        return Err(rejection(ordinal, code, field, value));
    }
    Ok(Some(parsed))
}

fn optional_text(cells: &BTreeMap<LogicalField, &str>, field: LogicalField) -> Option<String> {
    cells.get(&field).copied().and_then(|value| {
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
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
    example: &str,
) -> AssociationSourceRejectionV1 {
    AssociationSourceRejectionV1::new(ordinal, code, Some(field.to_string()), Some(example))
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
    hasher.update((point.len() as u64).to_be_bytes());
    hasher.update(point);
    hasher.update((warnings.len() as u64).to_be_bytes());
    for warning in warnings {
        let value = warning.as_str().as_bytes();
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    Ok(())
}

fn hash_rejected_item(
    hasher: &mut Sha256,
    ordinal: u64,
    rejection: &AssociationSourceRejectionV1,
    indexes: &BTreeMap<LogicalField, usize>,
    record: &ByteRecord,
) {
    hasher.update(b"rejected\0");
    hasher.update(ordinal.to_be_bytes());
    hasher.update((rejection.reason_code.len() as u64).to_be_bytes());
    hasher.update(rejection.reason_code.as_bytes());
    hasher.update((indexes.len() as u64).to_be_bytes());
    for (logical, index) in indexes {
        let name = logical.as_str().as_bytes();
        let value = record.get(*index).unwrap_or_default();
        hasher.update((name.len() as u64).to_be_bytes());
        hasher.update(name);
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
}

fn validate_headers(record: &ByteRecord) -> Result<Vec<String>, AssociationSourceError> {
    if record.len() == 0 {
        return Err(AssociationSourceError::new(
            "missing_header",
            "delimited source header is empty",
        ));
    }
    if record.len() > MAX_DELIMITED_HEADER_FIELDS {
        return Err(AssociationSourceError::new(
            "too_many_header_fields",
            format!("header exceeds {MAX_DELIMITED_HEADER_FIELDS} fields"),
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
        let header = std::str::from_utf8(raw).map_err(|_| {
            AssociationSourceError::new("invalid_header_utf8", "header must be valid UTF-8")
        })?;
        if header.is_empty() {
            return Err(AssociationSourceError::new(
                "empty_header",
                format!("header field {} is empty", index + 1),
            ));
        }
        if header.len() > 256 {
            return Err(AssociationSourceError::new(
                "header_field_too_long",
                format!("header field {} exceeds 256 UTF-8 bytes", index + 1),
            ));
        }
        if header
            .chars()
            .any(|character| matches!(character, '\0' | '\n' | '\r'))
        {
            return Err(AssociationSourceError::new(
                "invalid_header",
                format!("header field {} contains a control character", index + 1),
            ));
        }
        if !unique.insert(header.to_string()) {
            return Err(AssociationSourceError::new(
                "duplicate_header",
                format!("header field {header:?} is repeated"),
            ));
        }
        headers.push(header.to_string());
    }
    Ok(headers)
}

fn csv_error(error: csv::Error) -> AssociationSourceError {
    match error.kind() {
        ErrorKind::UnequalLengths { .. } => AssociationSourceError::new(
            "invalid_record_width",
            "delimited record width does not match the header",
        ),
        ErrorKind::Io(io_error) => {
            let message = io_error.to_string();
            if message.contains(FRAMING_PREFIX) {
                AssociationSourceError::new("invalid_csv_framing", bounded_error_message(&message))
            } else if message.contains(HEADER_BOUND_PREFIX) {
                AssociationSourceError::new("header_too_large", bounded_error_message(&message))
            } else if message.contains(RECORD_BOUND_PREFIX) {
                AssociationSourceError::new("record_too_large", bounded_error_message(&message))
            } else {
                AssociationSourceError::new("source_io", bounded_error_message(&message))
            }
        }
        _ => AssociationSourceError::new("invalid_csv", bounded_error_message(&error.to_string())),
    }
}

fn bounded_error_message(message: &str) -> String {
    const MAX: usize = 512;
    if message.len() <= MAX {
        message.to_string()
    } else {
        let mut end = MAX;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message[..end].to_string()
    }
}

fn source_io(operation: &str, error: io::Error) -> AssociationSourceError {
    AssociationSourceError::new(
        "source_io",
        format!("could not {operation} delimited source: {error}"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuoteState {
    FieldStart,
    Unquoted,
    Quoted,
    QuoteClosed,
}

struct StrictBoundedReader<R> {
    inner: R,
    delimiter: u8,
    header_limit: usize,
    record_limit: usize,
    record_bytes: usize,
    record_index: u64,
    state: QuoteState,
    previous_was_record_cr: bool,
    reached_eof: bool,
    pending_error: Option<io::Error>,
}

impl<R> StrictBoundedReader<R> {
    fn new(inner: R, delimiter: u8, header_limit: usize, record_limit: usize) -> Self {
        Self {
            inner,
            delimiter,
            header_limit,
            record_limit,
            record_bytes: 0,
            record_index: 0,
            state: QuoteState::FieldStart,
            previous_was_record_cr: false,
            reached_eof: false,
            pending_error: None,
        }
    }

    fn validate_byte(&mut self, byte: u8) -> io::Result<()> {
        // CR already completed and bounded the prior record. Preserve the LF
        // byte for the CSV parser without charging it to the next record.
        if self.previous_was_record_cr && byte == b'\n' {
            self.previous_was_record_cr = false;
            return Ok(());
        }
        self.previous_was_record_cr = false;

        self.record_bytes = self.record_bytes.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{RECORD_BOUND_PREFIX} size overflow"),
            )
        })?;
        let limit = if self.record_index == 0 {
            self.header_limit
        } else {
            self.record_limit
        };
        if self.record_bytes > limit {
            let prefix = if self.record_index == 0 {
                HEADER_BOUND_PREFIX
            } else {
                RECORD_BOUND_PREFIX
            };
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{prefix} record {} exceeds {limit} decoded bytes",
                    self.record_index
                ),
            ));
        }

        match self.state {
            QuoteState::FieldStart => match byte {
                b'"' => self.state = QuoteState::Quoted,
                value if value == self.delimiter => {}
                b'\n' => self.finish_record(false),
                b'\r' => self.finish_record(true),
                _ => self.state = QuoteState::Unquoted,
            },
            QuoteState::Unquoted => match byte {
                b'"' => return self.framing_error("quote inside an unquoted field"),
                value if value == self.delimiter => self.state = QuoteState::FieldStart,
                b'\n' => self.finish_record(false),
                b'\r' => self.finish_record(true),
                _ => {}
            },
            QuoteState::Quoted => {
                if byte == b'"' {
                    self.state = QuoteState::QuoteClosed;
                }
            }
            QuoteState::QuoteClosed => match byte {
                b'"' => self.state = QuoteState::Quoted,
                value if value == self.delimiter => self.state = QuoteState::FieldStart,
                b'\n' => self.finish_record(false),
                b'\r' => self.finish_record(true),
                _ => return self.framing_error("characters after a closing quote"),
            },
        }
        Ok(())
    }

    fn finish_record(&mut self, cr: bool) {
        self.record_index += 1;
        self.record_bytes = 0;
        self.state = QuoteState::FieldStart;
        self.previous_was_record_cr = cr;
    }

    fn framing_error<T>(&self, detail: &str) -> io::Result<T> {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{FRAMING_PREFIX} {detail} in record {}", self.record_index),
        ))
    }
}

impl<R: Read> Read for StrictBoundedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if self.reached_eof {
            return Ok(0);
        }
        let read = self.inner.read(buffer)?;
        if read == 0 {
            self.reached_eof = true;
            if self.state == QuoteState::Quoted {
                return self.framing_error("unterminated quoted field at EOF");
            }
            return Ok(0);
        }
        for (index, byte) in buffer[..read].iter().enumerate() {
            if let Err(error) = self.validate_byte(*byte) {
                if index == 0 {
                    return Err(error);
                }
                // `Read` cannot return bytes and an error together. Return the
                // already-validated prefix now and surface the fatal error on
                // the parser's next read, before any offending byte is exposed.
                self.pending_error = Some(error);
                return Ok(index);
            }
        }
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use genohype_association::AssociationSourceItem;
    use std::io::Write;
    use tempfile::tempdir;

    fn mapping() -> NormalizedFieldMappingV1 {
        NormalizedFieldMappingV1 {
            contig: "CHR".into(),
            position: "POS".into(),
            ref_allele: Some("REF".into()),
            alt_allele: Some("ALT".into()),
            pvalue: Some("P".into()),
            neg_log10_p: None,
            beta: Some("BETA".into()),
            se: None,
            af: Some("AF".into()),
            ac: None,
            gene: Some("GENE".into()),
            consequence: None,
            series: None,
        }
    }

    fn write_file(name: &str, bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let path = temp.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        (temp, path)
    }

    fn csv_kind(gzip: bool) -> DetectedAssociationSource {
        DetectedAssociationSource::Delimited {
            delimiter: b',',
            gzip,
        }
    }

    fn drain(
        source: &mut DelimitedAssociationSource,
    ) -> Result<Vec<AssociationSourceItem>, AssociationSourceError> {
        let mut items = Vec::new();
        while let Some(item) = source.next_item()? {
            items.push(item);
        }
        Ok(items)
    }

    #[test]
    fn streams_csv_and_tsv_with_stable_ordinals_and_recoverable_rows() {
        let (_temp, path) = write_file(
            "input.csv",
            b"CHR,POS,REF,ALT,P,BETA,AF,GENE\nchr1,10,A,G,1e-8,0.5,0.2,G1\nchr1,bad,C,T,0.5,,,\nchr2,20,G,A,2,,,G2\n",
        );
        let mut source =
            DelimitedAssociationSource::open(&path, csv_kind(false), mapping(), "GRCh38").unwrap();
        let items = drain(&mut source).unwrap();
        assert_eq!(items.len(), 3);
        match &items[0] {
            AssociationSourceItem::Point { point, .. } => {
                assert_eq!(point.source_ordinal, 0);
                assert_eq!(point.contig, "1");
            }
            _ => panic!("expected point"),
        }
        match &items[1] {
            AssociationSourceItem::Rejected(rejection) => {
                assert_eq!(rejection.source_ordinal, 1);
                assert_eq!(rejection.reason_code, "invalid_position");
            }
            _ => panic!("expected rejection"),
        }
        match &items[2] {
            AssociationSourceItem::Rejected(rejection) => {
                assert_eq!(rejection.source_ordinal, 2);
                assert_eq!(rejection.reason_code, "invalid_pvalue");
            }
            _ => panic!("expected rejection"),
        }
        assert!(source
            .finalized_source_fingerprint()
            .unwrap()
            .starts_with("sha256:"));
    }

    #[test]
    fn checked_in_tsv_fixture_normalizes_ten_rows_and_rejects_two() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../data/test/association/single-variant.tsv");
        let mapping = NormalizedFieldMappingV1 {
            contig: "CHR".into(),
            position: "POS".into(),
            ref_allele: Some("REF".into()),
            alt_allele: Some("ALT".into()),
            pvalue: Some("P".into()),
            neg_log10_p: Some("NEG_LOG10_P".into()),
            beta: Some("BETA".into()),
            se: Some("SE".into()),
            af: Some("AF".into()),
            ac: Some("AC".into()),
            gene: None,
            consequence: None,
            series: Some("SERIES".into()),
        };
        let mut source = DelimitedAssociationSource::open(
            &path,
            DetectedAssociationSource::Delimited {
                delimiter: b'\t',
                gzip: false,
            },
            mapping,
            "GRCh38",
        )
        .unwrap();
        let items = drain(&mut source).unwrap();
        assert_eq!(items.len(), 12);
        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, AssociationSourceItem::Point { .. }))
                .count(),
            10
        );
        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, AssociationSourceItem::Rejected(_)))
                .count(),
            2
        );
        let underflow = items
            .iter()
            .find_map(|item| match item {
                AssociationSourceItem::Point { point, warnings } if point.source_ordinal == 8 => {
                    Some((point, warnings))
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(underflow.0.pvalue, 0.0);
        assert_eq!(underflow.0.neg_log10_p, 320.0);
        assert_eq!(underflow.1, &[WarningCode::PvalueUnderflow]);
    }

    #[test]
    fn quoted_delimiters_newlines_quotes_and_crlf_are_supported() {
        let (_temp, path) = write_file(
            "quoted.csv",
            b"CHR,POS,REF,ALT,P,BETA,AF,GENE\r\nchr1,10,A,G,1e-8,,,\"A, B\"\r\nchr1,11,C,T,0.5,,,\"line 1\nline \"\"2\"\"\"\r\n",
        );
        let mut source =
            DelimitedAssociationSource::open(&path, csv_kind(false), mapping(), "GRCh38").unwrap();
        let items = drain(&mut source).unwrap();
        assert_eq!(items.len(), 2);
        let AssociationSourceItem::Point { point, .. } = &items[1] else {
            panic!("expected point");
        };
        assert_eq!(point.gene_symbol.as_deref(), Some("line 1\nline \"2\""));
    }

    #[test]
    fn bom_is_stripped_only_from_first_header_and_headers_are_unique_utf8_and_bounded() {
        let (_temp, path) = write_file(
            "bom.csv",
            b"\xef\xbb\xbfCHR,POS,REF,ALT,P,BETA,AF,GENE\nchr1,1,A,G,0.5,,,,\n",
        );
        let source =
            DelimitedAssociationSource::open(&path, csv_kind(false), mapping(), "GRCh38").unwrap();
        assert_eq!(source.metadata().observed_fields[0].name, "CHR");

        let (_temp2, duplicate) = write_file("duplicate.csv", b"CHR,POS,P,P\n1,1,.5,.5\n");
        let error = match DelimitedRecordReader::open(&duplicate, csv_kind(false)) {
            Ok(_) => panic!("duplicate header should fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "duplicate_header");

        let mut oversized = vec![b'x'; MAX_DELIMITED_HEADER_BYTES + 1];
        oversized.push(b'\n');
        let (_temp3, path) = write_file("large.csv", &oversized);
        let error = match DelimitedRecordReader::open(&path, csv_kind(false)) {
            Ok(_) => panic!("oversized header should fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "header_too_large");
    }

    #[test]
    fn invalid_utf8_in_a_mapped_cell_is_fatal() {
        let mut bytes = b"CHR,POS,REF,ALT,P,BETA,AF,GENE\nchr1,1,A,G,0.5,,,".to_vec();
        bytes.push(0xff);
        bytes.push(b'\n');
        let (_temp, path) = write_file("invalid-utf8.csv", &bytes);
        let mut source =
            DelimitedAssociationSource::open(&path, csv_kind(false), mapping(), "GRCh38").unwrap();
        assert_eq!(source.next_item().unwrap_err().code, "invalid_utf8");
    }

    #[test]
    fn width_quote_and_record_bounds_are_fatal_before_csv_growth() {
        let (_temp, width) = write_file("width.csv", b"A,B\n1,2\n3\n");
        let mut reader = DelimitedRecordReader::open(&width, csv_kind(false)).unwrap();
        let mut record = ByteRecord::new();
        assert!(reader.next_record(&mut record).unwrap());
        assert_eq!(
            reader.next_record(&mut record).unwrap_err().code,
            "invalid_record_width"
        );

        for (name, bytes) in [
            ("midquote.csv", b"A,B\nfoo\"bar,x\n".as_slice()),
            ("afterquote.csv", b"A,B\n\"foo\"x,y\n".as_slice()),
            ("unterminated.csv", b"A,B\n\"foo,y\n".as_slice()),
        ] {
            let (_temp, path) = write_file(name, bytes);
            let mut reader = DelimitedRecordReader::open(&path, csv_kind(false)).unwrap();
            assert_eq!(
                reader.next_record(&mut ByteRecord::new()).unwrap_err().code,
                "invalid_csv_framing"
            );
        }

        let mut bytes = b"A,B\n".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', MAX_DELIMITED_RECORD_BYTES + 1));
        bytes.extend_from_slice(b",y\n");
        let (_temp, path) = write_file("large-record.csv", &bytes);
        let mut reader = DelimitedRecordReader::open(&path, csv_kind(false)).unwrap();
        assert_eq!(
            reader.next_record(&mut ByteRecord::new()).unwrap_err().code,
            "record_too_large"
        );
    }

    fn gzip_member(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn concatenated_gzip_decodes_and_corrupt_or_truncated_gzip_is_fatal() {
        let mut concatenated = gzip_member(b"CHR,POS,REF,ALT,P,BETA,AF,GENE\nchr1,1,A,G,0.5,,,\n");
        concatenated.extend(gzip_member(b"chr2,2,C,T,0.25,,,\n"));
        let (_temp, path) = write_file("input.csv.gz", &concatenated);
        let mut source =
            DelimitedAssociationSource::open(&path, csv_kind(true), mapping(), "GRCh38").unwrap();
        assert_eq!(drain(&mut source).unwrap().len(), 2);

        let (_temp2, corrupt) = write_file("corrupt.csv.gz", b"not gzip");
        assert!(DelimitedRecordReader::open(&corrupt, csv_kind(true)).is_err());

        let mut truncated = gzip_member(b"CHR,POS,REF,ALT,P,BETA,AF,GENE\nchr1,1,A,G,0.5,,,\n");
        truncated.truncate(truncated.len() - 4);
        let (_temp3, path) = write_file("truncated.csv.gz", &truncated);
        let result = DelimitedRecordReader::open(&path, csv_kind(true)).and_then(|mut reader| {
            let mut record = ByteRecord::new();
            while reader.next_record(&mut record)? {}
            Ok(reader)
        });
        assert!(result.is_err());
    }

    #[test]
    fn semantic_fingerprint_ignores_unmapped_columns_but_includes_rejected_mapped_content() {
        fn fingerprint(unmapped: &str, bad_position: &str) -> String {
            let body = format!(
                "CHR,POS,REF,ALT,P,BETA,AF,GENE,IGNORED\nchr1,{bad_position},A,G,0.5,,,,{unmapped}\n"
            );
            let (_temp, path) = write_file("input.csv", body.as_bytes());
            let mut source =
                DelimitedAssociationSource::open(&path, csv_kind(false), mapping(), "GRCh38")
                    .unwrap();
            drain(&mut source).unwrap();
            source.finalized_source_fingerprint().unwrap().to_string()
        }
        assert_eq!(fingerprint("one", "bad"), fingerprint("two", "bad"));
        assert_ne!(fingerprint("one", "bad"), fingerprint("one", "worse"));

        fn accepted_fingerprint(pvalue: &str) -> String {
            let body = format!("CHR,POS,REF,ALT,P,BETA,AF,GENE\nchr1,1,A,G,{pvalue},,,G1\n");
            let (_temp, path) = write_file("accepted.csv", body.as_bytes());
            let mut source =
                DelimitedAssociationSource::open(&path, csv_kind(false), mapping(), "GRCh38")
                    .unwrap();
            drain(&mut source).unwrap();
            source.finalized_source_fingerprint().unwrap().to_string()
        }
        assert_eq!(accepted_fingerprint("0.5"), accepted_fingerprint(".5"));
    }
}
