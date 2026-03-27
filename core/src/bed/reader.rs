//! BED Data Source Implementation
//!
//! Reads Tabix-indexed BED files (.bed.gz) and exposes them as a `DataSource`.
//! Each line is converted to an `EncodedValue::Struct` with fields derived
//! from the file header or inferred from the data.
//!
//! Schema detection strategy:
//! 1. If the file has a `#`-prefixed header line, column names are read from it
//! 2. Otherwise, the first 3 columns are named `chrom`, `start`, `end` per the
//!    BED spec, and additional columns are named `col3`, `col4`, etc.
//! 3. Column types are inferred by sampling the first data line: values that
//!    parse as integers become Int32, floats become Float64, everything else
//!    is a String.

use crate::codec::{EncodedField, EncodedType, EncodedValue};
use crate::datasource::DataSource;
use crate::query::{IntervalList, KeyRange, KeyValue, QueryBound};
use crate::{HailError, Result};
use noodles::bgzf;
use noodles::core::Region;
use noodles::csi::BinningIndex;
use noodles::tabix;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use tracing::{debug, info};

/// Default names for the first three BED columns when no header is present
const DEFAULT_BED_NAMES: &[&str] = &["chrom", "start", "end"];

/// Inferred column type
#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnType {
    String,
    Int32,
    Float64,
}

/// Infer the type of a single value string
fn infer_type(value: &str) -> ColumnType {
    if value.parse::<i32>().is_ok() {
        ColumnType::Int32
    } else if value.parse::<f64>().is_ok() {
        ColumnType::Float64
    } else {
        ColumnType::String
    }
}

/// DataSource implementation for Tabix-indexed BED files
///
/// Automatically detects the schema from file headers and data. Works with
/// any tab-separated BED-like format (standard BED3-BED12, methylation BEDs,
/// bedGraph, etc.).
pub struct BedDataSource {
    /// Path to the BED file
    path: String,
    /// Generated schema
    schema: EncodedType,
    /// Column definitions: (name, type)
    columns: Vec<(String, ColumnType)>,
    /// The name of the first column (chrom/contig column) for region queries
    chrom_col: String,
    /// The name of the second column (start position) for region queries
    start_col: String,
    /// Optional Tabix index for region queries
    index: Option<tabix::Index>,
    /// Contig names from the tabix index
    contigs: Vec<String>,
}

impl BedDataSource {
    /// Open a BED file for reading
    ///
    /// Reads the first lines to detect column names and types:
    /// - A `#`-prefixed header provides column names
    /// - The first data line is used to infer column types
    ///
    /// # Arguments
    /// * `path` - Path to the .bed.gz file (local or cloud URL)
    pub fn new(path: &str) -> Result<Self> {
        // Read header + first data line to detect schema
        let (col_names, col_types, chrom_col, start_col) = Self::detect_schema(path)?;

        let columns: Vec<(String, ColumnType)> = col_names
            .into_iter()
            .zip(col_types)
            .collect();

        let schema = Self::build_schema(&columns);

        // Try to load tabix index
        let index = Self::load_index(path);

        // Extract contigs from index
        let contigs = if let Some(ref idx) = index {
            if let Some(idx_header) = idx.header() {
                let names: Vec<String> = idx_header
                    .reference_sequence_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                debug!("BED tabix index contains {} contigs", names.len());
                names
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        if index.is_some() {
            debug!("Loaded tabix index for BED file {}", path);
        }

        info!(
            "BED schema: {} columns detected for {}",
            columns.len(),
            path
        );

        Ok(Self {
            path: path.to_string(),
            schema,
            columns,
            chrom_col,
            start_col,
            index,
            contigs,
        })
    }

    /// Read the file header and first data line to detect column names and types
    fn detect_schema(path: &str) -> Result<(Vec<String>, Vec<ColumnType>, String, String)> {
        let reader = crate::io::get_reader(path)?;
        let bgzf_reader = bgzf::Reader::new(reader);
        let mut buf_reader = BufReader::new(bgzf_reader);

        let mut header_names: Option<Vec<String>> = None;
        let mut first_data_line: Option<String> = None;
        let mut line_buf = String::new();

        // Read lines until we find a data line (and optionally a header)
        loop {
            line_buf.clear();
            let bytes_read = buf_reader.read_line(&mut line_buf).map_err(HailError::Io)?;
            if bytes_read == 0 {
                break;
            }
            let line = line_buf.trim_end();
            if line.is_empty() {
                continue;
            }
            if line.starts_with('#') {
                // Parse header: strip leading # and split by tab
                let header_line = line.trim_start_matches('#');
                header_names = Some(
                    header_line
                        .split('\t')
                        .map(|s| s.trim().to_string())
                        .collect(),
                );
                continue;
            }
            // First non-comment, non-empty line is data
            first_data_line = Some(line.to_string());
            break;
        }

        let first_line = first_data_line.ok_or_else(|| {
            HailError::InvalidFormat("BED file has no data lines".to_string())
        })?;

        let parts: Vec<&str> = first_line.split('\t').collect();
        let num_cols = parts.len();

        // Determine column names
        let col_names: Vec<String> = if let Some(ref names) = header_names {
            // Use header names, pad with positional names if header has fewer columns
            let mut names = names.clone();
            while names.len() < num_cols {
                names.push(format!("col{}", names.len()));
            }
            names.truncate(num_cols);
            names
        } else {
            // No header: use BED defaults for first 3, then positional names
            (0..num_cols)
                .map(|i| {
                    if i < DEFAULT_BED_NAMES.len() {
                        DEFAULT_BED_NAMES[i].to_string()
                    } else {
                        format!("col{}", i)
                    }
                })
                .collect()
        };

        // Infer types from the first data line
        // Always force column 0 to String (chrom) and columns 1,2 to Int32 (positions)
        let col_types: Vec<ColumnType> = parts
            .iter()
            .enumerate()
            .map(|(i, val)| {
                if i == 0 {
                    ColumnType::String // chrom is always a string
                } else if i <= 2 {
                    ColumnType::Int32 // start/end are always integers
                } else {
                    infer_type(val)
                }
            })
            .collect();

        let chrom_col = col_names[0].clone();
        let start_col = col_names[1].clone();

        Ok((col_names, col_types, chrom_col, start_col))
    }

    /// Build an EncodedType schema from column definitions
    fn build_schema(columns: &[(String, ColumnType)]) -> EncodedType {
        let fields: Vec<EncodedField> = columns
            .iter()
            .enumerate()
            .map(|(idx, (name, ct))| EncodedField {
                name: name.clone(),
                encoded_type: match ct {
                    ColumnType::String => EncodedType::EBinary { required: true },
                    ColumnType::Int32 => EncodedType::EInt32 { required: true },
                    ColumnType::Float64 => EncodedType::EFloat64 { required: true },
                },
                index: idx,
            })
            .collect();

        EncodedType::EBaseStruct {
            required: true,
            fields,
        }
    }

    /// Try to load a tabix index for the BED file
    fn load_index(bed_path: &str) -> Option<tabix::Index> {
        let index_path = format!("{}.tbi", bed_path);
        match crate::io::get_reader(&index_path) {
            Ok(reader) => {
                let mut tabix_reader = tabix::io::Reader::new(reader);
                tabix_reader.read_index().ok()
            }
            Err(_) => None,
        }
    }

    /// Convert KeyRanges to a noodles Region for indexed queries
    ///
    /// Uses the detected chrom/start column names so this works regardless
    /// of whether the file uses "chrom"/"begin" or "#chrom"/"start" etc.
    fn ranges_to_region(&self, ranges: &[KeyRange]) -> Option<Region> {
        let mut contig: Option<String> = None;
        let mut start: Option<usize> = None;
        let mut end: Option<usize> = None;

        for range in ranges {
            let field = if range.field_path.len() == 1 {
                &range.field_path[0]
            } else {
                continue;
            };

            // Match on the detected chrom column name
            if field == &self.chrom_col {
                if let (
                    QueryBound::Included(KeyValue::String(s)),
                    QueryBound::Included(KeyValue::String(e)),
                ) = (&range.start, &range.end)
                {
                    if s == e {
                        contig = Some(s.clone());
                    }
                }
            }

            // Match on the detected start column name (for position range)
            if field == &self.start_col {
                match &range.start {
                    QueryBound::Included(KeyValue::Int32(v)) => {
                        let new_start = *v as usize;
                        start = Some(start.map_or(new_start, |s: usize| s.max(new_start)));
                    }
                    QueryBound::Excluded(KeyValue::Int32(v)) => {
                        let new_start = (*v + 1) as usize;
                        start = Some(start.map_or(new_start, |s: usize| s.max(new_start)));
                    }
                    _ => {}
                }
                match &range.end {
                    QueryBound::Included(KeyValue::Int32(v)) => {
                        let new_end = *v as usize;
                        end = Some(end.map_or(new_end, |e: usize| e.min(new_end)));
                    }
                    QueryBound::Excluded(KeyValue::Int32(v)) => {
                        let new_end = (*v - 1) as usize;
                        end = Some(end.map_or(new_end, |e: usize| e.min(new_end)));
                    }
                    _ => {}
                }
            }
        }

        contig.map(|c| {
            use noodles::core::Position;
            match (start, end) {
                (Some(s), Some(e)) => {
                    // BED is 0-based, noodles Region is 1-based
                    let start_pos = Position::try_from(s.max(1)).unwrap_or(Position::MIN);
                    let end_pos = Position::try_from(e).unwrap_or(Position::MAX);
                    Region::new(c, start_pos..=end_pos)
                }
                (Some(s), None) => {
                    let start_pos = Position::try_from(s.max(1)).unwrap_or(Position::MIN);
                    Region::new(c, start_pos..)
                }
                (None, Some(e)) => {
                    let end_pos = Position::try_from(e).unwrap_or(Position::MAX);
                    Region::new(c, ..=end_pos)
                }
                (None, None) => c.parse().unwrap_or_else(|_| Region::new(c, ..)),
            }
        })
    }

    /// Create a full-scan iterator (reads entire file)
    fn full_scan_iter(&self) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let reader = crate::io::get_reader(&self.path)?;
        let bgzf_reader = bgzf::Reader::new(reader);
        let buf_reader = BufReader::new(bgzf_reader);

        let columns = self.columns.clone();
        let iter = BedLineIterator {
            reader: buf_reader,
            columns,
            line_buf: String::new(),
        };
        Ok(Box::new(iter))
    }

    /// Perform an indexed query using the tabix index
    fn indexed_query(
        &self,
        region: &Region,
        index: &tabix::Index,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let reader = crate::io::get_reader(&self.path)?;
        let bgzf_reader = bgzf::Reader::new(reader);
        let mut buf_reader = BufReader::new(bgzf_reader);

        // Look up the reference sequence index for this contig
        let ref_seq_id = index
            .header()
            .and_then(|h| {
                h.reference_sequence_names()
                    .get_index_of(region.name())
            })
            .ok_or_else(|| {
                HailError::InvalidFormat(format!(
                    "Contig {} not found in tabix index",
                    region.name()
                ))
            })?;

        let chunks = index
            .query(ref_seq_id, region.interval())
            .map_err(|e| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        let columns = self.columns.clone();
        let ranges = ranges.to_vec();

        // Read all matching records from the indexed chunks
        let mut records: Vec<Result<EncodedValue>> = Vec::new();
        let mut line_buf = String::new();

        // Seek to the first chunk's start position
        if let Some(first_chunk) = chunks.first() {
            buf_reader.get_mut().seek(first_chunk.start()).map_err(HailError::Io)?;
            // Reconstruct BufReader to clear stale buffer after seek
            buf_reader = BufReader::new(buf_reader.into_inner());
        }

        // Extract the query end position from ranges for early termination.
        // Since BED data is sorted by position within a contig, we can stop
        // reading once we pass the query region's end.
        let query_end: Option<i32> = ranges.iter().find_map(|r| {
            if r.field_path.len() == 1 && r.field_path[0] == self.start_col {
                match &r.end {
                    QueryBound::Included(KeyValue::Int32(v)) => Some(*v),
                    QueryBound::Excluded(KeyValue::Int32(v)) => Some(*v),
                    _ => None,
                }
            } else {
                None
            }
        });

        // Read lines sequentially from the seeked position. We don't stop at
        // chunk boundaries because tabix chunks are bgzf-block-aligned — the
        // records matching our query may span into the next block after the
        // chunk's end virtual position.
        loop {
            line_buf.clear();
            let bytes_read = buf_reader.read_line(&mut line_buf).map_err(HailError::Io)?;
            if bytes_read == 0 {
                break;
            }

            let line = line_buf.trim_end();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Early termination: if we've read past the query end position,
            // stop (BED files are sorted by position within a contig)
            if let Some(end_pos) = query_end {
                let parts: Vec<&str> = line.splitn(3, '\t').collect();
                if parts.len() >= 2 {
                    if let Ok(pos) = parts[1].parse::<i32>() {
                        if pos > end_pos {
                            break;
                        }
                    }
                }
            }

            let row = parse_bed_line(line, &columns)?;

            if ranges.is_empty() || row_matches_ranges(&row, &ranges) {
                records.push(Ok(row));
            }
        }
        Ok(Box::new(records.into_iter()))
    }
}

/// Parse a single BED line given column definitions
fn parse_bed_line(line: &str, columns: &[(String, ColumnType)]) -> Result<EncodedValue> {
    let parts: Vec<&str> = line.split('\t').collect();

    let fields: Vec<(String, EncodedValue)> = columns
        .iter()
        .enumerate()
        .map(|(idx, (name, ct))| {
            let value = if idx < parts.len() {
                let raw = parts[idx];
                match ct {
                    ColumnType::String => EncodedValue::Binary(raw.as_bytes().to_vec()),
                    ColumnType::Int32 => EncodedValue::Int32(raw.parse::<i32>().unwrap_or(0)),
                    ColumnType::Float64 => {
                        EncodedValue::Float64(raw.parse::<f64>().unwrap_or(0.0))
                    }
                }
            } else {
                EncodedValue::Null
            };
            (name.clone(), value)
        })
        .collect();

    Ok(EncodedValue::Struct(fields))
}

/// Check if a row matches all the given key ranges
fn row_matches_ranges(row: &EncodedValue, ranges: &[KeyRange]) -> bool {
    for range in ranges {
        if !row_matches_single_range(row, range) {
            return false;
        }
    }
    true
}

/// Check if a row matches a single key range
fn row_matches_single_range(row: &EncodedValue, range: &KeyRange) -> bool {
    let field_value = get_nested_field(row, &range.field_path);
    let field_value = match field_value {
        Some(v) => v,
        None => return false,
    };

    let cmp_start = match &range.start {
        QueryBound::Unbounded => true,
        QueryBound::Included(key) => compare_values(field_value, key) >= std::cmp::Ordering::Equal,
        QueryBound::Excluded(key) => compare_values(field_value, key) > std::cmp::Ordering::Equal,
    };

    let cmp_end = match &range.end {
        QueryBound::Unbounded => true,
        QueryBound::Included(key) => compare_values(field_value, key) <= std::cmp::Ordering::Equal,
        QueryBound::Excluded(key) => compare_values(field_value, key) < std::cmp::Ordering::Equal,
    };

    cmp_start && cmp_end
}

/// Get a nested field value from an EncodedValue
fn get_nested_field<'a>(value: &'a EncodedValue, path: &[String]) -> Option<&'a EncodedValue> {
    if path.is_empty() {
        return Some(value);
    }

    if let EncodedValue::Struct(fields) = value {
        for (name, field_value) in fields {
            if name == &path[0] {
                return get_nested_field(field_value, &path[1..]);
            }
        }
    }

    None
}

/// Compare an EncodedValue to a KeyValue
fn compare_values(value: &EncodedValue, key: &KeyValue) -> std::cmp::Ordering {
    match (value, key) {
        (EncodedValue::Int32(v), KeyValue::Int32(k)) => v.cmp(k),
        (EncodedValue::Int64(v), KeyValue::Int64(k)) => v.cmp(k),
        (EncodedValue::Float32(v), KeyValue::Float32(k)) => {
            v.partial_cmp(k).unwrap_or(std::cmp::Ordering::Equal)
        }
        (EncodedValue::Float64(v), KeyValue::Float64(k)) => {
            v.partial_cmp(k).unwrap_or(std::cmp::Ordering::Equal)
        }
        (EncodedValue::Binary(v), KeyValue::String(k)) => {
            String::from_utf8_lossy(v).as_ref().cmp(k)
        }
        (EncodedValue::Boolean(v), KeyValue::Boolean(k)) => v.cmp(k),
        (EncodedValue::Int32(v), KeyValue::Int64(k)) => (*v as i64).cmp(k),
        (EncodedValue::Int64(v), KeyValue::Int32(k)) => v.cmp(&(*k as i64)),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Streaming iterator that reads BED lines from a bgzf reader
struct BedLineIterator<R: Read> {
    reader: BufReader<bgzf::Reader<R>>,
    columns: Vec<(String, ColumnType)>,
    line_buf: String,
}

impl<R: Read + Send + 'static> Iterator for BedLineIterator<R> {
    type Item = Result<EncodedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buf.clear();
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    let line = self.line_buf.trim_end();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    return Some(parse_bed_line(line, &self.columns));
                }
                Err(e) => return Some(Err(HailError::Io(e))),
            }
        }
    }
}

// BedLineIterator is Send if its reader is Send
unsafe impl<R: Read + Send> Send for BedLineIterator<R> {}

impl DataSource for BedDataSource {
    fn row_type(&self) -> &EncodedType {
        &self.schema
    }

    fn globals(&self) -> Result<EncodedValue> {
        Ok(EncodedValue::Struct(vec![]))
    }

    fn key_fields(&self) -> &[String] {
        &[]
    }

    fn num_partitions(&self) -> usize {
        if self.index.is_some() && !self.contigs.is_empty() {
            self.contigs.len()
        } else {
            1
        }
    }

    fn scan_partition_stream(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        if self.index.is_some() && !self.contigs.is_empty() {
            if partition_idx >= self.contigs.len() {
                return Ok(Box::new(std::iter::empty()));
            }

            let contig = &self.contigs[partition_idx];
            let index = self.index.as_ref().unwrap();
            let region: Region = contig
                .parse()
                .unwrap_or_else(|_| Region::new(contig.clone(), ..));

            debug!(
                "BED: scanning partition {} (contig {})",
                partition_idx, contig
            );
            self.indexed_query(&region, index, ranges)
        } else {
            if partition_idx != 0 {
                return Ok(Box::new(std::iter::empty()));
            }
            self.query_stream(ranges)
        }
    }

    fn query_stream_with_intervals(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Try indexed query first
        if let Some(ref index) = self.index {
            // Convert --where ranges to a region
            if let Some(region) = self.ranges_to_region(ranges) {
                debug!("BED: using indexed query for region {:?}", region);
                return self.indexed_query(&region, index, ranges);
            }

            // Convert --interval to regions for tabix
            if let Some(ref interval_list) = intervals {
                use noodles::core::Position;
                let mut all_results: Vec<Result<EncodedValue>> = Vec::new();

                for contig in interval_list.contigs() {
                    if let Some(ranges_for_contig) = interval_list.intervals_for_contig(contig) {
                        for range in ranges_for_contig {
                            let start = *range.start() as usize;
                            let end = *range.end() as usize;
                            let start_pos = Position::try_from(start.max(1)).unwrap_or(Position::MIN);
                            let end_pos = Position::try_from(end).unwrap_or(Position::MAX);
                            let region = Region::new(contig.as_str(), start_pos..=end_pos);

                            debug!("BED: indexed query for interval {}:{}-{}", contig, start, end);
                            let iter = self.indexed_query(&region, index, ranges)?;
                            all_results.extend(iter);
                        }
                    }
                }

                info!("BED: indexed interval query returned {} records", all_results.len());
                return Ok(Box::new(all_results.into_iter()));
            }
        }

        // Fall back to full scan with filtering
        debug!("BED: falling back to full scan");
        let iter = self.full_scan_iter()?;
        if ranges.is_empty() && intervals.is_none() {
            Ok(iter)
        } else {
            let ranges = ranges.to_vec();
            Ok(Box::new(iter.filter(move |result| match result {
                Ok(row) => row_matches_ranges(row, &ranges),
                Err(_) => true,
            })))
        }
    }
}
