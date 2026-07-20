//! VCF Data Source
//!
//! Implements the `DataSource` trait for VCF files using noodles.
//! Supports both plain VCF and BGZF-compressed VCF files (.vcf.gz).
//! Optional Tabix index support for efficient region queries.

use crate::codec::{EncodedType, EncodedValue};
use crate::datasource::DataSource;
use crate::query::{IntervalList, KeyRange, KeyValue, QueryBound};
use crate::{HailError, Result};
use indicatif::{ProgressBar, ProgressStyle};
use noodles::bgzf;
use noodles::core::Region;
use noodles::csi::BinningIndex;
use noodles::tabix;
use noodles::vcf;
use rand::Rng;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// DataSource implementation for VCF files
///
/// Provides read access to VCF files using noodles library.
/// Supports:
/// - Plain VCF files
/// - BGZF-compressed VCF files (.vcf.gz)
/// - Tabix-indexed VCF files (.vcf.gz.tbi) for efficient region queries
pub struct VcfDataSource {
    /// Path to the VCF file
    path: String,
    /// Parsed VCF header
    header: Arc<vcf::Header>,
    /// Generated schema from header
    schema: EncodedType,
    /// Optional Tabix index for region queries
    index: Option<tabix::Index>,
    /// Whether the file is BGZF compressed
    is_bgzf: bool,
    /// Contig names from the header (for chromosome-based partitioning)
    contigs: Vec<String>,
    /// Contig lengths from the header (if available)
    contig_lengths: Vec<Option<usize>>,
}

impl VcfDataSource {
    /// Open a VCF file for reading
    ///
    /// # Arguments
    /// * `path` - Path to the VCF file (local or cloud URL)
    ///
    /// Automatically detects:
    /// - BGZF compression (by .gz extension)
    /// - Tabix index presence (.tbi file)
    pub fn new(path: &str) -> Result<Self> {
        let is_bgzf = path.ends_with(".gz") || path.ends_with(".bgz");

        // Read header
        let mut header = if is_bgzf {
            let reader = crate::io::get_reader(path)?;
            let bgzf_reader = bgzf::Reader::new(reader);
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(bgzf_reader));
            vcf_reader.read_header().map_err(HailError::Io)?
        } else {
            let reader = crate::io::get_reader(path)?;
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(reader));
            vcf_reader.read_header().map_err(HailError::Io)?
        };

        // Patch FORMAT fields: convert Type=Character to Type=String.
        // Some VCFs (e.g., HPRC) encode Character fields without comma separators
        // (e.g., RNC=".." instead of ".,." ), which noodles rejects. Treating them
        // as String avoids the parse error and is lossless for export.
        Self::patch_character_formats(&mut header);

        // Extract contig information from header (for lengths)
        let (header_contigs, header_lengths) = Self::extract_contigs(&header);
        if !header_contigs.is_empty() {
            debug!("Found {} contigs in VCF header", header_contigs.len());
        }

        // Generate schema from header
        let schema = super::schema::extract_schema_from_header(&header)?;

        // Try to load tabix index
        let index = Self::load_index(path);

        // Determine which contigs to use for partitioning:
        // - If we have a tabix index with header, use only contigs that exist in the index
        //   (the index only contains contigs with actual data)
        // - Otherwise, fall back to header contigs
        let (contigs, contig_lengths) = if let Some(ref idx) = index {
            if let Some(idx_header) = idx.header() {
                let index_contigs: Vec<String> = idx_header
                    .reference_sequence_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();

                debug!(
                    "Tabix index contains {} contigs: {:?}",
                    index_contigs.len(),
                    index_contigs
                );

                // Map index contigs to their lengths from the header
                let lengths: Vec<Option<usize>> = index_contigs
                    .iter()
                    .map(|name| {
                        header_contigs
                            .iter()
                            .position(|h| h == name)
                            .and_then(|pos| header_lengths.get(pos).copied().flatten())
                    })
                    .collect();

                (index_contigs, lengths)
            } else {
                debug!("Tabix index has no header, falling back to VCF header contigs");
                (header_contigs, header_lengths)
            }
        } else {
            (header_contigs, header_lengths)
        };

        if index.is_some() {
            debug!("Loaded tabix index for {}", path);
        }

        Ok(Self {
            path: path.to_string(),
            header: Arc::new(header),
            schema,
            index,
            is_bgzf,
            contigs,
            contig_lengths,
        })
    }

    /// Rewrite any FORMAT fields with Type=Character to Type=String.
    /// This avoids noodles strict single-character validation which rejects
    /// VCFs that encode multi-character values without comma separators.
    fn patch_character_formats(header: &mut vcf::Header) {
        use noodles::vcf::header::record::value::map::format::Type;

        for (id, fmt) in header.formats_mut().iter_mut() {
            if fmt.ty() == Type::Character {
                *fmt.type_mut() = Type::String;
                warn!("Patched FORMAT/{} from Type=Character to Type=String", id);
            }
        }
    }

    /// Extract contig names and lengths from the VCF header
    fn extract_contigs(header: &vcf::Header) -> (Vec<String>, Vec<Option<usize>>) {
        let contigs = header.contigs();
        let names: Vec<String> = contigs.keys().map(|s| s.to_string()).collect();
        let lengths: Vec<Option<usize>> = contigs
            .values()
            .map(|c| c.length().map(|l| l.into()))
            .collect();
        (names, lengths)
    }

    /// Check if random access is available (has tabix index)
    pub fn has_index(&self) -> bool {
        self.index.is_some() && self.is_bgzf
    }

    /// Get list of contigs (chromosomes) in the VCF
    pub fn contigs(&self) -> &[String] {
        &self.contigs
    }

    /// Get the length of a contig if known from the header
    pub fn contig_length(&self, contig: &str) -> Option<usize> {
        self.contigs
            .iter()
            .position(|c| c == contig)
            .and_then(|idx| self.contig_lengths.get(idx).copied().flatten())
    }

    /// Try to load a tabix index for the VCF file
    fn load_index(vcf_path: &str) -> Option<tabix::Index> {
        let index_path = format!("{}.tbi", vcf_path);
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
    /// Currently supports:
    /// - Point queries on locus.contig (chromosome)
    /// - Range queries on locus.position
    fn ranges_to_region(&self, ranges: &[KeyRange]) -> Option<Region> {
        let mut contig: Option<String> = None;
        let mut start: Option<usize> = None;
        let mut end: Option<usize> = None;

        for range in ranges {
            // Check for locus.contig
            if range.field_path.len() >= 2
                && range.field_path[0] == "locus"
                && range.field_path[1] == "contig"
            {
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

            // Check for locus.position
            if range.field_path.len() >= 2
                && range.field_path[0] == "locus"
                && range.field_path[1] == "position"
            {
                // Extract start position
                match &range.start {
                    QueryBound::Included(KeyValue::Int32(v)) => start = Some(*v as usize),
                    QueryBound::Excluded(KeyValue::Int32(v)) => start = Some((*v + 1) as usize),
                    QueryBound::Unbounded => {}
                    _ => {}
                }
                // Extract end position
                match &range.end {
                    QueryBound::Included(KeyValue::Int32(v)) => end = Some(*v as usize),
                    QueryBound::Excluded(KeyValue::Int32(v)) => end = Some((*v - 1) as usize),
                    QueryBound::Unbounded => {}
                    _ => {}
                }
            }
        }

        // Build region if we have at least a contig
        contig.map(|c| {
            use noodles::core::Position;
            match (start, end) {
                (Some(s), Some(e)) => {
                    let start_pos = Position::try_from(s).unwrap_or(Position::MIN);
                    let end_pos = Position::try_from(e).unwrap_or(Position::MAX);
                    Region::new(c, start_pos..=end_pos)
                }
                (Some(s), None) => {
                    let start_pos = Position::try_from(s).unwrap_or(Position::MIN);
                    Region::new(c, start_pos..)
                }
                (None, Some(e)) => {
                    let end_pos = Position::try_from(e).unwrap_or(Position::MAX);
                    Region::new(c, ..=end_pos)
                }
                (None, None) => {
                    // Just contig, no position constraints
                    c.parse().unwrap_or_else(|_| Region::new(c, ..))
                }
            }
        })
    }

    /// Create an iterator for a full file scan
    fn full_scan_iter(&self) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let header = self.header.clone();

        if self.is_bgzf {
            let reader = crate::io::get_reader(&self.path)?;
            let bgzf_reader = bgzf::Reader::new(reader);
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(bgzf_reader));

            // Skip header (we already have it)
            let _ = vcf_reader.read_header().map_err(HailError::Io)?;

            let iter = VcfRecordIterator {
                reader: Box::new(vcf_reader),
                header,
            };
            Ok(Box::new(iter))
        } else {
            let reader = crate::io::get_reader(&self.path)?;
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(reader));

            // Skip header
            let _ = vcf_reader.read_header().map_err(HailError::Io)?;

            let iter = VcfRecordIterator {
                reader: Box::new(vcf_reader),
                header,
            };
            Ok(Box::new(iter))
        }
    }

    /// Perform an indexed query on a local file
    fn indexed_query_local(
        &self,
        region: &Region,
        index: &tabix::Index,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Open indexed reader using the file path
        let mut indexed_reader = vcf::io::indexed_reader::Builder::default()
            .set_index(index.clone())
            .build_from_path(&self.path)
            .map_err(HailError::Io)?;

        // Read header
        let header = Arc::new(indexed_reader.read_header().map_err(HailError::Io)?);

        // Create query iterator
        let query = indexed_reader
            .query(&header, region)
            .map_err(HailError::Io)?;

        // Note: The noodles Query iterator borrows from the reader, so we can't
        // easily return it as a streaming iterator. We collect records eagerly.
        // For huge result sets, consider a different approach (channel-based).
        let header_clone = header.clone();
        let ranges = ranges.to_vec();
        let records: Vec<Result<EncodedValue>> = query
            .filter_map(|result| match result {
                Ok(record) => match super::codec::record_to_row_lazy(&header_clone, &record) {
                    Ok(row) => {
                        if ranges.is_empty() || row_matches_ranges(&row, &ranges) {
                            Some(Ok(row))
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        warn!("Skipping VCF record (conversion error): {}", e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Skipping VCF record (query error): {}", e);
                    None
                }
            })
            .collect();

        info!("Indexed query returned {} records", records.len());
        Ok(Box::new(records.into_iter()))
    }

    /// Perform an indexed query on a remote file (GCS, S3, HTTP)
    fn indexed_query_remote(
        &self,
        region: &Region,
        index: &tabix::Index,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Get a reader for the remote file
        let reader = crate::io::get_reader(&self.path)?;

        // Create indexed reader using build_from_reader
        // This wraps our BoxedReader in bgzf::io::Reader
        let mut indexed_reader = vcf::io::indexed_reader::Builder::default()
            .set_index(index.clone())
            .build_from_reader(reader)
            .map_err(HailError::Io)?;

        // Read header
        let header = Arc::new(indexed_reader.read_header().map_err(HailError::Io)?);

        // Create query iterator
        let query = indexed_reader
            .query(&header, region)
            .map_err(HailError::Io)?;

        // Collect records eagerly (same as local), skipping unparseable records
        let header_clone = header.clone();
        let ranges = ranges.to_vec();
        let records: Vec<Result<EncodedValue>> = query
            .filter_map(|result| match result {
                Ok(record) => match super::codec::record_to_row_lazy(&header_clone, &record) {
                    Ok(row) => {
                        if ranges.is_empty() || row_matches_ranges(&row, &ranges) {
                            Some(Ok(row))
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        warn!("Skipping VCF record (conversion error): {}", e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Skipping VCF record (query error): {}", e);
                    None
                }
            })
            .collect();

        info!("Remote indexed query returned {} records", records.len());
        Ok(Box::new(records.into_iter()))
    }
}

/// Iterator over VCF records that converts them to EncodedValue
struct VcfRecordIterator<R: BufRead> {
    reader: Box<vcf::io::Reader<R>>,
    header: Arc<vcf::Header>,
}

impl<R: BufRead + Send + 'static> Iterator for VcfRecordIterator<R> {
    type Item = Result<EncodedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut record = vcf::variant::RecordBuf::default();
            match self.reader.read_record_buf(&self.header, &mut record) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    match super::codec::record_to_row(&self.header, &record) {
                        Ok(row) => return Some(Ok(row)),
                        Err(e) => {
                            // Skip records that fail conversion (e.g., malformed SVLEN,
                            // non-standard field values). Log and continue.
                            warn!("Skipping VCF record (conversion error): {}", e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    // Skip records that noodles can't parse (e.g., non-standard
                    // Character field encoding). Log and continue.
                    warn!("Skipping VCF record (parse error): {}", e);
                    continue;
                }
            }
        }
    }
}

// We need Send for the iterator
unsafe impl<R: BufRead + Send> Send for VcfRecordIterator<R> {}

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
    // Extract the field value using the field path
    let field_value = get_nested_field(row, &range.field_path);
    let field_value = match field_value {
        Some(v) => v,
        None => return false, // Missing field doesn't match
    };

    // Compare against range bounds
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
        // Cross-type comparisons
        (EncodedValue::Int32(v), KeyValue::Int64(k)) => (*v as i64).cmp(k),
        (EncodedValue::Int64(v), KeyValue::Int32(k)) => v.cmp(&(*k as i64)),
        _ => std::cmp::Ordering::Equal, // Default for unsupported comparisons
    }
}

impl DataSource for VcfDataSource {
    fn row_type(&self) -> &EncodedType {
        &self.schema
    }

    fn globals(&self) -> Result<EncodedValue> {
        // VCF headers contain metadata that could be mapped to globals
        // For now, return empty struct
        Ok(EncodedValue::Struct(vec![]))
    }

    fn key_fields(&self) -> &[String] {
        // VCF logical keys are [locus, alleles] in Hail's representation
        // Return empty for now as we don't enforce key constraints
        &[]
    }

    fn num_partitions(&self) -> usize {
        // When indexed and we have contigs, treat each chromosome as a partition
        if self.has_index() && !self.contigs.is_empty() {
            self.contigs.len()
        } else {
            // Single partition for unindexed files
            1
        }
    }

    fn scan_partition_stream(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // When indexed, partition_idx maps to a chromosome
        if self.has_index() && !self.contigs.is_empty() {
            if partition_idx >= self.contigs.len() {
                return Ok(Box::new(std::iter::empty()));
            }

            let contig = &self.contigs[partition_idx];
            let index = self.index.as_ref().unwrap();

            // Create a region for this entire chromosome
            let region: Region = contig
                .parse()
                .unwrap_or_else(|_| Region::new(contig.clone(), ..));

            // Check if this is a local file or remote
            let is_local = !self.path.starts_with("gs://")
                && !self.path.starts_with("s3://")
                && !self.path.starts_with("http");

            if is_local {
                debug!(
                    "Scanning partition {} (contig {}) locally",
                    partition_idx, contig
                );
                self.indexed_query_local(&region, index, ranges)
            } else {
                debug!(
                    "Scanning partition {} (contig {}) remotely",
                    partition_idx, contig
                );
                self.indexed_query_remote(&region, index, ranges)
            }
        } else {
            // Fall back to full scan for partition 0 on unindexed files
            if partition_idx != 0 {
                return Ok(Box::new(std::iter::empty()));
            }
            self.query_stream(ranges)
        }
    }

    fn sample_random(&self, sample_size: usize) -> Result<Vec<EncodedValue>> {
        // Require tabix index for random sampling
        if !self.has_index() {
            return Err(HailError::Index(
                "Random sampling requires tabix index (.tbi file)".to_string(),
            ));
        }

        if self.contigs.is_empty() {
            return Err(HailError::Index(
                "No contigs found in VCF header for random sampling".to_string(),
            ));
        }

        let index = self.index.as_ref().unwrap();

        debug!("Contigs for sampling: {:?}", self.contigs);

        // Calculate total genome length for weighted sampling
        let total_length: usize = self.contig_lengths.iter().filter_map(|l| *l).sum();

        // If we don't have length info, fall back to uniform sampling across contigs
        let use_uniform = total_length == 0;
        if use_uniform {
            warn!("Contig lengths not available in VCF header, using uniform sampling");
        }

        let mut rng = rand::thread_rng();
        let mut samples = Vec::with_capacity(sample_size);
        let max_retries = sample_size * 3; // Avoid infinite loops
        let mut attempts = 0;

        let is_local = !self.path.starts_with("gs://")
            && !self.path.starts_with("s3://")
            && !self.path.starts_with("http");

        // For local files, create and reuse an indexed reader for efficiency
        let mut indexed_reader = if is_local {
            Some(
                vcf::io::indexed_reader::Builder::default()
                    .set_index(index.clone())
                    .build_from_path(&self.path)
                    .map_err(HailError::Io)?,
            )
        } else {
            None
        };

        // Read header once if we have a reader
        let reader_header = if let Some(ref mut reader) = indexed_reader {
            Some(Arc::new(reader.read_header().map_err(HailError::Io)?))
        } else {
            None
        };

        // Progress bar for sampling
        let pb = ProgressBar::new(sample_size as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} Sampling: [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        while samples.len() < sample_size && attempts < max_retries {
            attempts += 1;

            // Pick a random contig (weighted by length if available)
            let (_contig_idx, contig, position) = if use_uniform {
                let idx = rng.gen_range(0..self.contigs.len());
                let contig = &self.contigs[idx];
                // Use a default large position range since we don't know the length
                let pos = rng.gen_range(1..250_000_000usize);
                (idx, contig.clone(), pos)
            } else {
                // Weighted selection by contig length
                let target = rng.gen_range(0..total_length);
                let mut cumulative = 0usize;
                let mut selected_idx = 0;
                let mut selected_pos = 1;

                for (idx, length_opt) in self.contig_lengths.iter().enumerate() {
                    if let Some(length) = length_opt {
                        if cumulative + length > target {
                            selected_idx = idx;
                            selected_pos = target - cumulative + 1; // 1-based position
                            break;
                        }
                        cumulative += length;
                    }
                }

                (
                    selected_idx,
                    self.contigs[selected_idx].clone(),
                    selected_pos,
                )
            };

            // Create a region around the selected position
            // Use a moderate window - larger windows increase hit rate for exomes
            // but we only fetch the first record so it's fast
            use noodles::core::Position;
            let window_size = 500_000; // 500kb window for good hit rate on exomes
            let start = Position::try_from(position.saturating_sub(window_size / 2).max(1))
                .unwrap_or(Position::MIN);
            let end = Position::try_from(position + window_size / 2).unwrap_or(Position::MAX);
            let region = Region::new(contig.clone(), start..=end);

            // Query the region - use optimized single-record fetch
            let query_result = if let (Some(ref mut reader), Some(ref header)) =
                (&mut indexed_reader, &reader_header)
            {
                // Use pre-opened reader for efficiency
                let query = reader.query(header, &region).map_err(HailError::Io)?;
                if let Some(result) = query.into_iter().next() {
                    let record = result.map_err(HailError::Io)?;
                    super::codec::record_to_row_lazy(header, &record).map(Some)
                } else {
                    Ok(None)
                }
            } else {
                // For remote files, still use the full query but take first
                self.indexed_query_remote(&region, index, &[])
                    .map(|mut iter| iter.next().and_then(|r| r.ok()))
            };

            match query_result {
                Ok(Some(record)) => {
                    samples.push(record);
                    pb.inc(1);
                    debug!(
                        "Sampled variant {} from contig {} position ~{}",
                        samples.len(),
                        contig,
                        position
                    );
                }
                Ok(None) => {
                    // If no record found, we just retry with a different position
                }
                Err(e) => {
                    debug!(
                        "Query error for contig {} position {}: {}",
                        contig, position, e
                    );
                    // Continue trying other positions
                }
            }
        }

        pb.finish_and_clear();

        if samples.is_empty() {
            return Err(HailError::Index(
                "Failed to sample any variants from indexed VCF".to_string(),
            ));
        }

        if samples.len() < sample_size {
            warn!(
                "Only sampled {} of {} requested variants after {} attempts",
                samples.len(),
                sample_size,
                attempts
            );
        }

        info!("Sampled {} random variants from indexed VCF", samples.len());
        Ok(samples)
    }

    fn query_stream_with_intervals(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Try indexed query if we have an index
        if let Some(index) = &self.index {
            if self.is_bgzf {
                // Check if this is a local file or remote
                let is_local = !self.path.starts_with("gs://")
                    && !self.path.starts_with("s3://")
                    && !self.path.starts_with("http");

                // If we have intervals, use tabix to query each interval directly
                if let Some(ref interval_list) = intervals {
                    info!(
                        "Using tabix indexed query for {} intervals",
                        interval_list.len()
                    );

                    // Collect all results from each interval using tabix
                    let mut all_results: Vec<EncodedValue> = Vec::new();

                    for contig in interval_list.contigs() {
                        if let Some(ranges_for_contig) = interval_list.intervals_for_contig(contig)
                        {
                            for range in ranges_for_contig {
                                let start = *range.start();
                                let end = *range.end();

                                // Build a region from the interval
                                let start_pos =
                                    noodles::core::Position::try_from(start as usize).ok();
                                let end_pos = noodles::core::Position::try_from(end as usize).ok();

                                if let (Some(s), Some(e)) = (start_pos, end_pos) {
                                    let region = Region::new(contig.as_str(), s..=e);

                                    let iter_result = if is_local {
                                        debug!(
                                            "Tabix query for interval: {}:{}-{}",
                                            contig, start, end
                                        );
                                        self.indexed_query_local(&region, index, ranges)
                                    } else {
                                        self.indexed_query_remote(&region, index, ranges)
                                    };

                                    if let Ok(iter) = iter_result {
                                        for result in iter {
                                            if let Ok(row) = result {
                                                // Apply any additional --where filters
                                                if ranges.is_empty()
                                                    || row_matches_ranges(&row, ranges)
                                                {
                                                    all_results.push(row);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return Ok(Box::new(all_results.into_iter().map(Ok)));
                }

                // No intervals, but try to build region from --where filters
                if let Some(region) = self.ranges_to_region(ranges) {
                    let base_iter = if is_local {
                        info!("Using local indexed query for region: {:?}", region);
                        self.indexed_query_local(&region, index, ranges)?
                    } else {
                        info!("Using remote indexed query for region: {:?}", region);
                        self.indexed_query_remote(&region, index, ranges)?
                    };

                    return Ok(base_iter);
                }
            }
        }

        // Fall back to full scan with post-filtering
        debug!("Performing full VCF scan with post-filtering");
        let base_iter = self.full_scan_iter()?;

        // Apply in-memory filtering
        let ranges = ranges.to_vec();
        let filtered = base_iter.filter(move |result| match result {
            Ok(row) => {
                // Check key ranges
                if !ranges.is_empty() && !row_matches_ranges(row, &ranges) {
                    return false;
                }
                // Check intervals
                if let Some(ref interval_list) = intervals {
                    if !row_matches_interval_list(row, interval_list) {
                        return false;
                    }
                }
                true
            }
            Err(_) => true, // Pass through errors
        });

        Ok(Box::new(filtered))
    }
}

/// Check if a row matches the interval list
fn row_matches_interval_list(row: &EncodedValue, intervals: &IntervalList) -> bool {
    // Extract locus.contig and locus.position
    let locus = get_nested_field(row, &["locus".to_string()]);
    let locus = match locus {
        Some(l) => l,
        None => return true, // No locus field, pass through
    };

    let contig = get_nested_field(locus, &["contig".to_string()]);
    let contig_str = match contig {
        Some(EncodedValue::Binary(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => return true, // Can't extract contig, pass through
    };

    let position = get_nested_field(locus, &["position".to_string()]);
    let position_val = match position {
        Some(EncodedValue::Int32(p)) => *p,
        Some(EncodedValue::Int64(p)) => *p as i32,
        _ => return true, // Can't extract position, pass through
    };

    intervals.contains(&contig_str, position_val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ranges_to_region_contig_only() {
        // Create a minimal header
        let header: vcf::Header =
            "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"
                .parse()
                .unwrap();
        let (contigs, contig_lengths) = VcfDataSource::extract_contigs(&header);
        let schema = super::super::schema::extract_schema_from_header(&header).unwrap();

        let source = VcfDataSource {
            path: "test.vcf".to_string(),
            header: Arc::new(header),
            schema,
            index: None,
            is_bgzf: false,
            contigs,
            contig_lengths,
        };

        let ranges = vec![KeyRange {
            field_path: vec!["locus".to_string(), "contig".to_string()],
            start: QueryBound::Included(KeyValue::String("chr1".to_string())),
            end: QueryBound::Included(KeyValue::String("chr1".to_string())),
        }];

        let region = source.ranges_to_region(&ranges);
        assert!(region.is_some());
    }

    #[test]
    fn test_has_index() {
        let header: vcf::Header =
            "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"
                .parse()
                .unwrap();
        let (contigs, contig_lengths) = VcfDataSource::extract_contigs(&header);
        let schema = super::super::schema::extract_schema_from_header(&header).unwrap();

        // Unindexed VCF
        let source = VcfDataSource {
            path: "test.vcf".to_string(),
            header: Arc::new(header.clone()),
            schema: schema.clone(),
            index: None,
            is_bgzf: false,
            contigs: contigs.clone(),
            contig_lengths: contig_lengths.clone(),
        };
        assert!(!source.has_index());

        // BGZF without index - still no random access
        let source2 = VcfDataSource {
            path: "test.vcf.gz".to_string(),
            header: Arc::new(header),
            schema,
            index: None,
            is_bgzf: true,
            contigs,
            contig_lengths,
        };
        assert!(!source2.has_index());
    }

    #[test]
    fn test_num_partitions_unindexed() {
        let header: vcf::Header =
            "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"
                .parse()
                .unwrap();
        let (contigs, contig_lengths) = VcfDataSource::extract_contigs(&header);
        let schema = super::super::schema::extract_schema_from_header(&header).unwrap();

        let source = VcfDataSource {
            path: "test.vcf".to_string(),
            header: Arc::new(header),
            schema,
            index: None,
            is_bgzf: false,
            contigs,
            contig_lengths,
        };

        // Without index, should be single partition
        assert_eq!(source.num_partitions(), 1);
    }
}
