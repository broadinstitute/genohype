//! Hail Table Data Source Adapter
//!
//! Implements the `DataSource` trait for native Hail Tables (.ht directories).

use crate::buffer::{BlockMap, BufferBuilder, InputBuffer};
use crate::codec::{EncodedType, EncodedValue, ETypeParser};
use crate::datasource::DataSource;
use crate::index::IndexReader;
use crate::io::join_path;
use crate::metadata::{CacheOptions, MetadataCache, RVDComponentSpec, TableMetadata};
use crate::projection::ProjectionTree;
use crate::query::{filter_partitions, filter_partitions_with_intervals, IntervalList, KeyRange, KeyValue, PartitionStream};
use crate::HailError;
use crate::Result;
use crossbeam_channel;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, info_span, instrument, warn};

/// Iterator that processes partitions sequentially in sorted order.
/// This ensures rows are yielded in key order for merge-join operations.
pub struct SortedPartitionIterator {
    partition_indices: Vec<usize>,
    current_partition: usize,
    current_stream: Option<PartitionStream>,
    rows_path: String,
    part_files: Vec<String>,
    row_type: EncodedType,
    ranges: Vec<KeyRange>,
}

impl SortedPartitionIterator {
    pub fn new(
        partition_indices: Vec<usize>,
        rows_path: String,
        part_files: Vec<String>,
        row_type: EncodedType,
        ranges: Vec<KeyRange>,
    ) -> Self {
        Self {
            partition_indices,
            current_partition: 0,
            current_stream: None,
            rows_path,
            part_files,
            row_type,
            ranges,
        }
    }

    fn open_partition(&mut self, idx: usize) -> Result<PartitionStream> {
        let part_file = &self.part_files[idx];
        let parts_path = join_path(&self.rows_path, "parts");
        let part_path = join_path(&parts_path, part_file);

        let buffer = BufferBuilder::from_path(&part_path)?.with_leb128().build();
        Ok(PartitionStream::new(
            buffer,
            self.row_type.clone(),
            self.ranges.clone(),
        ))
    }
}

impl Iterator for SortedPartitionIterator {
    type Item = Result<EncodedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get next row from current stream
            if let Some(ref mut stream) = self.current_stream {
                if let Some(row) = stream.next() {
                    return Some(row);
                }
                // Current stream exhausted, move to next partition
                self.current_stream = None;
            }

            // Check if we have more partitions
            if self.current_partition >= self.partition_indices.len() {
                return None;
            }

            // Open next partition
            let idx = self.partition_indices[self.current_partition];
            self.current_partition += 1;

            match self.open_partition(idx) {
                Ok(stream) => {
                    debug!("Opened partition {} for sorted iteration", idx);
                    self.current_stream = Some(stream);
                }
                Err(e) => {
                    warn!("Failed to open partition {}: {}", idx, e);
                    return Some(Err(e));
                }
            }
        }
    }
}

/// DataSource implementation for Hail Tables
///
/// Provides read access to Hail Table format (.ht directories) using:
/// - Partition pruning based on key ranges
/// - B-tree index lookups for efficient point queries
/// - Row decoding from partition files
///
/// Supports both local and cloud storage paths (GCS, S3).
#[derive(Clone)]
pub struct HailTableSource {
    /// Path to the rows directory
    rows_path: String,
    /// Base path to the table
    table_path: String,
    /// RVD metadata for rows
    rvd_spec: RVDComponentSpec,
    /// Parsed row type for decoding
    row_type: EncodedType,
    /// Cached index readers (one per partition) - Arc/Mutex for thread safety
    index_readers: Arc<Mutex<HashMap<usize, Arc<IndexReader>>>>,
    /// Cached block maps for efficient random access (one per partition) - Arc/Mutex for thread safety
    block_maps: Arc<Mutex<HashMap<usize, Arc<BlockMap>>>>,
    /// Per-partition row counts from top-level metadata (avoids fetching index files)
    partition_counts: Option<Vec<usize>>,
    /// Top-level table metadata (version info, references path)
    table_metadata: Option<TableMetadata>,
    /// Metadata cache for index file caching
    metadata_cache: Option<Arc<MetadataCache>>,
    /// Cache options
    cache_opts: Option<CacheOptions>,
}

impl HailTableSource {
    /// Open a Hail table
    ///
    /// # Arguments
    /// * `table_path` - Path to the table directory (local or cloud URL)
    pub fn new(table_path: &str) -> Result<Self> {
        Self::open(table_path, None)
    }

    /// Open a Hail table with optional metadata caching.
    ///
    /// When `cache_opts` is provided, metadata files are cached on the local
    /// filesystem to avoid re-downloading on every CLI invocation.
    pub fn open(table_path: &str, cache_opts: Option<CacheOptions>) -> Result<Self> {
        let _span = info_span!("HailTableSource::open", table_path).entered();

        let table_path = table_path.trim_end_matches('/').to_string();
        let rows_path = join_path(&table_path, "rows");

        // Set up cache if options provided
        let cache = cache_opts.as_ref().and_then(|_| MetadataCache::new());

        // Load RVD metadata
        let metadata_path = join_path(&rows_path, "metadata.json.gz");
        debug!("Loading RVD metadata from {}", metadata_path);
        let rvd_spec = RVDComponentSpec::from_path_cached(
            &metadata_path,
            &table_path,
            cache.as_ref(),
            cache_opts.as_ref(),
        )?;
        debug!("RVD metadata loaded: {} partitions, {} range bounds", rvd_spec.part_files.len(), rvd_spec.range_bounds.len());

        // Parse the row type from the codec spec
        let row_type = ETypeParser::parse(&rvd_spec.codec_spec.e_type)?;

        // Load top-level metadata for partition counts and version info
        let top_metadata_path = join_path(&table_path, "metadata.json.gz");
        debug!("Loading table metadata from {}", top_metadata_path);
        let table_metadata = TableMetadata::from_path_cached(
            &top_metadata_path,
            &table_path,
            cache.as_ref(),
            cache_opts.as_ref(),
        ).ok();
        let partition_counts = table_metadata.as_ref().and_then(|m| m.partition_counts());
        debug!("Partition counts from metadata: {}", if partition_counts.is_some() { "available" } else { "unavailable" });

        Ok(HailTableSource {
            rows_path,
            table_path,
            rvd_spec,
            row_type,
            index_readers: Arc::new(Mutex::new(HashMap::new())),
            block_maps: Arc::new(Mutex::new(HashMap::new())),
            partition_counts,
            table_metadata,
            metadata_cache: cache.map(Arc::new),
            cache_opts,
        })
    }

    /// Get the RVD specification
    ///
    /// This provides access to Hail-specific metadata for inspection commands.
    pub fn rvd_spec(&self) -> &RVDComponentSpec {
        &self.rvd_spec
    }

    /// Get the top-level table metadata (version info, references path)
    pub fn table_metadata(&self) -> Option<&TableMetadata> {
        self.table_metadata.as_ref()
    }

    /// Check if partition counts are available without scanning index files
    pub fn has_fast_row_count(&self) -> bool {
        self.partition_counts.is_some()
    }

    /// Get the path to a partition file
    fn get_partition_path(&self, partition_idx: usize) -> String {
        let part_file = &self.rvd_spec.part_files[partition_idx];
        let parts_path = join_path(&self.rows_path, "parts");
        join_path(&parts_path, part_file)
    }

    /// Convert an EncodedValue key to KeyRanges for partition pruning
    fn key_to_ranges(&self, key: &EncodedValue) -> Result<Vec<KeyRange>> {
        let mut ranges = Vec::new();

        if let EncodedValue::Struct(fields) = key {
            for (field_name, field_value) in fields {
                if let Some(key_value) = self.encoded_to_key_value(field_value) {
                    ranges.push(KeyRange::point(field_name.clone(), key_value));
                }
            }
        }

        Ok(ranges)
    }

    /// Convert an EncodedValue to a KeyValue
    fn encoded_to_key_value(&self, value: &EncodedValue) -> Option<KeyValue> {
        match value {
            EncodedValue::Binary(b) => {
                Some(KeyValue::String(String::from_utf8_lossy(b).into_owned()))
            }
            EncodedValue::Int32(i) => Some(KeyValue::Int32(*i)),
            EncodedValue::Int64(i) => Some(KeyValue::Int64(*i)),
            EncodedValue::Float32(f) => Some(KeyValue::Float32(*f)),
            EncodedValue::Float64(f) => Some(KeyValue::Float64(*f)),
            EncodedValue::Boolean(b) => Some(KeyValue::Boolean(*b)),
            _ => None,
        }
    }

    /// Get or create an index reader for a partition
    fn get_index_reader(&self, partition_idx: usize) -> Result<Arc<IndexReader>> {
        let mut cache = self.index_readers.lock().unwrap();
        if let Some(reader) = cache.get(&partition_idx) {
            return Ok(reader.clone());
        }

        let index_spec = self.rvd_spec.index_spec.as_ref().ok_or_else(|| {
            HailError::Index("Table does not have an index".to_string())
        })?;

        // Get the partition file name and derive the index directory name
        let part_file = &self.rvd_spec.part_files[partition_idx];

        // Index directory is named like the partition file but with .idx extension
        // e.g., part-0-xxx -> part-0-xxx.idx
        let index_dir_name = format!("{}.idx", part_file);
        let index_rel_path = index_spec.rel_path.trim_start_matches("../");
        let index_base = join_path(&self.table_path, index_rel_path);
        let index_path = join_path(&index_base, &index_dir_name);

        let reader = Arc::new(IndexReader::new_from_path_cached(
            &index_path,
            index_spec,
            self.metadata_cache.as_deref(),
            self.cache_opts.as_ref(),
        )?);
        cache.insert(partition_idx, reader.clone());
        Ok(reader)
    }

    /// Get or create a block map for a partition
    fn get_block_map(&self, partition_idx: usize) -> Result<Arc<BlockMap>> {
        let mut cache = self.block_maps.lock().unwrap();
        if let Some(map) = cache.get(&partition_idx) {
            return Ok(map.clone());
        }

        let part_path = self.get_partition_path(partition_idx);
        let block_map = Arc::new(BlockMap::build_from_path(&part_path)?);
        cache.insert(partition_idx, block_map.clone());
        Ok(block_map)
    }

    /// Read a row from a partition file at a specific virtual offset
    ///
    /// The offset is a Hail virtual offset: high 48 bits = compressed file offset,
    /// low 16 bits = byte offset within the decompressed block.
    fn read_row_at_offset(&self, partition_idx: usize, virtual_offset: i64) -> Result<EncodedValue> {
        let part_path = self.get_partition_path(partition_idx);

        // Decode virtual offset: high 48 bits = file offset, low 16 bits = local offset
        let file_offset = (virtual_offset as u64) >> 16;
        let local_offset = (virtual_offset & 0xFFFF) as usize;

        // Use the full buffer stack so rows spanning block boundaries are handled.
        // Seek to the compressed block, then skip local_offset decompressed bytes.
        let mut reader = crate::io::get_reader(&part_path)?;
        use std::io::Seek;
        reader.seek(std::io::SeekFrom::Start(file_offset))?;
        let mut buffer = BufferBuilder::from_reader(reader).with_leb128().build();

        // Skip past the local offset within the decompressed block
        if local_offset > 0 {
            let mut dummy = vec![0u8; local_offset];
            buffer.read_exact(&mut dummy)?;
        }

        // Read and decode the row
        let row_present = buffer.read_bool()?;
        if !row_present {
            return Ok(EncodedValue::Null);
        }
        self.row_type.read_present_value(&mut buffer)
    }

    /// Internal method to create a partition stream
    fn create_partition_stream(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<PartitionStream> {
        self.create_partition_stream_with_intervals(partition_idx, ranges, None)
    }

    /// Internal method to create a partition stream with interval filtering
    fn create_partition_stream_with_intervals(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<PartitionStream> {
        let part_file = &self.rvd_spec.part_files[partition_idx];
        let parts_path = join_path(&self.rows_path, "parts");
        let part_path = join_path(&parts_path, part_file);

        let buffer = BufferBuilder::from_path(&part_path)?
            .with_leb128()
            .build();

        Ok(PartitionStream::with_intervals(
            buffer,
            self.row_type.clone(),
            ranges.to_vec(),
            intervals,
            0,
        ))
    }
}

/// Construct an EncodedValue seek key from an IntervalList for index seeking.
///
/// For locus-keyed tables, builds a Struct key with the minimum (contig, position)
/// from the intervals that could appear in a given partition. Returns None if
/// a seek key can't be constructed.
fn build_seek_key_from_intervals(
    intervals: &IntervalList,
    key_fields: &[String],
) -> Option<EncodedValue> {
    // Only works for locus-keyed tables (key starts with "locus")
    if key_fields.is_empty() || key_fields[0] != "locus" {
        return None;
    }

    // Find the minimum start position across all contigs
    // Since partition pruning already narrowed to relevant partitions,
    // we use the global minimum as a conservative seek target
    let mut min_contig: Option<&str> = None;
    let mut min_pos: Option<i32> = None;

    for contig in intervals.contigs() {
        if let Some(ranges) = intervals.intervals_for_contig(contig) {
            if let Some(first_range) = ranges.first() {
                let start = *first_range.start();
                match (&min_contig, &min_pos) {
                    (None, _) => {
                        min_contig = Some(contig);
                        min_pos = Some(start);
                    }
                    (Some(mc), Some(mp)) => {
                        if contig.as_str() < *mc || (contig.as_str() == *mc && start < *mp) {
                            min_contig = Some(contig);
                            min_pos = Some(start);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let contig = min_contig?;
    let pos = min_pos?;

    Some(EncodedValue::Struct(vec![
        ("locus".to_string(), EncodedValue::Struct(vec![
            ("contig".to_string(), EncodedValue::Binary(contig.as_bytes().to_vec())),
            ("position".to_string(), EncodedValue::Int32(pos)),
        ])),
    ]))
}

impl DataSource for HailTableSource {
    fn row_type(&self) -> &EncodedType {
        &self.row_type
    }

    fn globals(&self) -> Result<EncodedValue> {
        // Load globals from {table_path}/globals/
        let globals_path = join_path(&self.table_path, "globals");
        let globals_metadata_path = join_path(&globals_path, "metadata.json.gz");

        // Load globals metadata
        let globals_spec = RVDComponentSpec::from_path(&globals_metadata_path)?;

        // Parse the globals type from the codec spec
        let globals_type = ETypeParser::parse(&globals_spec.codec_spec.e_type)?;

        // Get the partition file path (globals typically has only one partition)
        if globals_spec.part_files.is_empty() {
            return Ok(EncodedValue::Struct(vec![]));
        }
        let parts_path = join_path(&globals_path, "parts");
        let part_path = join_path(&parts_path, &globals_spec.part_files[0]);

        // Build buffer and decode
        let mut buffer = BufferBuilder::from_path(&part_path)?.with_leb128().build();

        // Hail partition streams always start with a row-present byte per row,
        // even for globals (which is a single-row partition). Consume it before
        // decoding, just like PartitionStream does.
        let row_present = buffer.read_bool()?;
        if !row_present {
            return Ok(EncodedValue::Null);
        }
        globals_type.read_present_value(&mut buffer)
    }

    fn key_fields(&self) -> &[String] {
        &self.rvd_spec.key
    }

    fn num_partitions(&self) -> usize {
        self.rvd_spec.part_files.len()
    }

    #[instrument(skip_all)]
    fn total_rows(&self) -> Option<usize> {
        // Fast path: use partition counts from top-level metadata (no extra I/O)
        if let Some(counts) = &self.partition_counts {
            return Some(counts.iter().sum());
        }

        // Fallback: fetch each partition's index metadata (slow for remote tables)
        let index_spec = self.rvd_spec.index_spec.as_ref()?;
        let index_rel_path = index_spec.rel_path.trim_start_matches("../");
        let index_base = join_path(&self.table_path, index_rel_path);

        let total: usize = self
            .rvd_spec
            .part_files
            .par_iter()
            .map(|part_file| {
                let metadata_path =
                    join_path(&index_base, &format!("{}.idx/metadata.json.gz", part_file));
                match crate::index::IndexMetadata::from_path(&metadata_path) {
                    Ok(meta) => meta.n_keys,
                    Err(e) => {
                        warn!("Failed to read index metadata for {}: {}", part_file, e);
                        0
                    }
                }
            })
            .sum();

        Some(total)
    }

    fn scan_partition_stream(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let stream = self.create_partition_stream(partition_idx, ranges)?;
        Ok(Box::new(stream))
    }

    #[instrument(skip_all, fields(num_ranges = ranges.len(), has_intervals = intervals.is_some()))]
    fn query_stream_with_intervals(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let matching_partitions = filter_partitions_with_intervals(
            &self.rvd_spec.range_bounds,
            ranges,
            intervals.as_deref(),
        );

        info!(
            "query_stream: {} partitions matched filter",
            matching_partitions.len()
        );

        // Use a bounded channel for backpressure
        let (tx, rx) = crossbeam_channel::bounded(100);

        // Capture necessary state for the thread
        let rows_path = self.rows_path.clone();
        let part_files = self.rvd_spec.part_files.clone();
        let row_type = self.row_type.clone();
        let ranges = ranges.to_vec();
        let has_index = self.rvd_spec.index_spec.is_some();
        let index_readers = self.index_readers.clone();
        let block_maps = self.block_maps.clone();
        let table_path = self.table_path.clone();
        let index_spec = self.rvd_spec.index_spec.clone();
        let key_fields = self.rvd_spec.key.clone();
        let metadata_cache = self.metadata_cache.clone();
        let cache_opts = self.cache_opts.clone();

        // Pre-compute seek key from intervals (if available)
        let seek_key = intervals.as_ref().and_then(|ivl| {
            build_seek_key_from_intervals(ivl, &key_fields)
        });

        let num_partitions = matching_partitions.len();

        std::thread::spawn(move || {
            info!(
                "Background query thread started. Processing {} partitions (index_seeking={}).",
                num_partitions,
                has_index && seek_key.is_some()
            );

            // Helper to get or create index reader for a partition
            let get_index_reader = |partition_idx: usize| -> crate::Result<Arc<IndexReader>> {
                {
                    let cache = index_readers.lock().unwrap();
                    if let Some(reader) = cache.get(&partition_idx) {
                        return Ok(reader.clone());
                    }
                }
                let spec = index_spec.as_ref().ok_or_else(|| {
                    HailError::Index("Table does not have an index".to_string())
                })?;
                let part_file = &part_files[partition_idx];
                let index_dir_name = format!("{}.idx", part_file);
                let index_rel_path = spec.rel_path.trim_start_matches("../");
                let index_base = join_path(&table_path, index_rel_path);
                let index_path = join_path(&index_base, &index_dir_name);
                let reader = Arc::new(IndexReader::new_from_path_cached(
                    &index_path,
                    spec,
                    metadata_cache.as_deref(),
                    cache_opts.as_ref(),
                )?);
                let mut cache = index_readers.lock().unwrap();
                cache.insert(partition_idx, reader.clone());
                Ok(reader)
            };

            // Helper to get or create block map for a partition
            let get_block_map = |partition_idx: usize, part_path: &str| -> crate::Result<Arc<BlockMap>> {
                {
                    let cache = block_maps.lock().unwrap();
                    if let Some(map) = cache.get(&partition_idx) {
                        return Ok(map.clone());
                    }
                }
                let map = Arc::new(BlockMap::build_from_path(part_path)?);
                let mut cache = block_maps.lock().unwrap();
                cache.insert(partition_idx, map.clone());
                Ok(map)
            };

            // Process partitions in parallel using rayon
            matching_partitions
                .into_par_iter()
                .for_each_with(tx, |sender, idx| {
                    let _span = info_span!("partition_worker", partition = idx).entered();
                    let partition_start = std::time::Instant::now();

                    let part_file = &part_files[idx];
                    let parts_path = join_path(&rows_path, "parts");
                    let part_path = join_path(&parts_path, part_file);

                    // Try index-seeking path
                    let seek_result = if has_index {
                        seek_key.as_ref().and_then(|key| {
                            let reader = match get_index_reader(idx) {
                                Ok(r) => r,
                                Err(e) => { debug!("Partition {}: index reader error: {}", idx, e); return None; }
                            };
                            let seek_result = reader.seek_lower_bound(key);
                            let virtual_offset = match seek_result {
                                Ok(Some(o)) => o,
                                Ok(None) => { debug!("Partition {}: seek_lower_bound returned None", idx); return None; }
                                Err(e) => { debug!("Partition {}: seek_lower_bound error: {}", idx, e); return None; }
                            };
                            // Index stores virtual offsets: high 48 bits = compressed file offset, low 16 bits = offset within decompressed block
                            let file_offset = (virtual_offset as u64) >> 16;
                            let local_offset = (virtual_offset & 0xFFFF) as usize;
                            debug!(
                                "Partition {}: virtual_offset={}, file_offset={} ({:.1} MB), local_offset={}",
                                idx, virtual_offset, file_offset, file_offset as f64 / 1024.0 / 1024.0, local_offset
                            );
                            Some((file_offset, local_offset))
                        })
                    } else {
                        None
                    };

                    let open_start = std::time::Instant::now();
                    let buffer_and_offset = {
                        let _open_span = info_span!("partition_open", partition = idx).entered();
                        match seek_result {
                            Some((file_offset, local_offset)) => {
                                // Seeked path: open reader at the target block
                                (|| -> crate::Result<_> {
                                    let mut reader = crate::io::get_reader(&part_path)?;
                                    use std::io::Seek;
                                    reader.seek(std::io::SeekFrom::Start(file_offset))?;
                                    let buffer = BufferBuilder::from_reader(reader)
                                        .with_leb128()
                                        .build();
                                    Ok((buffer, local_offset))
                                })()
                            }
                            None => {
                                // Fallback: full scan from offset 0
                                BufferBuilder::from_path(&part_path)
                                    .map(|b| (b.with_leb128().build(), 0usize))
                            }
                        }
                    };
                    let open_elapsed = open_start.elapsed();

                    match buffer_and_offset {
                        Ok((buffer, local_offset)) => {
                            debug!(
                                "Partition {} opened in {:?} (seek_offset={})",
                                idx, open_elapsed, local_offset
                            );
                            let stream = PartitionStream::with_intervals(
                                buffer,
                                row_type.clone(),
                                ranges.clone(),
                                intervals.clone(),
                                local_offset,
                            );

                            let _decode_span = info_span!("partition_decode", partition = idx).entered();
                            let decode_start = std::time::Instant::now();
                            let mut row_count = 0;
                            for row in stream {
                                if sender.send(row).is_err() {
                                    debug!("Partition {} sender dropped", idx);
                                    break;
                                }
                                row_count += 1;
                            }
                            debug!(
                                "Partition {} finished: {} rows decoded in {:?} (total {:?})",
                                idx, row_count, decode_start.elapsed(), partition_start.elapsed()
                            );
                        }
                        Err(e) => {
                            warn!("Failed to open partition {}: {}", idx, e);
                            let _ = sender.send(Err(e));
                        }
                    }
                });

            info!("Background query thread finished");
        });

        Ok(Box::new(rx.into_iter()))
    }

    #[instrument(skip_all, fields(num_ranges = ranges.len(), has_intervals = intervals.is_some(), has_projection = decode_projection.is_some()))]
    fn query_stream_with_projection(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
        decode_projection: Option<Arc<ProjectionTree>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let matching_partitions = filter_partitions_with_intervals(
            &self.rvd_spec.range_bounds,
            ranges,
            intervals.as_deref(),
        );

        info!(
            "query_stream_projected: {} partitions matched filter (decode_projection={})",
            matching_partitions.len(),
            decode_projection.is_some()
        );

        let (tx, rx) = crossbeam_channel::bounded(100);

        let rows_path = self.rows_path.clone();
        let part_files = self.rvd_spec.part_files.clone();
        let row_type = self.row_type.clone();
        let ranges = ranges.to_vec();
        let has_index = self.rvd_spec.index_spec.is_some();
        let index_readers = self.index_readers.clone();
        let table_path = self.table_path.clone();
        let index_spec = self.rvd_spec.index_spec.clone();
        let key_fields = self.rvd_spec.key.clone();
        let metadata_cache = self.metadata_cache.clone();
        let cache_opts = self.cache_opts.clone();

        let seek_key = intervals.as_ref().and_then(|ivl| {
            build_seek_key_from_intervals(ivl, &key_fields)
        });

        let num_partitions = matching_partitions.len();

        std::thread::spawn(move || {
            info!(
                "Background projected query thread started. Processing {} partitions.",
                num_partitions
            );

            let get_index_reader = |partition_idx: usize| -> crate::Result<Arc<IndexReader>> {
                {
                    let cache = index_readers.lock().unwrap();
                    if let Some(reader) = cache.get(&partition_idx) {
                        return Ok(reader.clone());
                    }
                }
                let spec = index_spec.as_ref().ok_or_else(|| {
                    HailError::Index("Table does not have an index".to_string())
                })?;
                let part_file = &part_files[partition_idx];
                let index_dir_name = format!("{}.idx", part_file);
                let index_rel_path = spec.rel_path.trim_start_matches("../");
                let index_base = join_path(&table_path, index_rel_path);
                let index_path = join_path(&index_base, &index_dir_name);
                let reader = Arc::new(IndexReader::new_from_path_cached(
                    &index_path,
                    spec,
                    metadata_cache.as_deref(),
                    cache_opts.as_ref(),
                )?);
                let mut cache = index_readers.lock().unwrap();
                cache.insert(partition_idx, reader.clone());
                Ok(reader)
            };

            matching_partitions
                .into_par_iter()
                .for_each_with(tx, |sender, idx| {
                    let _span = info_span!("partition_worker_projected", partition = idx).entered();

                    let part_file = &part_files[idx];
                    let parts_path = join_path(&rows_path, "parts");
                    let part_path = join_path(&parts_path, part_file);

                    let seek_result = if has_index {
                        seek_key.as_ref().and_then(|key| {
                            let reader = match get_index_reader(idx) {
                                Ok(r) => r,
                                Err(e) => { debug!("Partition {}: index reader error: {}", idx, e); return None; }
                            };
                            let virtual_offset = match reader.seek_lower_bound(key) {
                                Ok(Some(o)) => o,
                                Ok(None) => { return None; }
                                Err(e) => { debug!("Partition {}: seek error: {}", idx, e); return None; }
                            };
                            let file_offset = (virtual_offset as u64) >> 16;
                            let local_offset = (virtual_offset & 0xFFFF) as usize;
                            Some((file_offset, local_offset))
                        })
                    } else {
                        None
                    };

                    let buffer_and_offset = {
                        let _open_span = info_span!("partition_open", partition = idx).entered();
                        match seek_result {
                            Some((file_offset, local_offset)) => {
                                (|| -> crate::Result<_> {
                                    let mut reader = crate::io::get_reader(&part_path)?;
                                    use std::io::Seek;
                                    reader.seek(std::io::SeekFrom::Start(file_offset))?;
                                    let buffer = BufferBuilder::from_reader(reader)
                                        .with_leb128()
                                        .build();
                                    Ok((buffer, local_offset))
                                })()
                            }
                            None => {
                                BufferBuilder::from_path(&part_path)
                                    .map(|b| (b.with_leb128().build(), 0usize))
                            }
                        }
                    };

                    match buffer_and_offset {
                        Ok((buffer, local_offset)) => {
                            let _decode_span = info_span!("partition_decode", partition = idx).entered();
                            let stream = PartitionStream::with_intervals(
                                buffer,
                                row_type.clone(),
                                ranges.clone(),
                                intervals.clone(),
                                local_offset,
                            ).with_decode_projection(decode_projection.clone());

                            for row in stream {
                                if sender.send(row).is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to open partition {}: {}", idx, e);
                            let _ = sender.send(Err(e));
                        }
                    }
                });

            info!("Background projected query thread finished");
        });

        Ok(Box::new(rx.into_iter()))
    }

    fn query_stream_sorted(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Sequential iteration through partitions in sorted order
        let matching_partitions = filter_partitions(&self.rvd_spec.range_bounds, ranges);

        info!(
            "query_stream_sorted: {} partitions matched filter (sequential)",
            matching_partitions.len()
        );

        // Build a sequential iterator that processes partitions in order
        let rows_path = self.rows_path.clone();
        let part_files = self.rvd_spec.part_files.clone();
        let row_type = self.row_type.clone();
        let ranges = ranges.to_vec();

        // Create an iterator that chains partition streams in order
        let iter = SortedPartitionIterator::new(
            matching_partitions,
            rows_path,
            part_files,
            row_type,
            ranges,
        );

        Ok(Box::new(iter))
    }

    #[instrument(skip_all)]
    fn lookup(&self, key: &EncodedValue) -> Result<Option<EncodedValue>> {
        // For point lookups, skip partition pruning if there's only one partition
        let matching_partitions = if self.rvd_spec.part_files.len() == 1 {
            vec![0]
        } else {
            let key_ranges = self.key_to_ranges(key)?;
            filter_partitions(&self.rvd_spec.range_bounds, &key_ranges)
        };

        if matching_partitions.is_empty() {
            return Ok(None);
        }

        // For point lookups, we expect at most one partition to match
        for partition_idx in matching_partitions {
            // Get index reader for this partition
            let reader = self.get_index_reader(partition_idx)?;
            let offset_opt = reader.lookup(key)?;

            if let Some(data_offset) = offset_opt {
                // Read the row from the partition file at the given offset
                let row = self.read_row_at_offset(partition_idx, data_offset)?;
                return Ok(Some(row));
            }
        }

        Ok(None)
    }
}
