//! Data source abstraction for genomic tables
//!
//! This module defines the `DataSource` trait which provides a unified interface
//! for reading data from various formats (Hail Tables, VCFs, etc.).

use crate::codec::{EncodedType, EncodedValue};
use crate::query::{IntervalList, KeyRange};
use crate::Result;
use std::sync::Arc;

/// A source of genomic table data
///
/// This trait abstracts over different table formats (Hail Tables, VCFs, etc.)
/// and provides a common interface for querying and streaming data.
pub trait DataSource: Send + Sync {
    /// Get the row schema
    ///
    /// Returns the schema describing the structure of each row.
    fn row_type(&self) -> &EncodedType;

    /// Get global metadata (header information)
    ///
    /// For Hail tables, this is the globals struct.
    /// For VCFs, this is the header metadata.
    fn globals(&self) -> Result<EncodedValue>;

    /// Get the names of the key fields
    ///
    /// These are the fields used for indexing and partitioning.
    fn key_fields(&self) -> &[String];

    /// Get the total number of partitions
    ///
    /// For Hail tables, this is the number of partition files.
    /// For VCFs, this might be virtual partitions based on genomic regions.
    fn num_partitions(&self) -> usize;

    /// Stream rows from a specific partition
    ///
    /// This is the primary method for parallel partition processing. It returns
    /// an iterator that yields rows one at a time, avoiding loading entire
    /// partitions into memory. This is critical for memory-bounded parallel
    /// processing on large tables.
    ///
    /// # Arguments
    /// * `partition_idx` - The partition index to scan
    /// * `ranges` - Key range constraints for filtering rows
    fn scan_partition_stream(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>>;

    /// Scan a specific partition and return all matching rows (batch mode)
    ///
    /// This collects all rows from the partition into a Vec. Use this only
    /// for small partitions or sequential processing. For parallel processing
    /// on large tables, use `scan_partition_stream` instead to avoid OOM.
    ///
    /// # Arguments
    /// * `partition_idx` - The partition index to scan
    /// * `ranges` - Key range constraints for filtering rows
    fn scan_partition(&self, partition_idx: usize, ranges: &[KeyRange]) -> Result<Vec<EncodedValue>> {
        self.scan_partition_stream(partition_idx, ranges)?.collect()
    }

    /// Stream rows matching the given key ranges
    ///
    /// This is the primary method for querying. Implementations should return
    /// an iterator that yields rows lazily for memory-efficient processing.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for filtering rows
    fn query_stream(&self, ranges: &[KeyRange]) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        self.query_stream_with_intervals(ranges, None)
    }

    /// Stream rows matching the given key ranges and optional interval list
    ///
    /// This is the primary method for querying with genomic interval filtering.
    /// Implementations should return an iterator that yields rows lazily for
    /// memory-efficient processing.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for filtering rows
    /// * `intervals` - Optional interval list for genomic region filtering
    fn query_stream_with_intervals(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>>;

    /// Stream rows in SORTED key order (sequential partition iteration)
    ///
    /// Unlike `query_stream` which may use parallel iteration for performance,
    /// this method guarantees rows are returned in sorted key order. This is
    /// required for merge-join operations.
    ///
    /// Default implementation falls back to query_stream (may not be sorted).
    fn query_stream_sorted(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        self.query_stream(ranges)
    }

    /// Perform a point lookup for a specific key
    ///
    /// Returns the first row matching the key, or None if not found.
    /// This is an optimization for indexed sources that can do efficient
    /// random access lookups.
    ///
    /// Default implementation returns an error indicating lookup is not supported.
    /// Implementations with index support should override this.
    fn lookup(&self, _key: &EncodedValue) -> Result<Option<EncodedValue>> {
        Err(crate::HailError::Index(
            "Lookup not implemented for this data source".to_string(),
        ))
    }

    /// Get the total number of rows in the data source, if known from metadata
    ///
    /// Returns `None` if the count cannot be determined without a full data scan.
    fn total_rows(&self) -> Option<usize> {
        None
    }

    /// Sample random rows from the data source
    ///
    /// Returns a random sample of rows. Implementations may optimize this
    /// differently:
    /// - Hail tables: sample random partitions then random rows within
    /// - VCFs with tabix index: use index to seek to random positions
    /// - Unindexed sources: fall back to streaming with reservoir sampling
    ///
    /// Default implementation returns an error indicating sampling is not supported.
    /// Implementations should override this with optimized sampling strategies.
    fn sample_random(&self, _sample_size: usize) -> Result<Vec<EncodedValue>> {
        Err(crate::HailError::Index(
            "Random sampling not implemented for this data source".to_string(),
        ))
    }
}
