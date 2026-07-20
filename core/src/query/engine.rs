//! Query engine for Hail tables
//!
//! Provides a high-level API for querying Hail tables and other genomic data sources.
//! Delegates actual data access to implementations of the `DataSource` trait.

use crate::bed::BedDataSource;
use crate::codec::{EncodedType, EncodedValue};
use crate::datasource::DataSource;
use crate::hail_adapter::HailTableSource;
use crate::metadata::{CacheOptions, RVDComponentSpec};
use crate::projection::ProjectionTree;
use crate::query::{IntervalList, KeyRange};
use crate::vcf::VcfDataSource;
use crate::Result;
use std::path::Path;
use std::sync::Arc;
use tracing::instrument;

/// High-level query engine for Hail tables and other data sources
///
/// Supports both local and cloud storage paths:
/// - Local: `path/to/table.ht`
/// - GCS: `gs://bucket/path/to/table.ht`
/// - S3: `s3://bucket/path/to/table.ht`
pub struct QueryEngine {
    /// The underlying data source
    source: Box<dyn DataSource>,
    /// Keep raw hail source for inspection if needed (Hail tables only)
    hail_source: Option<HailTableSource>,
}

/// Result of a query operation
#[derive(Debug)]
pub struct QueryResult {
    /// The decoded rows matching the query
    pub rows: Vec<EncodedValue>,
    /// Number of partitions scanned
    pub partitions_scanned: usize,
    /// Number of partitions pruned (skipped)
    pub partitions_pruned: usize,
}

impl QueryEngine {
    /// Open a Hail table for querying from a local path
    ///
    /// # Arguments
    /// * `table_path` - Path to the table directory (e.g., "path/to/table.ht")
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::QueryEngine;
    ///
    /// let engine = QueryEngine::open("data/my_table.ht").unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(table_path: P) -> Result<Self> {
        let path_str = table_path.as_ref().to_string_lossy().to_string();
        Self::open_path(&path_str)
    }

    /// Open a Hail table for querying from a path string (local or cloud URL)
    ///
    /// # Arguments
    /// * `table_path` - Path to the table directory
    ///   - Local: `path/to/table.ht`
    ///   - GCS: `gs://bucket/path/to/table.ht`
    ///   - S3: `s3://bucket/path/to/table.ht`
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::QueryEngine;
    ///
    /// // Local file
    /// let engine = QueryEngine::open_path("data/my_table.ht").unwrap();
    ///
    /// // Google Cloud Storage
    /// let engine = QueryEngine::open_path("gs://my-bucket/data/my_table.ht").unwrap();
    /// ```
    #[instrument(skip_all, fields(path = table_path))]
    pub fn open_path(table_path: &str) -> Result<Self> {
        // Detect file type by extension
        if table_path.ends_with(".vcf")
            || table_path.ends_with(".vcf.gz")
            || table_path.ends_with(".vcf.bgz")
        {
            // VCF file
            let vcf_source = VcfDataSource::new(table_path)?;
            Ok(QueryEngine {
                source: Box::new(vcf_source),
                hail_source: None,
            })
        } else if table_path.ends_with(".bed.gz") || table_path.ends_with(".bed.bgz") {
            // BED file (Tabix-indexed)
            let bed_source = BedDataSource::new(table_path)?;
            Ok(QueryEngine {
                source: Box::new(bed_source),
                hail_source: None,
            })
        } else {
            // Assume Hail Table - single instance used for both trait and direct access
            let hail_source = HailTableSource::new(table_path)?;

            Ok(QueryEngine {
                source: Box::new(hail_source.clone()),
                hail_source: Some(hail_source),
            })
        }
    }

    /// Open a data source with optional metadata caching.
    ///
    /// For Hail tables, metadata files are cached on the local filesystem
    /// to avoid re-downloading on every CLI invocation.
    #[instrument(skip_all, fields(path = table_path))]
    pub fn open_path_cached(table_path: &str, cache_opts: Option<CacheOptions>) -> Result<Self> {
        if table_path.ends_with(".vcf")
            || table_path.ends_with(".vcf.gz")
            || table_path.ends_with(".vcf.bgz")
        {
            let vcf_source = VcfDataSource::new(table_path)?;
            Ok(QueryEngine {
                source: Box::new(vcf_source),
                hail_source: None,
            })
        } else if table_path.ends_with(".bed.gz") || table_path.ends_with(".bed.bgz") {
            let bed_source = BedDataSource::new(table_path)?;
            Ok(QueryEngine {
                source: Box::new(bed_source),
                hail_source: None,
            })
        } else {
            let hail_source = HailTableSource::open(table_path, cache_opts)?;
            Ok(QueryEngine {
                source: Box::new(hail_source.clone()),
                hail_source: Some(hail_source),
            })
        }
    }

    /// Get the key field names for this table
    pub fn key_fields(&self) -> &[String] {
        self.source.key_fields()
    }

    /// Get the number of partitions in this table
    pub fn num_partitions(&self) -> usize {
        self.source.num_partitions()
    }

    /// Get the total number of rows (if available from metadata)
    pub fn total_rows(&self) -> Option<usize> {
        self.source.total_rows()
    }

    /// Get the table globals (e.g., freq_meta population labels)
    pub fn globals(&self) -> crate::Result<crate::codec::EncodedValue> {
        self.source.globals()
    }

    /// Check if this table has indexes
    pub fn has_index(&self) -> bool {
        // Only Hail tables have indexes in the current implementation
        if let Some(hail) = &self.hail_source {
            hail.rvd_spec().index_spec.is_some()
        } else {
            false
        }
    }

    /// Get the RVD specification (Hail tables only)
    ///
    /// Returns Some(&RVDComponentSpec) for Hail tables, None for other sources (e.g., VCF).
    /// This is used by inspection commands.
    pub fn rvd_spec(&self) -> Option<&RVDComponentSpec> {
        self.hail_source.as_ref().map(|h| h.rvd_spec())
    }

    /// Get the top-level table metadata (Hail tables only)
    pub fn table_metadata(&self) -> Option<&crate::metadata::TableMetadata> {
        self.hail_source.as_ref().and_then(|h| h.table_metadata())
    }

    /// Check if total_rows() is available without an expensive scan
    pub fn has_fast_row_count(&self) -> bool {
        self.hail_source
            .as_ref()
            .map_or(false, |h| h.has_fast_row_count())
    }

    /// Get the genomic interval covered by a specific partition (Hail tables only)
    ///
    /// This allows querying the annotation table for the same genomic range
    /// during streaming merge-join operations.
    ///
    /// # Arguments
    /// * `partition_idx` - The partition index
    ///
    /// # Returns
    /// The interval covered by this partition, or an error if not a Hail table
    /// or if the partition index is out of bounds.
    pub fn get_partition_interval(
        &self,
        partition_idx: usize,
    ) -> Result<crate::metadata::Interval> {
        let hail = self
            .hail_source
            .as_ref()
            .ok_or_else(|| crate::HailError::InvalidFormat("Not a Hail table".into()))?;

        let range_bounds = &hail.rvd_spec().range_bounds;
        if partition_idx >= range_bounds.len() {
            return Err(crate::HailError::InvalidFormat(format!(
                "Partition index {} out of bounds (table has {} partitions)",
                partition_idx,
                range_bounds.len()
            )));
        }

        Ok(range_bounds[partition_idx].clone())
    }

    /// Get the row type (schema) for this table
    pub fn row_type(&self) -> &EncodedType {
        self.source.row_type()
    }

    /// Perform a point lookup by key
    ///
    /// Returns the row matching the exact key, or None if not found.
    ///
    /// # Arguments
    /// * `key` - The key value to search for (must match the table's key structure)
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::{QueryEngine, KeyValue};
    /// use genohype_core::codec::EncodedValue;
    ///
    /// let mut engine = QueryEngine::open("data/my_table.ht").unwrap();
    ///
    /// // Create key for lookup
    /// let key = EncodedValue::Struct(vec![
    ///     ("gene_id".to_string(), EncodedValue::Binary(b"ENSG00000141510".to_vec())),
    /// ]);
    ///
    /// if let Some(row) = engine.lookup(&key).unwrap() {
    ///     println!("Found: {:?}", row);
    /// }
    /// ```
    pub fn lookup(&mut self, key: &EncodedValue) -> Result<Option<EncodedValue>> {
        self.source.lookup(key)
    }

    /// Query with key ranges (for range scans)
    ///
    /// Returns all rows within the specified key range.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for the query
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::{QueryEngine, KeyRange, KeyValue};
    ///
    /// let mut engine = QueryEngine::open("data/my_table.ht").unwrap();
    ///
    /// // Query for all rows where chrom = "chr1"
    /// let ranges = vec![
    ///     KeyRange::point("chrom".to_string(), KeyValue::String("chr1".to_string())),
    /// ];
    ///
    /// let result = engine.query(&ranges).unwrap();
    /// println!("Found {} rows", result.rows.len());
    /// ```
    pub fn query(&mut self, ranges: &[KeyRange]) -> Result<QueryResult> {
        let total_partitions = self.source.num_partitions();

        // Collect all rows
        let iter = self.source.query_stream(ranges)?;
        let rows: Result<Vec<EncodedValue>> = iter.collect();
        let rows = rows?;

        Ok(QueryResult {
            rows,
            partitions_scanned: total_partitions,
            partitions_pruned: 0,
        })
    }

    /// Stream rows from a single partition
    ///
    /// Returns an iterator that yields rows one at a time, avoiding loading
    /// the entire partition into memory. Use this for parallel processing
    /// on large tables to prevent OOM.
    ///
    /// # Arguments
    /// * `partition_idx` - The index of the partition to scan
    /// * `ranges` - Key range constraints for filtering rows
    ///
    /// # Returns
    /// An iterator over decoded rows
    pub fn scan_partition_iter(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<impl Iterator<Item = Result<EncodedValue>> + '_> {
        self.source.scan_partition_stream(partition_idx, ranges)
    }

    /// Scan a single partition and return all rows (batch mode)
    ///
    /// This collects all rows into a Vec. For parallel processing on large
    /// tables, use `scan_partition_iter` instead to avoid OOM.
    ///
    /// # Arguments
    /// * `partition_idx` - The index of the partition to scan
    /// * `ranges` - Key range constraints for filtering rows
    ///
    /// # Returns
    /// A vector of decoded rows
    pub fn scan_partition(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Vec<EncodedValue>> {
        self.source.scan_partition(partition_idx, ranges)
    }

    /// Query with streaming output
    ///
    /// Returns an iterator that yields rows as they are read, instead of
    /// collecting all results into memory first. This is more memory-efficient
    /// for large result sets and allows processing to start immediately.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for the query
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::{QueryEngine, KeyRange, KeyValue};
    ///
    /// let engine = QueryEngine::open("data/my_table.ht").unwrap();
    ///
    /// // Stream results with a limit
    /// for row in engine.query_iter(&[]).unwrap().take(100) {
    ///     let row = row.unwrap();
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn query_iter(
        &self,
        ranges: &[KeyRange],
    ) -> Result<impl Iterator<Item = Result<EncodedValue>>> {
        self.source.query_stream(ranges)
    }

    /// Query with streaming output in SORTED key order
    ///
    /// Unlike `query_iter` which may use parallel iteration for performance,
    /// this method guarantees rows are returned in sorted key order. Use this
    /// for merge-join operations that require sorted input.
    pub fn query_iter_sorted(
        &self,
        ranges: &[KeyRange],
    ) -> Result<impl Iterator<Item = Result<EncodedValue>>> {
        self.source.query_stream_sorted(ranges)
    }

    /// Query with streaming output and interval filtering
    ///
    /// Returns an iterator that yields rows as they are read, filtering by
    /// both key ranges and genomic intervals. This is more memory-efficient
    /// for large result sets and allows processing to start immediately.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for the query
    /// * `intervals` - Optional interval list for genomic region filtering
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::{QueryEngine, KeyRange, IntervalList};
    /// use std::sync::Arc;
    ///
    /// let engine = QueryEngine::open("data/my_table.ht").unwrap();
    /// let intervals = IntervalList::from_strings(&["chr1:100-200".to_string()]).unwrap();
    ///
    /// // Stream results filtered by intervals
    /// for row in engine.query_iter_with_intervals(&[], Some(Arc::new(intervals))).unwrap().take(100) {
    ///     let row = row.unwrap();
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn query_iter_with_intervals(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
    ) -> Result<impl Iterator<Item = Result<EncodedValue>>> {
        self.source.query_stream_with_intervals(ranges, intervals)
    }

    /// Stream query results with Level 2 decode-time projection.
    ///
    /// Fields not in the decode_projection are skipped during binary decode,
    /// avoiding heap allocation for large unused fields.
    pub fn query_iter_with_projection(
        &self,
        ranges: &[KeyRange],
        intervals: Option<Arc<IntervalList>>,
        decode_projection: Option<Arc<ProjectionTree>>,
    ) -> Result<impl Iterator<Item = Result<EncodedValue>>> {
        self.source
            .query_stream_with_projection(ranges, intervals, decode_projection)
    }

    /// Sample random rows from the data source
    ///
    /// Returns a random sample of rows using an optimized strategy based on
    /// the underlying data source:
    /// - Hail tables: samples from random partitions
    /// - VCFs with tabix index: uses index seeking for O(N) vs O(file_size)
    /// - Unindexed sources: returns an error (caller should fall back to streaming)
    ///
    /// # Arguments
    /// * `sample_size` - Number of rows to sample
    ///
    /// # Example
    /// ```no_run
    /// use genohype_core::query::QueryEngine;
    ///
    /// let engine = QueryEngine::open_path("data/variants.vcf.gz").unwrap();
    ///
    /// // Sample 100 random variants using tabix index
    /// let samples = engine.sample_random(100).unwrap();
    /// println!("Sampled {} variants", samples.len());
    /// ```
    pub fn sample_random(&self, sample_size: usize) -> Result<Vec<EncodedValue>> {
        self.source.sample_random(sample_size)
    }

    /// Wrap the data source with VEP annotation.
    ///
    /// All subsequent queries will have a `vep` field appended to each row
    /// containing transcript-level consequence predictions from fastVEP.
    ///
    /// The AnnotationContext (GFF3, FASTA) is lazily loaded on first row access.
    #[cfg(feature = "vep")]
    pub fn with_vep(
        mut self,
        options: crate::datasource::annotating::VepInitOptions,
    ) -> Result<Self> {
        let annotating =
            crate::datasource::annotating::AnnotatingDataSource::new(self.source, options)?;
        self.source = Box::new(annotating);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed_fixture() -> &'static str {
        "tests/fixtures/tiny-keyed.ht"
    }

    #[test]
    fn test_query_engine_open() {
        let engine = QueryEngine::open_path(keyed_fixture()).expect("failed to open fixture table");

        assert_eq!(engine.key_fields(), &["gene_id", "chrom", "start"]);
        assert_eq!(engine.num_partitions(), 1);
        assert!(engine.has_index());
    }

    #[test]
    fn test_query_engine_lookup() {
        let mut engine =
            QueryEngine::open_path(keyed_fixture()).expect("failed to open fixture table");

        let key = EncodedValue::Struct(vec![
            (
                "gene_id".to_string(),
                EncodedValue::Binary(b"ENSG00000066468".to_vec()),
            ),
            ("chrom".to_string(), EncodedValue::Binary(b"10".to_vec())),
            ("start".to_string(), EncodedValue::Int32(121478332)),
        ]);

        let row = engine.lookup(&key).expect("lookup should succeed");
        assert!(row.is_some(), "fixture key should be present");
    }
}
