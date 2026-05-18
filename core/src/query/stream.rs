//! Streaming query support for Hail tables
//!
//! This module provides streaming iterators for lazy row reading,
//! enabling memory-efficient queries on large tables.

use crate::buffer::InputBuffer;
use crate::codec::{EncodedType, EncodedValue};
use crate::query::intervals::IntervalList;
use crate::query::{KeyRange, KeyValue, QueryBound};
use crate::HailError;
use crate::Result;
use std::sync::Arc;

/// Iterator that streams rows from a single partition
///
/// This struct enables lazy iteration over rows in a partition file,
/// reading and filtering rows one at a time instead of loading all into memory.
pub struct PartitionStream {
    buffer: Box<dyn InputBuffer>,
    row_type: EncodedType,
    ranges: Vec<KeyRange>,
    intervals: Option<Arc<IntervalList>>,
    /// Whether early termination has been triggered (row past interval bounds)
    terminated: bool,
}

impl PartitionStream {
    /// Create a new partition stream
    ///
    /// # Arguments
    /// * `buffer` - The initialized buffer for reading partition data
    /// * `row_type` - The row schema for decoding
    /// * `ranges` - Key range filters to apply
    pub fn new(buffer: Box<dyn InputBuffer>, row_type: EncodedType, ranges: Vec<KeyRange>) -> Self {
        Self {
            buffer,
            row_type,
            ranges,
            intervals: None,
            terminated: false,
        }
    }

    /// Create a new partition stream with interval filtering and optional seek offset
    ///
    /// # Arguments
    /// * `buffer` - The initialized buffer for reading partition data
    /// * `row_type` - The row schema for decoding
    /// * `ranges` - Key range filters to apply
    /// * `intervals` - Optional interval list for genomic region filtering
    /// * `seek_local_offset` - Bytes to skip in the decompressed stream before decoding.
    ///   This is the local offset within a block, obtained from the index seek.
    pub fn with_intervals(
        mut buffer: Box<dyn InputBuffer>,
        row_type: EncodedType,
        ranges: Vec<KeyRange>,
        intervals: Option<Arc<IntervalList>>,
        seek_local_offset: usize,
    ) -> Self {
        // Advance the decompressed stream to the row boundary
        if seek_local_offset > 0 {
            let mut dummy = vec![0u8; seek_local_offset];
            // If the skip fails, we'll just start from wherever we are
            let _ = buffer.read_exact(&mut dummy);
        }
        Self {
            buffer,
            row_type,
            ranges,
            intervals,
            terminated: false,
        }
    }
}

impl Iterator for PartitionStream {
    type Item = Result<EncodedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminated {
            return None;
        }

        loop {
            // Read row present flag
            let row_present = match self.buffer.read_bool() {
                Ok(p) => p,
                Err(HailError::UnexpectedEof) => return None,
                Err(e) => return Some(Err(e)),
            };

            if !row_present {
                // Skip null rows
                continue;
            }

            // Decode the row
            let row = match self.row_type.read_present_value(&mut *self.buffer) {
                Ok(r) => r,
                Err(HailError::UnexpectedEof) => return None,
                Err(e) => return Some(Err(e)),
            };

            // Apply key range filters
            if !row_matches_ranges(&row, &self.ranges) {
                continue;
            }

            // Apply interval filters if present — with early termination
            if let Some(ref intervals) = self.intervals {
                if row_is_past_intervals(&row, intervals) {
                    self.terminated = true;
                    return None;
                }
                if !row_matches_intervals(&row, intervals) {
                    continue;
                }
            }

            return Some(Ok(row));
        }
    }
}

/// Check if a row matches all the key range filters
pub fn row_matches_ranges(row: &EncodedValue, ranges: &[KeyRange]) -> bool {
    for range in ranges {
        // Extract the value using the field path (supports nested access)
        let field_value = extract_field_by_path(row, &range.field_path);

        if let Some(value) = field_value {
            if let Some(key_value) = encoded_to_key_value(value) {
                if !value_in_range(&key_value, range) {
                    return false;
                }
            }
        }
    }
    true
}

/// Check if a row's locus is strictly past all intervals (for early termination)
///
/// Since Hail partitions are sorted by (contig, position), if the row's position
/// is past the maximum end of all intervals for its contig, no future rows in this
/// partition can match either. Returns false if we can't determine (safe fallback).
fn row_is_past_intervals(row: &EncodedValue, intervals: &IntervalList) -> bool {
    let locus = extract_field_by_path(row, &["locus".to_string()]);
    let locus = match locus {
        Some(l) => l,
        None => return false,
    };

    let contig = extract_field_by_path(locus, &["contig".to_string()]);
    let contig_str = match contig {
        Some(EncodedValue::Binary(b)) => String::from_utf8_lossy(b),
        _ => return false,
    };

    let position = extract_field_by_path(locus, &["position".to_string()]);
    let position_val = match position {
        Some(EncodedValue::Int32(p)) => *p,
        Some(EncodedValue::Int64(p)) => *p as i32,
        _ => return false,
    };

    // Get the intervals for this contig
    if let Some(ranges) = intervals.intervals_for_contig(&contig_str) {
        if let Some(last_range) = ranges.last() {
            // If position is past the end of the last (highest) interval, we're done
            return position_val > *last_range.end();
        }
    }

    // If there are no intervals for this contig at all, we can't early-terminate
    // because the data might loop back to a contig that has intervals
    false
}

/// Check if a row's locus falls within any interval in the list
///
/// Extracts the locus.contig and locus.position from the row and checks
/// if the position falls within any interval for that contig.
pub fn row_matches_intervals(row: &EncodedValue, intervals: &IntervalList) -> bool {
    // Extract locus struct
    let locus = extract_field_by_path(row, &["locus".to_string()]);
    let locus = match locus {
        Some(l) => l,
        None => return true, // No locus field, pass through
    };

    // Extract contig
    let contig = extract_field_by_path(locus, &["contig".to_string()]);
    let contig_str = match contig {
        Some(EncodedValue::Binary(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => return true, // Can't extract contig, pass through
    };

    // Extract position
    let position = extract_field_by_path(locus, &["position".to_string()]);
    let position_val = match position {
        Some(EncodedValue::Int32(p)) => *p,
        Some(EncodedValue::Int64(p)) => *p as i32,
        _ => return true, // Can't extract position, pass through
    };

    // Check if position is within any interval for this contig
    intervals.contains(&contig_str, position_val)
}

/// Extract a field from an EncodedValue by following a field path
fn extract_field_by_path<'a>(value: &'a EncodedValue, path: &[String]) -> Option<&'a EncodedValue> {
    if path.is_empty() {
        return Some(value);
    }

    let mut current = value;
    for field_name in path {
        match current {
            EncodedValue::Struct(fields) => {
                current = fields
                    .iter()
                    .find(|(name, _)| name == field_name)
                    .map(|(_, v)| v)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Convert an EncodedValue to a KeyValue for comparison
fn encoded_to_key_value(value: &EncodedValue) -> Option<KeyValue> {
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

/// Check if a value is within a key range
fn value_in_range(value: &KeyValue, range: &KeyRange) -> bool {
    // Check start bound
    match &range.start {
        QueryBound::Included(start) => {
            if value < start {
                return false;
            }
        }
        QueryBound::Excluded(start) => {
            if value <= start {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    // Check end bound
    match &range.end {
        QueryBound::Included(end) => {
            if value > end {
                return false;
            }
        }
        QueryBound::Excluded(end) => {
            if value >= end {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_matches_ranges_empty() {
        let row = EncodedValue::Struct(vec![
            ("field1".to_string(), EncodedValue::Int32(42)),
        ]);
        assert!(row_matches_ranges(&row, &[]));
    }

    #[test]
    fn test_row_matches_ranges_point() {
        let row = EncodedValue::Struct(vec![
            ("field1".to_string(), EncodedValue::Int32(42)),
        ]);

        let range = KeyRange::point("field1".to_string(), KeyValue::Int32(42));
        assert!(row_matches_ranges(&row, &[range]));

        let range2 = KeyRange::point("field1".to_string(), KeyValue::Int32(99));
        assert!(!row_matches_ranges(&row, &[range2]));
    }

    #[test]
    fn test_row_matches_ranges_nested() {
        let row = EncodedValue::Struct(vec![
            ("outer".to_string(), EncodedValue::Struct(vec![
                ("inner".to_string(), EncodedValue::Binary(b"test".to_vec())),
            ])),
        ]);

        let range = KeyRange::point_nested(
            vec!["outer".to_string(), "inner".to_string()],
            KeyValue::String("test".to_string()),
        );
        assert!(row_matches_ranges(&row, &[range]));
    }

    fn create_locus_row(contig: &str, position: i32) -> EncodedValue {
        EncodedValue::Struct(vec![
            ("locus".to_string(), EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(contig.as_bytes().to_vec())),
                ("position".to_string(), EncodedValue::Int32(position)),
            ])),
            ("alleles".to_string(), EncodedValue::Array(vec![
                EncodedValue::Binary(b"A".to_vec()),
                EncodedValue::Binary(b"G".to_vec()),
            ])),
        ])
    }

    #[test]
    fn test_row_matches_intervals_basic() {
        let mut intervals = IntervalList::new();
        intervals.add("chr1".to_string(), 100, 200);
        intervals.add("chr1".to_string(), 300, 400);
        intervals.optimize();

        // Row within first interval
        let row1 = create_locus_row("chr1", 150);
        assert!(row_matches_intervals(&row1, &intervals));

        // Row within second interval
        let row2 = create_locus_row("chr1", 350);
        assert!(row_matches_intervals(&row2, &intervals));

        // Row outside intervals
        let row3 = create_locus_row("chr1", 250);
        assert!(!row_matches_intervals(&row3, &intervals));

        // Row on different contig
        let row4 = create_locus_row("chr2", 150);
        assert!(!row_matches_intervals(&row4, &intervals));
    }

    #[test]
    fn test_row_matches_intervals_boundary() {
        let mut intervals = IntervalList::new();
        intervals.add("chr1".to_string(), 100, 200);
        intervals.optimize();

        // Exact start
        let row1 = create_locus_row("chr1", 100);
        assert!(row_matches_intervals(&row1, &intervals));

        // Exact end
        let row2 = create_locus_row("chr1", 200);
        assert!(row_matches_intervals(&row2, &intervals));

        // Just before start
        let row3 = create_locus_row("chr1", 99);
        assert!(!row_matches_intervals(&row3, &intervals));

        // Just after end
        let row4 = create_locus_row("chr1", 201);
        assert!(!row_matches_intervals(&row4, &intervals));
    }

    #[test]
    fn test_row_matches_intervals_no_locus() {
        let intervals = IntervalList::from_strings(&["chr1:100-200".to_string()]).unwrap();

        // Row without locus field should pass through
        let row = EncodedValue::Struct(vec![
            ("field1".to_string(), EncodedValue::Int32(42)),
        ]);
        assert!(row_matches_intervals(&row, &intervals));
    }
}
