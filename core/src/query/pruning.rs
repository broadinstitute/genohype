//! Partition pruning logic for Hail tables

use crate::metadata::Interval;
use crate::query::intervals::IntervalList;
use crate::query::types::{KeyRange, KeyValue, QueryBound};
use serde_json::Value;
use tracing::debug;

/// Extract a KeyValue from a JSON object by field path (supports nested access)
fn extract_key_value(json: &Value, field_path: &[String]) -> Option<KeyValue> {
    let mut current = json;
    for field in field_path {
        current = current.get(field)?;
    }
    KeyValue::from_json(current)
}

/// Check if a query range overlaps with a partition interval for a specific field path
fn range_overlaps(
    interval: &Interval,
    field_path: &[String],
    query_start: &QueryBound,
    query_end: &QueryBound,
) -> bool {
    // Extract the field values from the interval start and end
    let interval_start = match extract_key_value(&interval.start, field_path) {
        Some(v) => v,
        None => return true, // If we can't extract, assume overlap (conservative)
    };

    let interval_end = match extract_key_value(&interval.end, field_path) {
        Some(v) => v,
        None => return true, // If we can't extract, assume overlap (conservative)
    };

    // Check if ranges overlap
    // Interval: [interval_start, interval_end]
    // Query: [query_start, query_end]
    // Overlaps if: interval_start <= query_end AND interval_end >= query_start

    // Check if query_end < interval_start (no overlap)
    match query_end {
        QueryBound::Included(qe) => {
            if qe < &interval_start {
                return false;
            }
        }
        QueryBound::Excluded(qe) => {
            if qe <= &interval_start {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    // Check if query_start > interval_end (no overlap)
    match query_start {
        QueryBound::Included(qs) => {
            if qs > &interval_end {
                return false;
            }
        }
        QueryBound::Excluded(qs) => {
            if qs >= &interval_end {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    // If we haven't returned false, there's overlap
    true
}

/// Filter partitions based on key ranges
///
/// Returns a vector of partition indices that may contain rows matching the query.
/// This is used for partition pruning - we can skip reading partitions that don't
/// overlap with the query range.
pub fn filter_partitions(range_bounds: &[Interval], ranges: &[KeyRange]) -> Vec<usize> {
    filter_partitions_with_intervals(range_bounds, ranges, None)
}

/// Filter partitions based on key ranges and optional interval list
///
/// Returns a vector of partition indices that may contain rows matching the query.
/// This is used for partition pruning - we can skip reading partitions that don't
/// overlap with the query range or interval list.
///
/// # Arguments
/// * `range_bounds` - Partition intervals from metadata
/// * `ranges` - Key range constraints for filtering
/// * `intervals` - Optional interval list for genomic region filtering
pub fn filter_partitions_with_intervals(
    range_bounds: &[Interval],
    ranges: &[KeyRange],
    intervals: Option<&IntervalList>,
) -> Vec<usize> {
    let start = std::time::Instant::now();
    let result: Vec<usize> = range_bounds
        .iter()
        .enumerate()
        .filter(|(_, interval)| {
            // A partition matches if ALL query ranges overlap with it
            let ranges_match = ranges.iter().all(|range| {
                range_overlaps(interval, &range.field_path, &range.start, &range.end)
            });

            if !ranges_match {
                return false;
            }

            // If we have an interval list, check if the partition overlaps any interval
            if let Some(interval_list) = intervals {
                return partition_overlaps_intervals(interval, interval_list);
            }

            true
        })
        .map(|(idx, _)| idx)
        .collect();
    debug!(
        "filter_partitions: {}/{} partitions matched in {:?}",
        result.len(),
        range_bounds.len(),
        start.elapsed()
    );
    result
}

/// Check if a partition interval overlaps with any genomic interval in the list
///
/// This extracts the locus information from the partition's start/end bounds
/// and checks if it overlaps with any interval in the list.
fn partition_overlaps_intervals(partition: &Interval, intervals: &IntervalList) -> bool {
    // Extract locus information from partition start and end
    // Expected format: {"locus": {"contig": "chr1", "position": 12345}, ...}

    let start_locus = partition.start.get("locus");
    let end_locus = partition.end.get("locus");

    match (start_locus, end_locus) {
        (Some(start), Some(end)) => {
            // Extract contig and position from start
            let start_contig = start.get("contig").and_then(|v| v.as_str());
            let start_pos = start.get("position").and_then(|v| v.as_i64()).map(|p| p as i32);

            // Extract contig and position from end
            let end_contig = end.get("contig").and_then(|v| v.as_str());
            let end_pos = end.get("position").and_then(|v| v.as_i64()).map(|p| p as i32);

            match (start_contig, start_pos, end_contig, end_pos) {
                (Some(sc), Some(sp), Some(ec), Some(ep)) => {
                    // If contigs are the same, check simple range overlap
                    if sc == ec {
                        return intervals.overlaps(sc, sp, ep);
                    }

                    // If contigs are different, the partition spans multiple chromosomes
                    // We need to check if any interval overlaps with either contig
                    // This is conservative - we include the partition if either contig has intervals
                    //
                    // For the start contig, any interval at position >= sp would overlap
                    // For the end contig, any interval at position <= ep would overlap
                    // For any contig in between, any interval would overlap

                    // Check if start contig has overlapping intervals
                    if let Some(ranges) = intervals.intervals_for_contig(sc) {
                        for range in ranges {
                            // Any interval that ends at or after start position overlaps
                            if *range.end() >= sp {
                                return true;
                            }
                        }
                    }

                    // Check if end contig has overlapping intervals
                    if let Some(ranges) = intervals.intervals_for_contig(ec) {
                        for range in ranges {
                            // Any interval that starts at or before end position overlaps
                            if *range.start() <= ep {
                                return true;
                            }
                        }
                    }

                    // Check any contigs in between (by checking all contigs in the interval list)
                    // This is approximate - we don't know the actual contig order
                    // So we check if any other contig in the interval list exists
                    for contig in intervals.contigs() {
                        if contig != sc && contig != ec {
                            if let Some(ranges) = intervals.intervals_for_contig(contig) {
                                if !ranges.is_empty() {
                                    // There are intervals on a contig that might be between start and end
                                    // Be conservative and include this partition
                                    return true;
                                }
                            }
                        }
                    }

                    false
                }
                _ => true, // If we can't extract locus info, be conservative and include
            }
        }
        _ => true, // If no locus field, be conservative and include the partition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_interval(
        gene_id: &str,
        chrom: &str,
        start: i32,
        gene_id_end: &str,
        chrom_end: &str,
        end: i32,
    ) -> Interval {
        Interval {
            start: json!({
                "gene_id": gene_id,
                "chrom": chrom,
                "start": start,
            }),
            end: json!({
                "gene_id": gene_id_end,
                "chrom": chrom_end,
                "start": end,
            }),
            include_start: true,
            include_end: true,
        }
    }

    #[test]
    fn test_filter_partitions_single_match() {
        let intervals = vec![
            create_test_interval("ENSG00000066468", "10", 121478332, "ENSG00000185960", "X", 624344),
        ];

        // Query for chromosome 10
        let ranges = vec![KeyRange::point(
            "chrom".to_string(),
            KeyValue::String("10".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_filter_partitions_no_match() {
        let intervals = vec![
            create_test_interval("ENSG00000066468", "10", 121478332, "ENSG00000185960", "X", 624344),
        ];

        // Query for chromosome 1 (not in the data)
        let ranges = vec![KeyRange::point(
            "chrom".to_string(),
            KeyValue::String("1".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, Vec::<usize>::new());
    }

    #[test]
    fn test_filter_partitions_range_query() {
        let intervals = vec![
            create_test_interval("ENSG00000066468", "10", 121478332, "ENSG00000185960", "X", 624344),
        ];

        // Query for chromosome range (includes both 10 and X)
        let ranges = vec![
            KeyRange::inclusive(
                "chrom".to_string(),
                KeyValue::String("1".to_string()),
                KeyValue::String("Z".to_string()),
            ),
        ];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_filter_partitions_multiple_intervals() {
        let intervals = vec![
            create_test_interval("ENSG00000000001", "1", 1000000, "ENSG00000000100", "1", 2000000),
            create_test_interval("ENSG00000000101", "2", 3000000, "ENSG00000000200", "2", 4000000),
            create_test_interval("ENSG00000000201", "3", 5000000, "ENSG00000000300", "3", 6000000),
        ];

        // Query for chromosome 2
        let ranges = vec![KeyRange::point(
            "chrom".to_string(),
            KeyValue::String("2".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![1]);
    }

    fn create_locus_interval(
        start_contig: &str,
        start_pos: i32,
        end_contig: &str,
        end_pos: i32,
    ) -> Interval {
        Interval {
            start: json!({
                "locus": {
                    "contig": start_contig,
                    "position": start_pos,
                },
                "alleles": ["A", "G"],
            }),
            end: json!({
                "locus": {
                    "contig": end_contig,
                    "position": end_pos,
                },
                "alleles": ["T", "C"],
            }),
            include_start: true,
            include_end: true,
        }
    }

    #[test]
    fn test_filter_partitions_nested_locus_contig() {
        let intervals = vec![
            create_locus_interval("chr1", 100000, "chr1", 200000),
            create_locus_interval("chr2", 300000, "chr2", 400000),
            create_locus_interval("chr3", 500000, "chr3", 600000),
        ];

        // Query for locus.contig = "chr2" using nested field path
        let ranges = vec![KeyRange::point_nested(
            vec!["locus".to_string(), "contig".to_string()],
            KeyValue::String("chr2".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_filter_partitions_nested_locus_position_range() {
        let intervals = vec![
            create_locus_interval("chr1", 100000, "chr1", 200000),
            create_locus_interval("chr1", 200001, "chr1", 300000),
            create_locus_interval("chr1", 300001, "chr1", 400000),
        ];

        // Query for locus.position >= 150000 AND locus.position <= 250000
        let ranges = vec![
            KeyRange::gte_nested(
                vec!["locus".to_string(), "position".to_string()],
                KeyValue::Int32(150000),
            ),
            KeyRange::lte_nested(
                vec!["locus".to_string(), "position".to_string()],
                KeyValue::Int32(250000),
            ),
        ];

        let result = filter_partitions(&intervals, &ranges);
        // Should match partitions 0 and 1 (100000-200000 and 200001-300000)
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn test_filter_partitions_with_interval_list() {
        let partitions = vec![
            create_locus_interval("chr1", 100000, "chr1", 200000),
            create_locus_interval("chr1", 200001, "chr1", 300000),
            create_locus_interval("chr1", 300001, "chr1", 400000),
            create_locus_interval("chr2", 100000, "chr2", 200000),
        ];

        // Create an interval list with regions on chr1
        let mut intervals = IntervalList::new();
        intervals.add("chr1".to_string(), 150000, 250000); // Overlaps partitions 0 and 1
        intervals.optimize();

        let result = filter_partitions_with_intervals(&partitions, &[], Some(&intervals));
        // Should match partitions 0 and 1 (chr1:100000-200000 and chr1:200001-300000)
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn test_filter_partitions_with_interval_list_multiple_contigs() {
        let partitions = vec![
            create_locus_interval("chr1", 100000, "chr1", 200000),
            create_locus_interval("chr2", 100000, "chr2", 200000),
            create_locus_interval("chr3", 100000, "chr3", 200000),
        ];

        // Create an interval list with regions on chr1 and chr3
        let mut intervals = IntervalList::new();
        intervals.add("chr1".to_string(), 150000, 175000);
        intervals.add("chr3".to_string(), 125000, 150000);
        intervals.optimize();

        let result = filter_partitions_with_intervals(&partitions, &[], Some(&intervals));
        // Should match partitions 0 and 2
        assert_eq!(result, vec![0, 2]);
    }

    #[test]
    fn test_filter_partitions_with_interval_list_no_overlap() {
        let partitions = vec![
            create_locus_interval("chr1", 100000, "chr1", 200000),
            create_locus_interval("chr2", 100000, "chr2", 200000),
        ];

        // Create an interval list with regions on chr3 (no overlap)
        let mut intervals = IntervalList::new();
        intervals.add("chr3".to_string(), 100000, 200000);
        intervals.optimize();

        let result = filter_partitions_with_intervals(&partitions, &[], Some(&intervals));
        // Should match no partitions
        assert_eq!(result, Vec::<usize>::new());
    }

    #[test]
    fn test_filter_partitions_with_interval_list_and_key_ranges() {
        let partitions = vec![
            create_locus_interval("chr1", 100000, "chr1", 200000),
            create_locus_interval("chr1", 200001, "chr1", 300000),
            create_locus_interval("chr1", 300001, "chr1", 400000),
        ];

        // Create an interval list that would match all chr1 partitions
        let mut intervals = IntervalList::new();
        intervals.add("chr1".to_string(), 100000, 400000);
        intervals.optimize();

        // But also add a key range that restricts to position >= 250000
        let ranges = vec![KeyRange::gte_nested(
            vec!["locus".to_string(), "position".to_string()],
            KeyValue::Int32(250000),
        )];

        let result = filter_partitions_with_intervals(&partitions, &ranges, Some(&intervals));
        // Key range restricts to partitions 1 and 2 (200001-300000 and 300001-400000)
        assert_eq!(result, vec![1, 2]);
    }
}
