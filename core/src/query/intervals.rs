//! Genomic interval list for efficient region filtering
//!
//! Supports multiple input formats:
//! - BED files (0-based, half-open)
//! - JSON files (array of {contig, start, end} objects)
//! - Text files with chr:start-end format (1-based, inclusive)
//! - Command-line strings in chr:start-end format

use crate::{HailError, Result};
use std::collections::HashMap;
use std::ops::RangeInclusive;

/// A list of genomic intervals optimized for efficient contains/overlaps queries.
///
/// Internally stores intervals as a HashMap from contig to sorted, non-overlapping
/// ranges. All coordinates are stored as 1-based inclusive to match Hail's Locus format.
#[derive(Debug, Clone)]
pub struct IntervalList {
    /// Map from contig name to sorted, non-overlapping ranges (1-based inclusive)
    intervals: HashMap<String, Vec<RangeInclusive<i32>>>,
    /// Whether the intervals have been optimized (sorted and merged)
    optimized: bool,
}

impl Default for IntervalList {
    fn default() -> Self {
        Self::new()
    }
}

impl IntervalList {
    /// Create a new empty interval list
    pub fn new() -> Self {
        Self {
            intervals: HashMap::new(),
            optimized: false,
        }
    }

    /// Add an interval to the list (1-based inclusive coordinates)
    ///
    /// # Arguments
    /// * `contig` - Chromosome/contig name
    /// * `start` - Start position (1-based, inclusive)
    /// * `end` - End position (1-based, inclusive)
    pub fn add(&mut self, contig: String, start: i32, end: i32) {
        self.intervals.entry(contig).or_default().push(start..=end);
        self.optimized = false;
    }

    /// Parse intervals from a list of strings in chr:start-end format
    ///
    /// Accepts formats:
    /// - `chr1:100-200` (1-based inclusive)
    /// - `chr1:100-200,chr2:300-400` (comma-separated)
    ///
    /// # Example
    /// ```
    /// use genohype_core::query::IntervalList;
    /// let intervals = IntervalList::from_strings(&["chr1:100-200".to_string()]).unwrap();
    /// assert!(intervals.contains("chr1", 150));
    /// ```
    pub fn from_strings(inputs: &[String]) -> Result<Self> {
        let mut list = Self::new();

        for input in inputs {
            // Handle comma-separated intervals
            for part in input.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }

                let (contig, start, end) = Self::parse_interval_string(part)?;
                list.add(contig, start, end);
            }
        }

        list.optimize();
        Ok(list)
    }

    /// Parse a single interval string in chr:start-end format
    fn parse_interval_string(s: &str) -> Result<(String, i32, i32)> {
        // Expected format: chr:start-end
        let colon_pos = s.find(':').ok_or_else(|| {
            HailError::InvalidFormat(format!(
                "Invalid interval format '{}': expected chr:start-end",
                s
            ))
        })?;

        let contig = s[..colon_pos].to_string();
        let range_part = &s[colon_pos + 1..];

        let dash_pos = range_part.find('-').ok_or_else(|| {
            HailError::InvalidFormat(format!(
                "Invalid interval format '{}': expected chr:start-end",
                s
            ))
        })?;

        let start: i32 = range_part[..dash_pos].parse().map_err(|_| {
            HailError::InvalidFormat(format!("Invalid start position in interval '{}'", s))
        })?;

        let end: i32 = range_part[dash_pos + 1..].parse().map_err(|_| {
            HailError::InvalidFormat(format!("Invalid end position in interval '{}'", s))
        })?;

        if start > end {
            return Err(HailError::InvalidFormat(format!(
                "Invalid interval '{}': start ({}) > end ({})",
                s, start, end
            )));
        }

        Ok((contig, start, end))
    }

    /// Load intervals from a file, auto-detecting format by extension
    ///
    /// Supported formats:
    /// - `.bed` - BED format (0-based, half-open)
    /// - `.json` - JSON array of {contig, start, end} objects
    /// - Other - Text file with chr:start-end lines (1-based inclusive)
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            HailError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to read interval file '{}': {}", path, e),
            ))
        })?;

        let mut list = if path.ends_with(".bed") {
            Self::parse_bed(&content)?
        } else if path.ends_with(".json") {
            Self::parse_json(&content)?
        } else {
            Self::parse_text(&content)?
        };

        list.optimize();
        Ok(list)
    }

    /// Parse BED format (0-based, half-open coordinates)
    ///
    /// BED format: tab-separated columns: chrom, chromStart, chromEnd, ...
    /// Coordinates are 0-based, half-open [start, end)
    /// We convert to 1-based inclusive: [start+1, end]
    fn parse_bed(content: &str) -> Result<Self> {
        let mut list = Self::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            // Skip empty lines and comments
            if line.is_empty()
                || line.starts_with('#')
                || line.starts_with("track")
                || line.starts_with("browser")
            {
                continue;
            }

            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 3 {
                return Err(HailError::InvalidFormat(format!(
                    "BED line {} has fewer than 3 columns: '{}'",
                    line_num + 1,
                    line
                )));
            }

            let contig = fields[0].to_string();
            let start_0based: i32 = fields[1].parse().map_err(|_| {
                HailError::InvalidFormat(format!(
                    "Invalid start position on BED line {}: '{}'",
                    line_num + 1,
                    fields[1]
                ))
            })?;
            let end_0based: i32 = fields[2].parse().map_err(|_| {
                HailError::InvalidFormat(format!(
                    "Invalid end position on BED line {}: '{}'",
                    line_num + 1,
                    fields[2]
                ))
            })?;

            // Convert from 0-based half-open [start, end) to 1-based inclusive [start+1, end]
            let start_1based = start_0based + 1;
            let end_1based = end_0based;

            if start_1based > end_1based {
                continue; // Skip zero-length intervals
            }

            list.add(contig, start_1based, end_1based);
        }

        Ok(list)
    }

    /// Parse JSON format
    ///
    /// Expected format: array of objects with contig, start, end fields
    /// Coordinates are assumed to be 1-based inclusive
    fn parse_json(content: &str) -> Result<Self> {
        let mut list = Self::new();

        let json: serde_json::Value = serde_json::from_str(content).map_err(|e| {
            HailError::InvalidFormat(format!("Invalid JSON in interval file: {}", e))
        })?;

        let array = json.as_array().ok_or_else(|| {
            HailError::InvalidFormat("JSON interval file must contain an array".to_string())
        })?;

        for (idx, item) in array.iter().enumerate() {
            let obj = item.as_object().ok_or_else(|| {
                HailError::InvalidFormat(format!("JSON interval {} must be an object", idx))
            })?;

            let contig = obj
                .get("contig")
                .or_else(|| obj.get("chrom"))
                .or_else(|| obj.get("chr"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HailError::InvalidFormat(format!(
                        "JSON interval {} missing 'contig' field",
                        idx
                    ))
                })?
                .to_string();

            let start = obj.get("start").and_then(|v| v.as_i64()).ok_or_else(|| {
                HailError::InvalidFormat(format!(
                    "JSON interval {} missing or invalid 'start' field",
                    idx
                ))
            })? as i32;

            let end = obj.get("end").and_then(|v| v.as_i64()).ok_or_else(|| {
                HailError::InvalidFormat(format!(
                    "JSON interval {} missing or invalid 'end' field",
                    idx
                ))
            })? as i32;

            if start > end {
                return Err(HailError::InvalidFormat(format!(
                    "JSON interval {} has start ({}) > end ({})",
                    idx, start, end
                )));
            }

            list.add(contig, start, end);
        }

        Ok(list)
    }

    /// Parse text format with chr:start-end lines (1-based inclusive)
    fn parse_text(content: &str) -> Result<Self> {
        let mut list = Self::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Try chr:start-end format
            if line.contains(':') {
                let (contig, start, end) = Self::parse_interval_string(line).map_err(|_| {
                    HailError::InvalidFormat(format!(
                        "Invalid interval on line {}: '{}'",
                        line_num + 1,
                        line
                    ))
                })?;
                list.add(contig, start, end);
            } else {
                // Try tab-separated: contig start end (1-based inclusive)
                let fields: Vec<&str> = line.split_whitespace().collect();
                if fields.len() >= 3 {
                    let contig = fields[0].to_string();
                    let start: i32 = fields[1].parse().map_err(|_| {
                        HailError::InvalidFormat(format!(
                            "Invalid start position on line {}: '{}'",
                            line_num + 1,
                            line
                        ))
                    })?;
                    let end: i32 = fields[2].parse().map_err(|_| {
                        HailError::InvalidFormat(format!(
                            "Invalid end position on line {}: '{}'",
                            line_num + 1,
                            line
                        ))
                    })?;

                    if start <= end {
                        list.add(contig, start, end);
                    }
                } else {
                    return Err(HailError::InvalidFormat(format!(
                        "Invalid interval format on line {}: '{}'",
                        line_num + 1,
                        line
                    )));
                }
            }
        }

        Ok(list)
    }

    /// Optimize the interval list by sorting and merging overlapping/adjacent intervals
    ///
    /// This should be called after all intervals are added to improve query performance.
    pub fn optimize(&mut self) {
        for ranges in self.intervals.values_mut() {
            if ranges.is_empty() {
                continue;
            }

            // Sort by start position
            ranges.sort_by_key(|r| *r.start());

            // Merge overlapping and adjacent ranges
            let mut merged: Vec<RangeInclusive<i32>> = Vec::with_capacity(ranges.len());

            for range in ranges.drain(..) {
                if merged.is_empty() {
                    merged.push(range);
                    continue;
                }

                let last = merged.last_mut().unwrap();
                // Check if ranges overlap or are adjacent
                // Adjacent: last.end + 1 >= range.start
                // Overlapping: last.end >= range.start
                if *last.end() + 1 >= *range.start() {
                    // Merge: extend the last range to include the new one
                    let new_end = (*last.end()).max(*range.end());
                    *last = *last.start()..=new_end;
                } else {
                    merged.push(range);
                }
            }

            *ranges = merged;
        }

        self.optimized = true;
    }

    /// Check if a position is contained in any interval for the given contig
    ///
    /// Uses binary search for O(log n) lookup on optimized lists.
    ///
    /// # Arguments
    /// * `contig` - Chromosome/contig name
    /// * `pos` - Position (1-based)
    pub fn contains(&self, contig: &str, pos: i32) -> bool {
        let ranges = match self.intervals.get(contig) {
            Some(r) => r,
            None => return false,
        };

        if ranges.is_empty() {
            return false;
        }

        if self.optimized {
            // Binary search for the range that could contain this position
            let idx = ranges.partition_point(|r| *r.end() < pos);
            if idx < ranges.len() && ranges[idx].contains(&pos) {
                return true;
            }
            false
        } else {
            // Linear search if not optimized
            ranges.iter().any(|r| r.contains(&pos))
        }
    }

    /// Check if a genomic range overlaps any interval for the given contig
    ///
    /// # Arguments
    /// * `contig` - Chromosome/contig name
    /// * `start` - Start position (1-based, inclusive)
    /// * `end` - End position (1-based, inclusive)
    pub fn overlaps(&self, contig: &str, start: i32, end: i32) -> bool {
        let ranges = match self.intervals.get(contig) {
            Some(r) => r,
            None => return false,
        };

        if ranges.is_empty() {
            return false;
        }

        if self.optimized {
            // Binary search for the first range that could overlap
            // A range [a, b] overlaps [start, end] if a <= end AND b >= start
            let idx = ranges.partition_point(|r| *r.end() < start);
            if idx < ranges.len() && *ranges[idx].start() <= end {
                return true;
            }
            false
        } else {
            // Linear search if not optimized
            ranges
                .iter()
                .any(|r| *r.start() <= end && *r.end() >= start)
        }
    }

    /// Check if this interval list is empty
    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty() || self.intervals.values().all(|v| v.is_empty())
    }

    /// Get the number of intervals (after optimization, merged intervals are counted as one)
    pub fn len(&self) -> usize {
        self.intervals.values().map(|v| v.len()).sum()
    }

    /// Get the contigs that have intervals
    pub fn contigs(&self) -> impl Iterator<Item = &String> {
        self.intervals.keys()
    }

    /// Get intervals for a specific contig
    pub fn intervals_for_contig(&self, contig: &str) -> Option<&[RangeInclusive<i32>]> {
        self.intervals.get(contig).map(|v| v.as_slice())
    }

    /// Merge another interval list into this one
    pub fn merge(&mut self, other: Self) {
        for (contig, ranges) in other.intervals {
            self.intervals.entry(contig).or_default().extend(ranges);
        }
        self.optimized = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_list_basic() {
        let mut list = IntervalList::new();
        list.add("chr1".to_string(), 100, 200);
        list.add("chr1".to_string(), 300, 400);
        list.optimize();

        assert!(list.contains("chr1", 100));
        assert!(list.contains("chr1", 150));
        assert!(list.contains("chr1", 200));
        assert!(!list.contains("chr1", 250));
        assert!(list.contains("chr1", 300));
        assert!(list.contains("chr1", 350));
        assert!(list.contains("chr1", 400));
        assert!(!list.contains("chr1", 401));
        assert!(!list.contains("chr2", 150));
    }

    #[test]
    fn test_interval_list_merge_overlapping() {
        let mut list = IntervalList::new();
        list.add("chr1".to_string(), 100, 200);
        list.add("chr1".to_string(), 150, 250); // Overlaps
        list.add("chr1".to_string(), 251, 300); // Adjacent
        list.optimize();

        // Should be merged into one interval: 100-300
        assert_eq!(list.len(), 1);
        assert!(list.contains("chr1", 100));
        assert!(list.contains("chr1", 225));
        assert!(list.contains("chr1", 300));
        assert!(!list.contains("chr1", 301));
    }

    #[test]
    fn test_interval_list_from_strings() {
        let list = IntervalList::from_strings(&[
            "chr1:100-200".to_string(),
            "chr1:300-400,chr2:500-600".to_string(),
        ])
        .unwrap();

        assert!(list.contains("chr1", 150));
        assert!(list.contains("chr1", 350));
        assert!(list.contains("chr2", 550));
        assert!(!list.contains("chr3", 100));
    }

    #[test]
    fn test_interval_list_overlaps() {
        let mut list = IntervalList::new();
        list.add("chr1".to_string(), 100, 200);
        list.add("chr1".to_string(), 300, 400);
        list.optimize();

        // Test overlaps
        assert!(list.overlaps("chr1", 50, 150)); // Overlaps start
        assert!(list.overlaps("chr1", 150, 250)); // Overlaps end
        assert!(list.overlaps("chr1", 100, 200)); // Exact match
        assert!(list.overlaps("chr1", 120, 180)); // Contained
        assert!(list.overlaps("chr1", 50, 500)); // Contains
        assert!(!list.overlaps("chr1", 201, 299)); // Gap
        assert!(!list.overlaps("chr2", 100, 200)); // Wrong contig
    }

    #[test]
    fn test_parse_bed() {
        let bed_content =
            "chr1\t99\t200\ntrack name=test\nchr1\t299\t400\n#comment\nchr2\t499\t600\n";
        let list = IntervalList::parse_bed(bed_content).unwrap();

        // BED is 0-based half-open, converted to 1-based inclusive
        // 99-200 (0-based half-open) -> 100-200 (1-based inclusive)
        assert!(list.contains("chr1", 100));
        assert!(list.contains("chr1", 200));
        assert!(!list.contains("chr1", 99)); // Excluded in 1-based

        assert!(list.contains("chr1", 300));
        assert!(list.contains("chr1", 400));

        assert!(list.contains("chr2", 500));
        assert!(list.contains("chr2", 600));
    }

    #[test]
    fn test_parse_json() {
        let json_content = r#"[
            {"contig": "chr1", "start": 100, "end": 200},
            {"chrom": "chr1", "start": 300, "end": 400},
            {"chr": "chr2", "start": 500, "end": 600}
        ]"#;
        let list = IntervalList::parse_json(json_content).unwrap();

        assert!(list.contains("chr1", 150));
        assert!(list.contains("chr1", 350));
        assert!(list.contains("chr2", 550));
    }

    #[test]
    fn test_parse_text() {
        let text_content = "chr1:100-200\n# comment\nchr1:300-400\nchr2\t500\t600\n";
        let list = IntervalList::parse_text(text_content).unwrap();

        assert!(list.contains("chr1", 150));
        assert!(list.contains("chr1", 350));
        assert!(list.contains("chr2", 550));
    }

    #[test]
    fn test_empty_interval_list() {
        let list = IntervalList::new();
        assert!(list.is_empty());
        assert!(!list.contains("chr1", 100));
        assert!(!list.overlaps("chr1", 100, 200));
    }

    #[test]
    fn test_invalid_interval_string() {
        assert!(IntervalList::from_strings(&["invalid".to_string()]).is_err());
        assert!(IntervalList::from_strings(&["chr1:abc-def".to_string()]).is_err());
        assert!(IntervalList::from_strings(&["chr1:200-100".to_string()]).is_err());
        // start > end
    }
}
