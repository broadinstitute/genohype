//! Statistics accumulation for Hail table summaries
//!
//! This module provides utilities to collect min/max/null statistics
//! for all fields in a Hail table during a full scan.

use crate::codec::EncodedValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for a single field
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FieldStat {
    /// Total count of values seen (including nulls)
    pub count: usize,
    /// Count of null values
    pub null_count: usize,
    /// Minimum value as a string representation
    pub min: Option<String>,
    /// Maximum value as a string representation
    pub max: Option<String>,
    /// Sample of distinct values for categorical fields
    pub distinct_sample: Vec<String>,
    /// Maximum number of distinct samples to keep
    max_distinct_samples: usize,
}

impl FieldStat {
    /// Create a new FieldStat with default settings
    pub fn new() -> Self {
        Self {
            count: 0,
            null_count: 0,
            min: None,
            max: None,
            distinct_sample: Vec::new(),
            max_distinct_samples: 10,
        }
    }

    /// Update statistics with a new value
    pub fn update(&mut self, value: &str) {
        self.count += 1;

        // Update min
        if self.min.as_ref().map_or(true, |m| value < m.as_str()) {
            self.min = Some(value.to_string());
        }

        // Update max
        if self.max.as_ref().map_or(true, |m| value > m.as_str()) {
            self.max = Some(value.to_string());
        }

        // Track distinct values for categorical analysis
        if self.distinct_sample.len() < self.max_distinct_samples
            && !self.distinct_sample.contains(&value.to_string())
        {
            self.distinct_sample.push(value.to_string());
        }
    }

    /// Record a null value
    pub fn update_null(&mut self) {
        self.count += 1;
        self.null_count += 1;
    }
}

/// Accumulator for collecting statistics across all fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsAccumulator {
    /// Statistics for each field, keyed by field path (e.g., "vep.transcript_consequences.gene_id")
    pub stats: HashMap<String, FieldStat>,
}

impl StatsAccumulator {
    /// Create a new empty accumulator
    pub fn new() -> Self {
        Self {
            stats: HashMap::new(),
        }
    }

    /// Process a row and update statistics for all fields
    pub fn process_row(&mut self, row: &EncodedValue) {
        self.visit(row, "");
    }

    /// Recursively visit all values in the row
    fn visit(&mut self, value: &EncodedValue, path: &str) {
        match value {
            EncodedValue::Struct(fields) => {
                for (name, val) in fields {
                    let new_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}.{}", path, name)
                    };
                    self.visit(val, &new_path);
                }
            }
            EncodedValue::Array(elements) => {
                // For arrays, aggregate stats under the same path
                // This treats all array elements as contributing to the same field stats
                for val in elements {
                    self.visit(val, path);
                }
            }
            EncodedValue::Null => {
                if !path.is_empty() {
                    let stat = self
                        .stats
                        .entry(path.to_string())
                        .or_insert_with(FieldStat::new);
                    stat.update_null();
                }
            }
            _ => {
                // Leaf value
                if !path.is_empty() {
                    let stat = self
                        .stats
                        .entry(path.to_string())
                        .or_insert_with(FieldStat::new);
                    let val_str = value_to_string(value);
                    stat.update(&val_str);
                }
            }
        }
    }

    /// Get sorted field names
    pub fn sorted_fields(&self) -> Vec<&String> {
        let mut keys: Vec<&String> = self.stats.keys().collect();
        keys.sort();
        keys
    }
}

impl Default for StatsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsAccumulator {
    /// Merge another accumulator into this one
    ///
    /// This is used to combine results from parallel partition scans.
    pub fn merge(&mut self, other: StatsAccumulator) {
        for (path, other_stat) in other.stats {
            let stat = self.stats.entry(path).or_insert_with(FieldStat::new);
            stat.merge(other_stat);
        }
    }
}

impl FieldStat {
    /// Merge another FieldStat into this one
    pub fn merge(&mut self, other: FieldStat) {
        self.count += other.count;
        self.null_count += other.null_count;

        // Merge min
        if let Some(other_min) = other.min {
            if self.min.as_ref().map_or(true, |m| other_min < *m) {
                self.min = Some(other_min);
            }
        }

        // Merge max
        if let Some(other_max) = other.max {
            if self.max.as_ref().map_or(true, |m| other_max > *m) {
                self.max = Some(other_max);
            }
        }

        // Merge distinct samples (keep up to max)
        for val in other.distinct_sample {
            if self.distinct_sample.len() < self.max_distinct_samples
                && !self.distinct_sample.contains(&val)
            {
                self.distinct_sample.push(val);
            }
        }
    }
}

/// Convert an EncodedValue to its string representation for statistics
fn value_to_string(v: &EncodedValue) -> String {
    match v {
        EncodedValue::Binary(b) => String::from_utf8_lossy(b).to_string(),
        EncodedValue::Int32(i) => i.to_string(),
        EncodedValue::Int64(i) => i.to_string(),
        EncodedValue::Float32(f) => {
            if f.is_nan() {
                "NaN".to_string()
            } else {
                format!("{:.6}", f)
            }
        }
        EncodedValue::Float64(f) => {
            if f.is_nan() {
                "NaN".to_string()
            } else {
                format!("{:.6}", f)
            }
        }
        EncodedValue::Boolean(b) => b.to_string(),
        EncodedValue::Struct(_) => "<struct>".to_string(),
        EncodedValue::Array(_) => "<array>".to_string(),
        EncodedValue::Null => "<null>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_stat_update() {
        let mut stat = FieldStat::new();
        stat.update("apple");
        stat.update("banana");
        stat.update("cherry");

        assert_eq!(stat.count, 3);
        assert_eq!(stat.null_count, 0);
        assert_eq!(stat.min, Some("apple".to_string()));
        assert_eq!(stat.max, Some("cherry".to_string()));
    }

    #[test]
    fn test_field_stat_with_nulls() {
        let mut stat = FieldStat::new();
        stat.update("value1");
        stat.update_null();
        stat.update("value2");

        assert_eq!(stat.count, 3);
        assert_eq!(stat.null_count, 1);
    }

    #[test]
    fn test_stats_accumulator() {
        let mut acc = StatsAccumulator::new();

        let row = EncodedValue::Struct(vec![
            ("name".to_string(), EncodedValue::Binary(b"test".to_vec())),
            ("value".to_string(), EncodedValue::Int32(42)),
        ]);

        acc.process_row(&row);

        assert!(acc.stats.contains_key("name"));
        assert!(acc.stats.contains_key("value"));
        assert_eq!(acc.stats["name"].count, 1);
        assert_eq!(acc.stats["value"].count, 1);
    }
}
