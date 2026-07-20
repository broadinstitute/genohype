//! Progress reporting types used by the parquet converter and other operations.
//!
//! These types are shared between the core library and CLI for progress tracking.

use std::collections::HashMap;

/// Progress update message sent from worker to coordinator.
///
/// Workers emit this as a JSON line to stdout when `--progress-json` is enabled.
/// The coordinator parses these to update aggregate progress display.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProgressUpdate {
    /// Message type discriminator
    #[serde(rename = "type")]
    pub msg_type: String,
    /// Worker ID (from --worker-id)
    pub worker_id: usize,
    /// Number of partitions completed
    pub partitions_done: usize,
    /// Total partitions assigned to this worker
    pub partitions_total: usize,
    /// Number of rows processed so far
    pub rows: usize,
    /// Elapsed time in seconds
    pub elapsed_secs: f64,
}

impl ProgressUpdate {
    /// Create a new progress update.
    pub fn new(
        worker_id: usize,
        partitions_done: usize,
        partitions_total: usize,
        rows: usize,
        elapsed_secs: f64,
    ) -> Self {
        Self {
            msg_type: "progress".to_string(),
            worker_id,
            partitions_done,
            partitions_total,
            rows,
            elapsed_secs,
        }
    }

    /// Serialize to a JSON line (no trailing newline).
    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Per-field size statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FieldSizeStats {
    /// Field name
    pub name: String,
    /// Field type description
    pub type_desc: String,
    /// Sum of sizes across all samples
    pub sum_bytes: u64,
    /// Number of samples
    pub count: usize,
    /// Min size seen
    pub min_bytes: usize,
    /// Max size seen
    pub max_bytes: usize,
}

impl FieldSizeStats {
    pub fn new(name: String, type_desc: String) -> Self {
        Self {
            name,
            type_desc,
            sum_bytes: 0,
            count: 0,
            min_bytes: usize::MAX,
            max_bytes: 0,
        }
    }

    pub fn add_sample(&mut self, size: usize) {
        self.sum_bytes += size as u64;
        self.count += 1;
        self.min_bytes = self.min_bytes.min(size);
        self.max_bytes = self.max_bytes.max(size);
    }

    pub fn avg_bytes(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_bytes as f64 / self.count as f64
        }
    }
}

/// Statistics about row sizes (collected via sampling)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RowSizeStats {
    /// Number of rows sampled
    pub sample_count: usize,
    /// Minimum row size in bytes
    pub min_bytes: usize,
    /// Maximum row size in bytes
    pub max_bytes: usize,
    /// Sum of all sampled row sizes (for computing average)
    pub sum_bytes: u64,
    /// Schema stats from first sampled row: (total_fields, max_depth, array_count)
    pub schema_stats: Option<(usize, usize, usize)>,
    /// Per-field size statistics (field_name -> stats)
    pub field_stats: HashMap<String, FieldSizeStats>,
}

impl RowSizeStats {
    /// Create a new empty stats collector
    pub fn new() -> Self {
        Self {
            sample_count: 0,
            min_bytes: usize::MAX,
            max_bytes: 0,
            sum_bytes: 0,
            schema_stats: None,
            field_stats: HashMap::new(),
        }
    }

    /// Add a row size sample
    pub fn add_sample(&mut self, size_bytes: usize, schema_stats: Option<(usize, usize, usize)>) {
        self.sample_count += 1;
        self.min_bytes = self.min_bytes.min(size_bytes);
        self.max_bytes = self.max_bytes.max(size_bytes);
        self.sum_bytes += size_bytes as u64;
        if self.schema_stats.is_none() {
            self.schema_stats = schema_stats;
        }
    }

    /// Add per-field size samples from a row
    pub fn add_field_samples(&mut self, field_sizes: Vec<(String, usize, String)>) {
        for (name, size, type_desc) in field_sizes {
            let stats = self
                .field_stats
                .entry(name.clone())
                .or_insert_with(|| FieldSizeStats::new(name, type_desc));
            stats.add_sample(size);
        }
    }

    /// Get average row size in bytes
    pub fn avg_bytes(&self) -> f64 {
        if self.sample_count == 0 {
            0.0
        } else {
            self.sum_bytes as f64 / self.sample_count as f64
        }
    }

    /// Get field stats sorted by average size (descending)
    pub fn sorted_field_stats(&self) -> Vec<&FieldSizeStats> {
        let mut stats: Vec<_> = self.field_stats.values().collect();
        stats.sort_by(|a, b| {
            b.avg_bytes()
                .partial_cmp(&a.avg_bytes())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        stats
    }

    /// Merge another stats collector into this one
    pub fn merge(&mut self, other: &RowSizeStats) {
        if other.sample_count == 0 {
            return;
        }
        self.sample_count += other.sample_count;
        self.min_bytes = self.min_bytes.min(other.min_bytes);
        self.max_bytes = self.max_bytes.max(other.max_bytes);
        self.sum_bytes += other.sum_bytes;
        if self.schema_stats.is_none() {
            self.schema_stats = other.schema_stats;
        }
        // Merge field stats
        for (name, other_stats) in &other.field_stats {
            let stats = self.field_stats.entry(name.clone()).or_insert_with(|| {
                FieldSizeStats::new(name.clone(), other_stats.type_desc.clone())
            });
            stats.sum_bytes += other_stats.sum_bytes;
            stats.count += other_stats.count;
            stats.min_bytes = stats.min_bytes.min(other_stats.min_bytes);
            stats.max_bytes = stats.max_bytes.max(other_stats.max_bytes);
        }
    }
}
