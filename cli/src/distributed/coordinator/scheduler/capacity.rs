//! Batch sizing and capacity heuristics for worker scheduling.
//!
//! This module contains the AIMD (Additive Increase Multiplicative Decrease)
//! batch sizing logic and helper functions for extracting capacity information
//! from error messages.

use crate::distributed::message::{HardwareSpec, JobSpec};

/// Determine the optimal batch size for a worker based on its hardware and the job type.
pub(crate) fn determine_batch_size(
    default_size: usize,
    hardware: Option<&HardwareSpec>,
    job_spec: &Option<JobSpec>,
    memory_weight_mb: Option<u64>,
) -> usize {
    if let Some(hw) = hardware {
        // Different jobs have different memory characteristics.
        // - Parquet/JSON: streaming, low memory, scale well
        // - Manhattan: higher memory footprint per partition (point rendering)
        // - Summary: very memory efficient, saturates cores easily
        let core_multiplier = match job_spec {
            Some(JobSpec::ExportParquet { .. }) | Some(JobSpec::ExportJson { .. }) => 2.0,
            Some(JobSpec::ManhattanScan(_))
            | Some(JobSpec::ManhattanBatch { .. })
            | Some(JobSpec::Manhattan { .. }) => 1.0,
            Some(JobSpec::ExportClickhouse { .. }) => 1.0,
            Some(JobSpec::Summary) => 3.0,
            _ => 1.5,
        };

        let core_based = (hw.num_cores as f64 * core_multiplier).ceil() as usize;

        // Phase 3: Use job-specific memory weight if provided, otherwise infer from job type
        let mem_per_partition_mb = memory_weight_mb.unwrap_or_else(|| match job_spec {
            Some(JobSpec::ManhattanScan(_))
            | Some(JobSpec::ManhattanBatch { .. })
            | Some(JobSpec::Manhattan { .. }) => 1024, // 1GB per partition for Manhattan
            Some(JobSpec::ExportClickhouse { .. }) => 1024, // 1GB per partition for ClickHouse buffered inserts
            Some(JobSpec::ExportParquet { .. }) | Some(JobSpec::ExportJson { .. }) => 256, // 256MB
            Some(JobSpec::Summary) => 64, // 64MB, very light
            _ => 500,
        });

        let max_by_memory = (hw.total_memory_mb / mem_per_partition_mb).max(1) as usize;

        // We want at least the default_size (so we don't regress if someone manually specified a good default),
        // but if memory dictates a lower cap, we respect it unless the default size itself exceeds memory.
        let target = core_based.max(default_size).min(max_by_memory.max(default_size));

        target
    } else {
        default_size
    }
}

/// Extract the batch capacity from a "batch too large" error message.
/// Returns the number of partitions the worker can handle, or None if not found.
/// Expected format: "... only N can fit ..."
pub(crate) fn extract_capacity_from_error(msg: &str) -> Option<usize> {
    let prefix = "only ";
    let suffix = " can fit";

    if let Some(start_idx) = msg.find(prefix) {
        let remainder = &msg[start_idx + prefix.len()..];
        if let Some(end_idx) = remainder.find(suffix) {
            let num_str = remainder[..end_idx].trim();
            return num_str.parse::<usize>().ok();
        }
    }
    None
}
