//! JSON export functionality for Hail tables
//!
//! This module provides functionality to export Hail tables to JSON (NDJSON) format.
//! Supports both local files and cloud storage.

use crate::codec::EncodedValue;
use crate::error::Result;
use crate::io::{is_cloud_path, StreamingCloudWriter};
use crate::partitioning::PartitionAllocator;
use crate::query::{row_matches_intervals, IntervalList, KeyRange, QueryEngine};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Convert EncodedValue to a JSON Value.
fn to_json_value(value: &EncodedValue) -> serde_json::Value {
    match value {
        EncodedValue::Null => serde_json::Value::Null,
        EncodedValue::Binary(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        EncodedValue::Int32(i) => serde_json::Value::Number((*i).into()),
        EncodedValue::Int64(i) => serde_json::Value::Number((*i).into()),
        EncodedValue::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        EncodedValue::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        EncodedValue::Boolean(b) => serde_json::Value::Bool(*b),
        EncodedValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(to_json_value).collect())
        }
        EncodedValue::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                map.insert(k.clone(), to_json_value(v));
            }
            serde_json::Value::Object(map)
        }
    }
}

/// Writer for NDJSON format (one JSON object per line).
pub struct JsonWriter<W: Write> {
    writer: W,
}

impl<W: Write> JsonWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write_row(&mut self, row: &EncodedValue) -> Result<()> {
        let json = to_json_value(row);
        serde_json::to_writer(&mut self.writer, &json)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Convert a Hail table to a directory of JSON files (sharded).
///
/// This is the main export function for JSON format, supporting:
/// - Sharded output across multiple files
/// - Distributed processing via PartitionAllocator
/// - Progress reporting (text or JSON)
/// - Filtering by key ranges and intervals
pub fn hail_to_json_sharded_full(
    input_path: &str,
    output_dir: &str,
    show_progress: bool,
    shard_count: Option<usize>,
    rows_counter: Option<Arc<AtomicUsize>>,
    partitions_counter: Option<Arc<AtomicUsize>>,
    allocator: Option<PartitionAllocator>,
    progress_json: bool,
    filters: Vec<KeyRange>,
    intervals: Option<Arc<IntervalList>>,
) -> Result<usize> {
    let output_is_cloud = is_cloud_path(output_dir);

    if !output_is_cloud {
        std::fs::create_dir_all(output_dir)?;
    }

    let engine = QueryEngine::open_path(input_path)?;
    let total_partitions = engine.num_partitions();

    let num_shards = shard_count.unwrap_or(total_partitions);
    if num_shards == 0 {
        return Ok(0);
    }

    let owned_shards: Vec<usize> = if let Some(ref alloc) = allocator {
        alloc.get_owned_indices(num_shards)
    } else {
        (0..num_shards).collect()
    };

    if owned_shards.is_empty() {
        return Ok(0);
    }

    // Count partitions this worker will process
    let owned_partition_count: usize = owned_shards
        .iter()
        .map(|&shard_id| {
            (0..total_partitions)
                .filter(|p| p % num_shards == shard_id)
                .count()
        })
        .sum();

    let worker_id = allocator.as_ref().map(|a| a.worker_id).unwrap_or(0);
    let total_workers = allocator.as_ref().map(|a| a.total_workers).unwrap_or(1);

    let pb = if show_progress && !progress_json {
        let pb = ProgressBar::new(owned_partition_count as u64);
        let worker_info = if allocator.is_some() {
            format!(" [worker {}/{}]", worker_id, total_workers)
        } else {
            String::new()
        };
        pb.set_style(
            ProgressStyle::default_bar()
                .template(&format!(
                    "{{spinner:.green}} [{{bar:40.cyan/blue}}] {{pos}}/{{len}} partitions{} ({{elapsed}} elapsed, {{eta}} remaining)",
                    worker_info
                ))
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    let start_time = std::time::Instant::now();
    let completed_partitions = AtomicUsize::new(0);
    let total_rows = AtomicUsize::new(0);

    let partitions_per_shard: Vec<Vec<usize>> = (0..num_shards)
        .map(|shard_id| {
            (0..total_partitions)
                .filter(|p| p % num_shards == shard_id)
                .collect()
        })
        .collect();

    let engine_ref = &engine;
    let output_dir_ref = output_dir;
    let total_rows_ref = &total_rows;
    let pb_ref = &pb;
    let rows_counter_ref = &rows_counter;
    let partitions_counter_ref = &partitions_counter;
    let completed_partitions_ref = &completed_partitions;
    let start_time_ref = &start_time;
    let filters_ref = &filters;
    let intervals_ref = &intervals;

    // Process shards
    owned_shards
        .into_par_iter()
        .try_for_each(|shard_id| -> Result<()> {
            let assigned_partitions = &partitions_per_shard[shard_id];
            if assigned_partitions.is_empty() {
                return Ok(());
            }

            let output_path = if output_is_cloud {
                let base = output_dir_ref.trim_end_matches('/');
                format!("{}/part-{:05}.json", base, shard_id)
            } else {
                format!("{}/part-{:05}.json", output_dir_ref, shard_id)
            };

            let mut shard_rows = 0;

            macro_rules! process_partitions {
                ($writer:expr) => {{
                    let mut json_writer = JsonWriter::new($writer);

                    for &partition_idx in assigned_partitions {
                        let iter = engine_ref.scan_partition_iter(partition_idx, filters_ref)?;

                        for row_result in iter {
                            let row = row_result?;

                            // Apply interval filtering if present
                            if let Some(ref ivl) = intervals_ref {
                                if !row_matches_intervals(&row, ivl) {
                                    continue;
                                }
                            }

                            json_writer.write_row(&row)?;
                            shard_rows += 1;

                            if let Some(ref counter) = rows_counter_ref {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        if let Some(ref pb) = pb_ref {
                            pb.inc(1);
                        }
                        if let Some(ref counter) = partitions_counter_ref {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }

                        if progress_json {
                            let done =
                                completed_partitions_ref.fetch_add(1, Ordering::Relaxed) + 1;
                            let rows = total_rows_ref.load(Ordering::Relaxed) + shard_rows;
                            let elapsed = start_time_ref.elapsed().as_secs_f64();
                            println!(
                                "{{\"type\":\"progress\",\"worker_id\":{},\"partitions_done\":{},\"partitions_total\":{},\"rows\":{},\"elapsed_secs\":{:.3}}}",
                                worker_id, done, owned_partition_count, rows, elapsed
                            );
                        }
                    }

                    json_writer.flush()?;
                    json_writer.into_inner()
                }};
            }

            if output_is_cloud {
                let cloud_writer = StreamingCloudWriter::new(&output_path)?;
                let writer = process_partitions!(cloud_writer);
                writer.finish()?;
            } else {
                let file = File::create(&output_path)?;
                let buf_writer = BufWriter::new(file);
                process_partitions!(buf_writer);
            }

            total_rows_ref.fetch_add(shard_rows, Ordering::Relaxed);
            Ok(())
        })?;

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    Ok(total_rows.load(Ordering::Relaxed))
}
