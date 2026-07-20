//! Hail to Parquet conversion utilities
//!
//! This module provides a reusable function for converting Hail tables to Parquet format.
//!
//! ## Cloud Storage Output
//!
//! The sharded export functions support writing directly to cloud storage (GCS, S3):
//!
//! ```no_run
//! use genohype_core::parquet::hail_to_parquet_sharded;
//!
//! // Write to GCS
//! hail_to_parquet_sharded("gs://bucket/input.ht", "gs://bucket/output/", true, None)?;
//!
//! // Write to local disk
//! hail_to_parquet_sharded("gs://bucket/input.ht", "/local/output/", true, None)?;
//! # Ok::<(), hail_decoder::HailError>(())
//! ```
//!
//! When output is a cloud path, each partition is buffered in memory and uploaded
//! on completion, providing a truly diskless pipeline.

use crate::io::{is_cloud_path, StreamingCloudWriter};
use crate::parquet::{build_record_batch, ParquetWriter};
use crate::partitioning::PartitionAllocator;
use crate::progress::ProgressUpdate;
use crate::query::QueryEngine;
use crate::Result;
use arrow::record_batch::RecordBatch;
use crossbeam_channel;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::thread;

/// Convert a Hail table to Parquet format.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_path` - Path for the output Parquet file
///
/// # Returns
///
/// The total number of rows written to the Parquet file.
///
/// # Example
///
/// ```rust,no_run
/// use genohype_core::parquet::hail_to_parquet;
///
/// let rows_written = hail_to_parquet("input.ht", "output.parquet")?;
/// println!("Wrote {} rows", rows_written);
/// # Ok::<(), hail_decoder::HailError>(())
/// ```
pub fn hail_to_parquet(input_path: &str, output_path: &str) -> Result<usize> {
    hail_to_parquet_with_progress(input_path, output_path, true)
}

/// Convert a Hail table to Parquet format with optional progress output.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_path` - Path for the output Parquet file
/// * `show_progress` - Whether to display progress bar
///
/// # Returns
///
/// The total number of rows written to the Parquet file.
pub fn hail_to_parquet_with_progress(
    input_path: &str,
    output_path: &str,
    show_progress: bool,
) -> Result<usize> {
    hail_to_parquet_with_options(input_path, output_path, show_progress, None)
}

/// Options for Hail to Parquet conversion
pub struct ConversionOptions {
    /// Maximum number of partitions to convert (None = all)
    pub max_partitions: Option<usize>,
    /// Whether to show progress bar
    pub show_progress: bool,
}

impl Default for ConversionOptions {
    fn default() -> Self {
        Self {
            max_partitions: None,
            show_progress: true,
        }
    }
}

/// Convert a Hail table to Parquet format with full options.
///
/// Uses a producer-consumer pattern with bounded channels for efficient parallelization.
/// Worker threads process partitions in parallel and send batches to a dedicated writer thread.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_path` - Path for the output Parquet file
/// * `show_progress` - Whether to display progress bar
/// * `max_partitions` - Maximum number of partitions to convert (None = all)
///
/// # Returns
///
/// The total number of rows written to the Parquet file.
pub fn hail_to_parquet_with_options(
    input_path: &str,
    output_path: &str,
    show_progress: bool,
    max_partitions: Option<usize>,
) -> Result<usize> {
    // Open the query engine to read the table
    let engine = QueryEngine::open_path(input_path)?;
    let total_partitions = engine.num_partitions();
    let num_partitions = max_partitions
        .map(|m| m.min(total_partitions))
        .unwrap_or(total_partitions);
    let row_type = engine.row_type().clone();

    // Create the Parquet writer to get the schema, then move to writer thread
    let writer = ParquetWriter::new(output_path, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Setup progress bar if requested
    let pb = if show_progress {
        let pb = ProgressBar::new(num_partitions as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    // Create a bounded channel - with streaming batches (4096 rows each), we can buffer more
    // This allows workers to stay busy while writer catches up
    let channel_capacity = std::env::var("HAIL_DECODER_CHANNEL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let (tx, rx) = crossbeam_channel::bounded::<Result<RecordBatch>>(channel_capacity);

    // Spawn the writer thread
    let writer_handle = thread::spawn(move || -> Result<usize> {
        let mut writer = writer;
        let mut total_rows = 0;

        // Receive and write batches from workers
        for batch_result in rx {
            let batch = batch_result?;
            total_rows += batch.num_rows();
            writer.write_batch(&batch)?;
        }

        // Close writer when all senders drop
        writer.close()?;
        Ok(total_rows)
    });

    // Run parallel workers using try_for_each_with
    // This clones the sender per-thread, enabling true parallelism
    let engine_ref = &engine;
    let row_type_ref = &row_type;
    let schema_ref = &arrow_schema;

    // Batch size for streaming - accumulate rows before building Arrow batch
    const BATCH_SIZE: usize = 4096;

    let worker_result: Result<()> =
        (0..num_partitions)
            .into_par_iter()
            .try_for_each_with(tx, |sender, i| -> Result<()> {
                // Stream partition rows instead of loading all into memory
                let iter = engine_ref.scan_partition_iter(i, &[])?;

                // Accumulate rows in batches to avoid OOM
                let mut batch_rows = Vec::with_capacity(BATCH_SIZE);

                for row_result in iter {
                    let row = row_result?;
                    batch_rows.push(row);

                    if batch_rows.len() >= BATCH_SIZE {
                        // Build and send Arrow batch
                        let batch =
                            build_record_batch(&batch_rows, row_type_ref, schema_ref.clone())?;

                        if sender.send(Ok(batch)).is_err() {
                            return Err(crate::HailError::Io(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "Writer thread disconnected",
                            )));
                        }

                        batch_rows.clear();
                    }
                }

                // Send remaining rows
                if !batch_rows.is_empty() {
                    let batch = build_record_batch(&batch_rows, row_type_ref, schema_ref.clone())?;
                    if sender.send(Ok(batch)).is_err() {
                        return Err(crate::HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "Writer thread disconnected",
                        )));
                    }
                }

                // Update progress after partition complete
                if let Some(ref pb) = pb {
                    pb.inc(1);
                }

                Ok(())
            });

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    // Handle results from workers and writer
    match worker_result {
        Ok(()) => {
            // Workers finished successfully, wait for writer
            match writer_handle.join() {
                Ok(writer_res) => writer_res,
                Err(e) => std::panic::resume_unwind(e),
            }
        }
        Err(e) => {
            // Workers failed. Wait for writer to clean up, then return worker error.
            let _ = writer_handle.join();
            Err(e)
        }
    }
}

/// Convert a Hail table to a directory of Parquet files (sharded).
///
/// This function uses true parallel writes - each shard gets its own writer thread,
/// eliminating the single-writer bottleneck of the channel-based approach.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_dir` - Path for the output directory containing Parquet files
/// * `show_progress` - Whether to display progress bar
/// * `shard_count` - Number of output files (None = one file per partition)
///
/// # Returns
///
/// The total number of rows written across all shards.
pub fn hail_to_parquet_sharded(
    input_path: &str,
    output_dir: &str,
    show_progress: bool,
    shard_count: Option<usize>,
) -> Result<usize> {
    hail_to_parquet_sharded_full(
        input_path,
        output_dir,
        show_progress,
        shard_count,
        None,
        None,
        None,
        None,  // No allocator - single worker mode
        false, // No JSON progress
    )
}

/// Convert a Hail table to a directory of Parquet files (sharded) with optional metrics.
///
/// Same as `hail_to_parquet_sharded` but accepts atomic counters for benchmark metrics.
pub fn hail_to_parquet_sharded_with_metrics(
    input_path: &str,
    output_dir: &str,
    show_progress: bool,
    shard_count: Option<usize>,
    rows_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    partitions_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
) -> Result<usize> {
    hail_to_parquet_sharded_full(
        input_path,
        output_dir,
        show_progress,
        shard_count,
        rows_counter,
        partitions_counter,
        None,
        None,  // No allocator - single worker mode
        false, // No JSON progress
    )
}

/// Convert a Hail table to a directory of Parquet files (sharded) with full metrics.
///
/// Includes row size sampling for detailed benchmark reports.
///
/// ## Cloud Output Support
///
/// When `output_dir` is a cloud path (gs://, s3://), files are written directly
/// to cloud storage without touching local disk. Each shard is buffered in memory
/// and uploaded on completion.
///
/// ## Distributed Processing
///
/// When `allocator` is provided, this worker only processes shards assigned to it.
/// This enables distributing work across multiple VMs without coordination:
///
/// ```bash
/// # Worker 0 of 4 processes shards 0, 4, 8, ...
/// genohype export parquet input.ht output/ --worker-id 0 --total-workers 4
///
/// # Worker 1 of 4 processes shards 1, 5, 9, ...
/// genohype export parquet input.ht output/ --worker-id 1 --total-workers 4
/// ```
pub fn hail_to_parquet_sharded_full(
    input_path: &str,
    output_dir: &str,
    show_progress: bool,
    shard_count: Option<usize>,
    rows_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    partitions_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    row_size_stats: Option<std::sync::Arc<std::sync::Mutex<crate::progress::RowSizeStats>>>,
    allocator: Option<PartitionAllocator>,
    progress_json: bool,
) -> Result<usize> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let output_is_cloud = is_cloud_path(output_dir);

    // Create output directory (only for local paths)
    if !output_is_cloud {
        std::fs::create_dir_all(output_dir)?;
    }

    // Open the query engine to read the table
    let engine = QueryEngine::open_path(input_path)?;
    let total_partitions = engine.num_partitions();
    let row_type = engine.row_type().clone();

    // Determine number of shards
    let num_shards = shard_count.unwrap_or(total_partitions);
    if num_shards == 0 {
        return Ok(0);
    }

    // Determine which shards this worker owns (for distributed processing)
    let owned_shards: Vec<usize> = if let Some(ref alloc) = allocator {
        alloc.get_owned_indices(num_shards)
    } else {
        (0..num_shards).collect()
    };

    // If this worker has no shards to process, return early
    if owned_shards.is_empty() {
        if show_progress {
            eprintln!(
                "Worker {}/{} has no shards to process (total shards: {})",
                allocator.map(|a| a.worker_id).unwrap_or(0),
                allocator.map(|a| a.total_workers).unwrap_or(1),
                num_shards
            );
        }
        return Ok(0);
    }

    // Count partitions this worker will process (for progress bar)
    let owned_partition_count: usize = owned_shards
        .iter()
        .map(|&shard_id| {
            (0..total_partitions)
                .filter(|p| p % num_shards == shard_id)
                .count()
        })
        .sum();

    // Get worker info for progress reporting
    let worker_id = allocator.map(|a| a.worker_id).unwrap_or(0);
    let total_workers = allocator.map(|a| a.total_workers).unwrap_or(1);

    // Setup progress bar tracking partitions (not shards) for better granularity
    // Skip visual progress bar when using JSON progress mode
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

    // Start time for JSON progress reporting
    let start_time = std::time::Instant::now();

    // Counter for completed partitions (for JSON progress)
    let completed_partitions = std::sync::atomic::AtomicUsize::new(0);

    // Total rows counter (atomic for thread-safety)
    let total_rows = AtomicUsize::new(0);

    // Batch size for streaming
    const BATCH_SIZE: usize = 4096;

    // Pre-calculate partition assignments for each shard
    // This distributes partitions as evenly as possible across shards
    let partitions_per_shard: Vec<Vec<usize>> = (0..num_shards)
        .map(|shard_id| {
            (0..total_partitions)
                .filter(|p| p % num_shards == shard_id)
                .collect()
        })
        .collect();

    // References for closures
    let engine_ref = &engine;
    let row_type_ref = &row_type;
    let output_dir_ref = output_dir;
    let total_rows_ref = &total_rows;
    let pb_ref = &pb;
    let rows_counter_ref = &rows_counter;
    let partitions_counter_ref = &partitions_counter;
    let row_size_stats_ref = &row_size_stats;
    let completed_partitions_ref = &completed_partitions;
    let start_time_ref = &start_time;

    // Row sampling interval (sample 1 in every N rows)
    const ROW_SAMPLE_INTERVAL: usize = 1000;

    // Process shards in parallel (only the shards this worker owns)
    let result: Result<()> = owned_shards.into_par_iter().try_for_each(|shard_id| {
        let assigned_partitions = &partitions_per_shard[shard_id];

        // Skip empty shards (more shards than partitions)
        if assigned_partitions.is_empty() {
            return Ok(());
        }

        // Create output path for this shard
        let output_path = if output_is_cloud {
            // Cloud path: use forward slashes, trim trailing slash
            let base = output_dir_ref.trim_end_matches('/');
            format!("{}/part-{:05}.parquet", base, shard_id)
        } else {
            format!("{}/part-{:05}.parquet", output_dir_ref, shard_id)
        };

        // Create appropriate writer based on output location
        let mut shard_rows = 0;
        let mut batch_rows = Vec::with_capacity(BATCH_SIZE);
        let mut sample_counter = 0usize;
        let mut first_row_sampled = false;

        // Helper macro to process partitions with any writer type
        macro_rules! process_partitions {
            ($writer:expr, $arrow_schema:expr) => {{
                let mut writer = $writer;
                let arrow_schema = $arrow_schema;

                // Process all assigned partitions
                for &partition_idx in assigned_partitions {
                    let iter = engine_ref.scan_partition_iter(partition_idx, &[])?;

                    for row_result in iter {
                        let row = row_result?;

                        // Sample row sizes periodically
                        if let Some(ref stats_arc) = row_size_stats_ref {
                            sample_counter += 1;
                            if sample_counter % ROW_SAMPLE_INTERVAL == 0 || !first_row_sampled {
                                let size = row.estimated_size_bytes();
                                let schema_stats = if !first_row_sampled {
                                    first_row_sampled = true;
                                    Some(row.schema_stats())
                                } else {
                                    None
                                };
                                // Get per-field sizes
                                let field_sizes = row.field_sizes();
                                if let Ok(mut stats) = stats_arc.lock() {
                                    stats.add_sample(size, schema_stats);
                                    stats.add_field_samples(field_sizes);
                                }
                            }
                        }

                        batch_rows.push(row);

                        if batch_rows.len() >= BATCH_SIZE {
                            let batch = build_record_batch(
                                &batch_rows,
                                row_type_ref,
                                arrow_schema.clone(),
                            )?;
                            writer.write_batch(&batch)?;
                            shard_rows += batch_rows.len();

                            // Update benchmark metrics counter
                            if let Some(ref counter) = rows_counter_ref {
                                counter.fetch_add(batch_rows.len(), Ordering::Relaxed);
                            }

                            batch_rows.clear();
                        }
                    }

                    // Update progress after each partition completes
                    if let Some(ref pb) = pb_ref {
                        pb.inc(1);
                    }
                    if let Some(ref counter) = partitions_counter_ref {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }

                    // Emit JSON progress if enabled
                    if progress_json {
                        let done = completed_partitions_ref.fetch_add(1, Ordering::Relaxed) + 1;
                        let rows = total_rows_ref.load(Ordering::Relaxed) + shard_rows;
                        let elapsed = start_time_ref.elapsed().as_secs_f64();
                        let update = ProgressUpdate::new(
                            worker_id,
                            done,
                            owned_partition_count,
                            rows,
                            elapsed,
                        );
                        println!("{}", update.to_json_line());
                    }
                }

                // Write remaining rows
                if !batch_rows.is_empty() {
                    let batch =
                        build_record_batch(&batch_rows, row_type_ref, arrow_schema.clone())?;
                    writer.write_batch(&batch)?;
                    shard_rows += batch_rows.len();

                    // Update benchmark metrics counter for remaining rows
                    if let Some(ref counter) = rows_counter_ref {
                        counter.fetch_add(batch_rows.len(), Ordering::Relaxed);
                    }
                }

                writer
            }};
        }

        if output_is_cloud {
            // Cloud output: use StreamingCloudWriter for background multipart upload
            let cloud_writer = StreamingCloudWriter::new(&output_path)?;
            let writer = crate::parquet::ParquetWriter::from_writer(cloud_writer, row_type_ref)?;
            let arrow_schema = writer.schema().clone();

            let writer = process_partitions!(writer, arrow_schema);

            // Get the StreamingCloudWriter back and finish it (complete multipart upload)
            let cloud_writer = writer.into_inner()?;
            cloud_writer.finish()?;
        } else {
            // Local output: use file
            let writer = crate::parquet::ParquetWriter::new(&output_path, row_type_ref)?;
            let arrow_schema = writer.schema().clone();

            let writer = process_partitions!(writer, arrow_schema);
            writer.close()?;
        }

        total_rows_ref.fetch_add(shard_rows, Ordering::Relaxed);

        Ok(())
    });

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    result?;
    Ok(total_rows.load(Ordering::Relaxed))
}

/// Get metadata about the conversion without performing it.
///
/// Useful for showing table info before conversion or for ClickHouse schema generation.
pub struct ConversionMetadata {
    pub num_partitions: usize,
    pub key_fields: Vec<String>,
    pub row_type: crate::codec::EncodedType,
}

impl ConversionMetadata {
    /// Gather metadata from a Hail table.
    pub fn from_path(input_path: &str) -> Result<Self> {
        let engine = QueryEngine::open_path(input_path)?;
        Ok(Self {
            num_partitions: engine.num_partitions(),
            key_fields: engine.key_fields().to_vec(),
            row_type: engine.row_type().clone(),
        })
    }
}
