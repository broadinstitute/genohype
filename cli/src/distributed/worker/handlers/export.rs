//! Export handlers for Parquet, JSON, and Summary jobs.
//!
//! Processes partitions in parallel and writes output in various formats.

use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
use genohype_core::parquet::{build_record_batch, ParquetWriter};
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Process partitions and write to Parquet output (synchronous version).
/// Uses rayon to process partitions in parallel across all CPU cores.
pub fn process_parquet_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    filters: &[KeyRange],
    intervals: Option<&Arc<IntervalList>>,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use genohype_core::query::row_matches_intervals;
    use rayon::prelude::*;

    println!("Processing {} partitions to Parquet...", partitions.len());

    let output_is_cloud = is_cloud_path(output_path);

    let engine = if let Some((cached_path, cached_eng)) = _cached_engine {
        if cached_path == input_path {
            cached_eng
        } else {
            QueryEngine::open_path(input_path)?
        }
    } else {
        QueryEngine::open_path(input_path)?
    };
    let engine_ref = &engine;

    let row_type = engine_ref.row_type().clone();
    let arrow_schema = Arc::new(genohype_core::parquet::schema::create_schema(&row_type)?);
    let row_type_ref = &row_type;
    let arrow_schema_ref = &arrow_schema;

    // Clone refs for the parallel closure
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            // Determine the output file path
            let output_file = if output_is_cloud {
                let base = output_path.trim_end_matches('/');
                format!("{}/part-{:05}.parquet", base, partition_id)
            } else {
                format!("{}/part-{:05}.parquet", output_path, partition_id)
            };

            const BATCH_SIZE: usize = 4096;
            let mut batch_rows = Vec::with_capacity(BATCH_SIZE);
            let mut partition_rows = 0;

            // Stream rows from this partition with filters
            let iter = engine_ref.scan_partition_iter(partition_id, &filters)?;

            // Clone telemetry for this thread
            let ts = telemetry.clone();

            // Helper macro for processing with any writer type
            macro_rules! process_with_writer {
                ($writer:expr) => {{
                    let mut writer = $writer;

                    for row_result in iter {
                        let row = row_result?;

                        // Apply interval filtering if present
                        if let Some(ref ivl) = intervals {
                            if !row_matches_intervals(&row, ivl) {
                                continue;
                            }
                        }

                        batch_rows.push(row);

                        if batch_rows.len() >= BATCH_SIZE {
                            let batch = build_record_batch(&batch_rows, row_type_ref, arrow_schema_ref.clone())?;
                            writer.write_batch(&batch)?;
                            partition_rows += batch_rows.len();
                            // Update telemetry row count
                            if let Some(ref t) = ts {
                                t.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                            }
                            batch_rows.clear();
                        }
                    }

                    // Write remaining rows
                    if !batch_rows.is_empty() {
                        let batch = build_record_batch(&batch_rows, row_type_ref, arrow_schema_ref.clone())?;
                        writer.write_batch(&batch)?;
                        partition_rows += batch_rows.len();
                        if let Some(ref t) = ts {
                            t.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                        }
                    }

                    writer
                }};
            }

            if output_is_cloud {
                let cloud_writer = StreamingCloudWriter::new(&output_file)?;
                let writer = ParquetWriter::from_writer(cloud_writer, row_type_ref)?;
                let writer = process_with_writer!(writer);
                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
            } else {
                let writer = ParquetWriter::new(&output_file, row_type_ref)?;
                let writer = process_with_writer!(writer);
                writer.close()?;
            }

            println!(
                "  Partition {} complete: {} rows -> {}",
                partition_id, partition_rows, output_file
            );

            Ok(partition_rows)
        })
        .collect();

    // Check for any errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();

    Ok((total, Some((input_path.to_string(), engine))))
}

/// Process partitions and write to JSON output (NDJSON format).
pub fn process_json_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    filters: &[KeyRange],
    intervals: Option<&Arc<IntervalList>>,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use genohype_core::export::JsonWriter;
    use genohype_core::query::row_matches_intervals;
    use rayon::prelude::*;
    use std::fs::File;
    use std::io::BufWriter;

    println!("Processing {} partitions to JSON...", partitions.len());

    let output_is_cloud = is_cloud_path(output_path);

    let engine = if let Some((cached_path, cached_eng)) = _cached_engine {
        if cached_path == input_path {
            cached_eng
        } else {
            QueryEngine::open_path(input_path)?
        }
    } else {
        QueryEngine::open_path(input_path)?
    };
    let engine_ref = &engine;

    // Clone refs for the parallel closure
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            let output_file = if output_is_cloud {
                let base = output_path.trim_end_matches('/');
                format!("{}/part-{:05}.json", base, partition_id)
            } else {
                format!("{}/part-{:05}.json", output_path, partition_id)
            };

            let mut partition_rows = 0;

            // Stream rows from this partition with filters
            let iter = engine_ref.scan_partition_iter(partition_id, &filters)?;

            let ts = telemetry.clone();

            macro_rules! process_json_with_writer {
                ($writer:expr) => {{
                    let mut json_writer = JsonWriter::new($writer);

                    for row_result in iter {
                        let row = row_result?;

                        // Apply interval filtering if present
                        if let Some(ref ivl) = intervals {
                            if !row_matches_intervals(&row, ivl) {
                                continue;
                            }
                        }

                        json_writer.write_row(&row)?;
                        partition_rows += 1;

                        if let Some(ref t) = ts {
                            t.total_rows.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    json_writer.flush()?;
                    json_writer.into_inner()
                }};
            }

            if output_is_cloud {
                let cloud_writer = StreamingCloudWriter::new(&output_file)?;
                let writer = process_json_with_writer!(cloud_writer);
                writer.finish()?;
            } else {
                let file = File::create(&output_file)?;
                let buf_writer = BufWriter::new(file);
                process_json_with_writer!(buf_writer);
            }

            println!(
                "  Partition {} complete: {} rows -> {}",
                partition_id, partition_rows, output_file
            );

            Ok(partition_rows)
        })
        .collect();

    // Check for any errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();
    Ok((total, Some((input_path.to_string(), engine))))
}

/// Process partitions and collect summary statistics.
/// Uses rayon to process partitions in parallel and merge stats.
pub fn process_summary(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, genohype_core::summary::stats::StatsAccumulator, Option<(String, QueryEngine)>)> {
    use genohype_core::summary::stats::StatsAccumulator;
    use rayon::prelude::*;

    println!("Processing {} partitions for summary...", partitions.len());

    let engine = if let Some((cached_path, cached_eng)) = _cached_engine {
        if cached_path == input_path {
            cached_eng
        } else {
            QueryEngine::open_path(input_path)?
        }
    } else {
        QueryEngine::open_path(input_path)?
    };
    let engine_ref = &engine;

    // Process partitions in parallel using rayon's fold/reduce
    // Each thread gets its own StatsAccumulator and they are merged at the end
    let (total_rows, stats) = partitions
        .par_iter()
        .fold(
            || (0usize, StatsAccumulator::new()),
            |(mut rows, mut acc), &partition_id| {
                // Track the active partition for this Rayon thread (RAII guard)
                let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

                match engine_ref.scan_partition_iter(partition_id, &[]) {
                    Ok(iter) => {
                        for row_result in iter {
                            match row_result {
                                Ok(row) => {
                                    acc.process_row(&row);
                                    rows += 1;

                                    // Update telemetry
                                    if let Some(ref t) = telemetry {
                                        t.total_rows.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Warning: Error reading row in partition {}: {}",
                                        partition_id, e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to scan partition {}: {}",
                            partition_id, e
                        );
                    }
                }
                (rows, acc)
            },
        )
        .reduce(
            || (0, StatsAccumulator::new()),
            |(rows_a, mut acc_a), (rows_b, acc_b)| {
                acc_a.merge(acc_b);
                (rows_a + rows_b, acc_a)
            },
        );

    println!("Summary complete: {} rows processed, {} fields tracked", total_rows, stats.stats.len());

    Ok((total_rows, stats, Some((input_path.to_string(), engine))))
}
