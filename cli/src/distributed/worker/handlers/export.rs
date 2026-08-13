//! Export handlers for Parquet, JSON, and Summary jobs.
//!
//! Processes partitions in parallel and writes output in various formats.

use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
use genohype_core::parquet::{build_record_batch, ParquetWriter};
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct SafePartitionWrite<W> {
    sink: W,
    stats: genohype_core::parquet::ParquetWriteStats,
    row_count: u64,
    decoded_rows_sha256: String,
    first_key: Option<genohype_core::codec::LosslessValue>,
    last_key: Option<genohype_core::codec::LosslessValue>,
}

/// Scan one already snapshot-pinned partition into an injected Parquet sink.
/// Keeping this below URI resolution lets local checked fixtures qualify the
/// real decode/write loop without making local destinations valid safe-export URLs.
fn write_pinned_safe_partition<W: Write + Send>(
    engine: &QueryEngine,
    partition_index: usize,
    key_fields: &[String],
    mut writer: ParquetWriter<W>,
) -> Result<SafePartitionWrite<W>> {
    use genohype_core::release::DecodedRowsHasher;

    let mut digest = DecodedRowsHasher::new();
    let mut first_key = None;
    let mut last_key = None;
    for row in engine.scan_partition_iter(partition_index, &[])? {
        let row = row?;
        digest.update(&row)?;
        if !key_fields.is_empty() {
            let key = lossless_key(&row, key_fields)?;
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            last_key = Some(key);
        }
        writer.write_row(&row)?;
    }
    let row_count = digest.row_count();
    let decoded_rows_sha256 = digest.finalize();
    let (sink, stats) = writer.finish_into_inner()?;
    if stats.row_count != row_count {
        return Err(crate::HailError::InvalidFormat(
            "safe-export decoded and Parquet row counts differ".to_string(),
        ));
    }
    Ok(SafePartitionWrite {
        sink,
        stats,
        row_count,
        decoded_rows_sha256,
        first_key,
        last_key,
    })
}

/// Execute exactly one manifest-pinned safe-export task into attempt-private
/// staging. This function never publishes a canonical object or receipt.
pub fn process_safe_export_part(
    spec: &genohype_core::release::SafeExportSpec,
    task: &genohype_core::release::SafeExportPartTask,
    assignment: &crate::distributed::message::AssignmentLease,
    worker_id: &str,
    worker_git_commit: &str,
    worker_binary_sha256: &str,
) -> Result<genohype_core::release::ExportPartReport> {
    use genohype_core::io::{
        build_attempt_staging_location, lease_identity_sha256, SourceManifest, SourceObject,
    };
    use genohype_core::release::GlobalsPayload;

    spec.validate()?;
    if assignment.task_id != task.task_id || assignment.assignment_attempt == 0 {
        return Err(crate::HailError::InvalidFormat(
            "safe-export assignment does not match task".to_string(),
        ));
    }
    let manifest = SourceManifest::from_path(&spec.source_manifest_uri)?;
    task.validate_against_manifest(spec, &manifest)?;
    let engine = QueryEngine::open_path_with_manifest(
        &spec.canonical_table_uri,
        &spec.source_manifest_uri,
        &spec.source_manifest_sha256,
    )?;
    if engine.num_partitions() != spec.table.partitions.len()
        || engine.row_partition_uri(task.partition_index)? != task.source_object.uri
        || engine.row_partition_source_object(task.partition_index)? != task.source_object
    {
        return Err(crate::HailError::InvalidFormat(
            "safe-export pinned partition identity mismatch".to_string(),
        ));
    }
    let mut actual_table = engine.table_descriptor()?;
    for (actual, expected) in actual_table
        .partitions
        .iter_mut()
        .zip(&spec.table.partitions)
    {
        actual.output_part_ids = expected.output_part_ids.clone();
        actual.row_count = expected.row_count;
        actual.index_objects = expected.index_objects.clone();
    }
    if actual_table != spec.table
        || GlobalsPayload::from_value(&engine.globals()?) != spec.globals
        || engine
            .parquet_plan(genohype_core::schema::PhysicalBinaryEncoding::Utf8)?
            .fingerprint_sha256()?
            != task.schema_fingerprint_sha256
    {
        return Err(crate::HailError::InvalidFormat(
            "safe-export table/globals/schema descriptor mismatch".to_string(),
        ));
    }
    if worker_git_commit != spec.expected_worker.git_commit
        || worker_binary_sha256 != spec.expected_worker.binary_sha256
    {
        return Err(crate::HailError::InvalidFormat(
            "safe-export running worker identity mismatch".to_string(),
        ));
    }

    let started_at = canonical_utc_now();
    let staging = build_attempt_staging_location(
        &spec.output_root_uri,
        &spec.export_id,
        &task.task_id,
        assignment.assignment_attempt,
        &assignment.lease_token,
        "parquet",
    )?;
    let plan = Arc::new(engine.parquet_plan(genohype_core::schema::PhysicalBinaryEncoding::Utf8)?);
    let cloud_writer = StreamingCloudWriter::new(&staging.uri)?;
    let writer = ParquetWriter::from_writer(cloud_writer, plan)?;
    let written = write_pinned_safe_partition(
        &engine,
        task.partition_index,
        &spec.table.key.fields,
        writer,
    )?;
    let uploaded = written.sink.finish()?;
    let etag = uploaded.put_result.e_tag.clone().ok_or_else(|| {
        crate::HailError::InvalidFormat("staging completion returned no ETag".to_string())
    })?;
    let version = uploaded.put_result.version.clone().ok_or_else(|| {
        crate::HailError::InvalidFormat("staging completion returned no generation".to_string())
    })?;
    let staging_object = SourceObject {
        uri: staging.uri,
        version,
        etag,
        size: uploaded.byte_count,
        crc32c: None,
        sha256: Some(uploaded.sha256),
    };
    let report = genohype_core::release::ExportPartReport {
        schema_version: genohype_core::release::SAFE_EXPORT_SCHEMA_VERSION,
        export_id: spec.export_id.clone(),
        task_id: task.task_id.clone(),
        partition_index: task.partition_index,
        source_manifest_sha256: task.source_manifest_sha256.clone(),
        source_part_file: task.source_part_file.clone(),
        source_object: task.source_object.clone(),
        schema_fingerprint_sha256: task.schema_fingerprint_sha256.clone(),
        byte_size: staging_object.size,
        staging_object,
        row_count: written.row_count,
        row_group_count: written.stats.row_group_count,
        decoded_rows_sha256: written.decoded_rows_sha256,
        first_key: written.first_key,
        last_key: written.last_key,
        worker_id: worker_id.to_string(),
        worker_git_commit: worker_git_commit.to_string(),
        worker_binary_sha256: worker_binary_sha256.to_string(),
        assignment_attempt: assignment.assignment_attempt,
        lease_identity_sha256: lease_identity_sha256(&assignment.lease_token)?,
        started_at,
        completed_at: canonical_utc_now(),
    };
    report.validate_against(
        spec,
        task,
        worker_id,
        assignment.assignment_attempt,
        &staging.lease_identity_sha256,
    )?;
    Ok(report)
}

fn lossless_key(
    row: &genohype_core::codec::EncodedValue,
    key_fields: &[String],
) -> Result<genohype_core::codec::LosslessValue> {
    let genohype_core::codec::EncodedValue::Struct(fields) = row else {
        return Err(crate::HailError::InvalidFormat(
            "safe-export key extraction requires a struct row".to_string(),
        ));
    };
    let mut key = Vec::with_capacity(key_fields.len());
    for name in key_fields {
        let value = fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.clone())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(format!(
                    "safe-export row is missing key field {name}"
                ))
            })?;
        key.push((name.clone(), value));
    }
    Ok(genohype_core::codec::LosslessValue::from(
        &genohype_core::codec::EncodedValue::Struct(key),
    ))
}

fn canonical_utc_now() -> String {
    // Civil-date conversion from Unix days; avoids locale/process dependencies.
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let hour = day_seconds / 3_600;
    let minute = day_seconds % 3_600 / 60;
    let second = day_seconds % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

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

    let plan =
        Arc::new(engine_ref.parquet_plan(genohype_core::schema::PhysicalBinaryEncoding::Utf8)?);
    let plan_ref = &plan;

    // Clone refs for the parallel closure
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry
                .as_ref()
                .map(|ts| CoreTaskGuard::partition(ts, partition_id));

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
                            let batch = build_record_batch(&batch_rows, plan_ref)?;
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
                        let batch = build_record_batch(&batch_rows, plan_ref)?;
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
                let writer =
                    ParquetWriter::from_writer_deterministic(cloud_writer, plan_ref.clone())?;
                let writer = process_with_writer!(writer);
                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
            } else {
                let writer = ParquetWriter::new_deterministic(&output_file, plan_ref.clone())?;
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
            let _core_guard = telemetry
                .as_ref()
                .map(|ts| CoreTaskGuard::partition(ts, partition_id));

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
) -> Result<(
    usize,
    genohype_core::summary::stats::StatsAccumulator,
    Option<(String, QueryEngine)>,
)> {
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
                let _core_guard = telemetry
                    .as_ref()
                    .map(|ts| CoreTaskGuard::partition(ts, partition_id));

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
                        eprintln!("Warning: Failed to scan partition {}: {}", partition_id, e);
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

    println!(
        "Summary complete: {} rows processed, {} fields tracked",
        total_rows,
        stats.stats.len()
    );

    Ok((total_rows, stats, Some((input_path.to_string(), engine))))
}

#[cfg(test)]
mod safe_export_tests {
    use super::*;
    use genohype_core::io::{SourceManifest, SourceObject, SourceSnapshot};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use sha2::{Digest, Sha256};
    use std::io::Cursor;
    use std::path::Path;

    fn copy_tree(source: &Path, destination: &Path) {
        std::fs::create_dir_all(destination).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = destination.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    fn snapshot(root: &Path) -> SourceSnapshot {
        fn collect(current: &Path, objects: &mut Vec<SourceObject>) {
            for entry in std::fs::read_dir(current).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    collect(&entry.path(), objects);
                } else {
                    let bytes = std::fs::read(entry.path()).unwrap();
                    let digest = format!("{:x}", Sha256::digest(&bytes));
                    objects.push(SourceObject {
                        uri: entry.path().to_string_lossy().to_string(),
                        version: "checked-fixture-v1".to_string(),
                        etag: digest.clone(),
                        size: bytes.len() as u64,
                        crc32c: None,
                        sha256: Some(digest),
                    });
                }
            }
        }

        let mut objects = Vec::new();
        collect(root, &mut objects);
        let manifest = SourceManifest::new(objects).unwrap();
        let digest = manifest.digest_sha256().unwrap();
        SourceSnapshot::new(manifest, &digest).unwrap()
    }

    #[test]
    fn pinned_local_hail_mutation_fails_before_safe_parquet_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let table = temp.path().join("tiny-keyed.ht");
        copy_tree(Path::new("../core/tests/fixtures/tiny-keyed.ht"), &table);
        let pinned = snapshot(&table);
        let engine =
            QueryEngine::open_path_with_snapshot(table.to_str().unwrap(), pinned.clone()).unwrap();
        let plan = Arc::new(
            engine
                .parquet_plan(genohype_core::schema::PhysicalBinaryEncoding::Utf8)
                .unwrap(),
        );
        let writer = ParquetWriter::from_writer(Cursor::new(Vec::new()), plan).unwrap();
        let written = write_pinned_safe_partition(&engine, 0, engine.key_fields(), writer).unwrap();
        assert_eq!(written.row_count, 2);
        let bytes = written.sink.into_inner();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let rows: usize = reader.by_ref().map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);

        let mutated_engine =
            QueryEngine::open_path_with_snapshot(table.to_str().unwrap(), pinned).unwrap();
        let row_part = table.join("rows/parts/part-0");
        let mut mutated = std::fs::read(&row_part).unwrap();
        let last = mutated.len() - 1;
        mutated[last] ^= 1;
        std::fs::write(&row_part, mutated).unwrap();
        let plan = Arc::new(
            mutated_engine
                .parquet_plan(genohype_core::schema::PhysicalBinaryEncoding::Utf8)
                .unwrap(),
        );
        let writer = ParquetWriter::from_writer(Cursor::new(Vec::new()), plan).unwrap();
        let error = match write_pinned_safe_partition(
            &mutated_engine,
            0,
            mutated_engine.key_fields(),
            writer,
        ) {
            Ok(_) => panic!("mutated pinned Hail part produced safe Parquet evidence"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("SHA-256 mismatch"), "{error}");
    }
}
