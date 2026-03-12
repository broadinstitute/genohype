//! ClickHouse export handler.
//!
//! Processes partitions and exports data to ClickHouse.

use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::query::QueryEngine;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// A simple counting semaphore for limiting concurrency.
///
/// Used to bound memory usage by limiting how many partitions are processed
/// concurrently, independent of the number of CPU cores available.
#[derive(Clone)]
pub struct Semaphore {
    inner: Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>,
    max: usize,
}

impl Semaphore {
    pub fn new(max: usize) -> Self {
        Semaphore {
            inner: Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new())),
            max,
        }
    }

    pub fn acquire(&self) -> SemaphorePermit {
        let (lock, cvar) = &*self.inner;
        let mut count = lock.lock().unwrap();
        while *count >= self.max {
            count = cvar.wait(count).unwrap();
        }
        *count += 1;
        SemaphorePermit { sem: self.clone() }
    }
}

pub struct SemaphorePermit {
    sem: Semaphore,
}

impl Drop for SemaphorePermit {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.sem.inner;
        let mut count = lock.lock().unwrap();
        *count -= 1;
        cvar.notify_one();
    }
}

/// Process partitions and export to ClickHouse.
///
/// Uses a producer-consumer pattern with bounded concurrency to prevent OOM:
/// - A semaphore limits how many partitions are processed concurrently (default: 8)
/// - Each partition spawns a reader thread that sends row chunks via bounded channel
/// - The consumer thread uploads chunks to ClickHouse
/// - Backpressure: if uploads are slow, the channel fills and reading pauses
///
/// Environment variables for tuning:
/// - HAIL_DECODER_MAX_CONCURRENT_UPLOADS: max concurrent partitions (default: 8)
/// - HAIL_DECODER_CLICKHOUSE_CHUNK_SIZE: rows per upload chunk (default: 25000)
pub fn process_clickhouse_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    url: &str,
    table: &str,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::export::ClickHouseClient;
    use crossbeam_channel::bounded;
    use rayon::prelude::*;

    // Configuration from environment (with defaults)
    let max_concurrent: usize = std::env::var("HAIL_DECODER_MAX_CONCURRENT_UPLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let chunk_size: usize = std::env::var("HAIL_DECODER_CLICKHOUSE_CHUNK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25_000);

    println!(
        "Processing {} partitions to ClickHouse table '{}' (concurrency: {}, chunk_size: {})...",
        partitions.len(),
        table,
        max_concurrent,
        chunk_size
    );

    // Semaphore to limit concurrent partition processing
    let semaphore = Semaphore::new(max_concurrent);

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

    let row_type = engine.row_type().clone();
    let arrow_schema = Arc::new(genohype_core::parquet::schema::create_schema(&row_type)?);
    let row_type_ref = &row_type;
    let arrow_schema_ref = &arrow_schema;

    // Clone refs for the parallel closure
    let url = url.to_string();
    let table = table.to_string();

    // Process partitions in parallel (but bounded by semaphore)
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            // Acquire semaphore permit - blocks if too many partitions in flight
            let _permit = semaphore.acquire();

            // Bounded channel with capacity 2: provides backpressure
            // If ClickHouse uploads are slow, reader thread will block
            let (tx, rx) = bounded::<Result<Vec<genohype_core::codec::EncodedValue>>>(2);

            let chunk_size_clone = chunk_size;
            let url_clone = url.clone();
            let table_clone = table.clone();
            let telemetry_clone = telemetry.clone();

            std::thread::scope(|s| {
                // Spawn reader thread: reads rows and sends chunks through channel
                s.spawn(move || {
                    match engine_ref.scan_partition_iter(partition_id, &[]) {
                        Ok(iter) => {
                            let mut batch = Vec::with_capacity(chunk_size_clone);
                            for row_res in iter {
                                match row_res {
                                    Ok(row) => {
                                        batch.push(row);
                                        if batch.len() >= chunk_size_clone {
                                            // Send chunk - blocks if channel is full (backpressure)
                                            if tx.send(Ok(std::mem::replace(
                                                &mut batch,
                                                Vec::with_capacity(chunk_size_clone),
                                            ))).is_err() {
                                                // Receiver dropped, stop reading
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(e));
                                        return;
                                    }
                                }
                            }
                            // Send remaining rows
                            if !batch.is_empty() {
                                let _ = tx.send(Ok(batch));
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e));
                        }
                    }
                    // tx is dropped here, closing the channel
                });

                // Consumer: receive chunks and upload to ClickHouse
                let client = ClickHouseClient::new(&url_clone);
                let ts = telemetry_clone;

                let mut partition_rows = 0;
                let mut chunks_uploaded = 0;
                const BATCH_SIZE: usize = 4096; // Internal batching for Parquet writing

                // Receive and upload chunks until channel closes
                for batch_res in rx {
                    let batch = batch_res?;

                    let uploaded = upload_chunk_to_clickhouse(
                        &client,
                        &table_clone,
                        &batch,
                        row_type_ref,
                        arrow_schema_ref.clone(),
                        BATCH_SIZE,
                    )?;

                    partition_rows += uploaded;
                    chunks_uploaded += 1;

                    if let Some(ref t) = ts {
                        t.total_rows.fetch_add(uploaded, Ordering::Relaxed);
                    }

                    // Log progress every 10 chunks
                    if chunks_uploaded % 10 == 0 {
                        println!(
                            "    Partition {} progress: {} rows in {} chunks",
                            partition_id, partition_rows, chunks_uploaded
                        );
                    }
                }

                println!(
                    "  Partition {} complete: {} rows in {} chunk(s) uploaded to ClickHouse",
                    partition_id, partition_rows, chunks_uploaded
                );

                Ok(partition_rows)
            })
        })
        .collect();

    // Check errors
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

/// Upload a chunk of rows to ClickHouse as an in-memory Parquet buffer.
///
/// This avoids writing to disk entirely - the Parquet data is built in memory
/// and streamed directly to ClickHouse via HTTP POST.
fn upload_chunk_to_clickhouse(
    client: &crate::export::ClickHouseClient,
    table: &str,
    rows: &[genohype_core::codec::EncodedValue],
    row_type: &genohype_core::codec::EncodedType,
    arrow_schema: std::sync::Arc<arrow::datatypes::Schema>,
    batch_size: usize,
) -> Result<usize> {
    use genohype_core::parquet::{build_record_batch, InMemoryParquetWriter};

    if rows.is_empty() {
        return Ok(0);
    }

    // Write rows to in-memory Parquet buffer
    let mut writer = InMemoryParquetWriter::new(row_type)?;

    for batch_start in (0..rows.len()).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(rows.len());
        let batch_rows = &rows[batch_start..batch_end];
        let record_batch = build_record_batch(batch_rows, row_type, arrow_schema.clone())?;
        writer.write_batch(&record_batch)?;
    }

    let parquet_bytes = writer.finish()?;
    let row_count = rows.len();

    // Upload to ClickHouse with retry logic
    let mut attempts = 0;
    loop {
        match client.insert_parquet_bytes(table, parquet_bytes.clone()) {
            Ok(_) => break,
            Err(e) => {
                attempts += 1;
                if attempts >= 3 {
                    return Err(crate::HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to upload chunk to ClickHouse after 3 attempts: {}", e)
                    )));
                }
                std::thread::sleep(std::time::Duration::from_secs(2 * attempts as u64));
            }
        }
    }

    Ok(row_count)
}
