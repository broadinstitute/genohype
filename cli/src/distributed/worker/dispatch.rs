//! Job dispatch module.
//!
//! Routes incoming JobSpec variants to the appropriate handler functions.

use crate::distributed::message::JobSpec;
use crate::distributed::worker::handlers;
use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use std::sync::Arc;

/// Dispatch job based on JobSpec to the appropriate processor.
///
/// Returns (rows_processed, result_json, cached_engine).
pub fn dispatch_job(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    job_spec: &JobSpec,
    filters: &[String],
    intervals: &[String],
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(
    usize,
    Option<serde_json::Value>,
    Option<(String, QueryEngine)>,
)> {
    // Parse filters from strings
    let key_ranges = parse_filter_strings(filters);
    let interval_list = parse_interval_strings(intervals);

    match job_spec {
        JobSpec::ExportParquet { output_path } => {
            let (rows, engine) = handlers::export::process_parquet_export(
                cached_engine,
                partitions,
                input_path,
                output_path,
                &key_ranges,
                interval_list.as_ref(),
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::ExportJson { output_path, .. } => {
            let (rows, engine) = handlers::export::process_json_export(
                cached_engine,
                partitions,
                input_path,
                output_path,
                &key_ranges,
                interval_list.as_ref(),
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::Summary => {
            let (rows, stats, engine) = handlers::export::process_summary(
                cached_engine,
                partitions,
                input_path,
                telemetry,
            )?;
            // Convert stats to JSON for aggregation on coordinator
            let result_json = serde_json::to_value(&stats).ok();
            Ok((rows, result_json, engine))
        }
        JobSpec::Validate { .. } => {
            // TODO: Implement distributed validation
            eprintln!("Validate job not yet implemented for distributed mode");
            Ok((0, None, cached_engine))
        }
        JobSpec::Manhattan { .. } => {
            // Manhattan is a coordinator-level job spec for submission
            Err(crate::HailError::InvalidFormat(
                "Manhattan should not be dispatched to worker - it's for coordinator submission"
                    .to_string(),
            ))
        }
        JobSpec::ManhattanBatch { .. } => {
            // ManhattanBatch is a coordinator-level job spec for submission
            // It should never be dispatched directly to workers
            Err(crate::HailError::InvalidFormat(
                "ManhattanBatch should not be dispatched to worker - it's for coordinator batch submission".to_string()
            ))
        }
        JobSpec::ManhattanScan(spec) => {
            // Set phenotype context for visibility tracking
            if let Some(ref ts) = telemetry {
                let source = match &spec.source {
                    crate::distributed::message::ManhattanSource::Exome => "exome",
                    crate::distributed::message::ManhattanSource::Genome => "genome",
                };
                ts.set_scan_phase(&spec.phenotype, source, Some(&spec.ancestry));
            }

            let (rows, engine) = handlers::manhattan::process_manhattan_scan_v2(
                cached_engine,
                partitions,
                spec,
                telemetry.clone(),
            )?;

            // Clear context when done
            if let Some(ref ts) = telemetry {
                ts.set_idle();
            }

            Ok((rows, None, engine))
        }
        JobSpec::ManhattanAggregate(spec) => {
            // Set phenotype context for visibility tracking
            if let Some(ref ts) = telemetry {
                let phenotype_id = spec.phenotype_id.as_deref().unwrap_or("unknown");
                let ancestry = spec.ancestry.as_deref();
                ts.set_aggregate_phase(phenotype_id, ancestry);
            }

            let (rows, summary) = handlers::manhattan::process_manhattan_aggregate(spec)?;

            // Clear context when done
            if let Some(ref ts) = telemetry {
                ts.set_idle();
            }

            Ok((rows, Some(summary), None))
        }
        JobSpec::ManhattanAggregateBatch { specs } => {
            use rayon::prelude::*;

            println!("Processing batch of {} aggregation tasks...", specs.len());

            // Set phenotype context to indicate aggregate batch mode
            if let Some(ref ts) = telemetry {
                let count = specs.len();
                let first_id = specs
                    .first()
                    .and_then(|s| s.phenotype_id.as_deref())
                    .unwrap_or("batch");
                let display_id = if count > 1 {
                    format!("{} (+{})", first_id, count - 1)
                } else {
                    first_id.to_string()
                };
                let ancestry = specs.first().and_then(|s| s.ancestry.as_deref());
                ts.set_aggregate_phase(&display_id, ancestry);
            }

            // Execute all aggregations in parallel using the worker's thread pool
            // This allows nested parallelism:
            // - Top level: parallel phenotypes
            // - Inner level: parallel locus plots (within process_manhattan_aggregate)
            let results: Vec<Result<(usize, serde_json::Value)>> = specs
                .par_iter()
                .map(|spec| {
                    // Track the phenotype being processed on this Rayon thread
                    // Use phenotype_id as the display label (not ancestry)
                    let phenotype_id = spec
                        .phenotype_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    let label = Some(phenotype_id.clone());
                    let _core_guard = telemetry
                        .as_ref()
                        .map(|ts| CoreTaskGuard::phenotype(ts, &phenotype_id, label));

                    handlers::manhattan::process_manhattan_aggregate(spec)
                })
                .collect();

            // Sum rows and collect summaries
            let mut total_rows = 0;
            let mut summaries = Vec::new();

            for res in results {
                let (rows, summary) = res?;
                total_rows += rows;
                summaries.push(summary);
            }

            // Combine summaries into a wrapper
            let combined_summary = serde_json::json!({
                "batch_results": summaries
            });

            // Clear context when done
            if let Some(ref ts) = telemetry {
                ts.set_idle();
            }

            Ok((total_rows, Some(combined_summary), None))
        }
        JobSpec::Loci(spec) => {
            let rows = handlers::loci::process_loci(spec)?;
            Ok((rows, None, None))
        }
        JobSpec::ExportClickhouse {
            clickhouse_url,
            table_name,
        } => {
            #[cfg(feature = "clickhouse")]
            {
                let (rows, engine) = handlers::clickhouse::process_clickhouse_export(
                    cached_engine,
                    partitions,
                    input_path,
                    clickhouse_url,
                    table_name,
                    telemetry,
                )?;
                Ok((rows, None, engine))
            }
            #[cfg(not(feature = "clickhouse"))]
            {
                let _ = (clickhouse_url, table_name); // suppress unused warning
                Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Worker binary not built with 'clickhouse' feature. Rebuild with --features clickhouse"
                )))
            }
        }
        JobSpec::IngestManhattan { .. } => {
            // This is a coordinator-level job spec, should never be sent to workers
            Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "IngestManhattan is a coordinator job spec, not a worker task",
            )))
        }
        JobSpec::IngestManhattanTask {
            phenotype_id,
            ancestry,
            base_path,
            clickhouse_url,
            database,
        } => {
            #[cfg(feature = "clickhouse")]
            {
                let rows = handlers::ingest::process_ingest_manhattan(
                    phenotype_id,
                    ancestry,
                    base_path,
                    clickhouse_url,
                    database,
                )?;
                Ok((rows, None, None))
            }
            #[cfg(not(feature = "clickhouse"))]
            {
                let _ = (phenotype_id, ancestry, base_path, clickhouse_url, database);
                Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Worker binary not built with 'clickhouse' feature. Rebuild with --features clickhouse"
                )))
            }
        }
        JobSpec::IngestManhattanBatch {
            tasks,
            clickhouse_url,
            database,
        } => {
            #[cfg(feature = "clickhouse")]
            {
                use rayon::prelude::*;

                println!(
                    "Processing batch of {} ingestion tasks concurrently...",
                    tasks.len()
                );

                let results: Vec<crate::Result<usize>> = tasks
                    .par_iter()
                    .map(|task| {
                        let _core_guard = telemetry.as_ref().map(|ts| {
                            crate::distributed::worker::telemetry::CoreTaskGuard::phenotype(
                                ts,
                                &task.phenotype_id,
                                Some(format!("{}/{}", task.ancestry, task.phenotype_id)),
                            )
                        });

                        handlers::ingest::process_ingest_manhattan(
                            &task.phenotype_id,
                            &task.ancestry,
                            &task.base_path,
                            clickhouse_url,
                            database,
                        )
                    })
                    .collect();

                let mut total_rows = 0;
                for result in results {
                    total_rows += result?;
                }

                println!(
                    "Batch ingestion complete: {} tasks, {} total rows",
                    tasks.len(),
                    total_rows
                );
                Ok((total_rows, None, None))
            }
            #[cfg(not(feature = "clickhouse"))]
            {
                let _ = (tasks, clickhouse_url, database);
                Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Worker binary not built with 'clickhouse' feature. Rebuild with --features clickhouse"
                )))
            }
        }
        JobSpec::Stress(spec) => {
            let rows = handlers::stress::process_stress(partitions, spec, telemetry)?;
            Ok((rows, None, cached_engine))
        }
        JobSpec::Custom { .. } => {
            // Custom jobs are handled by external binaries, not the genohype worker
            eprintln!("Custom job dispatched to genohype worker — this should not happen");
            Ok((0, None, cached_engine))
        }
    }
}

/// Parse filter strings back into KeyRanges.
///
/// Uses the shared filter parsing module to convert where clause strings
/// into KeyRange objects for row filtering.
pub fn parse_filter_strings(filters: &[String]) -> Vec<KeyRange> {
    use genohype_core::query::filter::parse_where_condition;

    filters
        .iter()
        .filter_map(|s| {
            let range = parse_where_condition(s);
            if range.is_none() {
                eprintln!("Warning: failed to parse filter condition: {}", s);
            }
            range
        })
        .collect()
}

/// Parse interval strings back into IntervalList.
///
/// Note: Similar to filters, interval parsing is complex and lives in main.rs.
/// For distributed mode, the coordinator handles interval validation.
pub fn parse_interval_strings(_intervals: &[String]) -> Option<Arc<IntervalList>> {
    // TODO: Move interval parsing to a shared module if needed on workers
    None
}
