//! Parquet export command.

use crate::benchmark::{InputMetadata, MetricsCollector};
use crate::cli::ExportParquetArgs;
use crate::commands::utils::{
    format_bytes, parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use genohype_core::io::{get_file_size, is_cloud_path, join_path, StreamingCloudWriter};
use genohype_core::parquet::{
    build_record_batch, hail_to_parquet_sharded_full, hail_to_parquet_with_options, ParquetWriter,
};
use genohype_core::partitioning::PartitionAllocator;
use genohype_core::projection::{Projection, SchemaWidth};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use std::sync::Arc;
use std::time::Duration;

pub fn run_export_parquet(args: ExportParquetArgs) -> Result<()> {
    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    // Validate partitioning args
    if args.common.partitioning.worker_id >= args.common.partitioning.total_workers {
        eprintln!(
            "{} Worker ID ({}) must be less than total workers ({})",
            "Error:".red().bold(),
            args.common.partitioning.worker_id,
            args.common.partitioning.total_workers
        );
        std::process::exit(1);
    }

    // Determine if sharded export is requested
    let use_sharded = args.per_partition || args.shard_count.is_some();

    if use_sharded {
        println!(
            "{} {} {} {} {}",
            "Converting".green(),
            args.common.input.bright_white(),
            "to".green(),
            args.output.bright_white(),
            "(sharded)".dimmed()
        );
    } else {
        println!(
            "{} {} {} {}",
            "Converting".green(),
            args.common.input.bright_white(),
            "to".green(),
            args.output.bright_white()
        );
    }

    // Open the query engine to get metadata
    let engine = QueryEngine::open_path(&args.common.input)?;

    // Wrap with VEP annotation if requested
    #[cfg(feature = "vep")]
    let engine = if let Some(vep_opts) = args.vep.to_init_options() {
        eprintln!("{} {}", "VEP annotation:".cyan(), "enabled".green());
        engine.with_vep(vep_opts)?
    } else {
        engine
    };

    let row_type = engine.row_type().clone();

    // Resolve the schema-width projection (defaults to full = no projection). For
    // browser-minimal both the Parquet/Arrow schema and the rows written are
    // narrowed, so the parquet the duckdb arm reads is apples-to-apples with the
    // other browser-minimal arms. Only the sequential (filtered/interval) path
    // honors this; the parallel/sharded converters export the full schema.
    let width_projection: Option<Projection> = match args.width.as_deref() {
        None | Some("full") => None,
        Some(other) => {
            let width = SchemaWidth::parse(other).unwrap_or_else(|e| {
                eprintln!("{} invalid --width: {}", "Error:".red().bold(), e);
                std::process::exit(1);
            });
            match width {
                SchemaWidth::Full => None,
                SchemaWidth::BrowserMinimal => {
                    let proj = Projection::browser_minimal_present_in(&row_type);
                    proj.validate(&row_type).unwrap_or_else(|e| {
                        eprintln!("{} {}", "Error:".red().bold(), e);
                        std::process::exit(1);
                    });
                    Some(proj)
                }
            }
        }
    };

    let num_partitions = engine.num_partitions();
    println!(
        "{} {}",
        "Partitions:".cyan(),
        num_partitions.to_string().bright_white()
    );
    println!("{} {:?}", "Key fields:".cyan(), engine.key_fields());
    if !where_filters.is_empty() {
        println!(
            "{} {:?}",
            "Filters:".cyan(),
            where_filters
                .iter()
                .map(|r| r.field_path_str())
                .collect::<Vec<_>>()
        );
    }
    if let Some(ref ivl) = intervals {
        println!(
            "{} {} intervals",
            "Interval filter:".cyan(),
            ivl.len().to_string().bright_white()
        );
    }
    if let Some(l) = args.common.limit {
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    if args.per_partition {
        println!(
            "{} {} files",
            "Output shards:".cyan(),
            num_partitions.to_string().bright_white()
        );
    } else if let Some(n) = args.shard_count {
        println!(
            "{} {} files",
            "Output shards:".cyan(),
            n.to_string().bright_white()
        );
    }
    if args.common.partitioning.is_distributed() {
        println!(
            "{} worker {}/{}",
            "Distributed mode:".cyan(),
            args.common
                .partitioning
                .worker_id
                .to_string()
                .bright_white(),
            args.common
                .partitioning
                .total_workers
                .to_string()
                .bright_white()
        );
    }
    println!();

    // `--width browser-minimal` narrows the schema, which only the sequential
    // streaming path implements. The parallel/sharded converters bypass the
    // per-row batch loop and always export the full schema, so reject the combo
    // up front rather than silently writing a full-width parquet.
    if width_projection.is_some()
        && (use_sharded
            || (where_filters.is_empty() && intervals.is_none() && args.common.limit.is_none()))
    {
        eprintln!(
            "{} --width browser-minimal is only supported for the streaming export path \
             (with --where/--interval/--limit); it is not supported with --per-partition, \
             --shard-count, or a full-table export.",
            "Error:".red().bold()
        );
        std::process::exit(1);
    }

    // Check for incompatible options with sharded export
    if use_sharded
        && (!where_filters.is_empty() || intervals.is_some() || args.common.limit.is_some())
    {
        eprintln!(
            "{} Sharded export (--per-partition or --shard-count) does not support --where, --interval, or --limit",
            "Error:".red().bold()
        );
        std::process::exit(1);
    }

    // Distributed mode requires sharded export
    if args.common.partitioning.is_distributed() && !use_sharded {
        eprintln!(
            "{} Distributed mode (--worker-id, --total-workers) requires --per-partition or --shard-count",
            "Error:".red().bold()
        );
        std::process::exit(1);
    }

    // Calculate input table size for benchmarking (before dropping engine)
    let input_size_bytes = if args.benchmark {
        // Try to calculate total size from partition files
        engine.rvd_spec().map(|rvd| {
            let parts_dir = join_path(&join_path(&args.common.input, "rows"), "parts");
            rvd.part_files
                .iter()
                .filter_map(|part| {
                    let path = join_path(&parts_dir, part);
                    get_file_size(&path).ok()
                })
                .sum::<u64>()
        })
    } else {
        None
    };

    // Count schema fields
    let num_fields = match engine.row_type() {
        genohype_core::codec::EncodedType::EBaseStruct { fields, .. } => fields.len(),
        _ => 0,
    };

    // Start metrics collector if benchmarking
    let metrics_collector = if args.benchmark && use_sharded {
        println!(
            "{}",
            "Benchmark mode enabled - collecting system metrics...".yellow()
        );
        let mut collector = MetricsCollector::start(Duration::from_secs(2), num_partitions);

        // Set input metadata
        collector.set_input_metadata(InputMetadata {
            path: args.common.input.clone(),
            num_partitions,
            total_size_bytes: input_size_bytes,
            key_fields: engine.key_fields().to_vec(),
            num_fields,
        });
        collector.set_output_path(args.output.clone());

        Some(collector)
    } else {
        None
    };

    let (total_rows, is_directory) = if use_sharded {
        // Use sharded export (one file per shard, true parallelism)
        let shard_count = if args.per_partition {
            None
        } else {
            args.shard_count
        };
        if args.common.partitioning.is_distributed() {
            println!(
                "{}",
                format!(
                    "Using sharded parallel export (worker {}/{})...",
                    args.common.partitioning.worker_id, args.common.partitioning.total_workers
                )
                .dimmed()
            );
        } else {
            println!(
                "{}",
                "Using sharded parallel export (all CPU cores)...".dimmed()
            );
        }
        drop(engine); // Close engine so converter can open its own

        // Create allocator for distributed processing
        let allocator = if args.common.partitioning.is_distributed() {
            Some(PartitionAllocator::new(
                args.common.partitioning.worker_id,
                args.common.partitioning.total_workers,
            ))
        } else {
            None
        };

        let rows = if let Some(ref collector) = metrics_collector {
            // Pass counters to the sharded export for metrics tracking
            hail_to_parquet_sharded_full(
                &args.common.input,
                &args.output,
                true,
                shard_count,
                Some(collector.rows_counter.clone()),
                Some(collector.partitions_counter.clone()),
                Some(collector.row_size_stats_handle()),
                allocator,
                args.common.progress_json,
            )?
        } else {
            hail_to_parquet_sharded_full(
                &args.common.input,
                &args.output,
                true,
                shard_count,
                None,
                None,
                None,
                allocator,
                args.common.progress_json,
            )?
        };
        (rows, true)
    } else {
        // Use parallel converter for full table exports (no filters/intervals/limit)
        // This uses producer-consumer pattern with all CPU cores
        let can_use_parallel =
            where_filters.is_empty() && intervals.is_none() && args.common.limit.is_none();

        let total_rows = if can_use_parallel {
            println!("{}", "Using parallel export (all CPU cores)...".dimmed());
            drop(engine); // Close engine so converter can open its own
            hail_to_parquet_with_options(&args.common.input, &args.output, true, None)?
        } else {
            // Fall back to sequential streaming for filtered exports
            println!("{}", "Using sequential export (filtered)...".dimmed());

            // For browser-minimal, narrow the schema to match the projected rows
            // (writer + record-batch builder both key off this `row_type`).
            let row_type = match &width_projection {
                Some(proj) => proj.project_type(&row_type),
                None => row_type,
            };
            if width_projection.is_some() {
                println!(
                    "{} {}",
                    "Schema width:".cyan(),
                    args.width.as_deref().unwrap_or("full").bright_white()
                );
            }

            // Materialize a scalar `xpos` Int64 column (appended last) and SORT the
            // rows by it before writing. gnomAD rows arrive locus-sorted but the
            // key is `(locus, alleles)` with `locus` a struct, so parquet row-group
            // min/max statistics on the struct can't prune a `position` range scan.
            // A scalar `xpos` column + xpos-sorted row order gives DuckDB usable
            // row-group statistics for region/gene range queries (apples-to-apples
            // with the BKD-indexed ES arm). xpos is derived per row, so query
            // *results* are unchanged. NOTE: the sequential path buffers all rows to
            // sort — fine at smoke scale (~554k rows); see the full-scale caveat in
            // the punch-list.
            let row_type = genohype_core::export::xpos::augment_type_with_xpos(&row_type);

            // Build the decode-time projection so dropped fields are never
            // decoded; keep `locus` for interval filtering.
            let decode_projection = match &width_projection {
                Some(Projection::Fields(tree)) => {
                    let mut decode_tree = tree.clone();
                    decode_tree.ensure_field("locus");
                    Some(Arc::new(decode_tree))
                }
                _ => None,
            };

            // Use streaming query with filters, intervals, and decode projection.
            let iterator =
                engine.query_iter_with_projection(&where_filters, intervals, decode_projection)?;

            // Apply the output projection so each row's top-level fields exactly
            // match the (narrowed) `row_type`, then append the derived `xpos`
            // (aligned with the schema augmentation above).
            let width_projection_ref = width_projection.clone();
            let iterator = iterator.map(move |row_result| {
                row_result.map(|row| {
                    let row = match &width_projection_ref {
                        Some(proj) => proj.apply(&row),
                        None => row,
                    };
                    genohype_core::export::xpos::augment_row_with_xpos(row)
                })
            });

            // Apply limit if specified
            let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
                Box::new(iterator.take(n))
            } else {
                Box::new(iterator)
            };

            // Progress indicator
            let pb = ProgressBar::new_spinner();
            pb.set_style(progress_style_spinner());

            // Buffer all (xpos-augmented) rows, sort by `xpos`, then write in 10k
            // batches. Buffering is required so parquet row groups are written in
            // global xpos order — giving each row group a tight `xpos` min/max so
            // DuckDB can skip groups outside a region/gene range. Stable sort keeps
            // the within-locus (allele) order, so results are byte-identical aside
            // from row ordering. Memory caveat (full scale) is noted in the report.
            macro_rules! drive_sequential {
                ($writer:expr, $arrow_schema:expr) => {{
                    let mut writer = $writer;
                    let arrow_schema = $arrow_schema;

                    let mut all_rows: Vec<genohype_core::codec::EncodedValue> = Vec::new();
                    for row_result in iterator {
                        all_rows.push(row_result?);
                        if all_rows.len() % 100_000 == 0 {
                            pb.set_message(format!("{} rows buffered...", all_rows.len()));
                        }
                    }

                    // Sort by the derived xpos (re-read from each row's locus).
                    pb.set_message(format!("sorting {} rows by xpos...", all_rows.len()));
                    all_rows.sort_by_key(|row| {
                        genohype_core::export::xpos::compute_xpos_for_row(row).unwrap_or(0)
                    });

                    let total_rows = all_rows.len();
                    let batch_size = 10000;
                    for start in (0..all_rows.len()).step_by(batch_size) {
                        let end = (start + batch_size).min(all_rows.len());
                        pb.set_message(format!("{} / {} rows written...", end, total_rows));
                        let batch = build_record_batch(
                            &all_rows[start..end],
                            &row_type,
                            arrow_schema.clone(),
                        )?;
                        writer.write_batch(&batch)?;
                    }

                    (writer, total_rows)
                }};
            }

            let total_rows = if is_cloud_path(&args.output) {
                // Cloud output: stream via multipart upload (mirrors sharded branch)
                let cloud_writer = StreamingCloudWriter::new(&args.output)?;
                let writer = ParquetWriter::from_writer(cloud_writer, &row_type)?;
                let arrow_schema = writer.schema().clone();

                let (writer, total_rows) = drive_sequential!(writer, arrow_schema);

                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
                total_rows
            } else {
                // Local output: write to file
                let writer = ParquetWriter::new(&args.output, &row_type)?;
                let arrow_schema = writer.schema().clone();

                let (writer, total_rows) = drive_sequential!(writer, arrow_schema);
                writer.close()?;
                total_rows
            };

            pb.finish_and_clear();
            total_rows
        };
        (total_rows, false)
    };

    // Print summary
    let output_size = if is_directory {
        // Calculate total size of all files in directory
        std::fs::read_dir(&args.output)
            .map(|entries| {
                let total: u64 = entries
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum();
                format_bytes(total)
            })
            .unwrap_or_else(|_| "unknown".to_string())
    } else {
        std::fs::metadata(&args.output)
            .map(|m| format_bytes(m.len()))
            .unwrap_or_else(|_| "unknown".to_string())
    };

    println!();
    println!("{}", "Conversion complete!".green().bold());
    println!(
        "  {} {}",
        "Rows written:".cyan(),
        total_rows.to_string().bright_white()
    );
    if is_directory {
        println!(
            "  {} {}",
            "Output directory:".cyan(),
            args.output.bright_white()
        );
    } else {
        println!("  {} {}", "Output file:".cyan(), args.output.bright_white());
    }
    println!("  {} {}", "Output size:".cyan(), output_size.bright_white());

    // Finish benchmark and print/write report
    if let Some(collector) = metrics_collector {
        let report = collector.finish(total_rows);

        // Print to console
        report.print();

        // Write to JSON file
        let report_path = if is_directory {
            format!("{}/benchmark-report.json", args.output)
        } else {
            format!("{}.benchmark.json", args.output)
        };

        let report_json = serde_json::json!({
            "input": report.input_metadata.as_ref().map(|m| serde_json::json!({
                "path": m.path,
                "num_partitions": m.num_partitions,
                "total_size_gb": m.total_size_bytes.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
                "key_fields": m.key_fields,
                "num_fields": m.num_fields,
            })),
            "output": {
                "path": report.output_path,
                "size_gb": report.output_size_bytes.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
            },
            "duration_secs": report.duration.as_secs_f64(),
            "total_rows": report.total_rows,
            "total_partitions": report.total_partitions,
            "num_cpus": report.num_cpus,
            "rows_per_sec": report.rows_per_sec(),
            "partitions_per_sec": report.partitions_per_sec(),
            "cpu": {
                "avg_percent": report.avg_cpu_percent(),
                "max_percent": report.max_cpu_percent(),
            },
            "memory": {
                "total_gb": report.total_memory_gb(),
                "avg_used_gb": report.avg_memory_gb(),
                "max_used_gb": report.max_memory_gb(),
            },
            "disk_io": {
                "avg_read_mb_sec": report.avg_disk_read_mb_sec(),
                "max_read_mb_sec": report.max_disk_read_mb_sec(),
                "avg_write_mb_sec": report.avg_disk_write_mb_sec(),
                "max_write_mb_sec": report.max_disk_write_mb_sec(),
            },
            "disk_space": {
                "available_gb": report.disk_space_available.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
                "total_gb": report.disk_space_total.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
            },
            "decoded_row_size": report.row_size_stats.as_ref().map(|stats| {
                let total_avg = stats.avg_bytes();
                serde_json::json!({
                    "_note": "In-memory sizes after decoding, not on-disk compressed sizes",
                    "sample_count": stats.sample_count,
                    "avg_bytes": stats.avg_bytes(),
                    "min_bytes": stats.min_bytes,
                    "max_bytes": stats.max_bytes,
                    "estimated_memory_footprint_gb": stats.avg_bytes() * report.total_rows as f64 / (1024.0 * 1024.0 * 1024.0),
                    "schema": stats.schema_stats.map(|(fields, depth, arrays)| serde_json::json!({
                        "total_fields": fields,
                        "max_depth": depth,
                        "array_count": arrays,
                    })),
                    "fields": stats.sorted_field_stats().iter().map(|f| serde_json::json!({
                        "name": f.name,
                        "type": f.type_desc,
                        "avg_bytes": f.avg_bytes(),
                        "min_bytes": f.min_bytes,
                        "max_bytes": f.max_bytes,
                        "pct_of_row": if total_avg > 0.0 { f.avg_bytes() / total_avg * 100.0 } else { 0.0 },
                    })).collect::<Vec<_>>(),
                })
            }),
            "bottleneck": format!("{:?}", report.identify_bottleneck()),
            "recommendations": report.scaling_recommendations(),
            "samples": report.samples.iter().map(|s| {
                serde_json::json!({
                    "elapsed_secs": s.elapsed_secs,
                    "cpu_percent": s.cpu_percent,
                    "memory_used_gb": s.memory_used as f64 / (1024.0 * 1024.0 * 1024.0),
                    "disk_read_mb_sec": s.disk_read_bytes_sec as f64 / (1024.0 * 1024.0),
                    "disk_write_mb_sec": s.disk_write_bytes_sec as f64 / (1024.0 * 1024.0),
                    "rows_processed": s.rows_processed,
                    "partitions_completed": s.partitions_completed,
                })
            }).collect::<Vec<_>>(),
        });

        if let Err(e) = std::fs::write(
            &report_path,
            serde_json::to_string_pretty(&report_json).unwrap(),
        ) {
            eprintln!(
                "{} Failed to write benchmark report: {}",
                "Warning:".yellow(),
                e
            );
        } else {
            println!();
            println!(
                "  {} {}",
                "Benchmark report:".cyan(),
                report_path.bright_white()
            );
        }
    }

    Ok(())
}
