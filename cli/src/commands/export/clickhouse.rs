//! ClickHouse export command.

use crate::cli::ExportClickhouseArgs;
use crate::commands::utils::{parse_export_filters, parse_export_intervals, progress_style_spinner};
use genohype_core::codec::{EncodedField, EncodedType, EncodedValue};
use genohype_core::export::clickhouse::generate_create_table;
use genohype_core::export::ClickHouseClient;
use genohype_core::parquet::{build_record_batch, InMemoryParquetWriter};
use genohype_core::projection::{Projection, SchemaWidth};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use std::path::Path;
use std::sync::Arc;

/// Resolve the `--width` flag into an optional projection against `row_type`
/// (mirrors the postgres/elasticsearch exports). `None`/`full` => no projection.
fn resolve_width_projection(
    width: Option<&str>,
    row_type: &genohype_core::codec::EncodedType,
) -> Option<Projection> {
    match width {
        None | Some("full") => None,
        Some(other) => {
            let width = SchemaWidth::parse(other).unwrap_or_else(|e| {
                eprintln!("{} invalid --width: {}", "Error:".red().bold(), e);
                std::process::exit(1);
            });
            match width {
                SchemaWidth::Full => None,
                SchemaWidth::BrowserMinimal => {
                    let proj = Projection::browser_minimal_present_in(row_type);
                    proj.validate(row_type).unwrap_or_else(|e| {
                        eprintln!("{} {}", "Error:".red().bold(), e);
                        std::process::exit(1);
                    });
                    Some(proj)
                }
            }
        }
    }
}

/// Check if a path is a cloud URL (gs://, s3://, http://, https://)
fn is_cloud_path(path: &str) -> bool {
    path.starts_with("gs://") || path.starts_with("s3://") || path.starts_with("http")
}

/// List files in a cloud directory matching a glob pattern.
/// Uses object_store to list objects and filters by the glob pattern.
fn list_cloud_files(dir_url: &str, pattern: &str) -> Result<Vec<String>> {
    use futures::StreamExt;
    use genohype_core::io::resolve_url;

    let (store, prefix) = resolve_url(dir_url)?;

    // Build a simple glob matcher from the pattern (supports * and ?)
    let glob_matcher = glob::Pattern::new(pattern).map_err(|e| {
        genohype_core::HailError::InvalidFormat(format!("Invalid glob pattern: {}", e))
    })?;

    // Reconstruct base URL for building full file paths
    let parsed_url = url::Url::parse(dir_url).map_err(|e| {
        genohype_core::HailError::InvalidFormat(format!("Invalid URL: {}", e))
    })?;
    let scheme = parsed_url.scheme().to_string();
    let host = parsed_url.host_str().unwrap_or("").to_string();
    let base_path = parsed_url.path().trim_end_matches('/').to_string();

    // List objects using tokio runtime (blocking from sync context)
    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        genohype_core::HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;

    let mut files: Vec<String> = rt.block_on(async {
        let mut stream = store.list(Some(&prefix));
        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let filename = meta.location.filename().unwrap_or_default().to_string();
                if glob_matcher.matches(&filename)
                    && !filename.ends_with(".tbi")
                    && !filename.ends_with(".csi")
                {
                    let full_url = format!("{}://{}/{}/{}", scheme, host, base_path.trim_start_matches('/'), filename);
                    results.push(full_url);
                }
            }
        }
        results
    });

    files.sort();

    if files.is_empty() {
        return Err(genohype_core::HailError::InvalidFormat(format!(
            "No files matched glob pattern '{}' in '{}'",
            pattern, dir_url
        )));
    }

    Ok(files)
}

/// Resolve input files: if --glob is provided, treat input as a directory and match files.
/// Supports both local filesystem globs and cloud storage (gs://, s3://) listing.
/// Otherwise, return a single-element vec with the input path.
fn resolve_input_files(input: &str, glob_pattern: Option<&str>) -> Result<Vec<String>> {
    match glob_pattern {
        Some(pattern) => {
            if is_cloud_path(input) {
                list_cloud_files(input, pattern)
            } else {
                // Local filesystem glob
                let dir = Path::new(input);
                let full_pattern = dir.join(pattern);
                let pattern_str = full_pattern.to_string_lossy();

                let mut files: Vec<String> = glob::glob(&pattern_str)
                    .map_err(|e| {
                        genohype_core::HailError::InvalidFormat(format!(
                            "Invalid glob pattern: {}",
                            e
                        ))
                    })?
                    .filter_map(|entry| entry.ok())
                    .filter(|p| !p.to_string_lossy().ends_with(".tbi"))
                    .map(|p| p.to_string_lossy().into_owned())
                    .collect();

                files.sort();

                if files.is_empty() {
                    return Err(genohype_core::HailError::InvalidFormat(format!(
                        "No files matched glob pattern '{}' in '{}'",
                        pattern, input
                    )));
                }

                Ok(files)
            }
        }
        None => Ok(vec![input.to_string()]),
    }
}

/// Extract the filename stem (first component before '.') from a path.
/// e.g., "HG002.model.pbmm2.combined.bed.gz" -> "HG002"
fn extract_filename_stem(path: &str) -> String {
    let filename = Path::new(path)
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    // Take the first component before '.' to get the sample ID
    filename.split('.').next().unwrap_or(&filename).to_string()
}

/// Add a filename column to the schema
fn augment_schema(schema: &EncodedType, column_name: &str) -> EncodedType {
    match schema {
        EncodedType::EBaseStruct {
            required, fields, ..
        } => {
            let mut new_fields = fields.clone();
            new_fields.push(EncodedField {
                name: column_name.to_string(),
                encoded_type: EncodedType::EBinary { required: true },
                index: new_fields.len(),
            });
            EncodedType::EBaseStruct {
                required: *required,
                fields: new_fields,
            }
        }
        other => other.clone(),
    }
}

/// Add a filename column value to each row
fn augment_row(row: EncodedValue, column_name: &str, value: &str) -> EncodedValue {
    match row {
        EncodedValue::Struct(mut fields) => {
            fields.push((
                column_name.to_string(),
                EncodedValue::Binary(value.as_bytes().to_vec()),
            ));
            EncodedValue::Struct(fields)
        }
        other => other,
    }
}

/// Export a single file to ClickHouse, returning the number of rows written
fn export_single_file(
    file_path: &str,
    args: &ExportClickhouseArgs,
    client: &ClickHouseClient,
    table_created: bool,
    filename_column: Option<&str>,
) -> Result<u64> {
    let where_filters = parse_export_filters(args);
    let intervals = parse_export_intervals(args)?;

    println!(
        "  {} {}",
        "Processing:".cyan(),
        file_path.bright_white()
    );

    // Open the query engine for this file
    let engine = QueryEngine::open_path(file_path)?;
    let row_type = engine.row_type().clone();

    // Resolve the schema-width projection (defaults to full = no projection). For
    // browser-minimal the stored ClickHouse schema is narrowed to match the
    // projected rows, so the disk/$ footprint is apples-to-apples with the other
    // browser-minimal arms.
    let projection = resolve_width_projection(args.width.as_deref(), &row_type);
    let projected_row_type = match &projection {
        Some(proj) => proj.project_type(&row_type),
        None => row_type.clone(),
    };
    if projection.is_some() {
        println!(
            "  {} {}",
            "Schema width:".cyan(),
            args.width.as_deref().unwrap_or("full").bright_white()
        );
    }

    // Augment schema if filename column is requested (after projection so the
    // injected column survives the browser-minimal allowlist).
    let effective_schema = match filename_column {
        Some(col) => augment_schema(&projected_row_type, col),
        None => projected_row_type.clone(),
    };

    // Create table on first file only
    if !table_created {
        println!("{}", "  Generating CREATE TABLE DDL...".dimmed());
        let ddl = generate_create_table(&args.table, &effective_schema, engine.key_fields())
            .map_err(|e| genohype_core::HailError::InvalidFormat(e.to_string()))?;
        println!("{}", ddl.dimmed());

        client.execute(&ddl).map_err(|e| {
            genohype_core::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        println!("  {}", "Table created (or already exists)".green());
    }

    // Stream filtered rows to ClickHouse in chunked, in-memory Parquet POSTs.
    // This avoids buffering an entire file's worth of rows into one INSERT (OOM).
    let chunk_size: usize = std::env::var("HAIL_DECODER_CLICKHOUSE_CHUNK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25_000);
    const PARQUET_BATCH_SIZE: usize = 4096;

    let arrow_schema = std::sync::Arc::new(genohype_core::parquet::schema::create_schema(
        &effective_schema,
    )?);

    // Build the decode-time projection (Level 2) for browser-minimal so dropped
    // fields are never decoded. Keep the ORDER BY / key columns (e.g. locus,
    // alleles) so they survive projection.
    let decode_projection = match &projection {
        Some(Projection::Fields(tree)) => {
            let mut decode_tree = tree.clone();
            for key in engine.key_fields() {
                decode_tree.ensure_field(key);
            }
            Some(Arc::new(decode_tree))
        }
        _ => None,
    };

    let iterator =
        engine.query_iter_with_projection(&where_filters, intervals, decode_projection)?;

    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    let filename_stem = extract_filename_stem(file_path);

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    // Flush a chunk of rows as a single in-memory Parquet POST to ClickHouse.
    let flush_chunk = |rows: &[EncodedValue]| -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut writer = InMemoryParquetWriter::new(&effective_schema)?;
        for start in (0..rows.len()).step_by(PARQUET_BATCH_SIZE) {
            let end = (start + PARQUET_BATCH_SIZE).min(rows.len());
            let batch = build_record_batch(&rows[start..end], &effective_schema, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
        }
        let bytes = writer.finish()?;
        client
            .insert_parquet_bytes(&args.table, bytes)
            .map_err(|e| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })
    };

    let mut chunk_rows: Vec<EncodedValue> = Vec::with_capacity(chunk_size);
    let mut total_rows: u64 = 0;

    for row_result in iterator {
        let row = row_result?;
        // Apply the output projection so the row's top-level fields exactly match
        // `projected_row_type` (drops any key field the decode projection kept but
        // the browser-minimal allowlist excludes), preserving the positional
        // row→schema alignment `build_record_batch` relies on.
        let row = match &projection {
            Some(proj) => proj.apply(&row),
            None => row,
        };
        let row = match filename_column {
            Some(col) => augment_row(row, col, &filename_stem),
            None => row,
        };
        chunk_rows.push(row);
        total_rows += 1;

        if chunk_rows.len() >= chunk_size {
            pb.set_message(format!("{} rows processed...", total_rows));
            flush_chunk(&chunk_rows)?;
            chunk_rows.clear();
        }
    }

    // Final trailing flush
    flush_chunk(&chunk_rows)?;

    pb.finish_and_clear();

    println!(
        "  {} {}",
        "Wrote".green(),
        format!("{} rows", total_rows).bright_white()
    );

    Ok(total_rows)
}

/// Export a Hail table to ClickHouse with optional filtering
pub fn run_export_clickhouse(args: ExportClickhouseArgs) -> Result<()> {
    println!(
        "{} {}",
        "Exporting to ClickHouse:".green().bold(),
        args.common.input.bright_white()
    );
    println!(
        "  {} {}",
        "ClickHouse URL:".cyan(),
        args.url.bright_white()
    );
    println!(
        "  {} {}",
        "Target table:".cyan(),
        args.table.bright_white()
    );
    if let Some(ref glob) = args.glob {
        println!("  {} {}", "Glob pattern:".cyan(), glob.bright_white());
    }
    if let Some(ref col) = args.filename_column {
        println!(
            "  {} {}",
            "Filename column:".cyan(),
            col.bright_white()
        );
    }
    println!();

    // Resolve input files
    let input_files = resolve_input_files(&args.common.input, args.glob.as_deref())?;
    let num_files = input_files.len();

    if num_files > 1 {
        println!(
            "{} {} files to process",
            "Multi-file mode:".green().bold(),
            num_files.to_string().bright_white()
        );
        println!();
    }

    let client = ClickHouseClient::new(&args.url);
    let mut grand_total: u64 = 0;

    for (i, file_path) in input_files.iter().enumerate() {
        if num_files > 1 {
            println!(
                "\n{} [{}/{}]",
                "File".cyan().bold(),
                (i + 1).to_string().bright_white(),
                num_files.to_string().bright_white()
            );
        }

        let rows = export_single_file(
            file_path,
            &args,
            &client,
            i > 0, // table_created = true after first file
            args.filename_column.as_deref(),
        )?;
        grand_total += rows;
    }

    // Verify final count
    let row_count = client.count_rows(&args.table).map_err(|e| {
        genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!(
        "  {} {} (from {} files, {} rows written this session)",
        "Rows in ClickHouse table:".cyan(),
        row_count.to_string().bright_white(),
        num_files,
        grand_total,
    );

    Ok(())
}
