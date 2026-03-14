//! ClickHouse export command.

use crate::cli::ExportClickhouseArgs;
use crate::commands::utils::{parse_export_filters, parse_export_intervals, progress_style_spinner};
use genohype_core::export::clickhouse::generate_create_table;
use genohype_core::export::ClickHouseClient;
use genohype_core::parquet::{build_record_batch, ParquetWriter};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use uuid::Uuid;

/// Export a Hail table to ClickHouse with optional filtering
pub fn run_export_clickhouse(args: ExportClickhouseArgs) -> Result<()> {
    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

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
    if !where_filters.is_empty() {
        println!(
            "  {} {:?}",
            "Filters:".cyan(),
            where_filters
                .iter()
                .map(|r| r.field_path_str())
                .collect::<Vec<_>>()
        );
    }
    if let Some(ref ivl) = intervals {
        println!(
            "  {} {} intervals",
            "Interval filter:".cyan(),
            ivl.len().to_string().bright_white()
        );
    }
    if let Some(l) = args.common.limit {
        println!(
            "  {} {}",
            "Row limit:".cyan(),
            l.to_string().bright_white()
        );
    }
    println!();

    // Step 1: Open the query engine
    println!("{}", "Reading table metadata...".dimmed());
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!(
        "  {} {}",
        "Partitions:".cyan(),
        engine.num_partitions().to_string().bright_white()
    );
    println!("  {} {:?}", "Key fields:".cyan(), engine.key_fields());
    println!();

    // Step 2: Create ClickHouse client and generate DDL
    let client = ClickHouseClient::new(&args.url);

    println!("{}", "Generating CREATE TABLE DDL...".dimmed());
    let ddl = generate_create_table(&args.table, &row_type, engine.key_fields())
        .map_err(|e| genohype_core::HailError::InvalidFormat(e.to_string()))?;
    println!("{}", ddl.dimmed());
    println!();

    // Step 3: Execute CREATE TABLE
    println!("{}", "Creating table in ClickHouse...".dimmed());
    client.execute(&ddl).map_err(|e| {
        genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;
    println!("  {}", "Table created (or already exists)".green());
    println!();

    // Step 4: Convert filtered rows to temporary Parquet file
    let temp_path = format!("/tmp/hail_export_{}.parquet", Uuid::new_v4());
    println!("{}", "Converting to temporary Parquet file...".dimmed());
    println!("  {} {}", "Temp file:".cyan(), temp_path.dimmed());

    // Create writer and get schema
    let mut writer = ParquetWriter::new(&temp_path, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Collect rows in batches for efficient parquet writing
    let batch_size = 10000;
    let mut batch_rows = Vec::with_capacity(batch_size);
    let mut total_rows = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        total_rows += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", total_rows));
            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write remaining rows
    if !batch_rows.is_empty() {
        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
        writer.write_batch(&batch)?;
    }

    pb.finish_and_clear();
    writer.close()?;
    println!(
        "  {} {}",
        "Converted".green(),
        format!("{} rows", total_rows).bright_white()
    );
    println!();

    // Step 5: Insert Parquet data into ClickHouse
    println!("{}", "Inserting data into ClickHouse...".dimmed());
    client.insert_parquet(&args.table, &temp_path).map_err(|e| {
        genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;

    // Step 6: Clean up temp file
    if let Err(e) = std::fs::remove_file(&temp_path) {
        eprintln!(
            "{} Failed to remove temp file {}: {}",
            "Warning:".yellow(),
            temp_path,
            e
        );
    }

    // Step 7: Verify
    let row_count = client.count_rows(&args.table).map_err(|e| {
        genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!(
        "  {} {}",
        "Rows in ClickHouse table:".cyan(),
        row_count.to_string().bright_white()
    );

    Ok(())
}
