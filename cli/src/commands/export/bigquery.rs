//! BigQuery export command.

use crate::cli::ExportBigqueryArgs;
use crate::commands::utils::{
    parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use genohype_core::export::BigQueryClient;
use genohype_core::parquet::{build_record_batch, ParquetWriter};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use uuid::Uuid;

/// Export a Hail table to BigQuery via GCS staging
pub fn run_export_bigquery(args: ExportBigqueryArgs) -> Result<()> {
    // Parse destination: project:dataset.table
    let (project, dataset_table) = args.destination.split_once(':').ok_or_else(|| {
        genohype_core::HailError::InvalidFormat(
            "Destination format must be project:dataset.table".to_string(),
        )
    })?;
    let (dataset, table) = dataset_table.split_once('.').ok_or_else(|| {
        genohype_core::HailError::InvalidFormat(
            "Destination format must be project:dataset.table".to_string(),
        )
    })?;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!(
        "{} {} {} {}:{}.{}",
        "Exporting".green().bold(),
        args.common.input.bright_white(),
        "to BigQuery".green(),
        project,
        dataset,
        table
    );
    println!(
        "  {} {}",
        "Staging bucket:".cyan(),
        args.bucket.bright_white()
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
        println!("  {} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // 1. Get Schema/Metadata
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

    // 2. Convert filtered rows to Parquet locally
    let temp_file_path =
        std::path::Path::new(&args.temp_dir).join(format!("{}.parquet", Uuid::new_v4()));
    println!(
        "{} {}",
        "Converting to temporary Parquet file:".dimmed(),
        temp_file_path.display()
    );

    // Create writer and get schema
    let mut writer = ParquetWriter::new(temp_file_path.to_string_lossy().as_ref(), &row_type)?;
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
    let mut rows_written = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        rows_written += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", rows_written));
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
        format!("{} rows", rows_written).bright_white()
    );
    println!();

    // 3. Upload and Load (Async)
    println!("{}", "Starting BigQuery export...".dimmed());
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let client = BigQueryClient::new(project, &args.bucket)
            .await
            .map_err(|e| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        println!("{}", "Uploading to GCS...".dimmed());
        let gcs_uri = client.upload_parquet(&temp_file_path).await.map_err(|e| {
            genohype_core::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        println!("  {} {}", "Uploaded to:".green(), gcs_uri.bright_white());

        println!("{}", "Triggering BigQuery Load Job...".dimmed());
        client
            .load_parquet(dataset, table, &gcs_uri, &row_type)
            .await
            .map_err(|e| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
        println!("  {}", "Load Job completed successfully.".green());

        println!("{}", "Cleaning up GCS staging object...".dimmed());
        let _ = client.delete_object(&gcs_uri).await;

        Ok::<(), genohype_core::HailError>(())
    });

    // 4. Cleanup Local temp file
    if std::fs::remove_file(&temp_file_path).is_err() {
        eprintln!("{} Failed to remove local temp file", "Warning:".yellow());
    }

    // Propagate any errors from the async block
    result?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!(
        "  {} {}",
        "Rows exported:".cyan(),
        rows_written.to_string().bright_white()
    );
    println!(
        "  {} {}:{}.{}",
        "BigQuery table:".cyan(),
        project,
        dataset,
        table
    );

    Ok(())
}
