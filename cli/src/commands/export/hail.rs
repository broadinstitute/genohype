//! Hail table export command.

use crate::cli::ExportHailArgs;
use crate::commands::utils::{parse_export_filters, parse_export_intervals, progress_style_spinner};
use genohype_core::export::hail::HailTableWriter;
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;

pub fn run_export_hail(args: ExportHailArgs) -> Result<()> {
    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!(
        "{} {} {} {}",
        "Converting".green(),
        args.common.input.bright_white(),
        "to Hail Table".green(),
        args.output.bright_white()
    );

    // Open the query engine
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    let key_fields = engine.key_fields().to_vec();
    let rvd_spec = engine.rvd_spec().cloned();

    println!(
        "{} {}",
        "Partitions:".cyan(),
        engine.num_partitions().to_string().bright_white()
    );
    println!("{} {:?}", "Key fields:".cyan(), key_fields);
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
        println!(
            "{} {}",
            "Row limit:".cyan(),
            l.to_string().bright_white()
        );
    }
    println!();

    // Create Hail table writer
    let mut writer =
        HailTableWriter::new(&args.output, &row_type, &key_fields, rvd_spec.as_ref())?;

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Progress indicator
    let mut total_rows = 0;
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    // Collect rows for writing (simple MVP: single partition)
    let mut rows = Vec::new();
    for row_result in iterator {
        let row = row_result?;
        rows.push(row);
        total_rows += 1;
        if total_rows % 10000 == 0 {
            pb.set_message(format!("{} rows collected...", total_rows));
        }
    }

    pb.set_message("Writing partition...");
    writer.write_partition(0, rows.into_iter())?;
    writer.finish()?;

    pb.finish_and_clear();

    println!();
    println!("{}", "Export complete!".green().bold());
    println!(
        "  {} {}",
        "Rows written:".cyan(),
        total_rows.to_string().bright_white()
    );
    println!(
        "  {} {}",
        "Output directory:".cyan(),
        args.output.bright_white()
    );

    Ok(())
}
