//! Postgres export command (Phase 2b).
//!
//! Streams a Hail table into the partitioned JSONB wide table used by the
//! gnomad-bench `postgres` / `tiered-*` arms via Postgres `COPY`. See
//! [`genohype_core::export::postgres`] for the schema and the COPY-into-staging +
//! upsert pattern that keeps re-loads idempotent.

use crate::cli::ExportPostgresArgs;
use crate::commands::utils::{
    parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use genohype_core::export::postgres::{CopyInserter, PostgresClient};
use genohype_core::projection::{Projection, SchemaWidth};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use std::sync::Arc;

/// Convert a Postgres export error into a genohype error.
fn pg_err(e: impl std::fmt::Display) -> genohype_core::HailError {
    genohype_core::HailError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    ))
}

/// Export a Hail table to Postgres with optional filtering / projection.
pub fn run_export_postgres(args: ExportPostgresArgs) -> Result<()> {
    println!(
        "{} {}",
        "Exporting to Postgres:".green().bold(),
        args.common.input.bright_white()
    );
    println!("  {} {}", "Table:".cyan(), args.table.bright_white());

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();

    // Resolve the schema-width projection (defaults to full = no projection).
    // Only the `data` JSONB payload is narrowed; the hoisted columns are always
    // extracted from the (projected) row.
    let projection: Option<Projection> = match args.width.as_deref() {
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
    println!(
        "  {} {}",
        "Schema width:".cyan(),
        args.width.as_deref().unwrap_or("full").bright_white()
    );
    println!();

    // Connect and prepare the partitioned wide table.
    let mut client = PostgresClient::connect(&args.url).map_err(pg_err)?;
    if args.recreate {
        client.drop_table(&args.table).map_err(pg_err)?;
        println!("  {}", "Dropped existing table (--recreate)".yellow());
    }
    client.create_table(&args.table).map_err(pg_err)?;
    println!("  {}", "Table ready (partitioned JSONB wide table)".green());

    // Build the decode-time projection (Level 2) for browser-minimal so dropped
    // fields are never decoded. The key columns (`locus`, `variant_id`,
    // `alleles`) must survive projection so they can be hoisted, plus `locus` for
    // interval filtering.
    let decode_projection = match &projection {
        Some(Projection::Fields(tree)) => {
            let mut decode_tree = tree.clone();
            decode_tree.ensure_field("locus");
            decode_tree.ensure_field("variant_id");
            decode_tree.ensure_field("alleles");
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

    let mut inserter = CopyInserter::new(&mut client, &args.table, args.batch_size).map_err(pg_err)?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        inserter.add(&row).map_err(pg_err)?;
        if inserter.total_rows > 0 && inserter.total_rows % 50_000 == 0 {
            pb.set_message(format!("{} rows loaded...", inserter.total_rows));
        }
    }
    inserter.finish().map_err(pg_err)?;
    pb.finish_and_clear();

    let total = inserter.total_rows;
    let insert_ms = inserter.insert_time_ms;
    let flush_count = inserter.flush_count;

    // Build the secondary indexes after the bulk load (faster than maintaining
    // them during ingest).
    if !args.no_indexes {
        println!("  {}", "Building indexes (contig,pos) + variant_id...".dimmed());
        client.create_indexes(&args.table).map_err(pg_err)?;
    }

    // Verify the final row count reconciles.
    let count = client.count_rows(&args.table).map_err(pg_err)?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!(
        "  {} {} rows upserted in {} COPY batches ({} ms in COPY+upsert)",
        "Loaded:".cyan(),
        total.to_string().bright_white(),
        flush_count,
        insert_ms,
    );
    println!(
        "  {} {}",
        "Rows in table:".cyan(),
        count.to_string().bright_white()
    );

    Ok(())
}
