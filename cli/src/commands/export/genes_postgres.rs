//! Genes-table Postgres loader (Phase 4 — closes the genes-table gap).
//!
//! Loads the `genes` lookup table the Phase-1a Postgres backend queries for
//! gene-view requests. See [`genohype_core::export::postgres`] for the schema.

use crate::cli::ExportGenesPostgresArgs;
use crate::commands::utils::{parse_export_filters, parse_export_intervals, progress_style_spinner};
use genohype_core::export::postgres::{GenesCopyInserter, PostgresClient};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;

fn pg_err(e: impl std::fmt::Display) -> genohype_core::HailError {
    genohype_core::HailError::Io(std::io::Error::other(e.to_string()))
}

pub fn run_export_genes_postgres(args: ExportGenesPostgresArgs) -> Result<()> {
    println!(
        "{} {}",
        "Loading genes table into Postgres:".green().bold(),
        args.common.input.bright_white()
    );
    println!("  {} {}", "Table:".cyan(), args.table.bright_white());

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    let engine = QueryEngine::open_path(&args.common.input)?;

    let mut client = PostgresClient::connect(&args.url).map_err(pg_err)?;
    if args.recreate {
        client.drop_table(&args.table).map_err(pg_err)?;
        println!("  {}", "Dropped existing table (--recreate)".yellow());
    }
    client.create_genes_table(&args.table).map_err(pg_err)?;
    println!("  {}", "Genes table ready".green());

    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    let mut inserter = GenesCopyInserter::new(&mut client, &args.table, args.batch_size).map_err(pg_err)?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        inserter.add(&row).map_err(pg_err)?;
        if inserter.total_rows > 0 && inserter.total_rows.is_multiple_of(5_000) {
            pb.set_message(format!("{} genes loaded...", inserter.total_rows));
        }
    }
    inserter.finish().map_err(pg_err)?;
    pb.finish_and_clear();

    let total = inserter.total_rows;
    let skipped = inserter.skipped_rows;

    if !args.no_indexes {
        println!("  {}", "Building upper(gencode_symbol) index...".dimmed());
        client.create_genes_indexes(&args.table).map_err(pg_err)?;
    }

    let count = client.count_rows(&args.table).map_err(pg_err)?;

    println!();
    println!("{}", "Genes load complete!".green().bold());
    println!(
        "  {} {} genes loaded ({} rows skipped, no gene_id)",
        "Loaded:".cyan(),
        total.to_string().bright_white(),
        skipped,
    );
    println!(
        "  {} {}",
        "Rows in table:".cyan(),
        count.to_string().bright_white()
    );

    Ok(())
}
