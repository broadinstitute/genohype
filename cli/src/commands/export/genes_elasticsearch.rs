//! Genes-index Elasticsearch loader (Phase 4 — closes the genes-table gap).
//!
//! Loads the genes lookup index the Phase-1b ES backend queries for gene-view
//! requests. See [`genohype_core::export::elasticsearch`] for the index shape.

use crate::cli::ExportGenesElasticsearchArgs;
use crate::commands::utils::{parse_export_filters, parse_export_intervals, progress_style_spinner};
use genohype_core::export::elasticsearch::{
    build_gene_document, build_genes_request_body, BulkInserter, ElasticsearchClient,
};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;

fn es_err(e: impl std::fmt::Display) -> genohype_core::HailError {
    genohype_core::HailError::Io(std::io::Error::other(e.to_string()))
}

pub fn run_export_genes_elasticsearch(args: ExportGenesElasticsearchArgs) -> Result<()> {
    println!(
        "{} {}",
        "Loading genes index into Elasticsearch:".green().bold(),
        args.common.input.bright_white()
    );
    println!("  {} {}", "ES URL:".cyan(), args.url.bright_white());
    println!("  {} {}", "Index:".cyan(), args.index.bright_white());

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    let engine = QueryEngine::open_path(&args.common.input)?;

    let client = ElasticsearchClient::new(&args.url);
    let body = build_genes_request_body(args.shards);
    let created = client
        .create_index(&args.index, &body, args.recreate)
        .map_err(es_err)?;
    if created {
        println!("  {}", "Index created".green());
    } else {
        println!("  {}", "Index already exists (reusing)".yellow());
    }

    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // The genes index uses `gene_id` as the document `_id` (stable → idempotent).
    let mut inserter = BulkInserter::new(&client, &args.index, Some("gene_id".to_string()), args.batch_size);
    let mut skipped = 0usize;

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        match build_gene_document(&row) {
            Some(doc) => inserter.add(&doc).map_err(es_err)?,
            None => skipped += 1,
        }
        if inserter.total_docs > 0 && inserter.total_docs.is_multiple_of(5_000) {
            pb.set_message(format!("{} genes indexed...", inserter.total_docs));
        }
    }
    inserter.finish().map_err(es_err)?;
    pb.finish_and_clear();

    let total = inserter.total_docs;
    let count = client.count(&args.index).map_err(es_err)?;

    println!();
    println!("{}", "Genes index complete!".green().bold());
    println!(
        "  {} {} genes indexed ({} rows skipped, no gene_id)",
        "Indexed:".cyan(),
        total.to_string().bright_white(),
        skipped,
    );
    println!(
        "  {} {}",
        "Docs in index:".cyan(),
        count.to_string().bright_white()
    );

    Ok(())
}
