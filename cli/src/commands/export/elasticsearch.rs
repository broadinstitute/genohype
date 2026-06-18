//! Elasticsearch export command.
//!
//! Streams a Hail table into a prod-shaped Elasticsearch variant index via the
//! `_bulk` API. See [`genohype_core::export::elasticsearch`] for the document
//! shape / mapping fidelity rationale.

use crate::cli::ExportElasticsearchArgs;
use crate::commands::utils::{
    parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use genohype_core::export::elasticsearch::{
    build_document, build_request_body, parse_index_fields, BulkInserter, ElasticsearchClient,
};
use genohype_core::projection::{Projection, SchemaWidth};
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use std::sync::Arc;

/// Convert an Elasticsearch export error into a genohype error.
fn es_err(e: impl std::fmt::Display) -> genohype_core::HailError {
    genohype_core::HailError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    ))
}

/// Export a Hail table to Elasticsearch with optional filtering / projection.
pub fn run_export_elasticsearch(args: ExportElasticsearchArgs) -> Result<()> {
    println!(
        "{} {}",
        "Exporting to Elasticsearch:".green().bold(),
        args.common.input.bright_white()
    );
    println!("  {} {}", "ES URL:".cyan(), args.url.bright_white());
    println!("  {} {}", "Index:".cyan(), args.index.bright_white());
    println!(
        "  {} {}",
        "Shards:".cyan(),
        args.shards.to_string().bright_white()
    );

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    // Open the query engine and capture the full source row type. The index
    // mapping is always built from the FULL schema (matching prod, which maps
    // `table.row_value.dtype`); the width dimension narrows the documents only.
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();

    // Resolve the schema-width projection (defaults to full = no projection).
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
                    // Strict allowlist intersected with the actual schema (tolerant
                    // of exomes-vs-genomes field differences), same as `query`.
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

    let index_fields = parse_index_fields(&args.index_fields);
    println!(
        "  {} {}",
        "Index fields:".cyan(),
        index_fields
            .iter()
            .map(|f| f.key.as_str())
            .collect::<Vec<_>>()
            .join(", ")
            .bright_white()
    );
    println!();

    let client = ElasticsearchClient::new(&args.url);

    // Create the index (recreate if requested) with the prod-shaped mapping.
    let body = build_request_body(&row_type, &index_fields, args.shards);
    let created = client
        .create_index(&args.index, &body, args.recreate)
        .map_err(es_err)?;
    if created {
        println!("  {}", "Index created".green());
    } else {
        println!(
            "  {}",
            "Index already exists (reusing; re-load is idempotent via _id)".yellow()
        );
    }

    // Build the decode-time projection (Level 2) for browser-minimal so dropped
    // fields are never decoded. Ensure index-field roots (and locus, for interval
    // filtering) survive the projection.
    let decode_projection = match &projection {
        Some(Projection::Fields(tree)) => {
            let mut decode_tree = tree.clone();
            if intervals.is_some() {
                decode_tree.ensure_field("locus");
            }
            for field in &index_fields {
                if let Some(root) = field.path.first() {
                    decode_tree.ensure_field(root);
                }
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

    let id_field = if args.id_field.trim().is_empty() {
        None
    } else {
        Some(args.id_field.clone())
    };
    let mut inserter = BulkInserter::new(&client, &args.index, id_field, args.batch_size);

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        let doc = build_document(&row, &index_fields);
        inserter.add(&doc).map_err(es_err)?;
        if inserter.total_docs % 10_000 == 0 && inserter.total_docs > 0 {
            pb.set_message(format!("{} docs indexed...", inserter.total_docs));
        }
    }
    inserter.finish().map_err(es_err)?;
    pb.finish_and_clear();

    let total = inserter.total_docs;
    let insert_ms = inserter.insert_time_ms;

    if args.forcemerge {
        println!("  {}", "Force-merging index...".dimmed());
        client.forcemerge(&args.index).map_err(es_err)?;
    }

    // Verify final count (refreshes the index first).
    let count = client.count(&args.index).map_err(es_err)?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!(
        "  {} {} docs sent in {} bulk requests ({} ms in _bulk)",
        "Indexed:".cyan(),
        total.to_string().bright_white(),
        inserter.flush_count,
        insert_ms,
    );
    println!(
        "  {} {}",
        "Docs in index:".cyan(),
        count.to_string().bright_white()
    );

    Ok(())
}
