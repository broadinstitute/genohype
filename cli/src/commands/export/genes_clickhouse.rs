//! Genes-table ClickHouse loader (closes the genes-table gap for the CH arm).
//!
//! Loads the flat `genes` lookup table the ClickHouse backend queries for
//! gene-view requests (gnomad-browser-lite `backend/src/backend/clickhouse.rs`:
//! `SELECT gene_id, gencode_symbol, chrom, start, stop, strand,
//! canonical_transcript_id, transcripts_json FROM genes ...`). Mirrors the
//! `genes-postgres` loader — same hoisted columns — but the api-shaped
//! `transcripts` land in a `transcripts_json String` column (the backend
//! deserializes it into `Vec<api::Transcript>`). The big nested `variants`
//! table is loaded separately by `export clickhouse`.

use crate::cli::ExportGenesClickhouseArgs;
use crate::commands::utils::{
    gene_row_in_intervals, parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use genohype_core::export::cache_builder::extract_gene;
use genohype_core::export::ClickHouseClient;
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;

fn ch_err(e: impl std::fmt::Display) -> genohype_core::HailError {
    genohype_core::HailError::Io(std::io::Error::other(e.to_string()))
}

/// Flat `genes` DDL matching the backend's column projection. `transcripts_json`
/// holds the api-shaped `transcripts` array as a JSON string. ~20k rows, so a
/// plain MergeTree ordered by `gene_id` is plenty.
fn create_genes_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS `{table}` (\n  \
         `gene_id` String,\n  \
         `gencode_symbol` Nullable(String),\n  \
         `chrom` String,\n  \
         `start` Int64,\n  \
         `stop` Int64,\n  \
         `strand` Nullable(String),\n  \
         `canonical_transcript_id` Nullable(String),\n  \
         `transcripts_json` Nullable(String)\n\
         ) ENGINE = MergeTree() ORDER BY `gene_id`"
    )
}

pub fn run_export_genes_clickhouse(args: ExportGenesClickhouseArgs) -> Result<()> {
    println!(
        "{} {}",
        "Loading genes table into ClickHouse:".green().bold(),
        args.common.input.bright_white()
    );
    println!("  {} {}", "Table:".cyan(), args.table.bright_white());

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;
    // The genes HT is keyed by gene_id, so the Hail interval scan can't slice by
    // position — keep the parsed intervals to filter overlapping genes post-decode.
    let locus_filter = intervals.clone();

    let engine = QueryEngine::open_path(&args.common.input)?;

    let client = ClickHouseClient::new(&args.url);
    if args.recreate {
        client
            .execute(&format!("DROP TABLE IF EXISTS `{}`", args.table))
            .map_err(ch_err)?;
        println!("  {}", "Dropped existing table (--recreate)".yellow());
    }
    client.execute(&create_genes_ddl(&args.table)).map_err(ch_err)?;
    println!("  {}", "Genes table ready".green());

    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    // Buffer flat genes as JSONEachRow and POST in batches.
    let mut ndjson = String::new();
    let mut batch_count = 0usize;
    let mut total: usize = 0;
    let mut skipped: usize = 0;
    let mut filtered_out: usize = 0;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    let mut flush = |ndjson: &mut String, batch_count: &mut usize| -> Result<()> {
        if *batch_count == 0 {
            return Ok(());
        }
        client
            .insert_json_each_row(&args.table, ndjson)
            .map_err(ch_err)?;
        ndjson.clear();
        *batch_count = 0;
        Ok(())
    };

    for row_result in iterator {
        let row = row_result?;
        if let Some(iv) = &locus_filter {
            if !gene_row_in_intervals(&row, iv) {
                filtered_out += 1;
                continue;
            }
        }
        let Some(gene) = extract_gene(&row) else {
            skipped += 1;
            continue;
        };
        // Idempotent: the genes HT can carry duplicate gene_ids; keep the first.
        if !seen.insert(gene.gene_id.clone()) {
            continue;
        }

        let transcripts_json = gene
            .transcripts
            .as_ref()
            .map(|t| serde_json::to_string(t).unwrap_or_else(|_| "null".to_string()));

        let obj = serde_json::json!({
            "gene_id": gene.gene_id,
            "gencode_symbol": gene.gencode_symbol.clone().or(gene.gene_symbol.clone()),
            "chrom": gene.chrom,
            "start": gene.start,
            "stop": gene.stop,
            "strand": gene.strand,
            "canonical_transcript_id": gene.canonical_transcript_id,
            "transcripts_json": transcripts_json,
        });
        ndjson.push_str(&obj.to_string());
        ndjson.push('\n');
        batch_count += 1;
        total += 1;

        if batch_count >= args.batch_size {
            pb.set_message(format!("{} genes loaded...", total));
            flush(&mut ndjson, &mut batch_count)?;
        }
    }
    flush(&mut ndjson, &mut batch_count)?;
    pb.finish_and_clear();

    let count = client.count_rows(&args.table).map_err(ch_err)?;

    println!();
    println!("{}", "Genes load complete!".green().bold());
    println!(
        "  {} {} genes loaded ({} rows skipped, no gene_id; {} outside interval)",
        "Loaded:".cyan(),
        total.to_string().bright_white(),
        skipped,
        filtered_out,
    );
    println!(
        "  {} {}",
        "Rows in table:".cyan(),
        count.to_string().bright_white()
    );

    Ok(())
}
