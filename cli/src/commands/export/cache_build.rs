//! Materialized gene-view cache-build command (Phase 4, `gcs-cache` arm).
//!
//! Writes one `{gene_id}.json` [`genohype_core::export::CacheGeneVariantsResponse`]
//! blob per gene to a local dir or `gs://` prefix. See
//! [`genohype_core::export::cache_builder`] for the pinned blob contract.

use crate::cli::ExportCacheBuildArgs;
use genohype_core::export::build_cache;
use genohype_core::Result;
use owo_colors::OwoColorize;

pub fn run_export_cache_build(args: ExportCacheBuildArgs) -> Result<()> {
    println!(
        "{}",
        "Building materialized gene-view cache".green().bold()
    );
    println!("  {} {}", "Genes:".cyan(), args.genes.bright_white());
    println!("  {} {}", "Variants:".cyan(), args.variants.bright_white());
    println!("  {} {}", "Output:".cyan(), args.output.bright_white());

    // Resolve the optional gene_id restriction (the per-task chunk a pool worker
    // receives) from --gene-ids or --gene-ids-file.
    let gene_ids: Option<Vec<String>> = if let Some(list) = &args.gene_ids {
        Some(
            list.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    } else if let Some(path) = &args.gene_ids_file {
        let text = std::fs::read_to_string(path)?;
        Some(
            text.lines()
                .map(|l| l.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    } else {
        None
    };
    if let Some(ids) = &gene_ids {
        println!(
            "  {} {} gene(s)",
            "Restricted to:".cyan(),
            ids.len().to_string().bright_white()
        );
    }
    println!();

    let stats = build_cache(&args.genes, &args.variants, &args.output, gene_ids.as_deref())?;

    println!("{}", "Cache build complete!".green().bold());
    println!(
        "  {} {}",
        "Genes seen:".cyan(),
        stats.genes_seen.to_string().bright_white()
    );
    println!(
        "  {} {}",
        "Blobs written:".cyan(),
        stats.blobs_written.to_string().bright_white()
    );
    println!(
        "  {} {}",
        "Genes with no variants:".cyan(),
        stats.genes_no_variants.to_string().bright_white()
    );
    println!(
        "  {} {}",
        "Total variants:".cyan(),
        stats.total_variants.to_string().bright_white()
    );
    println!(
        "  {} {} bytes",
        "Cache size:".cyan(),
        stats.total_bytes.to_string().bright_white()
    );

    // Acceptance check: every gene materialized exactly one blob.
    if stats.blobs_written != stats.genes_seen {
        eprintln!(
            "{} blobs_written ({}) != genes_seen ({})",
            "Warning:".yellow(),
            stats.blobs_written,
            stats.genes_seen
        );
    }

    Ok(())
}
