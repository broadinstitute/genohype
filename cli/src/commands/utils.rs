//! Shared CLI utilities (progress bars, formatting, filter parsing).

use crate::cli::HasCommonExportArgs;
use genohype_core::codec::EncodedValue;
use genohype_core::export::cache_builder::extract_gene;
use genohype_core::query::{IntervalList, KeyRange};
use genohype_core::Result;
use indicatif::ProgressStyle;
use owo_colors::OwoColorize;
use std::sync::Arc;

// Re-export filter parser from core for use across modules
pub use genohype_core::query::filter::parse_where_condition;

/// Whether a decoded genes-HT row overlaps any of `intervals`.
///
/// The genes Hail table is keyed by `gene_id`, so a Hail interval scan can't
/// slice it by genomic position — `--interval` is a silent no-op there and the
/// loader would otherwise pull every gene genome-wide. This post-decode check
/// restores interval scoping for the gene loaders (smoke/subset).
///
/// Genes carry an UNPREFIXED contig (`"21"`) while intervals are usually
/// chr-prefixed (`"chr21"`), so we test both conventions. A row with no
/// extractable gene/locus is treated as non-overlapping (dropped under a filter).
pub fn gene_row_in_intervals(row: &EncodedValue, intervals: &IntervalList) -> bool {
    let Some(gene) = extract_gene(row) else {
        return false;
    };
    let start = gene.start as i32;
    let stop = gene.stop as i32;
    let chrom = gene.chrom.as_str();
    let bare = chrom.strip_prefix("chr").unwrap_or(chrom);
    let prefixed = format!("chr{bare}");
    intervals.overlaps(chrom, start, stop)
        || intervals.overlaps(bare, start, stop)
        || intervals.overlaps(&prefixed, start, stop)
}

/// Create a standard progress bar style (no emojis)
pub fn progress_style_bar() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-")
}

/// Create a standard spinner style (no emojis)
pub fn progress_style_spinner() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg}")
        .unwrap()
}

/// Parse where filters from any export args implementing HasCommonExportArgs.
/// This enforces at compile time that all export targets have common args.
pub fn parse_export_filters(args: &impl HasCommonExportArgs) -> Vec<KeyRange> {
    let mut filters = Vec::new();
    for clause in &args.common().where_clauses {
        if let Some(range) = parse_where_condition(clause) {
            filters.push(range);
        } else {
            eprintln!(
                "{} Invalid --where format: {}",
                "Error:".red().bold(),
                clause
            );
            std::process::exit(1);
        }
    }
    filters
}

/// Parse interval list from CLI arguments (file and/or strings)
///
/// # Arguments
/// * `file` - Optional path to interval file (.bed, .json, or text)
/// * `strings` - Optional list of interval strings (chr:start-end format)
///
/// # Returns
/// * `Ok(None)` if no intervals specified
/// * `Ok(Some(Arc<IntervalList>))` with merged and optimized intervals
/// * `Err` on parse errors
pub fn parse_interval_list(
    file: Option<&str>,
    strings: &[String],
) -> Result<Option<Arc<IntervalList>>> {
    // Return None if no intervals specified
    if file.is_none() && strings.is_empty() {
        return Ok(None);
    }

    let mut list = IntervalList::new();

    // Load from file if specified
    if let Some(path) = file {
        let file_list = IntervalList::from_file(path)?;
        list.merge(file_list);
    }

    // Parse string intervals if specified
    if !strings.is_empty() {
        let string_list = IntervalList::from_strings(strings)?;
        list.merge(string_list);
    }

    // Optimize the combined list
    list.optimize();

    Ok(Some(Arc::new(list)))
}

/// Parse interval list from export args
pub fn parse_export_intervals(args: &impl HasCommonExportArgs) -> Result<Option<Arc<IntervalList>>> {
    parse_interval_list(
        args.common().intervals_file.as_deref(),
        &args.common().interval,
    )
}

/// Format bytes into a human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{} B", bytes);
    }
    if bytes < UNIT.pow(2) {
        return format!("{:.2} KiB", bytes as f64 / UNIT as f64);
    }
    if bytes < UNIT.pow(3) {
        return format!("{:.2} MiB", bytes as f64 / UNIT.pow(2) as f64);
    }
    format!("{:.2} GiB", bytes as f64 / UNIT.pow(3) as f64)
}
