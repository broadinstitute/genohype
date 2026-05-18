//! Table summary command with field statistics.

use crate::commands::utils::{format_bytes, progress_style_bar};
use genohype_core::io::{get_file_size, join_path};
use genohype_core::query::QueryEngine;
use genohype_core::summary::{format_schema_clean, StatsAccumulator};
use genohype_core::Result;
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Run the summary command
pub fn run_summary(table_path: &str) -> Result<()> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.set_message("Loading table metadata...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    let engine = QueryEngine::open_path(table_path)?;

    spinner.finish_and_clear();

    let part_count = engine.num_partitions();

    // Print header
    println!("{}", "Table Summary".bold().underline());
    println!();

    // Basic info
    let name = std::path::Path::new(table_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    println!("{} {}", "Name:".green(), name.bright_white());
    println!("{} {}", "Path:".green(), table_path.bright_white());
    println!(
        "{} {}",
        "Partitions:".green(),
        part_count.to_string().bright_white()
    );
    println!();

    // Key fields
    println!("{}", "Key Fields:".green());
    let keys = engine.key_fields();
    if keys.is_empty() {
        println!("  {}", "(none)".dimmed());
    } else {
        for (i, key) in keys.iter().enumerate() {
            println!(
                "  {}. {}",
                (i + 1).to_string().cyan(),
                key.bright_white()
            );
        }
    }
    println!();

    // Hail-specific partition size calculation
    if let Some(rvd) = engine.rvd_spec() {
        // Calculate partition sizes (parallel)
        println!("{}", "Calculating partition sizes...".dimmed());
        let pb = ProgressBar::new(part_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions")
                .unwrap()
                .progress_chars("#>-"),
        );

        let parts_dir = join_path(&join_path(table_path, "rows"), "parts");

        let sizes: Vec<u64> = rvd
            .part_files
            .par_iter()
            .map(|part| {
                let path = join_path(&parts_dir, part);
                let size = get_file_size(&path).unwrap_or(0);
                pb.inc(1);
                size
            })
            .collect();

        pb.finish_and_clear();

        let total_size: u64 = sizes.iter().sum();
        let mean_size = if part_count > 0 {
            total_size as f64 / part_count as f64
        } else {
            0.0
        };

        // Calculate standard deviation
        let variance = if part_count > 1 {
            let mean = mean_size;
            sizes
                .iter()
                .map(|&s| {
                    let diff = s as f64 - mean;
                    diff * diff
                })
                .sum::<f64>()
                / (part_count - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();

        println!("{}", "Size Statistics:".green());
        println!(
            "  {} {}",
            "Total Size:".cyan(),
            format_bytes(total_size).bright_white()
        );
        println!(
            "  {} {}",
            "Mean Partition Size:".cyan(),
            format_bytes(mean_size as u64).bright_white()
        );
        println!(
            "  {} {}",
            "Std Dev:".cyan(),
            format_bytes(std_dev as u64).bright_white()
        );
        println!();

        // Schema
        println!("{}", "Schema:".green());
        println!("{}", "-".repeat(40).dimmed());
        println!("{}", format_schema_clean(&rvd.codec_spec.v_type));
        println!("{}", "-".repeat(40).dimmed());
        println!();
    } else {
        // VCF or other non-Hail source
        println!(
            "{}",
            "(Size statistics not available for this format)".dimmed()
        );
        println!();
    }

    // Data scan for statistics (parallel)
    println!("{}", "Scanning data for field statistics...".dimmed());
    let pb = ProgressBar::new(part_count as u64);
    pb.set_style(progress_style_bar());

    let total_rows = AtomicUsize::new(0);

    // Parallel scan using rayon - each thread gets its own StatsAccumulator
    // Uses streaming to avoid loading entire partitions into memory (prevents OOM)
    let stats = (0..part_count)
        .into_par_iter()
        .fold(
            || StatsAccumulator::new(),
            |mut acc, i| {
                match engine.scan_partition_iter(i, &[]) {
                    Ok(iter) => {
                        for row_result in iter {
                            match row_result {
                                Ok(row) => {
                                    total_rows.fetch_add(1, Ordering::Relaxed);
                                    acc.process_row(&row);
                                }
                                Err(e) => {
                                    eprintln!(
                                        "{} Error reading row in partition {}: {}",
                                        "Warning:".yellow(),
                                        i,
                                        e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "{} Failed to scan partition {}: {}",
                            "Warning:".yellow(),
                            i,
                            e
                        );
                    }
                }
                pb.inc(1);
                acc
            },
        )
        .reduce(
            || StatsAccumulator::new(),
            |mut a, b| {
                a.merge(b);
                a
            },
        );

    pb.finish_and_clear();
    let total_rows = total_rows.load(Ordering::Relaxed);

    println!();
    println!(
        "{} {}",
        "Row Count:".green(),
        total_rows.to_string().bright_white().bold()
    );
    println!();

    // Print field statistics
    println!("{}", "Field Statistics:".green().bold());
    println!(
        "{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
        "Field".cyan(),
        "Count".cyan(),
        "Nulls".cyan(),
        "Min".cyan(),
        "Max".cyan()
    );
    println!("{}", "-".repeat(120).dimmed());

    for key in stats.sorted_fields() {
        let s = &stats.stats[key];

        // Truncate field name if too long
        let field_display = if key.len() > 48 {
            format!("...{}", &key[key.len() - 45..])
        } else {
            key.clone()
        };

        // Truncate min/max if too long
        let min_display = match &s.min {
            Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
            Some(m) => m.clone(),
            None => String::new(),
        };
        let max_display = match &s.max {
            Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
            Some(m) => m.clone(),
            None => String::new(),
        };

        println!(
            "{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
            field_display, s.count, s.null_count, min_display, max_display
        );
    }

    Ok(())
}
