//! Table information display command.

use crate::commands::utils::progress_style_spinner;
use genohype_core::query::QueryEngine;
use genohype_core::summary::format_schema_clean;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;

/// Format a number with comma separators (e.g., 40535147 → "40,535,147")
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result
}

pub fn show_info(table_path: &str, json: bool, count: bool, globals: bool) -> Result<()> {
    if globals {
        return show_globals(table_path);
    }
    if json {
        return show_info_json(table_path, count);
    }

    // Check if this is a VCF or BED file
    let is_vcf = table_path.ends_with(".vcf")
        || table_path.ends_with(".vcf.gz")
        || table_path.ends_with(".vcf.bgz");
    let is_bed = table_path.ends_with(".bed.gz") || table_path.ends_with(".bed.bgz");

    if is_vcf || is_bed {
        let label = if is_vcf { "VCF" } else { "BED" };
        println!("{}", format!("{} File Information", label).bold().underline());
        println!();
        println!("{} {}", "Path:".green(), table_path.bright_white());
        println!();

        let engine = QueryEngine::open_path(table_path)?;

        println!(
            "{} {}",
            "Partitions (contigs):".green(),
            engine.num_partitions().to_string().bright_white()
        );
        println!("{}", "Row Schema:".green());
        println!("{:?}", engine.row_type());
        return Ok(());
    }

    // Hail Table - open via QueryEngine (loads metadata once)
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(progress_style_spinner());
    spinner.set_message("Loading table metadata...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    let engine = QueryEngine::open_path(table_path)?;

    spinner.finish_and_clear();

    // Get version info from the already-loaded table metadata
    println!("{}", "Hail Table Information".bold().underline());
    println!();
    println!("{} {}", "Path:".green(), table_path.bright_white());

    if let Some(metadata) = engine.table_metadata() {
        println!(
            "{} {}",
            "Format version:".green(),
            metadata.file_version.to_string().bright_white()
        );
        println!(
            "{} {}",
            "Hail version:".green(),
            metadata.hail_version.bright_white()
        );
        println!(
            "{} {}",
            "References:".green(),
            metadata.references_rel_path.bright_white()
        );
    }
    println!();

    // Key information
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

    // Partition information
    println!(
        "{} {}",
        "Partitions:".green(),
        engine.num_partitions().to_string().bright_white()
    );

    // Total rows: fast path always shown, slow path only with --count
    if engine.has_fast_row_count() {
        if let Some(total_rows) = engine.total_rows() {
            println!(
                "{} {}",
                "Total Rows:".green(),
                format_number(total_rows).bright_white()
            );
        }
    } else if count {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(progress_style_spinner());
        spinner.set_message("Counting rows from partition indexes...");
        spinner.enable_steady_tick(std::time::Duration::from_millis(100));

        if let Some(total_rows) = engine.total_rows() {
            spinner.finish_and_clear();
            println!(
                "{} {}",
                "Total Rows:".green(),
                format_number(total_rows).bright_white()
            );
        } else {
            spinner.finish_and_clear();
            println!(
                "{} {}",
                "Total Rows:".green(),
                "(unavailable)".dimmed()
            );
        }
    } else {
        println!(
            "{} {}",
            "Total Rows:".green(),
            "(use --count to compute)".dimmed()
        );
    }

    if engine.has_index() {
        println!("{} {}", "Index:".green(), "Yes".bright_green());
    } else {
        println!("{} {}", "Index:".green(), "No".yellow());
    }
    println!();

    // Hail-specific structural information
    if let Some(rvd_spec) = engine.rvd_spec() {
        // Partition files
        if rvd_spec.part_files.len() <= 5 {
            println!("{}", "Partition Files:".green());
            for (i, part) in rvd_spec.part_files.iter().enumerate() {
                println!("  {}. {}", i.to_string().cyan(), part.dimmed());
            }
        } else {
            println!("{}", "Partition Files (first 5):".green());
            for (i, part) in rvd_spec.part_files.iter().take(5).enumerate() {
                println!("  {}. {}", i.to_string().cyan(), part.dimmed());
            }
            println!(
                "  {} ({} more)",
                "...".dimmed(),
                rvd_spec.part_files.len() - 5
            );
        }
        println!();

        // Index information
        if let Some(ref index_spec) = rvd_spec.index_spec {
            println!("{}", "Index Details:".green());
            println!("  {} {}", "Path:".cyan(), index_spec.rel_path.dimmed());
            println!("  {} {}", "Key Type:".cyan(), index_spec.key_type.dimmed());
            println!();
        }

        // Partition bounds (sample)
        println!("{}", "Partition Bounds (first 3):".green());
        for (i, interval) in rvd_spec.range_bounds.iter().take(3).enumerate() {
            println!("  {} {}:", "Partition".cyan(), i);
            let start = serde_json::to_string(&interval.start).unwrap_or_default();
            let end = serde_json::to_string(&interval.end).unwrap_or_default();
            println!("    {} .. {}", start.dimmed(), end.dimmed());
        }
        if rvd_spec.range_bounds.len() > 3 {
            println!("    {}", "...".dimmed());
        }
        println!();

        // Codec information
        println!("{}", "Row Codec:".green());
        println!("{}", format_schema_clean(&rvd_spec.codec_spec.v_type));
    }

    Ok(())
}

fn show_info_json(table_path: &str, count: bool) -> Result<()> {
    let is_vcf = table_path.ends_with(".vcf")
        || table_path.ends_with(".vcf.gz")
        || table_path.ends_with(".vcf.bgz");
    let is_bed = table_path.ends_with(".bed.gz") || table_path.ends_with(".bed.bgz");

    if is_vcf || is_bed {
        let format = if is_vcf { "vcf" } else { "bed" };
        let engine = QueryEngine::open_path(table_path)?;
        let info = serde_json::json!({
            "path": table_path,
            "format": format,
            "partitions": engine.num_partitions(),
            "schema": format!("{:?}", engine.row_type()),
        });
        println!("{}", serde_json::to_string_pretty(&info)?);
        return Ok(());
    }

    // Hail Table - single QueryEngine load
    let engine = QueryEngine::open_path(table_path)?;

    let mut info = serde_json::json!({
        "path": table_path,
        "format": "hail_table",
        "key_fields": engine.key_fields(),
        "partitions": engine.num_partitions(),
        "has_index": engine.has_index(),
    });

    if let Some(metadata) = engine.table_metadata() {
        info["file_version"] = serde_json::json!(metadata.file_version);
        info["hail_version"] = serde_json::json!(metadata.hail_version);
    }

    // Only compute total_rows if fast or --count requested
    if engine.has_fast_row_count() || count {
        if let Some(total_rows) = engine.total_rows() {
            info["total_rows"] = serde_json::json!(total_rows);
        }
    }

    if let Some(rvd_spec) = engine.rvd_spec() {
        if let Some(ref index_spec) = rvd_spec.index_spec {
            info["index"] = serde_json::json!({
                "path": index_spec.rel_path,
                "key_type": index_spec.key_type,
            });
        }

        info["schema"] = serde_json::json!(format_schema_clean(&rvd_spec.codec_spec.v_type));
    }

    println!("{}", serde_json::to_string_pretty(&info)?);
    Ok(())
}

fn show_globals(table_path: &str) -> Result<()> {
    let engine = QueryEngine::open_path(table_path)?;
    let globals = engine.globals()?;
    let json = serde_json::to_string_pretty(&globals)?;
    println!("{}", json);
    Ok(())
}
