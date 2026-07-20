//! VCF export command.

use crate::cli::ExportVcfArgs;
use crate::commands::utils::{
    format_bytes, parse_export_filters, parse_export_intervals, progress_style_spinner,
};
use genohype_core::query::QueryEngine;
use genohype_core::vcf::VcfWriter;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;

pub fn run_export_vcf(args: ExportVcfArgs) -> Result<()> {
    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!(
        "{} {} {} {}",
        "Converting".green(),
        args.common.input.bright_white(),
        "to VCF".green(),
        args.output.bright_white()
    );

    // Open the query engine
    let engine = QueryEngine::open_path(&args.common.input)?;

    #[cfg(feature = "vep")]
    let engine = if let Some(vep_opts) = args.vep.to_init_options() {
        eprintln!("{} {}", "VEP annotation:".cyan(), "enabled".green());
        engine.with_vep(vep_opts)?
    } else {
        engine
    };

    let row_type = engine.row_type().clone();
    println!(
        "{} {}",
        "Partitions:".cyan(),
        engine.num_partitions().to_string().bright_white()
    );
    println!("{} {:?}", "Key fields:".cyan(), engine.key_fields());
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
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    if args.bgzip {
        println!("{} {}", "Compression:".cyan(), "BGZF".bright_white());
    }
    println!();

    // Extract sample names from globals if available, or use empty list for sites-only VCF
    let sample_names: Vec<String> = Vec::new(); // TODO: Extract from globals if available

    // Create VCF writer
    let mut total_rows = 0;
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    if args.bgzip {
        let mut writer = VcfWriter::new_bgzf(&args.output, &row_type, sample_names)?;

        // Use streaming query with filters and intervals
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        for row_result in iterator {
            let row = row_result?;
            writer.write_row(&row)?;
            total_rows += 1;
            if total_rows % 10000 == 0 {
                pb.set_message(format!("{} rows written...", total_rows));
            }
        }

        writer.finish()?;
    } else {
        let mut writer = VcfWriter::new(&args.output, &row_type, sample_names)?;

        // Use streaming query with filters and intervals
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        for row_result in iterator {
            let row = row_result?;
            writer.write_row(&row)?;
            total_rows += 1;
            if total_rows % 10000 == 0 {
                pb.set_message(format!("{} rows written...", total_rows));
            }
        }

        writer.finish()?;
    }

    pb.finish_and_clear();

    // Print summary
    let output_size = std::fs::metadata(&args.output)
        .map(|m| format_bytes(m.len()))
        .unwrap_or_else(|_| "unknown".to_string());

    println!();
    println!("{}", "Conversion complete!".green().bold());
    println!(
        "  {} {}",
        "Rows written:".cyan(),
        total_rows.to_string().bright_white()
    );
    println!("  {} {}", "Output file:".cyan(), args.output.bright_white());
    println!("  {} {}", "Output size:".cyan(), output_size.bright_white());

    Ok(())
}
