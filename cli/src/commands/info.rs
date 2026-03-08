//! Table information display command.

use genohype_core::query::QueryEngine;
use genohype_core::summary::format_schema_clean;
use genohype_core::Result;
use owo_colors::OwoColorize;

pub fn show_info(table_path: &str) -> Result<()> {
    // Check if this is a VCF file
    if table_path.ends_with(".vcf")
        || table_path.ends_with(".vcf.gz")
        || table_path.ends_with(".vcf.bgz")
    {
        println!("{}", "VCF File Information".bold().underline());
        println!();
        println!("{} {}", "Path:".green(), table_path.bright_white());
        println!();

        // Open using query engine to get schema info
        let engine = QueryEngine::open_path(table_path)?;

        println!(
            "{} {}",
            "Contigs:".green(),
            engine.num_partitions().to_string().bright_white()
        );
        println!("{}", "Row Schema:".green());
        println!("{:?}", engine.row_type());
        return Ok(());
    }

    // Hail Table - Read basic metadata first
    let metadata_path = genohype_core::io::join_path(table_path, "metadata.json.gz");

    // Try reading metadata (might fail if not a hail table)
    let metadata = match genohype_core::io::get_reader(&metadata_path) {
        Ok(mut reader) => {
            let mut data = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut data)?;
            genohype_core::schema::Metadata::from_gzipped_json(&data)?
        }
        Err(_) => {
            println!("{} Not a valid Hail table or VCF file", "Error:".red());
            return Ok(());
        }
    };

    println!("{}", "Hail Table Information".bold().underline());
    println!();
    println!("{} {}", "Path:".green(), table_path.bright_white());
    println!(
        "{} {}",
        "Format version:".green(),
        metadata.file_version.bright_white()
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
    println!();

    // Open using query engine for structural inspection
    let engine = QueryEngine::open_path(table_path)?;

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
            // Just show start/end JSON cleanly
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
        // Clean format the VType
        println!("{}", format_schema_clean(&rvd_spec.codec_spec.v_type));
    }

    Ok(())
}
