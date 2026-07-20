//! Schema validation and generation commands.

use crate::cli::ValidateArgs;
use genohype_core::query::QueryEngine;
use genohype_core::validation::{SchemaGenerator, SchemaValidator};
use genohype_core::Result;
use owo_colors::OwoColorize;

/// Run the validate command
pub fn run_validate(args: ValidateArgs) -> Result<()> {
    // Validate that --limit and --sample aren't both specified
    if args.limit.is_some() && args.sample.is_some() {
        eprintln!(
            "{} Cannot use both --limit and --sample. Choose one.",
            "Error:".red().bold()
        );
        std::process::exit(1);
    }

    println!(
        "{} {}",
        "Validating table:".green(),
        args.table.bright_white()
    );
    println!("{} {}", "Using schema:".green(), args.schema.bright_white());
    if let Some(l) = args.limit {
        println!(
            "{} {} {}",
            "Row limit:".cyan(),
            l.to_string().bright_white(),
            "(sequential)".dimmed()
        );
    }
    if let Some(s) = args.sample {
        println!(
            "{} {} {}",
            "Sample size:".cyan(),
            s.to_string().bright_white(),
            "(random)".dimmed()
        );
    }
    if args.fail_fast {
        println!("{} {}", "Mode:".cyan(), "fail-fast".yellow());
    }
    println!();

    // Load the JSON schema
    let validator = SchemaValidator::from_file(&args.schema)?;

    // Open the table
    let engine = QueryEngine::open_path(&args.table)?;

    println!(
        "{} {}",
        "Partitions:".cyan(),
        engine.num_partitions().to_string().bright_white()
    );
    println!();

    // Run validation
    let report = if let Some(sample_size) = args.sample {
        if args.verbose {
            validator.validate_sample_verbose(&engine, sample_size, args.fail_fast)?
        } else {
            validator.validate_sample(&engine, sample_size, args.fail_fast)?
        }
    } else {
        println!("{}", "Validating rows sequentially...".dimmed());
        validator.validate(&engine, args.limit, args.fail_fast)?
    };

    // Print results
    println!();
    println!("{}", report);

    // Exit with error code if validation failed
    if report.invalid_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Run the generate-schema command
pub fn run_generate_schema(table_path: &str, output_path: Option<&str>) -> Result<()> {
    eprintln!(
        "{} {}",
        "Generating JSON schema for:".green(),
        table_path.bright_white()
    );

    // Open the table
    let engine = QueryEngine::open_path(table_path)?;

    // Get the table name from path for title
    let title = std::path::Path::new(table_path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_end_matches(".ht"));

    // Generate the schema
    let schema = SchemaGenerator::from_engine(&engine, title)?;

    if let Some(path) = output_path {
        // Write to file
        SchemaGenerator::write_to_file(&schema, path)?;
        eprintln!("{} {}", "Schema written to:".green(), path.bright_white());
    } else {
        // Print to stdout
        println!("{}", serde_json::to_string_pretty(&schema)?);
    }

    Ok(())
}
