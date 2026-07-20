//! Ingest commands for loading data into ClickHouse.

use crate::cli::IngestCommands;
use crate::ingest::manhattan::run_ingest_task;
use genohype_core::Result;

pub fn run_ingest_command(command: IngestCommands) -> Result<()> {
    match command {
        IngestCommands::Manhattan(args) => {
            // For local/non-distributed execution, we run the ingestion directly
            // by parsing the input_dir to discover phenotypes
            println!("Starting Manhattan ingestion (local mode)");
            println!("  Input dir: {}", args.input_dir);
            println!("  ClickHouse: {}", args.clickhouse_url);
            println!("  Database: {}", args.database);

            // Discover phenotypes
            let phenotypes =
                crate::distributed::coordinator::services::discover_phenotypes_for_ingestion(
                    &args.input_dir,
                    None,
                )?;

            if phenotypes.is_empty() {
                println!("No phenotypes found in {}", args.input_dir);
                return Ok(());
            }

            let mut total_rows = 0;
            let count = phenotypes.len();

            for (phenotype_id, ancestry, base_path) in phenotypes {
                println!("Ingesting {}/{} ({})...", ancestry, phenotype_id, base_path);
                match run_ingest_task(
                    &phenotype_id,
                    &ancestry,
                    &base_path,
                    &args.clickhouse_url,
                    &args.database,
                ) {
                    Ok(rows) => {
                        total_rows += rows;
                        println!("  Done: {} rows", rows);
                    }
                    Err(e) => {
                        eprintln!("  Error: {}", e);
                    }
                }
            }

            println!(
                "Ingestion complete: {} phenotypes, {} total rows",
                count, total_rows
            );
            Ok(())
        }
    }
}
