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

            // Discover phenotypes using gsutil
            let output = std::process::Command::new("gsutil")
                .args([
                    "-m",
                    "ls",
                    "-r",
                    &format!("{}/**/manifest.json", args.input_dir.trim_end_matches('/')),
                ])
                .output()
                .map_err(|e| {
                    genohype_core::HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to run gsutil: {}", e),
                    ))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !stderr.contains("matched no objects") {
                    return Err(genohype_core::HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("gsutil failed: {}", stderr),
                    )));
                }
                println!("No phenotypes found in {}", args.input_dir);
                return Ok(());
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut total_rows = 0;
            let mut count = 0;

            for line in stdout.lines() {
                let line = line.trim();
                if !line.ends_with("/manifest.json") {
                    continue;
                }

                let base_path = line.trim_end_matches("/manifest.json");
                let segments: Vec<&str> = base_path.split('/').collect();
                if segments.len() < 2 {
                    continue;
                }

                let phenotype_id = segments[segments.len() - 1].to_string();
                let ancestry = segments[segments.len() - 2].to_string();

                if ancestry.is_empty() || phenotype_id.is_empty() {
                    continue;
                }

                count += 1;
                println!(
                    "Ingesting {}/{} ({})...",
                    ancestry, phenotype_id, base_path
                );

                match run_ingest_task(
                    &phenotype_id,
                    &ancestry,
                    base_path,
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
