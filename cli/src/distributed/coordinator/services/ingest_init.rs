//! Initialization services for ingestion jobs.
//!
//! Contains logic for discovering phenotypes for ingestion and initializing
//! ClickHouse tables, extracted from the job submission handler.

use crate::distributed::coordinator::state::IngestionState;
use std::collections::HashMap;

/// Discover phenotypes for ingestion by scanning GCS for manifest.json files.
///
/// Expected directory structure: {input_dir}/{ancestry}/{phenotype_id}/manifest.json
/// Returns: Vec of (phenotype_id, ancestry, base_path)
pub fn discover_phenotypes_for_ingestion(
    input_dir: &str,
    filter: Option<&[(String, String)]>,
) -> crate::Result<Vec<(String, String, String)>> {
    use std::process::Command;

    let input_dir = input_dir.trim_end_matches('/');

    // Use gsutil to list all manifest.json files recursively
    // This is more reliable than object_store listing for discovering subdirectories
    let output = Command::new("gsutil")
        .args(["-m", "ls", "-r", &format!("{}/**/manifest.json", input_dir)])
        .output()
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to run gsutil: {}", e),
            ))
        })?;

    if !output.status.success() {
        // If no files found, return empty vec (not an error)
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("matched no objects") || stderr.contains("CommandException") {
            return Ok(Vec::new());
        }
        return Err(crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("gsutil failed: {}", stderr),
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut phenotypes = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if !line.ends_with("/manifest.json") {
            continue;
        }

        // Parse path: gs://bucket/base/{ancestry}/{phenotype_id}/manifest.json
        // Remove /manifest.json to get base_path
        let base_path = line.trim_end_matches("/manifest.json");

        // Extract ancestry and phenotype_id from path
        // Take last two segments
        let segments: Vec<&str> = base_path.split('/').collect();
        if segments.len() < 2 {
            continue;
        }

        let phenotype_id = segments[segments.len() - 1].to_string();
        let ancestry = segments[segments.len() - 2].to_string();

        // Skip if ancestry looks like a bucket or path component
        if ancestry.is_empty() || phenotype_id.is_empty() {
            continue;
        }

        // If a filter is provided, ensure this phenotype is in it
        if let Some(f) = filter {
            if !f.iter().any(|(id, anc)| id == &phenotype_id && anc == &ancestry) {
                continue;
            }
        }

        phenotypes.push((phenotype_id, ancestry, base_path.to_string()));
    }

    Ok(phenotypes)
}

/// Initialize ClickHouse tables for Manhattan ingestion based on init strategy.
///
/// - Create strategy: Creates tables if they don't exist
/// - Replace strategy: Drops and recreates tables
/// - Append strategy: Does nothing (tables must exist)
#[cfg(feature = "clickhouse")]
pub fn init_clickhouse_tables(
    clickhouse_url: &str,
    init_strategy: &crate::distributed::message::InitStrategy,
) -> Result<(), String> {
    use crate::distributed::message::InitStrategy;
    use crate::export::ClickHouseClient;
    use crate::ingest::get_manhattan_schemas;

    if *init_strategy == InitStrategy::Append {
        return Ok(());
    }

    println!("  Initializing tables...");
    let client = ClickHouseClient::new(clickhouse_url);
    let schemas = get_manhattan_schemas();

    for (table_name, create_sql) in schemas {
        // If replace strategy, drop the table first
        if *init_strategy == InitStrategy::Replace {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            client
                .execute(&drop_sql)
                .map_err(|e| format!("Failed to drop table {}: {}", table_name, e))?;
            println!("    Dropped table: {}", table_name);
        }

        // Create the table (for both Create and Replace strategies)
        client
            .execute(create_sql)
            .map_err(|e| format!("Failed to create table {}: {}", table_name, e))?;
        println!("    Created table: {}", table_name);
    }

    println!("  Table initialization complete.");
    Ok(())
}

/// Initialize ingestion state from discovered phenotypes.
pub fn create_ingestion_state(
    phenotypes: Vec<(String, String, String)>,
    clickhouse_url: &str,
    database: &str,
) -> IngestionState {
    let total = phenotypes.len();
    IngestionState {
        pending_tasks: phenotypes.into_iter().collect(),
        active_tasks: HashMap::new(),
        clickhouse_url: clickhouse_url.to_string(),
        database: database.to_string(),
        completed_count: 0,
        failed_count: 0,
        total_tasks: total,
    }
}
