//! Manhattan ingestion handler.
//!
//! Processes Manhattan data ingestion tasks into ClickHouse.

use crate::Result;

/// Process a Manhattan ingestion task (ingest phenotype data into ClickHouse).
pub fn process_ingest_manhattan(
    phenotype_id: &str,
    ancestry: &str,
    base_path: &str,
    clickhouse_url: &str,
    database: &str,
) -> Result<usize> {
    use crate::ingest::manhattan::run_ingest_task;

    println!("Processing ingestion task:");
    println!("  Phenotype: {}", phenotype_id);
    println!("  Ancestry: {}", ancestry);
    println!("  Base path: {}", base_path);
    println!("  ClickHouse: {}", clickhouse_url);
    println!("  Database: {}", database);

    let rows = run_ingest_task(phenotype_id, ancestry, base_path, clickhouse_url, database)?;

    println!("Ingestion complete: {} total rows", rows);
    Ok(rows)
}
