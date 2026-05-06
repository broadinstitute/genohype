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
    let input_dir = input_dir.trim_end_matches('/');

    // Fast path: construct deterministic paths directly from the filtered list,
    // bypassing the expensive GCS object listing. Used when the UI sends an
    // explicit set of pre-verified phenotypes.
    if let Some(explicit_phenotypes) = filter {
        let mut phenotypes = Vec::with_capacity(explicit_phenotypes.len());
        for (id, ancestry) in explicit_phenotypes {
            let base_path = format!("{}/{}/{}", input_dir, ancestry, id);
            phenotypes.push((id.clone(), ancestry.clone(), base_path));
        }
        return Ok(phenotypes);
    }

    // Slow path: no filter provided (e.g. CLI ingest). Fall back to full GCS scan.
    let url = url::Url::parse(input_dir).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid URL: {}", e),
        ))
    })?;

    let bucket = url.host_str().ok_or_else(|| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Missing bucket in GCS URL",
        ))
    })?;

    let client = genohype_core::io::adapter::get_gcs_client(bucket)?;
    let prefix = object_store::path::Path::from(url.path().trim_start_matches('/'));

    let manifest_paths = if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(list_manifests(&client, &prefix, bucket)))
    } else {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            crate::HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        rt.block_on(list_manifests(&client, &prefix, bucket))
    };

    let mut phenotypes = Vec::new();

    for full_path in manifest_paths {
        let base_path = full_path.trim_end_matches("/manifest.json");
        let segments: Vec<&str> = base_path.split('/').collect();
        if segments.len() < 2 {
            continue;
        }

        let phenotype_id = segments[segments.len() - 1].to_string();
        let ancestry = segments[segments.len() - 2].to_string();

        if ancestry.is_empty() || phenotype_id.is_empty() {
            continue;
        }

        if let Some(f) = filter {
            if !f.iter().any(|(id, anc)| id == &phenotype_id && anc == &ancestry) {
                continue;
            }
        }

        phenotypes.push((phenotype_id, ancestry, base_path.to_string()));
    }

    Ok(phenotypes)
}

/// List all manifest.json paths under a given prefix using object_store.
async fn list_manifests(
    client: &std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: &object_store::path::Path,
    bucket: &str,
) -> Vec<String> {
    use futures::StreamExt;

    let mut paths = Vec::new();
    let mut stream = client.list(Some(prefix));
    while let Some(res) = stream.next().await {
        if let Ok(meta) = res {
            let location = meta.location.to_string();
            if location.ends_with("/manifest.json") {
                paths.push(format!("gs://{}/{}", bucket, location));
            }
        }
    }
    paths
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
    use genohype_core::export::ClickHouseClient;
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

        // Clean up any orphaned temp tables from previous crashed jobs
        if let Err(e) = client.cleanup_orphaned_temp_tables(table_name) {
            println!(
                "    Warning: Failed to clean up orphaned tables for {}: {}",
                table_name, e
            );
        }
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
        dynamic_batch_size: 2,
        max_batch_size: 16,
    }
}
