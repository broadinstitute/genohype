//! ClickHouse introspection API handler.
//!
//! Proxies queries to the ClickHouse HTTP interface to provide storage metrics,
//! partition-level detail, and pipeline status for the dashboard.

use crate::distributed::coordinator::state::SharedState;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::Instant;

/// Response from GET /api/clickhouse/info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseInfo {
    pub tables: Vec<TableInfo>,
    pub partitions: Vec<PartitionInfo>,
    pub ingested_phenotypes: Vec<IngestedPhenotype>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub table: String,
    pub rows: u64,
    pub bytes_on_disk: u64,
    pub bytes_uncompressed: u64,
    pub part_count: u64,
    pub partition_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub table: String,
    pub phenotype: String,
    pub rows: u64,
    pub bytes_on_disk: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestedPhenotype {
    pub phenotype: String,
    pub ancestry: String,
    pub status: String,
    pub loci_count: u64,
    pub significant_variants: u64,
}

/// Cached response with TTL
static CACHE: Mutex<Option<(Instant, ClickHouseInfo)>> = Mutex::new(None);
const CACHE_TTL_SECS: u64 = 2;

/// Handler for GET /api/clickhouse/info
pub(crate) async fn get_clickhouse_info(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    // Check cache first
    {
        let cache = CACHE.lock().unwrap();
        if let Some((ts, ref info)) = *cache {
            if ts.elapsed().as_secs() < CACHE_TTL_SECS {
                return Json(serde_json::to_value(info).unwrap_or_default());
            }
        }
    }

    // Get clickhouse_url from catalog config
    let clickhouse_url = {
        let data = state.lock().expect("state lock poisoned");
        data.catalog
            .as_ref()
            .and_then(|c| c.config.ingest.clickhouse_url.clone())
    };

    let clickhouse_url = match clickhouse_url {
        Some(url) if !url.is_empty() => url,
        _ => {
            return Json(serde_json::json!({
                "error": "ClickHouse URL not configured (set ingest.clickhouse_url in config)"
            }));
        }
    };

    // Run queries against ClickHouse
    match fetch_clickhouse_info(&clickhouse_url).await {
        Ok(info) => {
            // Update ingested_phenotypes in coordinator state
            {
                let mut data = state.lock().expect("state lock poisoned");
                for p in &info.ingested_phenotypes {
                    if p.status.to_uppercase() == "INGESTED"
                        || p.status.to_uppercase() == "COMPLETED"
                    {
                        data.ingested_phenotypes
                            .insert((p.phenotype.clone(), p.ancestry.clone()));
                    }
                }
            }

            // Cache the result
            {
                let mut cache = CACHE.lock().unwrap();
                *cache = Some((Instant::now(), info.clone()));
            }

            Json(serde_json::to_value(&info).unwrap_or_default())
        }
        Err(e) => Json(serde_json::json!({ "error": format!("ClickHouse query failed: {}", e) })),
    }
}

async fn fetch_clickhouse_info(clickhouse_url: &str) -> Result<ClickHouseInfo, String> {
    let client = reqwest::Client::new();

    // Query 1: Table overview
    let table_query = r#"
        SELECT
            table,
            sum(rows) as rows,
            sum(bytes_on_disk) as bytes_on_disk,
            sum(data_uncompressed_bytes) as bytes_uncompressed,
            count() as part_count,
            uniqExact(partition) as partition_count
        FROM system.parts
        WHERE active AND database = 'default'
          AND table IN ('loci', 'loci_variants', 'significant_variants',
                        'phenotype_plots', 'gene_associations', 'qq_points', 'pipeline_status')
        GROUP BY table
        ORDER BY bytes_on_disk DESC
        FORMAT JSON
    "#;

    // Query 2: Partition-level detail
    let partition_query = r#"
        SELECT
            table,
            partition as phenotype,
            sum(rows) as rows,
            sum(bytes_on_disk) as bytes_on_disk
        FROM system.parts
        WHERE active AND database = 'default'
          AND table IN ('loci_variants', 'significant_variants', 'gene_associations', 'qq_points')
        GROUP BY table, partition
        ORDER BY table, bytes_on_disk DESC
        FORMAT JSON
    "#;

    // Query 3: Pipeline status
    let pipeline_query = r#"
        SELECT phenotype, ancestry, status, loci_count, significant_variants
        FROM pipeline_status
        FINAL
        ORDER BY phenotype, ancestry
        FORMAT JSON
    "#;

    let (tables_res, partitions_res, pipeline_res) = tokio::join!(
        query_clickhouse(&client, clickhouse_url, table_query),
        query_clickhouse(&client, clickhouse_url, partition_query),
        query_clickhouse(&client, clickhouse_url, pipeline_query),
    );

    let tables = parse_table_info(&tables_res?)?;
    let partitions = parse_partition_info(&partitions_res?)?;
    let ingested_phenotypes = parse_pipeline_status(&pipeline_res?)?;

    Ok(ClickHouseInfo {
        tables,
        partitions,
        ingested_phenotypes,
    })
}

async fn query_clickhouse(
    client: &reqwest::Client,
    base_url: &str,
    query: &str,
) -> Result<serde_json::Value, String> {
    let url = format!("{}/?default_format=JSON", base_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .body(query.to_string())
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("ClickHouse returned {}: {}", status, body));
    }

    resp.json::<serde_json::Value>()
        .await
        .map_err(|e| format!("Failed to parse JSON response: {}", e))
}

fn parse_table_info(json: &serde_json::Value) -> Result<Vec<TableInfo>, String> {
    let empty = vec![];
    let data = json
        .get("data")
        .and_then(|d| d.as_array())
        .unwrap_or(&empty);
    Ok(data
        .iter()
        .map(|row| TableInfo {
            table: row["table"].as_str().unwrap_or_default().to_string(),
            rows: parse_u64(&row["rows"]),
            bytes_on_disk: parse_u64(&row["bytes_on_disk"]),
            bytes_uncompressed: parse_u64(&row["bytes_uncompressed"]),
            part_count: parse_u64(&row["part_count"]),
            partition_count: parse_u64(&row["partition_count"]),
        })
        .collect())
}

fn parse_partition_info(json: &serde_json::Value) -> Result<Vec<PartitionInfo>, String> {
    let empty = vec![];
    let data = json
        .get("data")
        .and_then(|d| d.as_array())
        .unwrap_or(&empty);
    Ok(data
        .iter()
        .map(|row| PartitionInfo {
            table: row["table"].as_str().unwrap_or_default().to_string(),
            phenotype: row["phenotype"].as_str().unwrap_or_default().to_string(),
            rows: parse_u64(&row["rows"]),
            bytes_on_disk: parse_u64(&row["bytes_on_disk"]),
        })
        .collect())
}

fn parse_pipeline_status(json: &serde_json::Value) -> Result<Vec<IngestedPhenotype>, String> {
    let empty = vec![];
    let data = json
        .get("data")
        .and_then(|d| d.as_array())
        .unwrap_or(&empty);
    Ok(data
        .iter()
        .map(|row| IngestedPhenotype {
            phenotype: row["phenotype"].as_str().unwrap_or_default().to_string(),
            ancestry: row["ancestry"].as_str().unwrap_or_default().to_string(),
            status: row["status"].as_str().unwrap_or_default().to_string(),
            loci_count: parse_u64(&row["loci_count"]),
            significant_variants: parse_u64(&row["significant_variants"]),
        })
        .collect())
}

/// Parse a JSON value as u64, handling both number and string representations
/// (ClickHouse JSON format returns numbers as strings).
fn parse_u64(val: &serde_json::Value) -> u64 {
    val.as_u64()
        .or_else(|| val.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0)
}
