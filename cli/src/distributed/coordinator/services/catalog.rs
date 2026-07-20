use crate::distributed::message::CatalogEntry;
use crate::manhattan::batch::PhenotypeInput;
use crate::manhattan::config::ManhattanJobConfig;
use genohype_core::codec::EncodedValue;
use genohype_core::query::QueryEngine;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct CatalogState {
    pub config: ManhattanJobConfig,
    pub entries: Vec<CatalogEntry>,
    pub inputs: HashMap<(String, String), PhenotypeInput>,
}

pub type CatalogLoadResult = (
    CatalogState,
    HashSet<(String, String)>,
    HashSet<(String, String)>,
);

/// Load catalog from a TOML config file path.
pub fn load_catalog(config_path: &str) -> crate::Result<CatalogLoadResult> {
    println!("Loading catalog from config: {}", config_path);
    let path = std::path::Path::new(config_path);
    let config = ManhattanJobConfig::load(path)?;
    build_catalog(config)
}

/// Load catalog from an already-parsed config (e.g. embedded in a job spec).
pub fn load_catalog_from_config(config: ManhattanJobConfig) -> crate::Result<CatalogLoadResult> {
    println!("Loading catalog from embedded config");
    build_catalog(config)
}

/// Load catalog from just an assets JSON path (no TOML config needed).
/// Uses default settings - suitable for browsing the full phenotype list.
pub fn load_catalog_from_assets(assets_json: &str) -> crate::Result<CatalogLoadResult> {
    println!("Loading catalog from assets JSON: {}", assets_json);
    let mut config = ManhattanJobConfig::default();
    config.job.assets_json = Some(assets_json.to_string());
    build_catalog(config)
}

fn fetch_clickhouse_status(url: &str) -> HashSet<(String, String)> {
    let mut set = HashSet::new();

    #[cfg(feature = "clickhouse")]
    {
        let query = "SELECT phenotype, ancestry FROM pipeline_status WHERE status IN ('INGESTED', 'COMPLETED') FORMAT JSON";
        let formatted_url = format!("{}/?default_format=JSON", url.trim_end_matches('/'));

        if let Ok(resp) = reqwest::blocking::Client::new()
            .post(&formatted_url)
            .body(query)
            .send()
        {
            if let Ok(json) = resp.json::<serde_json::Value>() {
                if let Some(arr) = json.get("data").and_then(|d| d.as_array()) {
                    for row in arr {
                        if let (Some(p), Some(a)) = (
                            row.get("phenotype").and_then(|v| v.as_str()),
                            row.get("ancestry").and_then(|v| v.as_str()),
                        ) {
                            set.insert((p.to_string(), a.to_string()));
                        }
                    }
                }
            }
        }
    }

    #[cfg(not(feature = "clickhouse"))]
    let _ = url;

    set
}

fn build_catalog(config: ManhattanJobConfig) -> crate::Result<CatalogLoadResult> {
    let assets_json = config.job.assets_json.as_ref().ok_or_else(|| {
        crate::HailError::InvalidFormat("job.assets_json is required in config".to_string())
    })?;

    // Perform storage & ClickHouse scans to detect pre-existing progress
    let mut completed_phenos = HashSet::new();
    if let Some(ref output_dir) = config.job.output_dir {
        let base_dir = output_dir.trim_end_matches('/');

        // 1. Read legacy .completed file (backwards compatibility)
        let legacy_path = format!("{}/.completed", base_dir);
        if let Ok(completed_set) = crate::cloud::pool::read_completed_checkpoint(&legacy_path) {
            for rel_path in completed_set {
                let parts: Vec<&str> = rel_path.split('/').collect();
                if parts.len() >= 2 {
                    completed_phenos.insert((parts[1].to_string(), parts[0].to_string()));
                }
            }
        }

        // 2. Read new .completed_phenos/ marker files (race-free)
        let markers_dir = format!("{}/.completed_phenos", base_dir);
        if let Ok(markers) = crate::cloud::pool::list_completed_markers(&markers_dir) {
            for marker in markers {
                // Marker format: ancestry_id (e.g., "meta_1740556")
                if let Some((ancestry, id)) = marker.split_once('_') {
                    completed_phenos.insert((id.to_string(), ancestry.to_string()));
                }
            }
        }

        if !completed_phenos.is_empty() {
            println!(
                "  Storage scan: found {} completed phenotypes",
                completed_phenos.len()
            );
        }
    }

    let mut ingested_phenos = HashSet::new();
    if let Some(ref ch_url) = config.ingest.clickhouse_url {
        ingested_phenos = fetch_clickhouse_status(ch_url);
        if !ingested_phenos.is_empty() {
            println!(
                "  ClickHouse scan: found {} ingested phenotypes",
                ingested_phenos.len()
            );
        }
    }

    // Load ALL assets (no filters) to populate the catalog
    let raw_inputs =
        crate::manhattan::batch::load_and_group_assets(assets_json, None, None, None, None)?;

    let mut inputs_map = HashMap::new();
    let mut entries_map = HashMap::new();

    for input in raw_inputs {
        let key = (input.id.clone(), input.ancestry.clone());

        let status = if ingested_phenos.contains(&key) {
            "ingested".to_string()
        } else if completed_phenos.contains(&key) {
            "completed".to_string()
        } else {
            "idle".to_string()
        };

        let entry = CatalogEntry {
            id: input.id.clone(),
            ancestry: input.ancestry.clone(),
            description: None,
            category: None,
            trait_type: None,
            n_cases: None,
            n_controls: None,
            has_exome: input.exome_path.is_some(),
            has_genome: input.genome_path.is_some(),
            has_gene_burden: input.gene_burden_path.is_some(),
            status,
        };
        entries_map.insert(key.clone(), entry);
        inputs_map.insert(key, input);
    }

    // Enrich with metadata from Hail table if configured
    if let Some(meta_path) = &config.job.metadata_path {
        println!("Enriching catalog with metadata from {}", meta_path);

        if let Ok(engine) = QueryEngine::open_path(meta_path) {
            if let Ok(iter) = engine.query_iter(&[]) {
                for row_res in iter {
                    if let Ok(EncodedValue::Struct(fields)) = row_res {
                        let get_str = |k: &str| -> Option<String> {
                            fields
                                .iter()
                                .find(|(n, _)| n == k)
                                .and_then(|(_, v)| v.as_string())
                        };
                        let get_int = |k: &str| -> Option<i32> {
                            fields
                                .iter()
                                .find(|(n, _)| n == k)
                                .and_then(|(_, v)| v.as_i32())
                        };

                        if let (Some(id), Some(ancestry)) =
                            (get_str("phenoname"), get_str("ancestry"))
                        {
                            // Metadata table uses uppercase ancestry (e.g. "META"),
                            // assets JSON uses lowercase (e.g. "meta"). Normalize.
                            let key = (id, ancestry.to_lowercase());
                            if let Some(entry) = entries_map.get_mut(&key) {
                                entry.description = get_str("description");
                                entry.category =
                                    get_str("category").or_else(|| get_str("phecode_category"));
                                entry.trait_type = get_str("trait_type");
                                entry.n_cases = get_int("n_cases");
                                entry.n_controls = get_int("n_controls");
                            }
                        }
                    }
                }
            }
        } else {
            println!(
                "Warning: Could not open metadata table at {}, skipping enrichment.",
                meta_path
            );
        }
    } else {
        println!("No metadata_path provided in config, skipping enrichment.");
    }

    let mut entries: Vec<CatalogEntry> = entries_map.into_values().collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id).then(a.ancestry.cmp(&b.ancestry)));

    println!("Catalog loaded with {} phenotypes.", entries.len());

    Ok((
        CatalogState {
            config,
            entries,
            inputs: inputs_map,
        },
        completed_phenos,
        ingested_phenos,
    ))
}
