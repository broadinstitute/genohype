use crate::distributed::message::CatalogEntry;
use crate::manhattan::batch::PhenotypeInput;
use crate::manhattan::config::ManhattanJobConfig;
use genohype_core::codec::EncodedValue;
use genohype_core::query::QueryEngine;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CatalogState {
    pub config: ManhattanJobConfig,
    pub entries: Vec<CatalogEntry>,
    pub inputs: HashMap<(String, String), PhenotypeInput>,
}

pub fn load_catalog(config_path: &str) -> crate::Result<CatalogState> {
    println!("Loading catalog from config: {}", config_path);
    let path = std::path::Path::new(config_path);
    let config = ManhattanJobConfig::load(path)?;

    let assets_json = config.job.assets_json.as_ref().ok_or_else(|| {
        crate::HailError::InvalidFormat("job.assets_json is required in config".to_string())
    })?;

    // Load ALL assets (no filters) to populate the catalog
    let raw_inputs = crate::manhattan::batch::load_and_group_assets(
        assets_json, None, None, None, None,
    )?;

    let mut inputs_map = HashMap::new();
    let mut entries_map = HashMap::new();

    for input in raw_inputs {
        let key = (input.id.clone(), input.ancestry.clone());
        let entry = CatalogEntry {
            id: input.id.clone(),
            ancestry: input.ancestry.clone(),
            description: None,
            trait_type: None,
            n_cases: None,
            n_controls: None,
            has_exome: input.exome_path.is_some(),
            has_genome: input.genome_path.is_some(),
            has_gene_burden: input.gene_burden_path.is_some(),
            status: "idle".to_string(),
        };
        entries_map.insert(key.clone(), entry);
        inputs_map.insert(key, input);
    }

    // Enrich with metadata from Hail table
    let meta_path = "gs://aou_results/414k/utils/aou_phenotype_meta_info.ht";
    println!("Enriching catalog with metadata from {}", meta_path);

    if let Ok(engine) = QueryEngine::open_path(meta_path) {
        if let Ok(iter) = engine.query_iter(&[]) {
            for row_res in iter {
                if let Ok(EncodedValue::Struct(fields)) = row_res {
                    let get_str = |k: &str| -> Option<String> {
                        fields.iter().find(|(n, _)| n == k).and_then(|(_, v)| v.as_string())
                    };
                    let get_int = |k: &str| -> Option<i32> {
                        fields.iter().find(|(n, _)| n == k).and_then(|(_, v)| v.as_i32())
                    };

                    if let (Some(id), Some(ancestry)) = (get_str("phenoname"), get_str("ancestry")) {
                        let key = (id, ancestry);
                        if let Some(entry) = entries_map.get_mut(&key) {
                            entry.description = get_str("description");
                            entry.trait_type = get_str("trait_type");
                            entry.n_cases = get_int("n_cases");
                            entry.n_controls = get_int("n_controls");
                        }
                    }
                }
            }
        }
    } else {
        println!("Warning: Could not open metadata table at {}, skipping enrichment.", meta_path);
    }

    let mut entries: Vec<CatalogEntry> = entries_map.into_values().collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id).then(a.ancestry.cmp(&b.ancestry)));

    println!("Catalog loaded with {} phenotypes.", entries.len());

    Ok(CatalogState {
        config,
        entries,
        inputs: inputs_map,
    })
}
