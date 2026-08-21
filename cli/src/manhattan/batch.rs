//! Batch processing logic for Manhattan plots.
//!
//! Parses assets.json files into phenotype groups and generates ManhattanSpecs.

use crate::distributed::message::ManhattanSpec;
use crate::manhattan::config::ManhattanConfig;
use crate::{HailError, Result};
use serde::Deserialize;
use std::collections::HashMap;

/// A single entry from the assets.json file.
#[derive(Debug, Deserialize)]
pub struct AssetEntry {
    pub ancestry_group: String,
    pub analysis_id: String,
    pub uri: String,
    pub asset_type: String, // "variant", "gene", "variant_exp_p", etc.
    pub sequencing_type: Option<String>, // "exomes", "genomes" (only for variant)
}

/// A grouped phenotype ready to be converted to a spec.
#[derive(Debug, Clone)]
pub struct PhenotypeInput {
    pub ancestry: String,
    pub id: String,
    pub exome_path: Option<String>,
    pub genome_path: Option<String>,
    pub gene_burden_path: Option<String>,
    /// Expected p-values for exome QQ plots
    pub exome_exp_p_path: Option<String>,
    /// Expected p-values for genome QQ plots
    pub genome_exp_p_path: Option<String>,
}

/// Configuration for batch generation (subset of CLI args).
pub struct BatchConfig {
    pub output_dir: String,
    pub threshold: f64,
    pub gene_threshold: f64,
    pub locus_threshold: f64,
    pub locus_window: i32,
    pub locus_plots: bool,
    pub min_variants_per_locus: usize,
    pub width: u32,
    pub height: u32,
    pub y_field: String,
    pub genes_path: Option<String>,
    pub exome_annotations: Option<String>,
    pub genome_annotations: Option<String>,
    /// Styling configuration for plots
    pub styling: ManhattanConfig,
    /// Export only the specialized gene association Parquet.
    pub gene_associations_only: bool,
}

/// Summary statistics for loaded batch.
#[derive(Debug, Default)]
pub struct BatchSummary {
    pub total_phenotypes: usize,
    pub exome_only: usize,
    pub genome_only: usize,
    pub combined: usize,
    pub gene_burden_only: usize,
    pub by_ancestry: HashMap<String, usize>,
}

impl BatchSummary {
    /// Build summary from phenotype inputs.
    pub fn from_inputs(inputs: &[PhenotypeInput]) -> Self {
        let mut summary = BatchSummary {
            total_phenotypes: inputs.len(),
            ..Default::default()
        };

        for input in inputs {
            // Count by ancestry
            *summary
                .by_ancestry
                .entry(input.ancestry.clone())
                .or_insert(0) += 1;

            // Count by input type
            let has_exome = input.exome_path.is_some();
            let has_genome = input.genome_path.is_some();
            let has_gene = input.gene_burden_path.is_some();

            match (has_exome, has_genome, has_gene) {
                (true, true, _) => summary.combined += 1,
                (true, false, false) => summary.exome_only += 1,
                (false, true, false) => summary.genome_only += 1,
                (false, false, true) => summary.gene_burden_only += 1,
                _ => {
                    // Has gene burden but also exome/genome - count as combined
                    if has_gene && (has_exome || has_genome) {
                        summary.combined += 1;
                    }
                }
            }
        }

        summary
    }
}

/// Load assets.json, filter, and group into phenotypes.
///
/// Supports two JSON formats:
/// 1. Plain array: `[{...}, {...}]`
/// 2. Object with assets field: `{"assets": [{...}, {...}]}`
///
/// Fetch bytes from an object store path.
#[cfg(feature = "gcp")]
async fn fetch_bytes(
    client: &std::sync::Arc<dyn object_store::ObjectStore>,
    path: &object_store::path::Path,
) -> Result<Vec<u8>> {
    let result = client.get(path).await.map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("GCS get failed: {}", e),
        ))
    })?;
    let bytes = result.bytes().await.map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("GCS read failed: {}", e),
        ))
    })?;
    Ok(bytes.to_vec())
}

/// Read a file from GCS using the object_store crate.
#[cfg(feature = "gcp")]
fn read_gcs_file(gs_path: &str) -> Result<Vec<u8>> {
    use genohype_core::io::adapter::get_gcs_client;
    use object_store::path::Path as ObjPath;

    let url = url::Url::parse(gs_path).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid GCS URL: {}", e),
        ))
    })?;
    let bucket = url.host_str().ok_or_else(|| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Missing bucket in GCS URL",
        ))
    })?;
    let obj_path = ObjPath::from(url.path());
    let client = get_gcs_client(bucket)?;

    // Use Handle::current() if inside a tokio runtime, else create a new one.
    // This supports both async contexts (coordinator) and sync contexts (CLI).
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(fetch_bytes(&client, &obj_path)))
    } else {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        rt.block_on(fetch_bytes(&client, &obj_path))
    }
}

#[cfg(not(feature = "gcp"))]
fn read_gcs_file(_gs_path: &str) -> Result<Vec<u8>> {
    Err(HailError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "GCS support requires the 'gcp' feature",
    )))
}

/// Parameter behavior:
/// - `analysis_ids`: Guaranteed inclusions (always in result, processed first)
/// - `ancestries`: Filter applied to all phenotypes
/// - `sample`: Fraction of remaining phenotypes to randomly sample (after guaranteed)
/// - `limit`: Total max phenotypes (guaranteed + sampled)
///
/// Example: analysis_ids=[A,B,C], sample=0.1, limit=100
/// Result: A, B, C (guaranteed) + up to 97 randomly sampled phenotypes
pub fn load_and_group_assets(
    path: &str,
    analysis_ids: Option<&[String]>,
    ancestries: Option<&[String]>,
    sample: Option<f64>,
    limit: Option<usize>,
) -> Result<Vec<PhenotypeInput>> {
    // Support GCS paths using object_store
    let json_bytes: Vec<u8> = if path.starts_with("gs://") {
        read_gcs_file(path)?
    } else {
        std::fs::read(path).map_err(HailError::Io)?
    };

    let reader = std::io::Cursor::new(&json_bytes);

    // Try to parse as wrapped object first, fall back to plain array
    let value: serde_json::Value = serde_json::from_reader(reader)?;

    let assets: Vec<AssetEntry> = if value.is_array() {
        // Plain array format
        serde_json::from_value(value)?
    } else if let Some(assets_array) = value.get("assets") {
        // Wrapped object format: {"assets": [...]}
        serde_json::from_value(assets_array.clone())?
    } else {
        return Err(HailError::InvalidFormat(
            "Assets JSON must be an array or an object with an 'assets' field".to_string(),
        ));
    };

    // Build a set of guaranteed IDs for quick lookup
    let guaranteed_ids: std::collections::HashSet<&String> = analysis_ids
        .map(|ids| ids.iter().collect())
        .unwrap_or_default();

    // Group by (ancestry, analysis_id) into two buckets:
    // 1. guaranteed: phenotypes in analysis_ids (always included)
    // 2. candidates: all other phenotypes (for sampling)
    let mut guaranteed: HashMap<String, PhenotypeInput> = HashMap::new();
    let mut candidates: HashMap<String, PhenotypeInput> = HashMap::new();

    for asset in assets {
        // Filter by ancestry if requested (applies to both guaranteed and candidates)
        if let Some(ancs) = ancestries {
            if !ancs.contains(&asset.ancestry_group) {
                continue;
            }
        }

        // Only process variant, gene, and variant_exp_p assets
        if asset.asset_type != "variant"
            && asset.asset_type != "gene"
            && asset.asset_type != "variant_exp_p"
        {
            continue;
        }

        let key = format!("{}:{}", asset.ancestry_group, asset.analysis_id);
        let is_guaranteed = guaranteed_ids.contains(&asset.analysis_id);

        let groups = if is_guaranteed {
            &mut guaranteed
        } else {
            &mut candidates
        };

        let entry = groups.entry(key).or_insert_with(|| PhenotypeInput {
            ancestry: asset.ancestry_group.clone(),
            id: asset.analysis_id.clone(),
            exome_path: None,
            genome_path: None,
            gene_burden_path: None,
            exome_exp_p_path: None,
            genome_exp_p_path: None,
        });

        match asset.asset_type.as_str() {
            "gene" => {
                entry.gene_burden_path = Some(asset.uri);
            }
            "variant" => match asset.sequencing_type.as_deref() {
                Some("exomes") => entry.exome_path = Some(asset.uri),
                Some("genomes") => entry.genome_path = Some(asset.uri),
                _ => {}
            },
            "variant_exp_p" => match asset.sequencing_type.as_deref() {
                Some("exomes") => entry.exome_exp_p_path = Some(asset.uri),
                Some("genomes") => entry.genome_exp_p_path = Some(asset.uri),
                _ => {}
            },
            _ => {}
        }
    }

    // Start with guaranteed phenotypes
    let mut result: Vec<PhenotypeInput> = guaranteed.into_values().collect();
    result.sort_by(|a, b| a.ancestry.cmp(&b.ancestry).then(a.id.cmp(&b.id)));

    let guaranteed_count = result.len();

    // Calculate how many additional phenotypes we can add
    let remaining_slots = limit.map(|n| n.saturating_sub(guaranteed_count));

    // If we have room for more and there are candidates, sample from them
    if remaining_slots.map(|s| s > 0).unwrap_or(true) && !candidates.is_empty() {
        let mut candidate_list: Vec<PhenotypeInput> = candidates.into_values().collect();

        // Sample from candidates if requested
        if let Some(frac) = sample {
            use rand::seq::SliceRandom;
            use rand::SeedableRng;

            let frac = frac.clamp(0.0, 1.0);
            let sample_count = ((candidate_list.len() as f64) * frac).ceil() as usize;

            // Use seeded RNG for reproducibility
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            candidate_list.shuffle(&mut rng);
            candidate_list.truncate(sample_count);
        }

        // Sort candidates for deterministic order
        candidate_list.sort_by(|a, b| a.ancestry.cmp(&b.ancestry).then(a.id.cmp(&b.id)));

        // Apply remaining slots limit if set
        if let Some(slots) = remaining_slots {
            candidate_list.truncate(slots);
        }

        // Append sampled candidates to guaranteed
        result.extend(candidate_list);
    }

    // Final sort for consistent output order
    result.sort_by(|a, b| a.ancestry.cmp(&b.ancestry).then(a.id.cmp(&b.id)));

    Ok(result)
}

/// Enforce the clone-oriented gene-only selection contract.
///
/// When explicit analysis IDs are supplied they are an exact request in this
/// mode (not merely guaranteed additions): each unique ID must resolve to one
/// ancestry-filtered input and that input must have a gene asset. Without
/// explicit IDs, every input selected by the normal filters must have one.
pub(crate) fn validate_gene_associations_only_inputs(
    mut inputs: Vec<PhenotypeInput>,
    requested_ids: Option<&[String]>,
) -> Result<Vec<PhenotypeInput>> {
    if let Some(requested_ids) = requested_ids {
        let requested: std::collections::HashSet<&str> =
            requested_ids.iter().map(String::as_str).collect();
        if requested.len() != requested_ids.len() {
            return Err(HailError::InvalidFormat(
                "gene-associations-only analysis_ids must be unique".to_string(),
            ));
        }

        // load_and_group_assets treats analysis_ids as guaranteed additions;
        // gene-only migration treats them as the exact manifest instead.
        inputs.retain(|input| requested.contains(input.id.as_str()));
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for input in &inputs {
            *counts.entry(input.id.as_str()).or_default() += 1;
        }
        let missing: Vec<&str> = requested
            .iter()
            .copied()
            .filter(|id| !counts.contains_key(id))
            .collect();
        let ambiguous: Vec<&str> = requested
            .iter()
            .copied()
            .filter(|id| counts.get(id).copied().unwrap_or_default() != 1)
            .collect();
        if !missing.is_empty() || !ambiguous.is_empty() {
            return Err(HailError::InvalidFormat(format!(
                "gene-associations-only requested {} phenotype IDs but could not resolve exactly one input per ID (missing: {:?}; non-unique after ancestry filtering: {:?})",
                requested_ids.len(), missing, ambiguous
            )));
        }
    }

    let without_gene: Vec<String> = inputs
        .iter()
        .filter(|input| input.gene_burden_path.is_none())
        .map(|input| format!("{}:{}", input.ancestry, input.id))
        .collect();
    if !without_gene.is_empty() {
        return Err(HailError::InvalidFormat(format!(
            "gene-associations-only requires a gene asset for every selected phenotype; {} missing: {}",
            without_gene.len(),
            without_gene.join(", ")
        )));
    }

    if let Some(requested_ids) = requested_ids {
        if inputs.len() != requested_ids.len() {
            return Err(HailError::InvalidFormat(format!(
                "gene-associations-only requested {} phenotype IDs but produced {} inputs",
                requested_ids.len(),
                inputs.len()
            )));
        }
    }
    Ok(inputs)
}

/// Convert grouped inputs into ManhattanSpecs.
pub fn create_specs(inputs: Vec<PhenotypeInput>, config: &BatchConfig) -> Vec<ManhattanSpec> {
    inputs
        .into_iter()
        .map(|input| {
            // Construct output path: {base}/{ancestry}/{id}
            // e.g. gs://bucket/manhattans/v8/meta/3016293
            let output_path = format!(
                "{}/{}/{}",
                config.output_dir.trim_end_matches('/'),
                input.ancestry,
                input.id
            );

            ManhattanSpec {
                // Identity metadata (for ClickHouse ingestion)
                phenotype: Some(input.id.clone()),
                ancestry: Some(input.ancestry.clone()),

                exome: if config.gene_associations_only {
                    None
                } else {
                    input.exome_path
                },
                exome_annotations: if config.gene_associations_only {
                    None
                } else {
                    config.exome_annotations.clone()
                },
                genome: if config.gene_associations_only {
                    None
                } else {
                    input.genome_path
                },
                genome_annotations: if config.gene_associations_only {
                    None
                } else {
                    config.genome_annotations.clone()
                },
                gene_burden: input.gene_burden_path,
                genes: if config.gene_associations_only {
                    None
                } else {
                    config.genes_path.clone()
                },
                exome_exp_p: if config.gene_associations_only {
                    None
                } else {
                    input.exome_exp_p_path
                },
                genome_exp_p: if config.gene_associations_only {
                    None
                } else {
                    input.genome_exp_p_path
                },
                gene_associations_only: config.gene_associations_only,

                threshold: config.threshold,
                gene_threshold: config.gene_threshold,
                locus_threshold: config.locus_threshold,
                locus_window: config.locus_window,
                locus_plots: config.locus_plots,
                min_variants_per_locus: config.min_variants_per_locus,

                width: config.width,
                height: config.height,
                y_field: config.y_field.clone(),
                output_path,

                // Runtime fields (filled by coordinator)
                layout: None,
                y_scale: None,
                contig_lengths: None,  // Filled by coordinator
                skip_composite: false, // Batch mode assumes we want the final output
                exome_partitions: None,
                genome_partitions: None,

                // Styling configuration
                styling: config.styling.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_assets() {
        let json = r#"[
            {"ancestry_group": "meta", "analysis_id": "1001", "uri": "gs://bucket/exome.ht", "asset_type": "variant", "sequencing_type": "exomes"},
            {"ancestry_group": "meta", "analysis_id": "1001", "uri": "gs://bucket/genome.ht", "asset_type": "variant", "sequencing_type": "genomes"},
            {"ancestry_group": "meta", "analysis_id": "1001", "uri": "gs://bucket/gene.ht", "asset_type": "gene", "sequencing_type": null},
            {"ancestry_group": "afr", "analysis_id": "1001", "uri": "gs://bucket/afr_exome.ht", "asset_type": "variant", "sequencing_type": "exomes"}
        ]"#;

        // Write to temp file
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("assets.json");
        std::fs::write(&path, json).unwrap();

        let result = load_and_group_assets(path.to_str().unwrap(), None, None, None, None).unwrap();

        assert_eq!(result.len(), 2);

        // Results are sorted by ancestry then id
        let afr = &result[0];
        assert_eq!(afr.ancestry, "afr");
        assert_eq!(afr.id, "1001");
        assert!(afr.exome_path.is_some());
        assert!(afr.genome_path.is_none());

        let meta = &result[1];
        assert_eq!(meta.ancestry, "meta");
        assert_eq!(meta.id, "1001");
        assert!(meta.exome_path.is_some());
        assert!(meta.genome_path.is_some());
        assert!(meta.gene_burden_path.is_some());
    }

    #[test]
    fn test_group_assets_wrapped_format() {
        // Test the {"assets": [...]} wrapper format
        let json = r#"{
            "assets": [
                {"ancestry_group": "meta", "analysis_id": "1001", "uri": "gs://bucket/exome.ht", "asset_type": "variant", "sequencing_type": "exomes"},
                {"ancestry_group": "afr", "analysis_id": "1002", "uri": "gs://bucket/genome.ht", "asset_type": "variant", "sequencing_type": "genomes"}
            ]
        }"#;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("assets.json");
        std::fs::write(&path, json).unwrap();

        let result = load_and_group_assets(path.to_str().unwrap(), None, None, None, None).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].ancestry, "afr");
        assert_eq!(result[0].id, "1002");
        assert_eq!(result[1].ancestry, "meta");
        assert_eq!(result[1].id, "1001");
    }

    #[test]
    fn gene_only_explicit_ids_are_exact_and_require_gene_assets() {
        let input = |id: &str, gene: bool| PhenotypeInput {
            ancestry: "meta".to_string(),
            id: id.to_string(),
            exome_path: Some(format!("gs://bucket/{id}.ht")),
            genome_path: None,
            gene_burden_path: gene.then(|| format!("gs://bucket/{id}-gene.ht")),
            exome_exp_p_path: None,
            genome_exp_p_path: None,
        };
        let requested = vec!["1001".to_string(), "1002".to_string()];

        let selected = validate_gene_associations_only_inputs(
            vec![
                input("1001", true),
                input("1002", true),
                input("extra", true),
            ],
            Some(&requested),
        )
        .unwrap();
        assert_eq!(selected.len(), requested.len());
        assert_eq!(
            selected
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["1001", "1002"]
        );

        let error = validate_gene_associations_only_inputs(
            vec![input("1001", true), input("1002", false)],
            Some(&requested),
        )
        .unwrap_err();
        assert!(error.to_string().contains("meta:1002"));

        let error =
            validate_gene_associations_only_inputs(vec![input("1001", true)], Some(&requested))
                .unwrap_err();
        assert!(error.to_string().contains("missing"));
    }

    #[test]
    fn test_create_specs() {
        let inputs = vec![PhenotypeInput {
            ancestry: "meta".to_string(),
            id: "1001".to_string(),
            exome_path: Some("gs://bucket/exome.ht".to_string()),
            genome_path: Some("gs://bucket/genome.ht".to_string()),
            gene_burden_path: None,
            exome_exp_p_path: None,
            genome_exp_p_path: None,
        }];

        let config = BatchConfig {
            output_dir: "gs://bucket/output".to_string(),
            threshold: 5e-8,
            gene_threshold: 2.5e-6,
            locus_threshold: 0.01,
            locus_window: 1_000_000,
            locus_plots: false,
            min_variants_per_locus: 1,
            width: 3000,
            height: 800,
            y_field: "Pvalue".to_string(),
            genes_path: None,
            exome_annotations: None,
            genome_annotations: None,
            styling: ManhattanConfig::default(),
            gene_associations_only: false,
        };

        let specs = create_specs(inputs, &config);

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].output_path, "gs://bucket/output/meta/1001");
        assert_eq!(specs[0].exome, Some("gs://bucket/exome.ht".to_string()));
        assert_eq!(specs[0].genome, Some("gs://bucket/genome.ht".to_string()));
        // Verify identity metadata is set
        assert_eq!(specs[0].phenotype, Some("1001".to_string()));
        assert_eq!(specs[0].ancestry, Some("meta".to_string()));
    }

    #[test]
    fn gene_associations_only_specs_drop_all_unrelated_inputs() {
        let inputs = vec![PhenotypeInput {
            ancestry: "meta".to_string(),
            id: "1001".to_string(),
            exome_path: Some("gs://bucket/exome.ht".to_string()),
            genome_path: Some("gs://bucket/genome.ht".to_string()),
            gene_burden_path: Some("gs://bucket/gene.ht".to_string()),
            exome_exp_p_path: Some("gs://bucket/exome-qq.ht".to_string()),
            genome_exp_p_path: Some("gs://bucket/genome-qq.ht".to_string()),
        }];
        let config = BatchConfig {
            output_dir: "gs://bucket/output".to_string(),
            threshold: 5e-8,
            gene_threshold: 2.5e-6,
            locus_threshold: 0.01,
            locus_window: 1_000_000,
            locus_plots: true,
            min_variants_per_locus: 1,
            width: 3000,
            height: 800,
            y_field: "Pvalue".to_string(),
            genes_path: Some("gs://bucket/genes.ht".to_string()),
            exome_annotations: Some("gs://bucket/exome-annotations.ht".to_string()),
            genome_annotations: Some("gs://bucket/genome-annotations.ht".to_string()),
            styling: ManhattanConfig::default(),
            gene_associations_only: true,
        };

        let specs = create_specs(inputs, &config);
        let spec = &specs[0];
        assert!(spec.gene_associations_only);
        assert_eq!(spec.gene_burden.as_deref(), Some("gs://bucket/gene.ht"));
        assert!(spec.exome.is_none());
        assert!(spec.genome.is_none());
        assert!(spec.exome_exp_p.is_none());
        assert!(spec.genome_exp_p.is_none());
        assert!(spec.genes.is_none());
        assert!(spec.exome_annotations.is_none());
        assert!(spec.genome_annotations.is_none());
    }

    #[test]
    fn test_batch_summary() {
        let inputs = vec![
            PhenotypeInput {
                ancestry: "meta".to_string(),
                id: "1001".to_string(),
                exome_path: Some("gs://bucket/exome.ht".to_string()),
                genome_path: Some("gs://bucket/genome.ht".to_string()),
                gene_burden_path: None,
                exome_exp_p_path: None,
                genome_exp_p_path: None,
            },
            PhenotypeInput {
                ancestry: "meta".to_string(),
                id: "1002".to_string(),
                exome_path: Some("gs://bucket/exome.ht".to_string()),
                genome_path: None,
                gene_burden_path: None,
                exome_exp_p_path: None,
                genome_exp_p_path: None,
            },
            PhenotypeInput {
                ancestry: "afr".to_string(),
                id: "1001".to_string(),
                exome_path: None,
                genome_path: Some("gs://bucket/genome.ht".to_string()),
                gene_burden_path: None,
                exome_exp_p_path: None,
                genome_exp_p_path: None,
            },
        ];

        let summary = BatchSummary::from_inputs(&inputs);

        assert_eq!(summary.total_phenotypes, 3);
        assert_eq!(summary.combined, 1);
        assert_eq!(summary.exome_only, 1);
        assert_eq!(summary.genome_only, 1);
        assert_eq!(summary.by_ancestry.get("meta"), Some(&2));
        assert_eq!(summary.by_ancestry.get("afr"), Some(&1));
    }
}
