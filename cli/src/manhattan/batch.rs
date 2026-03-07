//! Batch processing logic for Manhattan plots.
//!
//! Parses assets.json files into phenotype groups and generates ManhattanSpecs.

use crate::distributed::message::ManhattanSpec;
use crate::manhattan::config::ManhattanConfig;
use crate::{HailError, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

/// A single entry from the assets.json file.
#[derive(Debug, Deserialize)]
pub struct AssetEntry {
    pub ancestry_group: String,
    pub analysis_id: String,
    pub uri: String,
    pub asset_type: String,      // "variant", "gene", "variant_exp_p", etc.
    pub sequencing_type: Option<String>, // "exomes", "genomes" (only for variant)
}

/// A grouped phenotype ready to be converted to a spec.
#[derive(Debug)]
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
            *summary.by_ancestry.entry(input.ancestry.clone()).or_insert(0) += 1;

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
    let file = File::open(path).map_err(HailError::Io)?;
    let reader = BufReader::new(file);

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
            "Assets JSON must be an array or an object with an 'assets' field".to_string()
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

                exome: input.exome_path,
                exome_annotations: config.exome_annotations.clone(),
                genome: input.genome_path,
                genome_annotations: config.genome_annotations.clone(),
                gene_burden: input.gene_burden_path,
                genes: config.genes_path.clone(),
                exome_exp_p: input.exome_exp_p_path,
                genome_exp_p: input.genome_exp_p_path,

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
                contig_lengths: None, // Filled by coordinator
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
