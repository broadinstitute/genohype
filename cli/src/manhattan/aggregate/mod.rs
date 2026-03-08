//! Manhattan aggregate phase (Phase 2 of V2 pipeline).
//!
//! This module handles the aggregation of scan phase outputs:
//! 1. Compositing partial PNGs into final Manhattan plots
//! 2. Processing gene burden table
//! 3. Merging significant hits from scan phase
//! 4. Generating locus plots for significant regions
//! 5. Writing manifest.json

pub mod io;
pub mod locus;
pub mod merge;
pub mod utils;

use crate::distributed::message::ManhattanAggregateSpec;
use crate::manhattan::config::{BackgroundStyle, PlotType};
use crate::manhattan::data::{
    Manifest, ManifestInputs, ManifestLocus, ManifestManhattan, ManifestManhattans,
    ManifestSigHits, ManifestSignificantHits, ManifestStats,
};
use crate::manhattan::genes::{
    render_gene_manhattan_styled, scan_gene_burden_to_parquet, scan_qq_to_parquet,
};
use crate::manhattan::layout::ChromosomeLayout;
use crate::manhattan::pipeline::composite_partial_pngs_with_style;
use genohype_core::error::Result;
use genohype_core::io::is_cloud_path;
use genohype_core::query::QueryEngine;
use std::collections::HashMap;
use std::time::Instant;

use io::{
    cleanup_chrom_intermediates, cleanup_intermediates, discover_chromosomes,
    has_partial_pngs, write_locus_file,
};
use locus::{generate_locus_plots, generate_loci_from_parquet};
use merge::{merge_and_combine_hits, merge_significant_hits};
use utils::{chrono_now_iso, extract_phenotype_name};

// Re-export public items
pub use io::get_gcs_dir_size;
pub use locus::{extract_sig_positions, SigPosition};

/// Generate locus plots from an existing Manhattan output directory (standalone CLI command).
///
/// This allows generating locus plots after the scan/merge phase has completed,
/// without re-running the full pipeline.
pub fn generate_loci_standalone(
    output_dir: &str,
    exome_table: Option<&str>,
    genome_table: Option<&str>,
    gene_burden_table: Option<&str>,
    locus_window: i32,
    threshold: f64,
    gene_threshold: f64,
    num_threads: usize,
    render_images: bool,
    min_variants_per_locus: usize,
) -> Result<Vec<ManifestLocus>> {
    let output_base = output_dir.trim_end_matches('/');

    // Extract phenotype from path; use default ancestry
    let phenotype = extract_phenotype_name(output_base);
    let ancestry = "meta".to_string();

    let loci = generate_loci_from_parquet(
        output_base,
        exome_table,
        genome_table,
        gene_burden_table,
        locus_window,
        threshold,
        gene_threshold,
        num_threads,
        &phenotype,
        &ancestry,
        None, // No styling config for standalone CLI
        render_images,
        min_variants_per_locus,
    )?;

    // Update manifest.json with loci info
    update_manifest_with_loci(output_base, &loci)?;

    Ok(loci)
}

/// Update the manifest.json file with loci information.
fn update_manifest_with_loci(output_base: &str, loci: &[ManifestLocus]) -> Result<()> {
    let manifest_path = format!("{}/manifest.json", output_base);

    // Read existing manifest
    let manifest_content = if is_cloud_path(&manifest_path) {
        use genohype_core::io::{get_file_size, range_read};
        let size = get_file_size(&manifest_path)?;
        let data = range_read(&manifest_path, 0, size as usize)?;
        String::from_utf8(data).map_err(|e| crate::HailError::InvalidFormat(e.to_string()))?
    } else {
        std::fs::read_to_string(&manifest_path)?
    };

    // Parse and update
    let mut manifest: serde_json::Value = serde_json::from_str(&manifest_content)?;

    manifest["loci"] = serde_json::to_value(loci)?;
    manifest["stats"]["total_loci"] = serde_json::json!(loci.len());

    // Write back
    let updated = serde_json::to_string_pretty(&manifest)?;

    if is_cloud_path(&manifest_path) {
        use genohype_core::io::CloudWriter;
        use std::io::Write;
        let mut writer = CloudWriter::new(&manifest_path)?;
        writer.write_all(updated.as_bytes())?;
        writer.finish()?;
    } else {
        std::fs::write(&manifest_path, &updated)?;
    }

    println!("  Updated manifest.json with {} loci", loci.len());
    Ok(())
}

/// Run the Manhattan aggregation phase.
///
/// This is called by the worker when assigned a ManhattanAggregate job.
/// Returns (row_count, summary_json) where summary_json contains stats about the aggregation.
pub fn run_aggregation(spec: &ManhattanAggregateSpec) -> Result<(usize, serde_json::Value)> {
    let start = Instant::now();
    let scan_duration = 0.0; // We don't know the scan duration here

    println!("Starting Manhattan aggregation phase...");

    let output_base = spec.output_path.trim_end_matches('/');

    // Create plots directory for consolidated output
    let plots_dir = format!("{}/plots", output_base);
    if !is_cloud_path(&plots_dir) {
        std::fs::create_dir_all(&plots_dir)?;
    }

    // Resolve styles for compositing
    let exome_style = spec.styling.resolve(PlotType::Exome);
    let genome_style = spec.styling.resolve(PlotType::Genome);

    // Step 1: Composite PNGs
    println!("  Compositing partial PNGs...");
    let exome_count = if spec.exome_results.is_some() {
        composite_source_pngs(output_base, "exome", spec.width, spec.height, &exome_style.background)?
    } else {
        0
    };

    let genome_count = if spec.genome_results.is_some() {
        composite_source_pngs(output_base, "genome", spec.width, spec.height, &genome_style.background)?
    } else {
        0
    };

    // Step 1b: Composite Per-Chromosome PNGs
    println!("  Compositing per-chromosome PNGs...");
    let chroms_dir = format!("{}/chroms", output_base);
    let mut chrom_manhattans: HashMap<String, ManifestManhattans> = HashMap::new();

    // Discover chromosomes that have outputs
    let discovered_chroms = discover_chromosomes(&chroms_dir)?;
    if !discovered_chroms.is_empty() {
        println!("    Found {} chromosomes with data", discovered_chroms.len());
    }

    for chrom in discovered_chroms {
        let mut chrom_entry = ManifestManhattans {
            exome: None,
            genome: None,
            gene: None,
        };
        let chrom_path_base = format!("{}/{}", chroms_dir, chrom);
        let mut has_data = false;

        // Exome
        if spec.exome_results.is_some() {
            let exome_parts = format!("{}/exome", chrom_path_base);
            if has_partial_pngs(&exome_parts)? {
                let out = format!("{}/exome_manhattan.png", chrom_path_base);
                composite_partial_pngs_with_style(&exome_parts, &out, spec.width, spec.height, 0.0, &exome_style.background)?;
                chrom_entry.exome = Some(ManifestManhattan {
                    png: format!("{}/chroms/{}/exome_manhattan.png", output_base, chrom),
                    count: 0,
                });
                has_data = true;
            }
        }

        // Genome
        if spec.genome_results.is_some() {
            let genome_parts = format!("{}/genome", chrom_path_base);
            if has_partial_pngs(&genome_parts)? {
                let out = format!("{}/genome_manhattan.png", chrom_path_base);
                composite_partial_pngs_with_style(&genome_parts, &out, spec.width, spec.height, 0.0, &genome_style.background)?;
                chrom_entry.genome = Some(ManifestManhattan {
                    png: format!("{}/chroms/{}/genome_manhattan.png", output_base, chrom),
                    count: 0,
                });
                has_data = true;
            }
        }

        if has_data {
            chrom_manhattans.insert(chrom, chrom_entry);
        }
    }

    // Step 2: Process gene burden (if provided)
    let (gene_count, _gene_sig_regions) = if let Some(ref gene_burden_path) = spec.gene_burden {
        println!("  Processing gene burden table...");

        let phenotype = extract_phenotype_name(output_base);
        let ancestry = "meta";

        // Export to Parquet
        let parquet_path = format!("{}/gene_associations.parquet", output_base);
        let scan_result = scan_gene_burden_to_parquet(
            gene_burden_path,
            &phenotype,
            ancestry,
            &parquet_path,
            spec.gene_threshold,
            None, // No MAF filter during aggregation
        )?;

        println!(
            "    Exported {} gene rows, {} significant genes",
            scan_result.total_rows,
            scan_result.significant_genes.len()
        );

        // Write significant genes JSON
        let genes_json = serde_json::to_string_pretty(&scan_result.significant_genes)?;
        let genes_path = format!("{}/significant_genes.json", output_base);
        if is_cloud_path(&genes_path) {
            use genohype_core::io::CloudWriter;
            use std::io::Write;
            let mut writer = CloudWriter::new(&genes_path)?;
            writer.write_all(genes_json.as_bytes())?;
            writer.finish()?;
        } else {
            std::fs::write(&genes_path, &genes_json)?;
        }

        // Build layout from reference genome (GRCh38)
        let contigs = crate::manhattan::reference::get_contig_lengths(
            // Dummy engine - we just need the default contig lengths
            &QueryEngine::open_path(gene_burden_path)?,
        );
        let layout = ChromosomeLayout::new(&contigs, spec.width, 4);

        // Render gene Manhattan plot with styling
        let gene_style = spec.styling.resolve(PlotType::GeneBurden);
        if !scan_result.plot_points.is_empty() {
            // Render combined/legacy gene Manhattan plot
            let gene_png = render_gene_manhattan_styled(
                &scan_result.plot_points,
                spec.width,
                spec.height,
                spec.gene_threshold,
                &layout,
                Some(&gene_style),
            )?;

            let png_path = format!("{}/plots/gene_manhattan.png", output_base);
            write_locus_file(&png_path, &gene_png)?;

            // Render grouped plots for each (annotation, MAF) combination
            for ((annotation, maf_str), points) in &scan_result.plot_points_by_group {
                if points.is_empty() {
                    continue;
                }
                let group_png = render_gene_manhattan_styled(
                    points,
                    spec.width,
                    spec.height,
                    spec.gene_threshold,
                    &layout,
                    Some(&gene_style),
                )?;
                let group_path = format!("{}/plots/gene_manhattan_{}_maf{}.png", output_base, annotation, maf_str);
                write_locus_file(&group_path, &group_png)?;
            }
        }

        // Collect significant gene regions for locus plots
        let mut gene_regions = genohype_core::query::IntervalList::new();
        for gene in &scan_result.significant_genes {
            gene_regions.add(
                gene.interval.0.clone(),
                gene.interval.1,
                gene.interval.2,
            );
        }

        (scan_result.total_rows as u64, gene_regions)
    } else {
        (0u64, genohype_core::query::IntervalList::new())
    };

    // Step 2b: Process QQ tables (expected p-values for QQ plots)
    let phenotype = extract_phenotype_name(output_base);
    let ancestry = "meta"; // Default ancestry

    let mut qq_stats_map = serde_json::Map::new();

    if let Some(ref exome_exp_p_path) = spec.exome_exp_p {
        println!("  Processing exome QQ table...");
        let parquet_path = format!("{}/qq_exome.parquet", output_base);
        match scan_qq_to_parquet(
            exome_exp_p_path,
            &phenotype,
            ancestry,
            "exomes",
            &parquet_path,
        ) {
            Ok(result) => {
                println!("    Exported {} QQ points for exome", result.total_rows);
                qq_stats_map.insert("exome".to_string(), serde_json::to_value(&result.stats).unwrap_or_default());
            }
            Err(e) => {
                eprintln!("    Warning: Failed to process exome QQ table: {}", e);
            }
        }
    }

    if let Some(ref genome_exp_p_path) = spec.genome_exp_p {
        println!("  Processing genome QQ table...");
        let parquet_path = format!("{}/qq_genome.parquet", output_base);
        match scan_qq_to_parquet(
            genome_exp_p_path,
            &phenotype,
            ancestry,
            "genomes",
            &parquet_path,
        ) {
            Ok(result) => {
                println!("    Exported {} QQ points for genome", result.total_rows);
                qq_stats_map.insert("genome".to_string(), serde_json::to_value(&result.stats).unwrap_or_default());
            }
            Err(e) => {
                eprintln!("    Warning: Failed to process genome QQ table: {}", e);
            }
        }
    }

    // Write QQ stats JSON if we have any
    if !qq_stats_map.is_empty() {
        let qq_stats_json = serde_json::to_string_pretty(&qq_stats_map)?;
        let stats_path = format!("{}/qq_stats.json", output_base);
        if is_cloud_path(&stats_path) {
            use genohype_core::io::CloudWriter;
            use std::io::Write;
            let mut writer = CloudWriter::new(&stats_path)?;
            writer.write_all(qq_stats_json.as_bytes())?;
            writer.finish()?;
        } else {
            std::fs::write(&stats_path, &qq_stats_json)?;
        }
    }

    // Step 3: Merge significant hits (combined exome + genome into one file)
    println!("  Merging significant hits (combined exome + genome)...");
    let has_exome = spec.exome_results.is_some();
    let has_genome = spec.genome_results.is_some();
    let (combined_sig_count, _combined_top_hit) =
        merge_and_combine_hits(output_base, has_exome, has_genome)?;

    // For backward compatibility, also generate per-source files
    let (exome_sig_count, exome_top_hit) = if has_exome {
        merge_significant_hits(output_base, "exome")?
    } else {
        (0, None)
    };

    let (genome_sig_count, genome_top_hit) = if has_genome {
        merge_significant_hits(output_base, "genome")?
    } else {
        (0, None)
    };

    // Extract phenotype and ancestry from spec or path
    let phenotype = extract_phenotype_name(output_base);
    let ancestry = "meta".to_string(); // Default ancestry; could be extracted from spec if available

    // Step 4: Compute locus regions (always) and generate plots (if enabled)
    // Locus data (loci.parquet, loci_variants.parquet) is always written.
    // PNG rendering is controlled by spec.locus_plots.
    println!("  Generating locus data...");
    let loci = generate_locus_plots(spec, output_base, &phenotype, &ancestry)?;

    println!(
        "  Combined significant hits: {} total, {} exome, {} genome",
        combined_sig_count, exome_sig_count, genome_sig_count
    );

    // Step 5: Calculate storage sizes
    println!("  Calculating storage sizes...");
    let input_ht_size_bytes = {
        let mut total: u64 = 0;
        if let Some(ref path) = spec.exome_results {
            if let Some(size) = get_gcs_dir_size(path) {
                total += size;
            }
        }
        if let Some(ref path) = spec.genome_results {
            if let Some(size) = get_gcs_dir_size(path) {
                total += size;
            }
        }
        if let Some(ref path) = spec.gene_burden {
            if let Some(size) = get_gcs_dir_size(path) {
                total += size;
            }
        }
        if total > 0 { Some(total) } else { None }
    };

    let output_dir_size_bytes = get_gcs_dir_size(output_base);

    if let Some(input_size) = input_ht_size_bytes {
        println!("    Input HT size: {:.2} GB", input_size as f64 / 1e9);
    }
    if let Some(output_size) = output_dir_size_bytes {
        println!("    Output dir size: {:.2} GB", output_size as f64 / 1e9);
    }

    // Step 6: Write manifest.json
    println!("  Writing manifest.json...");
    let aggregate_duration = start.elapsed().as_secs_f64();

    let manifest = Manifest {
        phenotype: phenotype.clone(),
        ancestry: Some(ancestry.clone()),
        created_at: chrono_now_iso(),
        inputs: ManifestInputs {
            exome_results: spec.exome_results.clone(),
            genome_results: spec.genome_results.clone(),
            gene_burden: spec.gene_burden.clone(),
        },
        manhattans: ManifestManhattans {
            exome: if spec.exome_results.is_some() {
                Some(ManifestManhattan {
                    png: format!("{}/plots/exome_manhattan.png", output_base),
                    count: exome_count,
                })
            } else {
                None
            },
            genome: if spec.genome_results.is_some() {
                Some(ManifestManhattan {
                    png: format!("{}/plots/genome_manhattan.png", output_base),
                    count: genome_count,
                })
            } else {
                None
            },
            gene: if spec.gene_burden.is_some() {
                Some(ManifestManhattan {
                    png: format!("{}/plots/gene_manhattan.png", output_base),
                    count: gene_count,
                })
            } else {
                None
            },
        },
        chrom_manhattans,
        significant_hits: ManifestSignificantHits {
            // Only include entries when there are actual significant hits
            // (merge_significant_hits returns 0 and creates no file when empty)
            exome: if exome_sig_count > 0 {
                Some(ManifestSigHits {
                    path: format!("{}/exome_significant.parquet", output_base),
                    count: exome_sig_count,
                    top_hit: exome_top_hit,
                })
            } else {
                None
            },
            genome: if genome_sig_count > 0 {
                Some(ManifestSigHits {
                    path: format!("{}/genome_significant.parquet", output_base),
                    count: genome_sig_count,
                    top_hit: genome_top_hit,
                })
            } else {
                None
            },
            gene: None,
        },
        loci: loci.clone(),
        stats: ManifestStats {
            scan_duration_sec: scan_duration,
            aggregate_duration_sec: aggregate_duration,
            total_loci: loci.len(),
            input_ht_size_bytes,
            output_dir_size_bytes,
        },
    };

    let manifest_path = format!("{}/manifest.json", output_base);
    let manifest_json = serde_json::to_string_pretty(&manifest)?;

    if is_cloud_path(&manifest_path) {
        use genohype_core::io::CloudWriter;
        use std::io::Write;
        let mut writer = CloudWriter::new(&manifest_path)?;
        writer.write_all(manifest_json.as_bytes())?;
        writer.finish()?;
    } else {
        std::fs::write(&manifest_path, &manifest_json)?;
    }

    // Step 6: Cleanup intermediate files (if requested)
    if spec.cleanup {
        println!("  Cleaning up intermediate files...");
        cleanup_intermediates(output_base)?;
        cleanup_chrom_intermediates(output_base)?;
    }

    println!(
        "Manhattan aggregation complete in {:.1}s",
        aggregate_duration
    );

    // Build summary for return
    let summary = serde_json::json!({
        "phenotype": extract_phenotype_name(output_base),
        "exome_sig_count": exome_sig_count,
        "genome_sig_count": genome_sig_count,
        "total_loci": loci.len(),
        "aggregate_duration_sec": aggregate_duration,
    });

    Ok(((exome_count + genome_count) as usize, summary))
}

/// Composite partial PNGs for a source (exome or genome).
fn composite_source_pngs(
    output_base: &str,
    source: &str,
    width: u32,
    height: u32,
    background: &BackgroundStyle,
) -> Result<u64> {
    let parts_dir = format!("{}/{}", output_base, source);
    // Output to plots/ subdirectory
    let output_path = format!("{}/plots/{}_manhattan.png", output_base, source);

    // Use existing composite function with background style
    // Note: threshold is not used for compositing, pass 0.0
    composite_partial_pngs_with_style(&parts_dir, &output_path, width, height, 0.0, background)?;

    // Count total variants by counting files (rough estimate)
    // TODO: Track actual counts during scan phase
    Ok(0)
}
