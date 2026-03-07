//! Manhattan aggregate phase (Phase 2 of V2 pipeline).
//!
//! This module handles the aggregation of scan phase outputs:
//! 1. Compositing partial PNGs into final Manhattan plots
//! 2. Processing gene burden table
//! 3. Merging significant hits from scan phase
//! 4. Generating locus plots for significant regions
//! 5. Writing manifest.json

use crate::distributed::message::ManhattanAggregateSpec;
use genohype_core::error::Result;
use genohype_core::io::is_cloud_path;
use crate::manhattan::config::{BackgroundStyle, PlotType};
use crate::manhattan::data::{
    LocusDefinitionRow, LocusVariantRow, Manifest, ManifestInputs, ManifestLocus,
    ManifestLocusVariants, ManifestManhattan, ManifestManhattans, ManifestRegion, ManifestSigHits,
    ManifestSignificantHits, ManifestStats, ManifestTopHit,
};
use crate::manhattan::genes::{render_gene_manhattan_styled, scan_gene_burden_to_parquet, scan_qq_to_parquet};
use crate::manhattan::layout::ChromosomeLayout;
use crate::manhattan::pipeline::composite_partial_pngs_with_style;
use crate::manhattan::loci_writer::{LocusDefinitionWriter, LocusVariantWriter};
use crate::manhattan::locus::{LocusPlotConfig, LocusRenderer, RenderVariant};
use crate::manhattan::data::VariantSource;
use crate::manhattan::reference::calculate_xpos;
use genohype_core::query::{IntervalList, QueryEngine};
use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

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
    use genohype_core::io::is_cloud_path;

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
    use genohype_core::io::is_cloud_path;

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
        let mut gene_regions = IntervalList::new();
        for gene in &scan_result.significant_genes {
            gene_regions.add(
                gene.interval.0.clone(),
                gene.interval.1,
                gene.interval.2,
            );
        }

        (scan_result.total_rows as u64, gene_regions)
    } else {
        (0u64, IntervalList::new())
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

/// Merge significant hits from scan phase parquet files.
///
/// Optimized approach:
/// - Parallel file reads with rayon
/// - Find top hit by scanning for min pvalue (no full sort needed)
/// - Concatenate batches without sorting for output (partitions are already sorted)
fn merge_significant_hits(
    output_base: &str,
    source: &str,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use genohype_core::io::is_cloud_path;
    use rayon::prelude::*;
    use std::path::Path;
    use std::time::Instant;

    let sig_dir = format!("{}/{}", output_base, source);
    let output_file = format!("{}/{}_significant.parquet", output_base, source);

    // Collect all sig parquet files
    let sig_files = if is_cloud_path(&sig_dir) {
        list_cloud_parquet_files(&sig_dir, "-sig.parquet")?
    } else {
        let path = Path::new(&sig_dir);
        if !path.exists() {
            return Ok((0, None));
        }
        list_local_parquet_files(&sig_dir, "-sig.parquet")?
    };

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    let start = Instant::now();
    println!("    Reading {} sig.parquet files in parallel...", sig_files.len());

    // Read all parquet files in parallel
    let results: Vec<Result<(Vec<RecordBatch>, Option<TopHitCandidate>)>> = sig_files
        .par_iter()
        .map(|file_path| {
            let batches = read_parquet_file(file_path)?;
            // Find top hit candidate in this file while we have it in memory
            let top_candidate = find_top_hit_in_batches(&batches);
            Ok((batches, top_candidate))
        })
        .collect();

    // Collect batches and find global top hit
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;
    let mut global_top: Option<TopHitCandidate> = None;

    for result in results {
        let (batches, top_candidate) = result?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
        // Update global top hit if this file has a better one
        if let Some(candidate) = top_candidate {
            global_top = Some(match global_top {
                None => candidate,
                Some(current) if candidate.pvalue < current.pvalue => candidate,
                Some(current) => current,
            });
        }
    }

    let read_time = start.elapsed();
    println!("    Read {} batches in {:.1}s", all_batches.len(), read_time.as_secs_f64());

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Write concatenated output (no sorting - files are already partition-sorted)
    let write_start = Instant::now();
    write_parquet_batches(&output_file, &schema, &all_batches)?;
    println!("    Wrote {} rows in {:.1}s", total_count, write_start.elapsed().as_secs_f64());

    // Convert top candidate to ManifestTopHit
    let top_hit = global_top.map(|c| ManifestTopHit {
        id: format!("{}:{}:{}:{}", c.contig, c.position, c.ref_allele, c.alt_allele),
        pvalue: c.pvalue,
        gene: None,
        consequence: None,
    });

    Ok((total_count, top_hit))
}

/// Merge and combine significant hits from both exome and genome into a single file.
///
/// This function reads all `*-sig.parquet` files from both the `exome/` and `genome/`
/// directories and writes them to a single `significant.parquet` at the output root.
/// Since the scan phase now includes `sequencing_type` in the output, we can safely
/// merge them into one file.
fn merge_and_combine_hits(
    output_base: &str,
    has_exome: bool,
    has_genome: bool,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use genohype_core::io::is_cloud_path;
    use rayon::prelude::*;
    use std::path::Path;
    use std::time::Instant;

    let output_file = format!("{}/significant.parquet", output_base);

    // Collect all sig parquet files from both sources
    let mut sig_files: Vec<String> = Vec::new();

    if has_exome {
        let exome_dir = format!("{}/exome", output_base);
        if is_cloud_path(&exome_dir) {
            if let Ok(files) = list_cloud_parquet_files(&exome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        } else if Path::new(&exome_dir).exists() {
            if let Ok(files) = list_local_parquet_files(&exome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        }
    }

    if has_genome {
        let genome_dir = format!("{}/genome", output_base);
        if is_cloud_path(&genome_dir) {
            if let Ok(files) = list_cloud_parquet_files(&genome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        } else if Path::new(&genome_dir).exists() {
            if let Ok(files) = list_local_parquet_files(&genome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        }
    }

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    let start = Instant::now();
    println!(
        "    Reading {} sig.parquet files from exome+genome in parallel...",
        sig_files.len()
    );

    // Read all parquet files in parallel
    let results: Vec<Result<(Vec<RecordBatch>, Option<TopHitCandidate>)>> = sig_files
        .par_iter()
        .map(|file_path| {
            let batches = read_parquet_file(file_path)?;
            // Find top hit candidate in this file while we have it in memory
            let top_candidate = find_top_hit_in_batches(&batches);
            Ok((batches, top_candidate))
        })
        .collect();

    // Collect batches and find global top hit
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;
    let mut global_top: Option<TopHitCandidate> = None;

    for result in results {
        let (batches, top_candidate) = result?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
        // Update global top hit if this file has a better one
        if let Some(candidate) = top_candidate {
            global_top = Some(match global_top {
                None => candidate,
                Some(current) if candidate.pvalue < current.pvalue => candidate,
                Some(current) => current,
            });
        }
    }

    let read_time = start.elapsed();
    println!(
        "    Read {} batches in {:.1}s",
        all_batches.len(),
        read_time.as_secs_f64()
    );

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Write concatenated output (no sorting - files are already partition-sorted)
    let write_start = Instant::now();
    write_parquet_batches(&output_file, &schema, &all_batches)?;
    println!(
        "    Wrote {} rows to significant.parquet in {:.1}s",
        total_count,
        write_start.elapsed().as_secs_f64()
    );

    // Convert top candidate to ManifestTopHit
    let top_hit = global_top.map(|c| ManifestTopHit {
        id: format!("{}:{}:{}:{}", c.contig, c.position, c.ref_allele, c.alt_allele),
        pvalue: c.pvalue,
        gene: None,
        consequence: None,
    });

    Ok((total_count, top_hit))
}

/// Candidate for top hit found during parallel scan.
#[derive(Clone)]
struct TopHitCandidate {
    contig: String,
    position: i32,
    ref_allele: String,
    alt_allele: String,
    pvalue: f64,
}

/// Find the top hit (lowest pvalue) in a set of batches.
fn find_top_hit_in_batches(batches: &[RecordBatch]) -> Option<TopHitCandidate> {
    let mut best: Option<TopHitCandidate> = None;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let schema = batch.schema();

        // Get column indices
        let pvalue_idx = schema.fields().iter().position(|f| f.name() == "pvalue")?;
        let contig_idx = schema.fields().iter().position(|f| f.name() == "contig")?;
        let position_idx = schema.fields().iter().position(|f| f.name() == "position")?;
        let ref_idx = schema.fields().iter().position(|f| f.name() == "ref")?;
        let alt_idx = schema.fields().iter().position(|f| f.name() == "alt")?;

        let pvalue_col = batch.column(pvalue_idx).as_any().downcast_ref::<Float64Array>()?;
        let contig_col = batch.column(contig_idx).as_any().downcast_ref::<StringArray>()?;
        let position_col = batch.column(position_idx).as_any().downcast_ref::<arrow::array::Int32Array>()?;
        let ref_col = batch.column(ref_idx).as_any().downcast_ref::<StringArray>()?;
        let alt_col = batch.column(alt_idx).as_any().downcast_ref::<StringArray>()?;

        for i in 0..batch.num_rows() {
            if pvalue_col.is_null(i) {
                continue;
            }
            let pvalue = pvalue_col.value(i);

            let dominated = best.as_ref().map(|b| pvalue >= b.pvalue).unwrap_or(false);
            if dominated {
                continue;
            }

            best = Some(TopHitCandidate {
                contig: contig_col.value(i).to_string(),
                position: position_col.value(i),
                ref_allele: ref_col.value(i).to_string(),
                alt_allele: alt_col.value(i).to_string(),
                pvalue,
            });
        }
    }

    best
}

/// List parquet files matching a suffix in a local directory.
fn list_local_parquet_files(dir: &str, suffix: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(suffix) {
                files.push(path.to_string_lossy().to_string());
            }
        }
    }
    files.sort();
    Ok(files)
}

/// List parquet files matching a suffix in a cloud directory.
fn list_cloud_parquet_files(dir: &str, suffix: &str) -> Result<Vec<String>> {
    use crate::HailError;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use url::Url;

    let url = Url::parse(dir)
        .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    let (store, prefix, base_url): (Arc<dyn object_store::ObjectStore>, ObjPath, String) = match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str()
                .ok_or_else(|| HailError::InvalidFormat("Missing bucket in GCS URL".to_string()))?;
            let path = url.path().trim_start_matches('/');
            (genohype_core::io::get_gcs_client(bucket)?, ObjPath::from(path), format!("gs://{}/", bucket))
        }
        #[cfg(feature = "aws")]
        "s3" => {
            let bucket = url.host_str()
                .ok_or_else(|| HailError::InvalidFormat("Missing bucket in S3 URL".to_string()))?;
            let path = url.path().trim_start_matches('/');
            let s3 = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| HailError::InvalidFormat(format!("Failed to create S3 client: {}", e)))?;
            (Arc::new(s3), ObjPath::from(path), format!("s3://{}/", bucket))
        }
        scheme => {
            return Err(HailError::InvalidFormat(format!("Unsupported URL scheme: {}", scheme)));
        }
    };

    // Use blocking runtime for object_store async operations
    let rt = tokio::runtime::Runtime::new()?;
    let list_result = rt.block_on(async {
        let mut files = Vec::new();
        let stream = store.list(Some(&prefix));
        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;
        for result in results {
            if let Ok(meta) = result {
                let path = meta.location.to_string();
                if path.ends_with(suffix) {
                    // Reconstruct full URL
                    let full_path = format!("{}{}", base_url, path);
                    files.push(full_path);
                }
            }
        }
        files
    });

    let mut files = list_result;
    files.sort();
    Ok(files)
}

/// Read all record batches from a parquet file.
fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    use genohype_core::io::is_cloud_path;

    if is_cloud_path(path) {
        read_cloud_parquet_file(path)
    } else {
        read_local_parquet_file(path)
    }
}

/// Read parquet from local filesystem.
fn read_local_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Read parquet from cloud storage.
fn read_cloud_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    use genohype_core::io::{get_file_size, range_read};

    // Download entire file to memory (sig.parquet files are small)
    let file_size = get_file_size(path)?;
    let data = range_read(path, 0, file_size as usize)?;

    // bytes::Bytes implements ChunkReader
    let bytes = bytes::Bytes::from(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Write record batches to a parquet file.
fn write_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    use genohype_core::io::is_cloud_path;

    if is_cloud_path(path) {
        write_cloud_parquet_batches(path, schema, batches)
    } else {
        write_local_parquet_batches(path, schema, batches)
    }
}

/// Write parquet to local filesystem.
fn write_local_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

/// Write parquet to cloud storage.
fn write_cloud_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    use genohype_core::io::CloudWriter;

    let cloud_writer = CloudWriter::new(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(cloud_writer, schema.clone(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    let cloud_writer = writer.into_inner()?;
    cloud_writer.finish()?;
    Ok(())
}

/// Clean up intermediate partition files.
fn cleanup_intermediates(output_base: &str) -> Result<()> {
    use genohype_core::io::is_cloud_path;

    // Delete exome/part-*.png and exome/part-*-sig.parquet
    // Delete genome/part-*.png and genome/part-*-sig.parquet

    if is_cloud_path(output_base) {
        // For cloud, we'd need to list and delete
        // TODO: Implement cloud cleanup
        println!("    Cloud cleanup not yet implemented");
    } else {
        for source in &["exome", "genome"] {
            let dir = format!("{}/{}", output_base, source);
            if std::path::Path::new(&dir).exists() {
                std::fs::remove_dir_all(&dir)?;
            }
        }
    }

    Ok(())
}

/// Discover chromosomes by listing directories in the chroms/ folder
fn discover_chromosomes(chroms_dir: &str) -> Result<Vec<String>> {
    if is_cloud_path(chroms_dir) {
        let dir = chroms_dir.trim_end_matches('/');
        // gsutil ls gs://bucket/path/chroms/ returns directories
        let output = std::process::Command::new("gsutil")
            .args(["ls", dir])
            .output()
            .map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to run gsutil: {}", e),
                ))
            })?;

        if !output.status.success() {
            // It's okay if dir doesn't exist (no chroms found)
            return Ok(vec![]);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let chroms: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                // line is like gs://bucket/path/chroms/chr1/
                let trimmed = line.trim().trim_end_matches('/');
                trimmed.rsplit('/').next().map(|s| s.to_string())
            })
            .collect();
        Ok(chroms)
    } else {
        if !std::path::Path::new(chroms_dir).exists() {
            return Ok(vec![]);
        }

        let mut chroms = Vec::new();
        for entry in std::fs::read_dir(chroms_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    chroms.push(name.to_string());
                }
            }
        }
        Ok(chroms)
    }
}

/// Check if a directory contains any partial PNGs
fn has_partial_pngs(dir_path: &str) -> Result<bool> {
    if is_cloud_path(dir_path) {
        let dir = dir_path.trim_end_matches('/');
        // Check for at least one file
        let output = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/part-*.png", dir)])
            .output();

        Ok(output.map(|o| o.status.success()).unwrap_or(false))
    } else {
        if !std::path::Path::new(dir_path).exists() {
            return Ok(false);
        }

        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("part-") && name.ends_with(".png") {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Cleanup intermediate files in chrom directories
fn cleanup_chrom_intermediates(output_base: &str) -> Result<()> {
    if is_cloud_path(output_base) {
        println!("    Cloud cleanup of chroms not yet implemented");
    } else {
        let chroms_dir = format!("{}/chroms", output_base);
        if std::path::Path::new(&chroms_dir).exists() {
            // Walk through chroms dir and delete part-*.png files
            for chrom_entry in std::fs::read_dir(&chroms_dir)? {
                let chrom_entry = chrom_entry?;
                if chrom_entry.file_type()?.is_dir() {
                    // Each chrom has exome/ and genome/ subdirs
                    for source in &["exome", "genome"] {
                        let source_dir = chrom_entry.path().join(source);
                        if source_dir.exists() {
                            std::fs::remove_dir_all(&source_dir)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Extract phenotype name from output path.
fn extract_phenotype_name(output_path: &str) -> String {
    output_path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("unknown")
        .to_string()
}

/// Get current timestamp in ISO format.
fn chrono_now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Simple ISO format without chrono dependency
    format!("{}", secs) // TODO: Proper ISO format
}

// =============================================================================
// Locus Plot Generation
// =============================================================================

/// A significant hit position extracted from merged parquet.
#[derive(Debug, Clone)]
pub struct SigPosition {
    pub contig: String,
    pub position: i32,
    pub pvalue: f64,
    pub source: String, // "exome" or "genome"
}

/// Generate locus plots for significant regions (called from aggregation phase).
fn generate_locus_plots(
    spec: &ManhattanAggregateSpec,
    output_base: &str,
    phenotype: &str,
    ancestry: &str,
) -> Result<Vec<ManifestLocus>> {
    generate_loci_from_parquet(
        output_base,
        spec.exome_results.as_deref(),
        spec.genome_results.as_deref(),
        spec.gene_burden.as_deref(),
        spec.locus_window,
        spec.threshold,
        spec.gene_threshold,
        8, // Default thread count for aggregation
        phenotype,
        ancestry,
        Some(&spec.styling),
        spec.locus_plots,
        spec.min_variants_per_locus,
    )
}

/// Core locus generation logic shared by aggregation and standalone CLI.
///
/// Now writes consolidated loci.parquet and loci_variants.parquet files.
fn generate_loci_from_parquet(
    output_base: &str,
    exome_table: Option<&str>,
    genome_table: Option<&str>,
    gene_burden_table: Option<&str>,
    locus_window: i32,
    threshold: f64,
    gene_threshold: f64,
    num_threads: usize,
    phenotype: &str,
    ancestry: &str,
    styling: Option<&crate::manhattan::config::ManhattanConfig>,
    render_images: bool,
    min_variants_per_locus: usize,
) -> Result<Vec<ManifestLocus>> {
    use genohype_core::io::is_cloud_path;
    use crate::manhattan::genes::process_complex_gene_burden;
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Step 1: Extract significant positions from merged parquet file
    println!("    Extracting significant positions...");
    let sig_path = format!("{}/significant.parquet", output_base);

    let mut sig_positions = if std::path::Path::new(&sig_path).exists() || is_cloud_path(&sig_path) {
        let positions = extract_sig_positions(&sig_path)?;
        println!("      Found {} significant hits from significant.parquet", positions.len());
        positions
    } else {
        println!("      No significant.parquet found, skipping variant hits");
        Vec::new()
    };

    // Step 1b: Extract significant genes from gene burden table
    let mut gene_regions: Vec<(String, i32, i32)> = Vec::new();
    if let Some(gene_burden_path) = gene_burden_table {
        println!("    Processing gene burden table for significant genes...");
        match process_complex_gene_burden(gene_burden_path, gene_threshold) {
            Ok((sig_genes, _intervals)) => {
                println!("      Found {} significant genes", sig_genes.len());
                for gene in &sig_genes {
                    let (chrom, start, end) = &gene.interval;
                    // Add gene as a "position" for region computation
                    // Use the gene midpoint as the position, but we'll handle the full span in region computation
                    sig_positions.push(SigPosition {
                        contig: chrom.clone(),
                        position: (start + end) / 2, // midpoint
                        pvalue: gene.best_pvalue,
                        source: "gene".to_string(),
                    });
                    // Also track the full gene bounds for proper region expansion
                    gene_regions.push((chrom.clone(), *start, *end));
                }
            }
            Err(e) => {
                eprintln!("      Warning: failed to process gene burden: {}", e);
            }
        }
    }

    if sig_positions.is_empty() {
        println!("    No significant hits found, skipping locus plots");
        return Ok(vec![]);
    }

    // Step 2: Compute locus regions (Greedy P-value clumping + merge with genes)
    println!("    Computing locus regions (clumping window: {}bp, min_variants: {})...", locus_window, min_variants_per_locus);
    let regions = compute_locus_regions_with_genes(&sig_positions, &gene_regions, locus_window, min_variants_per_locus);
    println!("      Found {} clumped/merged locus regions", regions.len());

    if regions.is_empty() {
        return Ok(vec![]);
    }

    // Step 3: Create loci directory (for local paths)
    let loci_dir = format!("{}/loci", output_base);
    if !is_cloud_path(&loci_dir) {
        std::fs::create_dir_all(&loci_dir)?;
    }

    // Step 4: Generate plots for each region in parallel
    let completed = AtomicUsize::new(0);
    let total_regions = regions.len();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap_or_else(|_| rayon::ThreadPoolBuilder::new().build().unwrap());

    println!(
        "    Generating {} locus plots ({} threads)...",
        total_regions, num_threads
    );

    // Generate loci in parallel, collecting rows for parquet output
    let results: Vec<
        Result<Option<(ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)>>,
    > = pool.install(|| {
        regions
            .par_iter()
            .map(|(contig, start, end)| {
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == 1 || done % 20 == 0 || done == total_regions {
                    println!(
                        "      Progress: {}/{} - {}:{}-{}",
                        done, total_regions, contig, start, end
                    );
                }

                generate_single_locus_core(
                    &loci_dir,
                    &sig_positions,
                    exome_table,
                    genome_table,
                    contig,
                    *start,
                    *end,
                    threshold,
                    phenotype,
                    ancestry,
                    styling,
                    render_images,
                )
            })
            .collect()
    });

    // Collect successful results
    let mut manifest_loci = Vec::new();
    let mut locus_definitions: Vec<LocusDefinitionRow> = Vec::new();
    let mut locus_variants: Vec<LocusVariantRow> = Vec::new();
    let mut errors = 0;

    for result in results {
        match result {
            Ok(Some((locus, def_row, var_rows))) => {
                manifest_loci.push(locus);
                locus_definitions.push(def_row);
                locus_variants.extend(var_rows);
            }
            Ok(None) => {}
            Err(e) => {
                errors += 1;
                eprintln!("      Warning: locus generation failed: {}", e);
            }
        }
    }

    if errors > 0 {
        println!("      Completed with {} errors", errors);
    }

    // Write loci.parquet and loci_variants.parquet
    if !locus_definitions.is_empty() {
        println!(
            "    Writing {} locus definitions to loci.parquet...",
            locus_definitions.len()
        );
        write_loci_parquet(output_base, &locus_definitions, &locus_variants)?;
    }

    // Sort by chromosome and position for consistent output
    manifest_loci.sort_by(|a, b| {
        let chr_a = parse_chrom_order(&a.region.contig);
        let chr_b = parse_chrom_order(&b.region.contig);
        chr_a.cmp(&chr_b).then(a.region.start.cmp(&b.region.start))
    });

    Ok(manifest_loci)
}

/// Generate a single locus plot and its associated data.
///
/// Returns a tuple of (ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)
/// for use in manifest.json and consolidated parquet output.
fn generate_single_locus_core(
    loci_dir: &str,
    sig_positions: &[SigPosition],
    exome_table: Option<&str>,
    genome_table: Option<&str>,
    contig: &str,
    start: i32,
    end: i32,
    threshold: f64,
    phenotype: &str,
    ancestry: &str,
    styling: Option<&crate::manhattan::config::ManhattanConfig>,
    render_images: bool,
) -> Result<Option<(ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)>> {
    let region_id = format!("{}_{}_{}",
        contig.replace("chr", ""),
        start,
        end
    );

    // Find lead variant in this region
    let lead = find_lead_variant(sig_positions, contig, start, end);

    // Read variants from original Hail tables for this region
    let exome_variants = if let Some(table_path) = exome_table {
        read_locus_variants(table_path, contig, start, end, VariantSource::Exome)
            .unwrap_or_default()
    } else {
        vec![]
    };

    let genome_variants = if let Some(table_path) = genome_table {
        read_locus_variants(table_path, contig, start, end, VariantSource::Genome)
            .unwrap_or_default()
    } else {
        vec![]
    };

    if exome_variants.is_empty() && genome_variants.is_empty() {
        return Ok(None);
    }

    let plot_path = if render_images {
        // Render locus plot
        let all_variants: Vec<RenderVariant> = exome_variants
            .iter()
            .chain(genome_variants.iter())
            .cloned()
            .collect();

        let png_data = render_locus_plot(&all_variants, start, end, threshold, styling)?;

        // Write plot file (this remains as a file)
        let plot_uri = format!("{}/{}/plot.png", loci_dir, region_id);
        write_locus_file(&plot_uri, &png_data)?;
        Some(plot_uri)
    } else {
        None
    };

    // Build LocusVariantRow records (replaces JSON files)
    let variant_rows: Vec<LocusVariantRow> = exome_variants
        .iter()
        .map(|v| LocusVariantRow {
            locus_id: region_id.clone(),
            phenotype: phenotype.to_string(),
            ancestry: ancestry.to_string(),
            sequencing_type: "exome".to_string(),
            contig: contig.to_string(),
            xpos: calculate_xpos(contig, v.position),
            position: v.position,
            ref_allele: v.ref_allele.clone(),
            alt_allele: v.alt_allele.clone(),
            pvalue: v.pvalue,
            neg_log10_p: if v.pvalue > 0.0 {
                -v.pvalue.log10() as f32
            } else {
                0.0
            },
            is_significant: v.is_significant,
            beta: v.beta,
            se: v.se,
            af: v.af,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        })
        .chain(genome_variants.iter().map(|v| LocusVariantRow {
            locus_id: region_id.clone(),
            phenotype: phenotype.to_string(),
            ancestry: ancestry.to_string(),
            sequencing_type: "genome".to_string(),
            contig: contig.to_string(),
            xpos: calculate_xpos(contig, v.position),
            position: v.position,
            ref_allele: v.ref_allele.clone(),
            alt_allele: v.alt_allele.clone(),
            pvalue: v.pvalue,
            neg_log10_p: if v.pvalue > 0.0 {
                -v.pvalue.log10() as f32
            } else {
                0.0
            },
            is_significant: v.is_significant,
            beta: v.beta,
            se: v.se,
            af: v.af,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        }))
        .collect();

    // Build LocusDefinitionRow
    let source_str = lead
        .as_ref()
        .map(|l| l.source.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let lead_variant_str = lead
        .as_ref()
        .map(|l| format!("{}:{}::", l.contig, l.position)) // Note: ref/alt not available from SigPosition
        .unwrap_or_else(|| "unknown".to_string());
    let lead_pvalue = lead.as_ref().map(|l| l.pvalue).unwrap_or(1.0);

    let definition_row = LocusDefinitionRow {
        locus_id: region_id.clone(),
        phenotype: phenotype.to_string(),
        ancestry: ancestry.to_string(),
        contig: contig.to_string(),
        start,
        stop: end,
        xstart: calculate_xpos(contig, start),
        xstop: calculate_xpos(contig, end),
        source: source_str.clone(),
        lead_variant: lead_variant_str.clone(),
        lead_pvalue,
        exome_count: exome_variants.len() as u32,
        genome_count: genome_variants.len() as u32,
    };

    // Build ManifestLocus (still needed for manifest.json)
    let manifest_locus = ManifestLocus {
        id: region_id.clone(),
        region: ManifestRegion {
            contig: contig.to_string(),
            start: start as i64,
            end: end as i64,
        },
        source: source_str,
        lead_variant: lead_variant_str,
        lead_pvalue,
        lead_gene: None,
        plot: plot_path,
        exome_variants: if !exome_variants.is_empty() {
            Some(ManifestLocusVariants {
                path: format!("loci_variants.parquet (locus_id={})", region_id),
                count: exome_variants.len() as u64,
            })
        } else {
            None
        },
        genome_variants: if !genome_variants.is_empty() {
            Some(ManifestLocusVariants {
                path: format!("loci_variants.parquet (locus_id={})", region_id),
                count: genome_variants.len() as u64,
            })
        } else {
            None
        },
        genes: vec![],
    };

    Ok(Some((manifest_locus, definition_row, variant_rows)))
}

/// Extract significant positions from the consolidated significant.parquet file.
pub fn extract_sig_positions(parquet_path: &str) -> Result<Vec<SigPosition>> {
    use genohype_core::io::is_cloud_path;

    let batches = if is_cloud_path(parquet_path) {
        // For cloud paths, handle 404 (file not found) gracefully -
        // phenotypes with no significant hits won't have this file
        match read_cloud_parquet_file(parquet_path) {
            Ok(b) => b,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("not found") || err_str.contains("NotFound") {
                    return Ok(vec![]);
                }
                return Err(e);
            }
        }
    } else {
        if !std::path::Path::new(parquet_path).exists() {
            return Ok(vec![]);
        }
        read_local_parquet_file(parquet_path)?
    };

    let mut positions = Vec::new();

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let schema = batch.schema();
        let contig_idx = schema.fields().iter().position(|f| f.name() == "contig");
        let position_idx = schema.fields().iter().position(|f| f.name() == "position");
        let pvalue_idx = schema.fields().iter().position(|f| f.name() == "pvalue");
        let seq_type_idx = schema.fields().iter().position(|f| f.name() == "sequencing_type");

        if contig_idx.is_none() || position_idx.is_none() || pvalue_idx.is_none() || seq_type_idx.is_none() {
            continue;
        }

        let contig_col = batch.column(contig_idx.unwrap()).as_any().downcast_ref::<StringArray>();
        let position_col = batch.column(position_idx.unwrap()).as_any().downcast_ref::<Int32Array>();
        let pvalue_col = batch.column(pvalue_idx.unwrap()).as_any().downcast_ref::<Float64Array>();
        let seq_type_col = batch.column(seq_type_idx.unwrap()).as_any().downcast_ref::<StringArray>();

        if let (Some(contig_arr), Some(pos_arr), Some(pval_arr), Some(seq_arr)) =
            (contig_col, position_col, pvalue_col, seq_type_col)
        {
            for i in 0..batch.num_rows() {
                if contig_arr.is_null(i) || pos_arr.is_null(i) || pval_arr.is_null(i) || seq_arr.is_null(i) {
                    continue;
                }

                positions.push(SigPosition {
                    contig: contig_arr.value(i).to_string(),
                    position: pos_arr.value(i),
                    pvalue: pval_arr.value(i),
                    source: seq_arr.value(i).to_string(),
                });
            }
        }
    }

    Ok(positions)
}

/// Compute locus regions including gene bounds using Greedy P-value Clumping.
///
/// 1. Variants are sorted by p-value.
/// 2. Iteratively take the most significant unabsorbed variant to form a clump.
/// 3. Clumps containing fewer than `min_variants` are discarded to remove noise.
/// 4. Add gene regions (gene bounds expanded by window).
/// 5. Merge all overlapping regions together.
fn compute_locus_regions_with_genes(
    positions: &[SigPosition],
    gene_regions: &[(String, i32, i32)],
    window: i32,
    min_variants: usize,
) -> Vec<(String, i32, i32)> {
    let mut all_regions: Vec<(String, i32, i32)> = Vec::new();

    // 1. Separate variant positions (filter out "gene" source variants which are added directly)
    let mut variant_positions: Vec<SigPosition> = positions.iter()
        .filter(|p| p.source != "gene")
        .cloned()
        .collect();

    // 2. Greedy clumping
    if !variant_positions.is_empty() {
        // Sort by p-value ascending (best first)
        variant_positions.sort_by(|a, b| a.pvalue.partial_cmp(&b.pvalue).unwrap_or(std::cmp::Ordering::Equal));

        let mut absorbed = vec![false; variant_positions.len()];

        for i in 0..variant_positions.len() {
            if absorbed[i] { continue; }
            let lead = &variant_positions[i];

            let clump_start = (lead.position - window).max(1);
            let clump_end = lead.position + window;

            let mut clump_count = 0;

            // Absorb any variants within window
            for j in i..variant_positions.len() {
                if !absorbed[j] {
                    let candidate = &variant_positions[j];
                    if candidate.contig == lead.contig && candidate.position >= clump_start && candidate.position <= clump_end {
                        absorbed[j] = true;
                        clump_count += 1;
                    }
                }
            }

            // Keep clump if it meets the variant count threshold
            if clump_count >= min_variants {
                all_regions.push((lead.contig.clone(), clump_start, clump_end));
            }
        }
    }

    // 3. Add gene regions (gene bounds expanded by window)
    for (chrom, start, end) in gene_regions {
        let expanded_start = (start - window).max(1);
        let expanded_end = end + window;
        all_regions.push((chrom.clone(), expanded_start, expanded_end));
    }

    // 4. Group by chromosome
    let mut by_chrom: HashMap<String, Vec<(i32, i32)>> = HashMap::new();
    for (chrom, start, end) in all_regions {
        by_chrom.entry(chrom).or_default().push((start, end));
    }

    // 5. Merge overlapping regions per chromosome
    let mut merged_regions = Vec::new();

    for (contig, mut intervals) in by_chrom {
        // Sort by start position
        intervals.sort_by_key(|(start, _)| *start);

        let mut current_start: Option<i32> = None;
        let mut current_end: Option<i32> = None;

        for (start, end) in intervals {
            match (current_start, current_end) {
                (Some(_cs), Some(ce)) if start <= ce => {
                    // Overlapping - extend current region
                    current_end = Some(end.max(ce));
                }
                (Some(cs), Some(ce)) => {
                    // Non-overlapping - emit current and start new
                    merged_regions.push((contig.clone(), cs, ce));
                    current_start = Some(start);
                    current_end = Some(end);
                }
                _ => {
                    // First region
                    current_start = Some(start);
                    current_end = Some(end);
                }
            }
        }

        // Emit final region
        if let (Some(start), Some(end)) = (current_start, current_end) {
            merged_regions.push((contig, start, end));
        }
    }

    // Sort by chromosome and position
    merged_regions.sort_by(|a, b| {
        let chr_a = parse_chrom_order(&a.0);
        let chr_b = parse_chrom_order(&b.0);
        chr_a.cmp(&chr_b).then(a.1.cmp(&b.1))
    });

    merged_regions
}

/// Parse chromosome for sorting (1-22, then X, Y, MT).
fn parse_chrom_order(chrom: &str) -> (i32, String) {
    let c = chrom.trim_start_matches("chr");
    match c.parse::<i32>() {
        Ok(n) => (n, String::new()),
        Err(_) => (100, c.to_string()), // X, Y, MT sort after numbered
    }
}

/// Find the lead variant (lowest p-value) in a region.
fn find_lead_variant(
    positions: &[SigPosition],
    contig: &str,
    start: i32,
    end: i32,
) -> Option<SigPosition> {
    positions
        .iter()
        .filter(|p| p.contig == contig && p.position >= start && p.position <= end)
        .min_by(|a, b| a.pvalue.partial_cmp(&b.pvalue).unwrap_or(std::cmp::Ordering::Equal))
        .cloned()
}

/// Read variants from a Hail table for a specific genomic region.
fn read_locus_variants(
    table_path: &str,
    contig: &str,
    start: i32,
    end: i32,
    source: VariantSource,
) -> Result<Vec<RenderVariant>> {
    use genohype_core::query::QueryEngine;
    use std::time::Instant;

    let t0 = Instant::now();
    let engine = QueryEngine::open_path(table_path)?;
    let open_time = t0.elapsed();
    if open_time.as_secs() > 1 {
        eprintln!("      [slow] QueryEngine::open_path took {:.1}s for {}",
            open_time.as_secs_f64(), table_path);
    }

    // Create interval list for this region
    let mut intervals = IntervalList::new();
    intervals.add(contig.to_string(), start, end);

    // Also try with "chr" prefix if not present, or without if present
    let alt_contig = if contig.starts_with("chr") {
        contig.trim_start_matches("chr").to_string()
    } else {
        format!("chr{}", contig)
    };
    intervals.add(alt_contig, start, end);

    let intervals = Arc::new(intervals);

    // Query with interval filter
    let t1 = Instant::now();
    let iter = engine.query_iter_with_intervals(&[], Some(intervals))?;

    let mut variants = Vec::new();
    let threshold = 5e-8; // Genome-wide significance

    let mut row_count = 0;
    for row_result in iter {
        row_count += 1;
        let row = row_result?;

        // Extract locus, pvalue, and alleles
        if let Some(info) = extract_locus_info(&row) {
            if info.pvalue > 0.0 && info.pvalue <= 1.0 && info.pvalue.is_finite() {
                variants.push(RenderVariant {
                    position: info.position,
                    ref_allele: info.ref_allele,
                    alt_allele: info.alt_allele,
                    pvalue: info.pvalue,
                    beta: info.beta,
                    se: info.se,
                    af: info.af,
                    source,
                    is_significant: info.pvalue < threshold,
                    ac_cases: info.ac_cases,
                    ac_controls: info.ac_controls,
                    af_cases: info.af_cases,
                    af_controls: info.af_controls,
                    association_ac: info.association_ac,
                });
            }
        }
    }

    let query_time = t1.elapsed();
    if query_time.as_secs() > 2 {
        eprintln!("      [slow] Interval query took {:.1}s, {} rows for {}:{}-{}",
            query_time.as_secs_f64(), row_count, contig, start, end);
    }

    Ok(variants)
}

/// Extracted locus variant info including effect size fields.
struct ExtractedLocusInfo {
    position: i32,
    pvalue: f64,
    ref_allele: String,
    alt_allele: String,
    beta: Option<f64>,
    se: Option<f64>,
    af: Option<f64>,
    ac_cases: Option<f64>,
    ac_controls: Option<f64>,
    af_cases: Option<f64>,
    af_controls: Option<f64>,
    association_ac: Option<f64>,
}

/// Extract position, p-value, alleles, and effect size fields from an encoded row.
fn extract_locus_info(row: &genohype_core::codec::EncodedValue) -> Option<ExtractedLocusInfo> {
    use genohype_core::codec::EncodedValue;

    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
        let mut current = value;
        for &field_name in path {
            if let EncodedValue::Struct(fields) = current {
                current = fields.iter().find(|(n, _)| n == field_name).map(|(_, v)| v)?;
            } else {
                return None;
            }
        }
        Some(current)
    }

    fn get_float(row: &EncodedValue, names: &[&str]) -> Option<f64> {
        for name in names {
            if let Some(v) = get_field(row, &[name]) {
                match v {
                    EncodedValue::Float64(f) => return Some(*f),
                    EncodedValue::Float32(f) => return Some(*f as f64),
                    _ => continue,
                }
            }
        }
        None
    }

    let position = get_field(row, &["locus", "position"])?.as_i32()?;

    // Try common p-value field names
    let pvalue = get_float(row, &["Pvalue", "pvalue", "p_value", "P"])?;

    // Extract alleles from the "alleles" array field
    let (ref_allele, alt_allele) = if let Some(EncodedValue::Array(alleles)) = get_field(row, &["alleles"]) {
        let r = alleles.first().and_then(|v| v.as_string()).unwrap_or_default();
        let a = alleles.get(1).and_then(|v| v.as_string()).unwrap_or_default();
        (r, a)
    } else {
        (String::new(), String::new())
    };

    // Extract beta (effect size)
    let beta = get_float(row, &["BETA", "beta", "Beta"]);

    // Extract SE (standard error)
    let se = get_float(row, &["SE", "se", "Se"]);

    // Extract AF (allele frequency)
    let af = get_float(row, &["AF_Allele2", "AF", "af", "allele_frequency"]);

    // Extract case/control fields
    let ac_cases = get_float(row, &["AC_case", "ac_case", "ac_cases"]);
    let ac_controls = get_float(row, &["AC_ctrl", "ac_ctrl", "ac_controls"]);
    let af_cases = get_float(row, &["AF_case", "af_case", "af_cases"]);
    let af_controls = get_float(row, &["AF_ctrl", "af_ctrl", "af_controls"]);

    // Extract association allele count (AC_Allele2) - can be int or float
    let association_ac = get_float(row, &["AC_Allele2"]).or_else(|| {
        get_field(row, &["AC_Allele2"]).and_then(|v| match v {
            EncodedValue::Int64(i) => Some(*i as f64),
            EncodedValue::Int32(i) => Some(*i as f64),
            _ => None,
        })
    });

    Some(ExtractedLocusInfo {
        position,
        pvalue,
        ref_allele,
        alt_allele,
        beta,
        se,
        af,
        ac_cases,
        ac_controls,
        af_cases,
        af_controls,
        association_ac,
    })
}

/// Render a locus plot and return PNG bytes.
fn render_locus_plot(
    variants: &[RenderVariant],
    start: i32,
    end: i32,
    threshold: f64,
    styling: Option<&crate::manhattan::config::ManhattanConfig>,
) -> Result<Vec<u8>> {
    // Calculate y_max from data
    let y_max = variants
        .iter()
        .filter(|v| v.pvalue > 0.0 && v.pvalue.is_finite())
        .map(|v| -v.pvalue.log10())
        .fold(10.0f64, |a, b| a.max(b))
        * 1.1; // 10% padding

    let config = LocusPlotConfig {
        width: 800,
        height: 400,
        start_pos: start,
        end_pos: end,
        y_max,
    };

    let mut renderer = if let Some(style_config) = styling {
        let locus_style = style_config.resolve(PlotType::Locus);
        let (exome_color, genome_color) = style_config.locus_colors();
        LocusRenderer::new_with_style(
            config,
            &locus_style.background,
            exome_color,
            genome_color,
            locus_style.point_radius,
        )
    } else {
        LocusRenderer::new(config)
    };
    renderer.draw_threshold_line(threshold);
    renderer.draw_variants(variants);

    renderer.encode_png()
}

/// Write a file (handles both local and cloud paths).
fn write_locus_file(path: &str, data: &[u8]) -> Result<()> {
    use genohype_core::io::is_cloud_path;

    if is_cloud_path(path) {
        use genohype_core::io::CloudWriter;
        use std::io::Write;

        // Ensure parent directory structure is implied in the path
        let mut writer = CloudWriter::new(path)?;
        writer.write_all(data)?;
        writer.finish()?;
    } else {
        // Create parent directory
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, data)?;
    }

    Ok(())
}

/// Write loci.parquet and loci_variants.parquet files.
fn write_loci_parquet(
    output_base: &str,
    definitions: &[LocusDefinitionRow],
    variants: &[LocusVariantRow],
) -> Result<()> {
    use genohype_core::io::is_cloud_path;

    let loci_path = format!("{}/loci.parquet", output_base);
    let variants_path = format!("{}/loci_variants.parquet", output_base);

    // Write loci definitions
    if is_cloud_path(&loci_path) {
        use genohype_core::io::CloudWriter;
        let cloud_writer = CloudWriter::new(&loci_path)?;
        let mut writer = LocusDefinitionWriter::from_writer(cloud_writer)?;
        writer.write_batch(definitions)?;
        let cloud_writer = writer.into_inner()?;
        cloud_writer.finish()?;
    } else {
        let mut writer = LocusDefinitionWriter::new(&loci_path)?;
        writer.write_batch(definitions)?;
        let count = writer.finish()?;
        println!("      Wrote {} locus definitions to loci.parquet", count);
    }

    // Write loci variants
    if !variants.is_empty() {
        if is_cloud_path(&variants_path) {
            use genohype_core::io::CloudWriter;
            let cloud_writer = CloudWriter::new(&variants_path)?;
            let mut writer = LocusVariantWriter::from_writer(cloud_writer)?;
            writer.write_batch(variants)?;
            let cloud_writer = writer.into_inner()?;
            cloud_writer.finish()?;
        } else {
            let mut writer = LocusVariantWriter::new(&variants_path)?;
            writer.write_batch(variants)?;
            let count = writer.finish()?;
            println!("      Wrote {} locus variants to loci_variants.parquet", count);
        }
    }

    Ok(())
}

// =============================================================================
// GCS Size Calculation
// =============================================================================

/// Get the total size of a GCS directory in bytes using gsutil du.
/// Returns None for local paths or if the command fails.
pub fn get_gcs_dir_size(path: &str) -> Option<u64> {
    if !is_cloud_path(path) {
        // For local paths, use std::fs to calculate directory size
        return get_local_dir_size(path);
    }

    let output = std::process::Command::new("gsutil")
        .args(["du", "-s", path])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Output format: "12345  gs://bucket/path"
    stdout.lines().next()?.split_whitespace().next()?.parse().ok()
}

/// Get the total size of a local directory in bytes.
fn get_local_dir_size(path: &str) -> Option<u64> {
    use std::path::Path;

    let path = Path::new(path);
    if !path.exists() {
        return None;
    }

    fn dir_size(path: &std::path::Path) -> u64 {
        let mut size = 0;
        if path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        size += dir_size(&entry_path);
                    } else if let Ok(metadata) = entry.metadata() {
                        size += metadata.len();
                    }
                }
            }
        } else if let Ok(metadata) = std::fs::metadata(path) {
            size = metadata.len();
        }
        size
    }

    Some(dir_size(path))
}
