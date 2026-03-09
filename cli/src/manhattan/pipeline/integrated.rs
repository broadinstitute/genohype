//! Integrated local pipeline for Manhattan plots.
//!
//! This module orchestrates the full workflow for local execution:
//! 1. Load gene map and process gene burden table (if provided)
//! 2. Scan exome variants with annotation merge-join
//! 3. Scan genome variants with annotation merge-join
//! 4. Compute locus regions from significant signals
//! 5. Generate locus plots and JSON exports

use crate::manhattan::data::{
    BufferedVariant, LocusDefinitionRow, LocusVariantRow, ManifestLocus, ManifestLocusVariants,
    ManifestRegion, SigHitRow, VariantSource,
};
use crate::manhattan::genes::{
    render_gene_manhattan, scan_gene_burden_to_parquet, GeneMap, SignificantGene,
};
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::loci_writer::{LocusDefinitionWriter, LocusVariantWriter};
use crate::manhattan::locus::{LocusPlotConfig, LocusRenderer, RenderVariant};
use crate::manhattan::reference::{calculate_xpos, get_contig_lengths, normalize_contig_name};
use crate::manhattan::render::ManhattanRenderer;
use crate::manhattan::sig_writer::SigHitWriter;
use crate::Result;
use genohype_core::codec::EncodedValue;
use genohype_core::query::join::{JoinedRow, SortedMergeIterator};
use genohype_core::query::{IntervalList, QueryEngine};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Configuration for the integrated pipeline.
///
/// This is separate from CLI args to allow the pipeline to be called
/// from library code without depending on the binary's CLI module.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    // Data inputs
    pub exome: Option<String>,
    pub exome_annotations: Option<String>,
    pub genome: Option<String>,
    pub genome_annotations: Option<String>,
    pub gene_burden: Option<String>,
    pub genes: Option<String>,

    // Thresholds
    pub threshold: f64,
    pub gene_threshold: f64,
    pub gene_maf_filter: Option<f64>,
    pub locus_threshold: f64,
    pub locus_window: i32,
    pub locus_plots: bool,
    #[allow(dead_code)]
    pub min_variants_per_locus: usize,

    // Output
    pub output: Option<String>,
    pub width: u32,
    pub height: u32,
    pub y_field: String,

    // Execution modes
    pub scan_only: bool,
    pub aggregate_only: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            exome: None,
            exome_annotations: None,
            genome: None,
            genome_annotations: None,
            gene_burden: None,
            genes: None,
            threshold: 5e-8,
            gene_threshold: 2.5e-6,
            gene_maf_filter: None,
            locus_threshold: 0.01,
            locus_window: 1_000_000,
            locus_plots: false,
            min_variants_per_locus: 1,
            output: None,
            width: 3000,
            height: 800,
            y_field: "Pvalue".to_string(),
            scan_only: false,
            aggregate_only: false,
        }
    }
}

/// Run the integrated multi-table pipeline.
///
/// This replaces the simple single-table manhattan command when multiple
/// inputs are provided (exome, genome, burden tables).
pub fn run_integrated_pipeline(config: &PipelineConfig) -> Result<()> {
    let output_base = Path::new(config.output.as_deref().unwrap_or("."));
    fs::create_dir_all(output_base)?;

    // Create plots directory for consolidated output
    let plots_dir = output_base.join("plots");
    fs::create_dir_all(&plots_dir)?;

    // 1. Load Gene Map if provided (used for locus gene tracks)
    let _gene_map = if let Some(path) = &config.genes {
        println!("Loading genes from: {}", path);
        Some(GeneMap::load(path)?)
    } else {
        None
    };

    let run_scan = !config.aggregate_only;
    let run_aggregate = !config.scan_only;

    if config.scan_only || config.aggregate_only {
        println!("Note: --scan-only and --aggregate-only modes skip certain local processing steps.");
    }

    // 2. Process Gene Burden (if provided)
    let mut interest_regions = IntervalList::new();
    let mut sig_genes: Vec<SignificantGene> = Vec::new();
    let mut gene_plot_points: Vec<crate::manhattan::data::GenePlotPoint> = Vec::new();
    let mut gene_plot_points_by_group: HashMap<(String, String), Vec<crate::manhattan::data::GenePlotPoint>> = HashMap::new();

    if run_scan && config.gene_burden.is_some() {
        let burden_path = config.gene_burden.as_ref().unwrap();
        println!("Processing gene burden: {}", burden_path);

        // Extract phenotype from output path
        let phenotype = output_base
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        // Scan and export to Parquet
        let parquet_path = output_base.join("gene_associations.parquet");
        let scan_result = scan_gene_burden_to_parquet(
            burden_path,
            phenotype,
            "meta", // Default ancestry
            parquet_path.to_str().unwrap(),
            config.gene_threshold,
            config.gene_maf_filter,
        )?;

        println!(
            "Exported {} gene rows to Parquet, found {} significant genes",
            scan_result.total_rows,
            scan_result.significant_genes.len()
        );

        // Write significant genes JSON
        let genes_json = serde_json::to_string_pretty(&scan_result.significant_genes)?;
        fs::write(output_base.join("significant_genes.json"), &genes_json)?;

        sig_genes = scan_result.significant_genes;
        gene_plot_points = scan_result.plot_points;
        gene_plot_points_by_group = scan_result.plot_points_by_group;

        // Add significant gene regions to interest regions
        for gene in &sig_genes {
            interest_regions.add(
                gene.interval.0.clone(),
                gene.interval.1,
                gene.interval.2,
            );
        }
    }

    // Prepare Buffers
    let mut exome_buffer: Vec<BufferedVariant> = Vec::new();
    let mut genome_buffer: Vec<BufferedVariant> = Vec::new();

    // Writers for consolidated output
    let sig_path = output_base.join("significant.parquet");
    let mut sig_writer = SigHitWriter::new(sig_path.to_str().unwrap())?;

    let mut locus_definitions = Vec::new();
    let mut locus_variants = Vec::new();
    let mut manifest_loci = Vec::new();

    // We need at least one table to establish layout
    let layout_path = config
        .exome
        .as_ref()
        .or(config.genome.as_ref());

    let (chrom_layout, y_scale) = if let Some(path) = layout_path {
        let layout_engine = QueryEngine::open_path(path)?;
        let all_contigs = get_contig_lengths(&layout_engine);

        // Filter to standard chromosomes
        let contigs: Vec<(String, u32)> = all_contigs
            .into_iter()
            .filter(|(name, _)| name.len() <= 5)
            .collect();

        let layout = ChromosomeLayout::new(&contigs, config.width, 4);
        let scale = YScale::new(config.height, 350.0); // Support p-values down to ~1e-350
        (layout, scale)
    } else {
        return Err(crate::HailError::InvalidFormat(
            "No variant tables provided".into(),
        ));
    };

    // 2b. Render Gene Manhattan plot (now that we have the layout)
    if run_aggregate && !gene_plot_points.is_empty() {
        println!("Rendering gene Manhattan plot ({} genes)...", gene_plot_points.len());
        // Render combined/legacy gene Manhattan
        let gene_png = render_gene_manhattan(
            &gene_plot_points,
            config.width,
            config.height,
            config.gene_threshold,
            &chrom_layout,
        )?;
        fs::write(plots_dir.join("gene_manhattan.png"), &gene_png)?;

        // Render grouped gene Manhattan plots for each (annotation, MAF) combination
        for ((annotation, maf_str), points) in &gene_plot_points_by_group {
            if points.is_empty() {
                continue;
            }
            let group_png = render_gene_manhattan(
                points,
                config.width,
                config.height,
                config.gene_threshold,
                &chrom_layout,
            )?;
            let filename = format!("gene_manhattan_{}_maf{}.png", annotation, maf_str);
            fs::write(plots_dir.join(filename), &group_png)?;
        }
    }

    // 3. Scan Exomes (if provided) - NO annotations during main scan for speed
    if run_scan {
        if let Some(res_path) = &config.exome {
            println!("Scanning Exome variants: {}", res_path);

            scan_variant_table(
                res_path,
                None, // Skip annotations during main scan
                VariantSource::Exome,
                &chrom_layout,
                &y_scale,
                config,
                &interest_regions,
                &mut exome_buffer,
                &mut sig_writer,
                plots_dir.join("exome_manhattan.png").to_str().unwrap(),
            )?;
        }

        // 4. Scan Genomes (if provided) - NO annotations during main scan for speed
        if let Some(res_path) = &config.genome {
            println!("Scanning Genome variants: {}", res_path);

            scan_variant_table(
                res_path,
                None, // Skip annotations during main scan
                VariantSource::Genome,
                &chrom_layout,
                &y_scale,
                config,
                &interest_regions,
                &mut genome_buffer,
                &mut sig_writer,
                plots_dir.join("genome_manhattan.png").to_str().unwrap(),
            )?;
        }
    }

    // 4b. Enrich buffered variants with annotations (targeted lookups)
    if !exome_buffer.is_empty() {
        if let Some(annot_path) = &config.exome_annotations {
            println!("Enriching {} exome variants with annotations...", exome_buffer.len());
            enrich_variants_with_annotations(&mut exome_buffer, annot_path)?;
        }
    }
    if !genome_buffer.is_empty() {
        if let Some(annot_path) = &config.genome_annotations {
            println!("Enriching {} genome variants with annotations...", genome_buffer.len());
            enrich_variants_with_annotations(&mut genome_buffer, annot_path)?;
        }
    }

    // Finish significant hits writer
    let sig_count = sig_writer.finish()?;
    println!("Wrote {} significant hits to significant.parquet", sig_count);

    if run_aggregate {
        // 5. Compute Locus Regions
        println!("Defining locus regions...");

        // We need to re-read significant positions to define regions since we streamed them to parquet
        // Ideally we would have tracked them in memory too, but let's read back from the file we just wrote
        // or just rely on gene regions if variants are too many.
        // For local pipeline, we can just use the buffered variants that are significant?
        // No, buffered variants are only for specific regions.

        // Let's re-read significant hits for region definition
        // Note: For large datasets this might be slow, but it's consistent with distributed flow
        use crate::manhattan::aggregate::extract_sig_positions;
        let sig_path_str = sig_path.to_str().unwrap();
        if let Ok(positions) = extract_sig_positions(sig_path_str) {
            for pos in positions {
                let start = (pos.position - config.locus_window).max(1);
                let end = pos.position + config.locus_window;
                interest_regions.add(pos.contig, start, end);
            }
        }

        interest_regions.optimize();
        println!("Total locus regions: {}", interest_regions.len());

        // 6. Generate Locus Outputs
    let loci_dir = output_base.join("loci");
    if config.locus_plots {
        fs::create_dir_all(&loci_dir)?;
    }

    // Collect contigs to iterate
    let contigs: Vec<String> = interest_regions.contigs().cloned().collect();

    for contig in &contigs {
            if let Some(ranges) = interest_regions.intervals_for_contig(contig) {
                for range in ranges {
                    let start = *range.start();
                    let end = *range.end();

                    // Filter buffers for this region
                    let exome_subset: Vec<&BufferedVariant> = exome_buffer
                        .iter()
                        .filter(|v| v.contig == *contig && v.position >= start && v.position <= end)
                        .collect();

                    let genome_subset: Vec<&BufferedVariant> = genome_buffer
                        .iter()
                        .filter(|v| v.contig == *contig && v.position >= start && v.position <= end)
                        .collect();

                    if exome_subset.is_empty() && genome_subset.is_empty() {
                        continue;
                    }

                    // Create region output directory (for plot only)
                    let region_name = format!("{}_{}_{}", contig, start, end);
                    let region_dir = loci_dir.join(&region_name);
                    if config.locus_plots {
                        fs::create_dir_all(&region_dir)?;
                    }

                    // Collect variants for parquet
                    // Phenotype/Ancestry defaults for local run
                    let phenotype_name = "local_run";
                    let ancestry_name = "meta";

                    let region_variants: Vec<LocusVariantRow> = exome_subset.iter()
                        .map(|v| LocusVariantRow {
                            locus_id: region_name.clone(),
                            phenotype: phenotype_name.to_string(),
                            ancestry: ancestry_name.to_string(),
                            sequencing_type: "exome".to_string(),
                            contig: contig.to_string(),
                            xpos: calculate_xpos(contig, v.position),
                            position: v.position,
                            ref_allele: v.alleles.first().cloned().unwrap_or_default(),
                            alt_allele: v.alleles.get(1).cloned().unwrap_or_default(),
                            pvalue: v.pvalue,
                            neg_log10_p: if v.pvalue > 0.0 { -v.pvalue.log10() as f32 } else { 0.0 },
                            is_significant: v.pvalue < config.threshold,
                            beta: v.beta,
                            se: v.se,
                            af: v.af,
                            ac_cases: v.ac_cases,
                            ac_controls: v.ac_controls,
                            af_cases: v.af_cases,
                            af_controls: v.af_controls,
                            association_ac: v.association_ac,
                        })
                        .chain(genome_subset.iter().map(|v| LocusVariantRow {
                            locus_id: region_name.clone(),
                            phenotype: phenotype_name.to_string(),
                            ancestry: ancestry_name.to_string(),
                            sequencing_type: "genome".to_string(),
                            contig: contig.to_string(),
                            xpos: calculate_xpos(contig, v.position),
                            position: v.position,
                            ref_allele: v.alleles.first().cloned().unwrap_or_default(),
                            alt_allele: v.alleles.get(1).cloned().unwrap_or_default(),
                            pvalue: v.pvalue,
                            neg_log10_p: if v.pvalue > 0.0 { -v.pvalue.log10() as f32 } else { 0.0 },
                            is_significant: v.pvalue < config.threshold,
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

                    locus_variants.extend(region_variants);

                    // Add Locus Definition
                    // Find lead variant
                    let lead = exome_subset.iter().chain(genome_subset.iter())
                        .min_by(|a, b| a.pvalue.partial_cmp(&b.pvalue).unwrap_or(std::cmp::Ordering::Equal));

                    let source_str = if !exome_subset.is_empty() && !genome_subset.is_empty() { "both" }
                        else if !exome_subset.is_empty() { "exome" }
                        else { "genome" };

                    locus_definitions.push(LocusDefinitionRow {
                        locus_id: region_name.clone(),
                        phenotype: phenotype_name.to_string(),
                        ancestry: ancestry_name.to_string(),
                        contig: normalize_contig_name(contig),
                        start,
                        stop: end,
                        xstart: calculate_xpos(contig, start),
                        xstop: calculate_xpos(contig, end),
                        source: source_str.to_string(),
                        lead_variant: lead.map(|v| format!("{}:{}", v.contig, v.position)).unwrap_or_default(),
                        lead_pvalue: lead.map(|v| v.pvalue).unwrap_or(1.0),
                        exome_count: exome_subset.len() as u32,
                        genome_count: genome_subset.len() as u32,
                    });

                    let plot_path = if config.locus_plots {
                        Some(format!("loci/{}/plot.png", region_name))
                    } else {
                        None
                    };

                    // Add to Manifest
                    manifest_loci.push(ManifestLocus {
                        id: region_name.clone(),
                        region: ManifestRegion { contig: normalize_contig_name(contig), start: start as i64, end: end as i64 },
                        source: source_str.to_string(),
                        lead_variant: lead.map(|v| format!("{}:{}", v.contig, v.position)).unwrap_or_default(),
                        lead_pvalue: lead.map(|v| v.pvalue).unwrap_or(1.0),
                        lead_gene: lead.and_then(|v| v.gene_symbol.clone()),
                        plot: plot_path,
                        exome_variants: if !exome_subset.is_empty() { Some(ManifestLocusVariants { path: format!("loci_variants.parquet (locus_id={})", region_name), count: exome_subset.len() as u64 }) } else { None },
                        genome_variants: if !genome_subset.is_empty() { Some(ManifestLocusVariants { path: format!("loci_variants.parquet (locus_id={})", region_name), count: genome_subset.len() as u64 }) } else { None },
                        genes: vec![],
                    });

                    // Render locus plot
                    if config.locus_plots {
                        let png_data = render_locus_plot(
                            &exome_subset,
                            &genome_subset,
                            contig,
                            start,
                            end,
                            config.threshold,
                        )?;
                        fs::write(region_dir.join("plot.png"), png_data)?;
                    }

                    println!("  Generated locus data: {}", region_name);
                }
            }
        }

        // Write loci parquet files
        if !locus_definitions.is_empty() {
            println!("Writing loci.parquet...");
            let mut writer = LocusDefinitionWriter::new(output_base.join("loci.parquet").to_str().unwrap())?;
            writer.write_batch(&locus_definitions)?;
            writer.finish()?;

            println!("Writing loci_variants.parquet...");
            let mut writer = LocusVariantWriter::new(output_base.join("loci_variants.parquet").to_str().unwrap())?;
            writer.write_batch(&locus_variants)?;
            writer.finish()?;
        }

        // Write summary
        let summary = serde_json::json!({
            "significant_genes": sig_genes.len(),
            "significant_variants": sig_count,
            "exome_variants_buffered": exome_buffer.len(),
            "genome_variants_buffered": genome_buffer.len(),
            "locus_regions": interest_regions.len(),
        });
        fs::write(
            output_base.join("summary.json"),
            serde_json::to_string_pretty(&summary)?,
        )?;
    }

    // Suppress unused warnings for manifest_loci (used in JSON export which we skipped for now)
    let _ = manifest_loci;

    println!("Pipeline complete. Output: {:?}", output_base);
    Ok(())
}

/// Scan a variant table (with optional annotation join), render Manhattan, and buffer variants.
#[allow(clippy::too_many_arguments)]
fn scan_variant_table(
    results_path: &str,
    annotations_path: Option<&str>,
    source: VariantSource,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    config: &PipelineConfig,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut SigHitWriter<std::fs::File>,
    output_png: &str,
) -> Result<()> {
    let results_engine = QueryEngine::open_path(results_path)?;
    let mut renderer = ManhattanRenderer::new(config.width, config.height);

    // Per-chromosome renderers/layouts for local run
    let mut chrom_renderers: HashMap<String, ManhattanRenderer> = HashMap::new();
    let mut chrom_layouts: HashMap<String, ChromosomeLayout> = HashMap::new();

    // Build contig lengths map for per-chromosome layouts
    let contigs = get_contig_lengths(&results_engine);
    let contig_lengths: HashMap<String, u32> = contigs.iter().cloned().collect();

    // Render threshold line
    renderer.render_threshold_line(y_scale.threshold_y(config.threshold), config.width);

    // Progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg} {pos} rows")
            .unwrap(),
    );
    pb.set_message(match source {
        VariantSource::Exome => "Exome:",
        VariantSource::Genome => "Genome:",
    });

    let y_field = &config.y_field;
    let locus_threshold = config.locus_threshold;
    let variant_threshold = config.threshold;

    if let Some(annot_path) = annotations_path {
        // Merge-join with annotations
        let annot_engine = QueryEngine::open_path(annot_path)?;

        let keys = vec!["locus".to_string(), "alleles".to_string()];
        // Use sorted iterators for merge-join (sequential partition iteration)
        let results_iter = results_engine.query_iter_sorted(&[])?;
        let annot_iter = annot_engine.query_iter_sorted(&[])?;
        let join_iter = SortedMergeIterator::new(results_iter, annot_iter, keys);

        let mut matched = 0usize;
        let mut unmatched = 0usize;
        for (i, join_res) in join_iter.enumerate() {
            let row = join_res?;

            // Track match status
            if row.right.is_some() {
                matched += 1;
            } else {
                unmatched += 1;
            }

            process_joined_row(
                &row,
                source,
                layout,
                y_scale,
                y_field,
                locus_threshold,
                variant_threshold,
                interest_regions,
                buffer,
                sig_writer,
                &mut renderer,
                &contig_lengths,
                &mut chrom_renderers,
                &mut chrom_layouts,
                config.width,
                config.height,
            );

            if i % 10_000 == 0 {
                pb.set_position(i as u64);
            }
        }
        println!("  Merge complete: {} matched, {} unmatched", matched, unmatched);
    } else {
        // No annotations - just scan results
        let results_iter = results_engine.query_iter(&[])?;

        for (i, row_res) in results_iter.enumerate() {
            let row = row_res?;
            process_single_row(
                &row,
                source,
                layout,
                y_scale,
                y_field,
                locus_threshold,
                variant_threshold,
                interest_regions,
                buffer,
                sig_writer,
                &mut renderer,
                &contig_lengths,
                &mut chrom_renderers,
                &mut chrom_layouts,
                config.width,
                config.height,
            );

            if i % 10_000 == 0 {
                pb.set_position(i as u64);
            }
        }
    }

    pb.finish_with_message("complete");

    // Save WG Manhattan PNG
    let png_data = renderer.encode_png()?;
    fs::write(output_png, png_data)?;
    println!("Saved: {}", output_png);

    // Save Per-Chromosome PNGs
    let output_dir = Path::new(output_png).parent().unwrap();
    let file_name = Path::new(output_png)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();

    for (chrom, mut chrom_renderer) in chrom_renderers {
        // Draw threshold line on per-chrom plot
        chrom_renderer.render_threshold_line(y_scale.threshold_y(config.threshold), config.width);

        let data = chrom_renderer.encode_png()?;
        let chrom_dir = output_dir.join("chroms").join(&chrom);
        fs::create_dir_all(&chrom_dir)?;
        let out_path = chrom_dir.join(file_name);
        fs::write(&out_path, data)?;
    }

    Ok(())
}

/// Process a joined row (results + annotations).
#[allow(clippy::too_many_arguments)]
fn process_joined_row(
    row: &JoinedRow,
    source: VariantSource,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    y_field: &str,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut SigHitWriter<std::fs::File>,
    renderer: &mut ManhattanRenderer,
    // Per-chromosome rendering support
    contig_lengths: &HashMap<String, u32>,
    chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
    chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    width: u32,
    height: u32,
) {
    // Extract from left (results)
    let v = match extract_variant_fields(&row.left, y_field) {
        Some(v) => v,
        None => return,
    };

    // Extract from right (annotations) if present
    let (gene_symbol, consequence) = if let Some(ref annot) = row.right {
        extract_annotation_fields(annot)
    } else {
        (None, None)
    };

    // Render point to WG
    render_variant(
        &v.contig, v.position, v.neg_log10_p, layout, y_scale, renderer,
    );

    // Render point to per-chromosome plot
    render_variant_per_chrom(
        &v.contig, v.position, v.neg_log10_p, layout, y_scale,
        contig_lengths, chrom_renderers, chrom_layouts, width, height,
    );

    // Check significance
    if v.pvalue < variant_threshold {
        // Write to parquet
        let _ = sig_writer.write(SigHitRow {
            phenotype: "local_run".to_string(),
            ancestry: "meta".to_string(),
            sequencing_type: match source {
                VariantSource::Exome => "exome".to_string(),
                VariantSource::Genome => "genome".to_string(),
            },
            xpos: calculate_xpos(&v.contig, v.position),
            contig: normalize_contig_name(&v.contig),
            position: v.position,
            ref_allele: v.alleles.first().cloned().unwrap_or_default(),
            alt_allele: v.alleles.get(1).cloned().unwrap_or_default(),
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }

    // Buffer if interesting
    let should_buffer =
        v.pvalue < locus_threshold || interest_regions.contains(&v.contig, v.position);

    if should_buffer {
        buffer.push(BufferedVariant {
            contig: v.contig,
            position: v.position,
            alleles: v.alleles,
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            source,
            gene_symbol,
            consequence,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }
}

/// Process a single row (no annotations).
#[allow(clippy::too_many_arguments)]
fn process_single_row(
    row: &EncodedValue,
    source: VariantSource,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    y_field: &str,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut SigHitWriter<std::fs::File>,
    renderer: &mut ManhattanRenderer,
    // Per-chromosome rendering support
    contig_lengths: &HashMap<String, u32>,
    chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
    chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    width: u32,
    height: u32,
) {
    let v = match extract_variant_fields(row, y_field) {
        Some(v) => v,
        None => return,
    };

    // Render point to WG
    render_variant(
        &v.contig, v.position, v.neg_log10_p, layout, y_scale, renderer,
    );

    // Render point to per-chromosome plot
    render_variant_per_chrom(
        &v.contig, v.position, v.neg_log10_p, layout, y_scale,
        contig_lengths, chrom_renderers, chrom_layouts, width, height,
    );

    // Check significance
    if v.pvalue < variant_threshold {
        // Write to parquet
        let _ = sig_writer.write(SigHitRow {
            phenotype: "local_run".to_string(),
            ancestry: "meta".to_string(),
            sequencing_type: match source {
                VariantSource::Exome => "exome".to_string(),
                VariantSource::Genome => "genome".to_string(),
            },
            xpos: calculate_xpos(&v.contig, v.position),
            contig: normalize_contig_name(&v.contig),
            position: v.position,
            ref_allele: v.alleles.first().cloned().unwrap_or_default(),
            alt_allele: v.alleles.get(1).cloned().unwrap_or_default(),
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }

    // Buffer if interesting
    let should_buffer =
        v.pvalue < locus_threshold || interest_regions.contains(&v.contig, v.position);

    if should_buffer {
        buffer.push(BufferedVariant {
            contig: v.contig,
            position: v.position,
            alleles: v.alleles,
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            source,
            gene_symbol: None,
            consequence: None,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }
}

/// Render a single variant point on the Manhattan plot.
fn render_variant(
    contig: &str,
    position: i32,
    neg_log10_p: f64,
    layout: &ChromosomeLayout,
    y_scale: &YScale,
    renderer: &mut ManhattanRenderer,
) {
    // Normalize contig (strip chr prefix)
    let contig_name = if contig.starts_with("chr") {
        &contig[3..]
    } else {
        contig
    };

    if let Some(x) = layout.get_x(contig_name, position) {
        let y = y_scale.get_y(neg_log10_p);
        let color = layout.get_color(contig_name);
        renderer.render_point(x, y, color, 0.6);
    }
}

/// Render a variant to its per-chromosome plot.
#[allow(clippy::too_many_arguments)]
fn render_variant_per_chrom(
    contig: &str,
    position: i32,
    neg_log10_p: f64,
    wg_layout: &ChromosomeLayout,
    y_scale: &YScale,
    contig_lengths: &HashMap<String, u32>,
    chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
    chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    width: u32,
    height: u32,
) {
    // Normalize contig (strip chr prefix for layout lookup)
    let contig_for_layout = if contig.starts_with("chr") {
        &contig[3..]
    } else {
        contig
    };

    // Get normalized contig name for map keys (e.g., "chr1")
    let normalized_contig = normalize_contig_name(contig);

    // Look up length using short name
    if let Some(&len) = contig_lengths
        .get(contig_for_layout)
        .or_else(|| contig_lengths.get(&normalized_contig))
    {
        // Initialize renderer and layout for this chromosome if needed
        let chrom_layout = chrom_layouts
            .entry(normalized_contig.clone())
            .or_insert_with(|| {
                // Create a layout where this single chromosome fills the width
                ChromosomeLayout::new(&[(contig_for_layout.to_string(), len)], width, 0)
            });

        let chrom_renderer = chrom_renderers
            .entry(normalized_contig)
            .or_insert_with(|| ManhattanRenderer::new(width, height));

        if let Some(x) = chrom_layout.get_x(contig_for_layout, position) {
            let y = y_scale.get_y(neg_log10_p);
            // Use same color scheme as WG plot
            let color = wg_layout.get_color(contig_for_layout);
            chrom_renderer.render_point(x, y, color, 0.6);
        }
    }
}

/// Maximum -log10(p) for display (caps underflowed p-values)
const MAX_NEG_LOG10_P: f64 = 350.0;

/// Extracted variant fields from a Hail table row.
#[derive(Debug, Clone)]
struct ExtractedVariant {
    contig: String,
    position: i32,
    pvalue: f64,
    neg_log10_p: f64,
    beta: Option<f64>,
    se: Option<f64>,
    af: Option<f64>,
    alleles: Vec<String>,
    ac_cases: Option<f64>,
    ac_controls: Option<f64>,
    af_cases: Option<f64>,
    af_controls: Option<f64>,
    association_ac: Option<f64>,
}

/// Extract variant fields from a row.
fn extract_variant_fields(
    row: &EncodedValue,
    y_field: &str,
) -> Option<ExtractedVariant> {
    if let EncodedValue::Struct(fields) = row {
        // Helper to extract optional float
        let get_float = |name: &str| -> Option<f64> {
            fields
                .iter()
                .find(|(n, _)| n == name)
                .and_then(|(_, v)| match v {
                    EncodedValue::Float64(f) => Some(*f),
                    EncodedValue::Float32(f) => Some(*f as f64),
                    _ => None,
                })
        };

        // Extract locus
        let locus = fields.iter().find(|(n, _)| n == "locus").map(|(_, v)| v)?;

        let (contig, position) = if let EncodedValue::Struct(locus_fields) = locus {
            let contig = locus_fields
                .iter()
                .find(|(n, _)| n == "contig")
                .and_then(|(_, v)| v.as_string())?;
            let pos = locus_fields
                .iter()
                .find(|(n, _)| n == "position")
                .and_then(|(_, v)| v.as_i32())?;
            (contig, pos)
        } else {
            return None;
        };

        // Extract p-value
        let pvalue = fields
            .iter()
            .find(|(n, _)| n == y_field)
            .and_then(|(_, v)| match v {
                EncodedValue::Float64(f) => Some(*f),
                EncodedValue::Float32(f) => Some(*f as f64),
                _ => None,
            })?;

        // Try to get pre-computed Pvalue_log10 from source (preserves precision)
        let pvalue_log10_field = format!("{}_log10", y_field);
        let source_neg_log10_p = get_float(&pvalue_log10_field)
            .or_else(|| get_float("Pvalue_log10"))
            .filter(|v| v.is_finite());

        // Compute neg_log10_p: prefer source field, fallback to computing
        let neg_log10_p = match source_neg_log10_p {
            Some(v) => v,
            None => {
                if pvalue <= 0.0 {
                    // P-value underflowed to 0 - cap at max displayable value
                    MAX_NEG_LOG10_P
                } else if pvalue > 1.0 || !pvalue.is_finite() {
                    // Truly invalid p-value
                    return None;
                } else {
                    -pvalue.log10()
                }
            }
        };

        // Extract beta (optional) - try BETA first, then beta
        let beta = get_float("BETA").or_else(|| get_float("beta"));

        // Extract SE (optional) - try SE first, then se
        let se = get_float("SE").or_else(|| get_float("se"));

        // Extract AF (optional) - try AF_Allele2 first, then AF, then af
        let af = get_float("AF_Allele2")
            .or_else(|| get_float("AF"))
            .or_else(|| get_float("af"));

        // Extract case/control fields
        let ac_cases = get_float("AC_case").or_else(|| get_float("ac_case"));
        let ac_controls = get_float("AC_ctrl").or_else(|| get_float("ac_ctrl"));
        let af_cases = get_float("AF_case").or_else(|| get_float("af_case"));
        let af_controls = get_float("AF_ctrl").or_else(|| get_float("af_ctrl"));

        // Extract association allele count (AC_Allele2) - can be int or float
        let association_ac = get_float("AC_Allele2").or_else(|| {
            fields
                .iter()
                .find(|(n, _)| n == "AC_Allele2")
                .and_then(|(_, v)| match v {
                    EncodedValue::Int64(i) => Some(*i as f64),
                    EncodedValue::Int32(i) => Some(*i as f64),
                    _ => None,
                })
        });

        // Extract alleles
        let alleles = fields
            .iter()
            .find(|(n, _)| n == "alleles")
            .and_then(|(_, v)| {
                if let EncodedValue::Array(arr) = v {
                    Some(
                        arr.iter()
                            .filter_map(|a| a.as_string())
                            .collect::<Vec<String>>(),
                    )
                } else {
                    None
                }
            })
            .unwrap_or_default();

        Some(ExtractedVariant {
            contig,
            position,
            pvalue,
            neg_log10_p,
            beta,
            se,
            af,
            alleles,
            ac_cases,
            ac_controls,
            af_cases,
            af_controls,
            association_ac,
        })
    } else {
        None
    }
}

/// Extract annotation fields from an annotation row.
fn extract_annotation_fields(row: &EncodedValue) -> (Option<String>, Option<String>) {
    if let EncodedValue::Struct(fields) = row {
        let gene_symbol = fields
            .iter()
            .find(|(n, _)| n == "gene_symbol")
            .and_then(|(_, v)| v.as_string());

        let consequence = fields
            .iter()
            .find(|(n, _)| n == "most_severe_csq_variant" || n == "consequence")
            .and_then(|(_, v)| v.as_string());

        (gene_symbol, consequence)
    } else {
        (None, None)
    }
}

/// Render a locus plot for a specific region.
fn render_locus_plot(
    exome_variants: &[&BufferedVariant],
    genome_variants: &[&BufferedVariant],
    _contig: &str,
    start: i32,
    end: i32,
    threshold: f64,
) -> Result<Vec<u8>> {
    let config = LocusPlotConfig {
        width: 800,
        height: 400,
        start_pos: start,
        end_pos: end,
        y_max: 30.0,
    };

    let mut renderer = LocusRenderer::new(config);
    renderer.draw_threshold_line(threshold);

    // Convert buffered variants to render variants
    let mut render_variants: Vec<RenderVariant> = Vec::new();

    for v in genome_variants {
        render_variants.push(RenderVariant {
            position: v.position,
            ref_allele: v.alleles.first().cloned().unwrap_or_default(),
            alt_allele: v.alleles.get(1).cloned().unwrap_or_default(),
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            source: VariantSource::Genome,
            is_significant: v.pvalue < threshold,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }

    for v in exome_variants {
        render_variants.push(RenderVariant {
            position: v.position,
            ref_allele: v.alleles.first().cloned().unwrap_or_default(),
            alt_allele: v.alleles.get(1).cloned().unwrap_or_default(),
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            source: VariantSource::Exome,
            is_significant: v.pvalue < threshold,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }

    renderer.draw_variants(&render_variants);
    renderer.encode_png()
}

/// Enrich buffered variants with annotations via targeted lookups.
///
/// This is much faster than merge-join for small numbers of variants
/// because it uses index lookups instead of scanning the entire table.
fn enrich_variants_with_annotations(
    variants: &mut [BufferedVariant],
    annot_path: &str,
) -> Result<()> {
    let mut annot_engine = QueryEngine::open_path(annot_path)?;

    for variant in variants.iter_mut() {
        // Build a key for lookup
        let key = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    ("contig".to_string(), EncodedValue::Binary(variant.contig.as_bytes().to_vec())),
                    ("position".to_string(), EncodedValue::Int32(variant.position)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(
                    variant.alleles.iter()
                        .map(|a| EncodedValue::Binary(a.as_bytes().to_vec()))
                        .collect()
                ),
            ),
        ]);

        // Try point lookup
        if let Ok(Some(annot_row)) = annot_engine.lookup(&key) {
            let (gene_symbol, consequence) = extract_annotation_fields(&annot_row);
            variant.gene_symbol = gene_symbol;
            variant.consequence = consequence;
        }
    }

    Ok(())
}
