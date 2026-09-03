//! Integrated local pipeline for Manhattan plots.
//!
//! This module orchestrates the full workflow for local execution:
//! 1. Load gene map and process gene burden table (if provided)
//! 2. Scan exome variants with annotation merge-join
//! 3. Scan genome variants with annotation merge-join
//! 4. Compute locus regions from significant signals
//! 5. Generate locus plots and JSON exports

use crate::manhattan::data::{
    extract_association_point, BufferedVariant, LocusDefinitionRow, LocusVariantRow, ManifestLocus,
    ManifestLocusVariants, ManifestRegion, SigHitRow, VariantSource,
};
use crate::manhattan::genes::{
    render_gene_manhattan, scan_gene_burden_to_parquet, GeneMap, SignificantGene,
};
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::legacy_manifest::{
    merge_partition_summaries, write_manifest, LegacyPartitionSummary, LegacyScanSummary,
};
use crate::manhattan::loci_writer::{LocusDefinitionWriter, LocusVariantWriter};
use crate::manhattan::locus::{LocusPlotConfig, LocusRenderer, RenderVariant};
use crate::manhattan::reference::{calculate_xpos, get_contig_lengths, normalize_contig_name};
use crate::manhattan::render::ManhattanRenderer;
use crate::manhattan::sig_writer::SigHitWriter;
use crate::Result;
use genohype_association::{
    canonical_contig_lengths, Assembly, AssociationPointV1, GenomeLayoutV1,
};
use genohype_core::codec::EncodedValue;
use genohype_core::query::join::{JoinedRow, SortedMergeIterator};
use genohype_core::query::{IntervalList, QueryEngine};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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
        println!(
            "Note: --scan-only and --aggregate-only modes skip certain local processing steps."
        );
    }

    // 2. Process Gene Burden (if provided)
    let mut interest_regions = IntervalList::new();
    let mut sig_genes: Vec<SignificantGene> = Vec::new();
    let mut gene_plot_points: Vec<crate::manhattan::data::GenePlotPoint> = Vec::new();
    let mut gene_plot_points_by_group: HashMap<
        (String, String),
        Vec<crate::manhattan::data::GenePlotPoint>,
    > = HashMap::new();

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
            interest_regions.add(gene.interval.0.clone(), gene.interval.1, gene.interval.2);
        }
    }

    // Prepare Buffers
    let mut exome_buffer: Vec<BufferedVariant> = Vec::new();
    let mut genome_buffer: Vec<BufferedVariant> = Vec::new();

    // Aggregate-only consumes the scan artifact and must never truncate it.
    let sig_path = output_base.join("significant.parquet");
    let mut sig_writer = if run_scan {
        Some(SigHitWriter::new(sig_path.to_str().unwrap())?)
    } else {
        None
    };

    let mut locus_definitions = Vec::new();
    let mut locus_variants = Vec::new();
    let mut manifest_loci = Vec::new();

    if config.exome.is_none() && config.genome.is_none() {
        return Err(crate::HailError::InvalidFormat(
            "No variant tables provided".into(),
        ));
    }

    // V1 whole-genome rasters have one assembly-defined geometry regardless of which
    // table was supplied. Legacy gene/per-chromosome outputs retain their old renderer.
    let canonical_contigs = canonical_contig_lengths(Assembly::Grch38);
    let association_layout =
        GenomeLayoutV1::new(canonical_contigs.clone(), config.width, config.height, 4)
            .map_err(|error| crate::HailError::InvalidFormat(error.to_string()))?;
    let compatibility_contigs: Vec<(String, u32)> = canonical_contigs
        .iter()
        .map(|(name, length)| {
            u32::try_from(*length)
                .map(|length| (name.clone(), length))
                .map_err(|_| {
                    crate::HailError::InvalidFormat(format!(
                        "canonical contig {name} exceeds compatibility layout capacity"
                    ))
                })
        })
        .collect::<Result<_>>()?;
    let chrom_layout = ChromosomeLayout::new(&compatibility_contigs, config.width, 4);
    let y_scale = YScale::new(config.height, 350.0);

    // 2b. Render Gene Manhattan plot (now that we have the layout)
    if run_aggregate && !gene_plot_points.is_empty() {
        println!(
            "Rendering gene Manhattan plot ({} genes)...",
            gene_plot_points.len()
        );
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

    // Every successful local whole-genome raster gets a canonical sibling manifest.
    if run_scan {
        if let Some(res_path) = &config.exome {
            println!("Scanning Exome variants: {}", res_path);
            let scan = scan_variant_table(
                res_path,
                None, // Keep the fast scan; buffered hits are enriched below.
                VariantSource::Exome,
                &association_layout,
                &chrom_layout,
                &y_scale,
                config,
                &interest_regions,
                &mut exome_buffer,
                sig_writer.as_mut().expect("scan writer is initialized"),
                &plots_dir.join("exome_manhattan.png"),
            )?;
            report_scan_summary("Exome", &scan);
            publish_scan_manifest(&scan, &association_layout, config.threshold)?;
        }

        if let Some(res_path) = &config.genome {
            println!("Scanning Genome variants: {}", res_path);
            let scan = scan_variant_table(
                res_path,
                None, // Keep the fast scan; buffered hits are enriched below.
                VariantSource::Genome,
                &association_layout,
                &chrom_layout,
                &y_scale,
                config,
                &interest_regions,
                &mut genome_buffer,
                sig_writer.as_mut().expect("scan writer is initialized"),
                &plots_dir.join("genome_manhattan.png"),
            )?;
            report_scan_summary("Genome", &scan);
            publish_scan_manifest(&scan, &association_layout, config.threshold)?;
        }
    }

    // 4b. Enrich buffered variants with annotations (targeted lookups)
    if !exome_buffer.is_empty() {
        if let Some(annot_path) = &config.exome_annotations {
            println!(
                "Enriching {} exome variants with annotations...",
                exome_buffer.len()
            );
            enrich_variants_with_annotations(&mut exome_buffer, annot_path)?;
        }
    }
    if !genome_buffer.is_empty() {
        if let Some(annot_path) = &config.genome_annotations {
            println!(
                "Enriching {} genome variants with annotations...",
                genome_buffer.len()
            );
            enrich_variants_with_annotations(&mut genome_buffer, annot_path)?;
        }
    }

    // Finish scan output, or inspect the pre-existing scan artifact without replacing it.
    let sig_count = if let Some(writer) = sig_writer {
        let count = writer.finish()?;
        println!("Wrote {} significant hits to significant.parquet", count);
        count
    } else {
        use crate::manhattan::aggregate::extract_sig_positions;
        let count = extract_sig_positions(sig_path.to_str().unwrap())?.len();
        println!(
            "Aggregate-only mode found {} existing significant hits",
            count
        );
        count
    };

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

                    let region_variants: Vec<LocusVariantRow> = exome_subset
                        .iter()
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
                            neg_log10_p: if v.pvalue > 0.0 {
                                -v.pvalue.log10() as f32
                            } else {
                                0.0
                            },
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
                            neg_log10_p: if v.pvalue > 0.0 {
                                -v.pvalue.log10() as f32
                            } else {
                                0.0
                            },
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
                    let lead = exome_subset
                        .iter()
                        .chain(genome_subset.iter())
                        .min_by(|a, b| {
                            a.pvalue
                                .partial_cmp(&b.pvalue)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });

                    let source_str = if !exome_subset.is_empty() && !genome_subset.is_empty() {
                        "both"
                    } else if !exome_subset.is_empty() {
                        "exome"
                    } else {
                        "genome"
                    };

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
                        lead_variant: lead
                            .map(|v| format!("{}:{}", v.contig, v.position))
                            .unwrap_or_default(),
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
                        region: ManifestRegion {
                            contig: normalize_contig_name(contig),
                            start: start as i64,
                            end: end as i64,
                        },
                        source: source_str.to_string(),
                        lead_variant: lead
                            .map(|v| format!("{}:{}", v.contig, v.position))
                            .unwrap_or_default(),
                        lead_pvalue: lead.map(|v| v.pvalue).unwrap_or(1.0),
                        lead_gene: lead.and_then(|v| v.gene_symbol.clone()),
                        plot: plot_path,
                        exome_variants: if !exome_subset.is_empty() {
                            Some(ManifestLocusVariants {
                                path: format!("loci_variants.parquet (locus_id={})", region_name),
                                count: exome_subset.len() as u64,
                            })
                        } else {
                            None
                        },
                        genome_variants: if !genome_subset.is_empty() {
                            Some(ManifestLocusVariants {
                                path: format!("loci_variants.parquet (locus_id={})", region_name),
                                count: genome_subset.len() as u64,
                            })
                        } else {
                            None
                        },
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
            let mut writer =
                LocusDefinitionWriter::new(output_base.join("loci.parquet").to_str().unwrap())?;
            writer.write_batch(&locus_definitions)?;
            writer.finish()?;

            println!("Writing loci_variants.parquet...");
            let mut writer = LocusVariantWriter::new(
                output_base.join("loci_variants.parquet").to_str().unwrap(),
            )?;
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

/// Bounded canonical result of one integrated association scan.
///
/// PNG bytes are retained so the sibling manifest describes the exact raster bytes
/// written by this scan without a second read.
#[derive(Debug)]
pub(crate) struct IntegratedScanSummary {
    pub row_count: u64,
    pub valid_row_count: u64,
    pub skipped_row_count: u64,
    pub rendered_point_count: u64,
    pub significant_hit_count: u64,
    /// Canonically ordered best 100 points.
    pub top_hits: Vec<AssociationPointV1>,
    /// Stable warning codes, deduplicated and lexically ordered.
    pub warnings: Vec<String>,
    /// SHA-256 over decoded source rows only; input paths are not included.
    pub source_hash: String,
    pub png_bytes: Vec<u8>,
    pub png_path: PathBuf,
}

impl IntegratedScanSummary {
    fn from_scan(scan: LegacyScanSummary, png_bytes: Vec<u8>, png_path: PathBuf) -> Self {
        Self {
            row_count: scan.row_count,
            valid_row_count: scan.valid_row_count,
            skipped_row_count: scan.skipped_row_count,
            rendered_point_count: scan.rendered_point_count,
            significant_hit_count: scan.significant_hit_count,
            top_hits: scan.top_hits,
            warnings: scan.warnings,
            source_hash: scan.source_fingerprint,
            png_bytes,
            png_path,
        }
    }

    fn legacy_summary(&self) -> LegacyScanSummary {
        LegacyScanSummary {
            row_count: self.row_count,
            valid_row_count: self.valid_row_count,
            skipped_row_count: self.skipped_row_count,
            rendered_point_count: self.rendered_point_count,
            significant_hit_count: self.significant_hit_count,
            top_hits: self.top_hits.clone(),
            warnings: self.warnings.clone(),
            source_fingerprint: self.source_hash.clone(),
        }
    }
}

/// Publish only after `scan_variant_table` has successfully written the whole-genome PNG.
fn publish_scan_manifest(
    scan: &IntegratedScanSummary,
    layout: &GenomeLayoutV1,
    threshold: f64,
) -> Result<()> {
    let output_base = scan.png_path.with_extension("");
    let output_base = output_base.to_str().ok_or_else(|| {
        crate::HailError::InvalidFormat("Manhattan output path is not valid UTF-8".into())
    })?;
    let manifest_path = write_manifest(
        output_base,
        layout.clone(),
        threshold,
        scan.legacy_summary(),
        &scan.png_bytes,
    )?;
    println!("Saved: {manifest_path}");
    Ok(())
}

fn report_scan_summary(label: &str, scan: &IntegratedScanSummary) {
    println!(
        "  {label} scan: {} rows, {} valid, {} skipped, {} rendered, {} significant, {} top hits",
        scan.row_count,
        scan.valid_row_count,
        scan.skipped_row_count,
        scan.rendered_point_count,
        scan.significant_hit_count,
        scan.top_hits.len()
    );
    println!(
        "  Source: {}; raster: {} bytes at {}; warnings: {}",
        scan.source_hash,
        scan.png_bytes.len(),
        scan.png_path.display(),
        if scan.warnings.is_empty() {
            "none".to_string()
        } else {
            scan.warnings.join(", ")
        }
    );
}

trait SignificantHitSink {
    fn write_hit(&mut self, row: SigHitRow) -> Result<()>;
}

impl<W: Write + Send> SignificantHitSink for SigHitWriter<W> {
    fn write_hit(&mut self, row: SigHitRow) -> Result<()> {
        self.write(row)
    }
}

fn sequential_ordinal(index: usize) -> Result<u64> {
    u64::try_from(index).map_err(|_| {
        crate::HailError::InvalidFormat("integrated scan row ordinal overflowed".into())
    })
}

/// Scan a variant table (with optional annotation join), render Manhattan, and buffer variants.
#[allow(clippy::too_many_arguments)]
fn scan_variant_table(
    results_path: &str,
    annotations_path: Option<&str>,
    source: VariantSource,
    layout: &GenomeLayoutV1,
    compatibility_layout: &ChromosomeLayout,
    compatibility_y_scale: &YScale,
    config: &PipelineConfig,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut SigHitWriter<std::fs::File>,
    output_png: &Path,
) -> Result<IntegratedScanSummary> {
    let results_engine = QueryEngine::open_path(results_path)?;
    let mut renderer = ManhattanRenderer::new(config.width, config.height);
    let mut scan_summary = LegacyPartitionSummary::new(0)?;

    // Per-chromosome renderers/layouts for local run
    let mut chrom_renderers: HashMap<String, ManhattanRenderer> = HashMap::new();
    let mut chrom_layouts: HashMap<String, ChromosomeLayout> = HashMap::new();

    // Build contig lengths map for per-chromosome layouts
    let contigs = get_contig_lengths(&results_engine);
    let contig_lengths: HashMap<String, u32> = contigs.iter().cloned().collect();

    let threshold_y = layout
        .y_scale
        .threshold_y(config.threshold)
        .ok_or_else(|| {
            crate::HailError::InvalidFormat("invalid Manhattan significance threshold".into())
        })?;
    renderer.render_threshold_line(threshold_y, config.width);

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

            let ordinal = sequential_ordinal(i)?;
            scan_summary.observe_source_row(&row.left)?;
            match extract_association_point(&row.left, y_field, ordinal) {
                Ok((point, warnings)) => {
                    let variant = extract_variant_fields(&row.left, &point)?;
                    let significant = point.pvalue < variant_threshold;
                    let rendered = process_joined_row(
                        &row,
                        &point,
                        variant,
                        source,
                        layout,
                        compatibility_layout,
                        compatibility_y_scale,
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
                    )?;
                    scan_summary.observe_valid_point(point, warnings, rendered, significant)?;
                }
                Err(_) => scan_summary.observe_skipped_row()?,
            }

            if i % 10_000 == 0 {
                pb.set_position(ordinal);
            }
        }
        println!(
            "  Merge complete: {} matched, {} unmatched",
            matched, unmatched
        );
    } else {
        // No annotations - just scan results
        let results_iter = results_engine.query_iter(&[])?;

        for (i, row_res) in results_iter.enumerate() {
            let row = row_res?;
            let ordinal = sequential_ordinal(i)?;
            scan_summary.observe_source_row(&row)?;
            match extract_association_point(&row, y_field, ordinal) {
                Ok((point, warnings)) => {
                    let variant = extract_variant_fields(&row, &point)?;
                    let significant = point.pvalue < variant_threshold;
                    let rendered = process_single_row(
                        &point,
                        variant,
                        source,
                        layout,
                        compatibility_layout,
                        compatibility_y_scale,
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
                    )?;
                    scan_summary.observe_valid_point(point, warnings, rendered, significant)?;
                }
                Err(_) => scan_summary.observe_skipped_row()?,
            }

            if i % 10_000 == 0 {
                pb.set_position(ordinal);
            }
        }
    }

    pb.finish_with_message("complete");

    // The manifest is published by the caller only after this whole-genome raster exists.
    let png_data = renderer.encode_png()?;
    fs::write(output_png, &png_data)?;
    println!("Saved: {}", output_png.display());

    // Compatibility-only per-chromosome outputs retain their historical scale and names.
    let output_dir = output_png.parent().unwrap_or_else(|| Path::new("."));
    let file_name = output_png
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            crate::HailError::InvalidFormat("Manhattan output has no UTF-8 basename".into())
        })?;
    for (chrom, mut chrom_renderer) in chrom_renderers {
        chrom_renderer.render_threshold_line(
            compatibility_y_scale.threshold_y(config.threshold),
            config.width,
        );
        let data = chrom_renderer.encode_png()?;
        let chrom_dir = output_dir.join("chroms").join(&chrom);
        fs::create_dir_all(&chrom_dir)?;
        fs::write(chrom_dir.join(file_name), data)?;
    }

    let scan = merge_partition_summaries(vec![scan_summary])?;
    Ok(IntegratedScanSummary::from_scan(
        scan,
        png_data,
        output_png.to_path_buf(),
    ))
}

/// Process a joined row (results + annotations) that was already canonically validated.
#[allow(clippy::too_many_arguments)]
fn process_joined_row<S: SignificantHitSink>(
    row: &JoinedRow,
    point: &AssociationPointV1,
    variant: ExtractedVariant,
    source: VariantSource,
    layout: &GenomeLayoutV1,
    compatibility_layout: &ChromosomeLayout,
    compatibility_y_scale: &YScale,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut S,
    renderer: &mut ManhattanRenderer,
    contig_lengths: &HashMap<String, u32>,
    chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
    chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    width: u32,
    height: u32,
) -> Result<bool> {
    let annotation = row
        .right
        .as_ref()
        .map(extract_annotation_fields)
        .unwrap_or((None, None));
    process_result_row(
        point,
        variant,
        source,
        annotation,
        layout,
        compatibility_layout,
        compatibility_y_scale,
        locus_threshold,
        variant_threshold,
        interest_regions,
        buffer,
        sig_writer,
        renderer,
        contig_lengths,
        chrom_renderers,
        chrom_layouts,
        width,
        height,
    )
}

/// Process a single row (no annotations) that was already canonically validated.
#[allow(clippy::too_many_arguments)]
fn process_single_row<S: SignificantHitSink>(
    point: &AssociationPointV1,
    variant: ExtractedVariant,
    source: VariantSource,
    layout: &GenomeLayoutV1,
    compatibility_layout: &ChromosomeLayout,
    compatibility_y_scale: &YScale,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut S,
    renderer: &mut ManhattanRenderer,
    contig_lengths: &HashMap<String, u32>,
    chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
    chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    width: u32,
    height: u32,
) -> Result<bool> {
    process_result_row(
        point,
        variant,
        source,
        (None, None),
        layout,
        compatibility_layout,
        compatibility_y_scale,
        locus_threshold,
        variant_threshold,
        interest_regions,
        buffer,
        sig_writer,
        renderer,
        contig_lengths,
        chrom_renderers,
        chrom_layouts,
        width,
        height,
    )
}

#[allow(clippy::too_many_arguments)]
fn process_result_row<S: SignificantHitSink>(
    point: &AssociationPointV1,
    v: ExtractedVariant,
    source: VariantSource,
    annotation: (Option<String>, Option<String>),
    layout: &GenomeLayoutV1,
    compatibility_layout: &ChromosomeLayout,
    compatibility_y_scale: &YScale,
    locus_threshold: f64,
    variant_threshold: f64,
    interest_regions: &IntervalList,
    buffer: &mut Vec<BufferedVariant>,
    sig_writer: &mut S,
    renderer: &mut ManhattanRenderer,
    contig_lengths: &HashMap<String, u32>,
    chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
    chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    width: u32,
    height: u32,
) -> Result<bool> {
    let rendered = render_variant(point, layout, renderer);
    render_variant_per_chrom(
        &v.contig,
        v.position,
        v.neg_log10_p,
        compatibility_layout,
        compatibility_y_scale,
        contig_lengths,
        chrom_renderers,
        chrom_layouts,
        width,
        height,
    );

    if v.pvalue < variant_threshold {
        sig_writer.write_hit(SigHitRow {
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
        })?;
    }

    if v.pvalue < locus_threshold || interest_regions.contains(&v.contig, v.position) {
        buffer.push(BufferedVariant {
            contig: v.contig,
            position: v.position,
            alleles: v.alleles,
            pvalue: v.pvalue,
            beta: v.beta,
            se: v.se,
            af: v.af,
            source,
            gene_symbol: annotation.0,
            consequence: annotation.1,
            ac_cases: v.ac_cases,
            ac_controls: v.ac_controls,
            af_cases: v.af_cases,
            af_controls: v.af_controls,
            association_ac: v.association_ac,
        });
    }

    Ok(rendered)
}

/// Render a single variant point on the Manhattan plot.
fn render_variant(
    point: &AssociationPointV1,
    layout: &GenomeLayoutV1,
    renderer: &mut ManhattanRenderer,
) -> bool {
    let Some(x) = layout.x_for(&point.contig, point.position) else {
        return false;
    };
    let Some(color) = layout.color_for(&point.contig) else {
        return false;
    };
    renderer.render_point(x, layout.y_scale.y_for(point.neg_log10_p), color, 0.6);
    true
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

/// Add legacy-only fields to an already canonicalized association point.
fn extract_variant_fields(
    row: &EncodedValue,
    point: &AssociationPointV1,
) -> Result<ExtractedVariant> {
    let position = i32::try_from(point.position).map_err(|_| {
        crate::HailError::InvalidFormat(format!(
            "position {} cannot be represented by legacy Manhattan outputs",
            point.position
        ))
    })?;
    let fields = match row {
        EncodedValue::Struct(fields) => fields,
        _ => {
            return Err(crate::HailError::InvalidFormat(
                "canonical association row is not a struct".into(),
            ))
        }
    };
    let get_float = |name: &str| -> Option<f64> {
        fields
            .iter()
            .find(|(field, _)| field == name)
            .and_then(|(_, value)| match value {
                EncodedValue::Float64(value) => Some(*value),
                EncodedValue::Float32(value) => Some(*value as f64),
                _ => None,
            })
    };

    let alleles = fields
        .iter()
        .find(|(field, _)| field == "alleles")
        .and_then(|(_, value)| match value {
            EncodedValue::Array(values) => Some(
                values
                    .iter()
                    .filter_map(EncodedValue::as_string)
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .filter(|alleles| !alleles.is_empty())
        .unwrap_or_else(|| {
            point
                .ref_allele
                .iter()
                .chain(point.alt_allele.iter())
                .cloned()
                .collect()
        });

    Ok(ExtractedVariant {
        contig: point.contig.clone(),
        position,
        pvalue: point.pvalue,
        neg_log10_p: point.neg_log10_p,
        beta: point.beta,
        se: point.standard_error,
        af: point.allele_frequency,
        alleles,
        ac_cases: get_float("AC_case").or_else(|| get_float("ac_case")),
        ac_controls: get_float("AC_ctrl").or_else(|| get_float("ac_ctrl")),
        af_cases: get_float("AF_case").or_else(|| get_float("af_case")),
        af_controls: get_float("AF_ctrl").or_else(|| get_float("af_ctrl")),
        association_ac: point.allele_count,
    })
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
                    (
                        "contig".to_string(),
                        EncodedValue::Binary(variant.contig.as_bytes().to_vec()),
                    ),
                    (
                        "position".to_string(),
                        EncodedValue::Int32(variant.position),
                    ),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(
                    variant
                        .alleles
                        .iter()
                        .map(|a| EncodedValue::Binary(a.as_bytes().to_vec()))
                        .collect(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn association_row(position: i32, pvalue: f64) -> EncodedValue {
        EncodedValue::Struct(vec![
            (
                "locus".into(),
                EncodedValue::Struct(vec![
                    ("contig".into(), EncodedValue::Binary(b"1".to_vec())),
                    ("position".into(), EncodedValue::Int32(position)),
                ]),
            ),
            (
                "alleles".into(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary(b"A".to_vec()),
                    EncodedValue::Binary(b"C".to_vec()),
                ]),
            ),
            ("Pvalue".into(), EncodedValue::Float64(pvalue)),
        ])
    }

    #[allow(clippy::too_many_arguments)]
    fn process_test_row<S: SignificantHitSink>(
        row: &EncodedValue,
        ordinal: u64,
        writer: &mut S,
        summary: &mut LegacyPartitionSummary,
        buffer: &mut Vec<BufferedVariant>,
        renderer: &mut ManhattanRenderer,
        chrom_renderers: &mut HashMap<String, ManhattanRenderer>,
        chrom_layouts: &mut HashMap<String, ChromosomeLayout>,
    ) -> Result<()> {
        let layout = GenomeLayoutV1::new([("1", 1_000)], 100, 50, 4)
            .map_err(|error| crate::HailError::InvalidFormat(error.to_string()))?;
        let compatibility_layout = ChromosomeLayout::new(&[("1".into(), 1_000)], 100, 4);
        let compatibility_y_scale = YScale::new(50, 350.0);
        let contig_lengths = HashMap::from([("1".into(), 1_000)]);
        let intervals = IntervalList::new();
        summary.observe_source_row(row)?;
        match extract_association_point(row, "Pvalue", ordinal) {
            Ok((point, warnings)) => {
                let variant = extract_variant_fields(row, &point)?;
                let significant = point.pvalue < 5e-8;
                let rendered = process_single_row(
                    &point,
                    variant,
                    VariantSource::Exome,
                    &layout,
                    &compatibility_layout,
                    &compatibility_y_scale,
                    0.01,
                    5e-8,
                    &intervals,
                    buffer,
                    writer,
                    renderer,
                    &contig_lengths,
                    chrom_renderers,
                    chrom_layouts,
                    100,
                    50,
                )?;
                summary.observe_valid_point(point, warnings, rendered, significant)
            }
            Err(_) => summary.observe_skipped_row(),
        }
    }

    #[test]
    fn canonical_summary_counts_skips_and_preserves_zero_pvalue() {
        let rows = [
            association_row(10, 0.0),
            association_row(20, -1.0),
            association_row(30, 0.5),
        ];
        let mut writer = SigHitWriter::from_writer(Vec::new()).unwrap();
        let mut summary = LegacyPartitionSummary::new(0).unwrap();
        let mut buffer = Vec::new();
        let mut renderer = ManhattanRenderer::new(100, 50);
        let mut chrom_renderers = HashMap::new();
        let mut chrom_layouts = HashMap::new();

        for (ordinal, row) in rows.iter().enumerate() {
            process_test_row(
                row,
                ordinal as u64,
                &mut writer,
                &mut summary,
                &mut buffer,
                &mut renderer,
                &mut chrom_renderers,
                &mut chrom_layouts,
            )
            .unwrap();
        }
        let summary = merge_partition_summaries(vec![summary]).unwrap();

        assert_eq!(summary.row_count, 3);
        assert_eq!(summary.valid_row_count, 2);
        assert_eq!(summary.skipped_row_count, 1);
        assert_eq!(summary.rendered_point_count, 2);
        assert_eq!(summary.significant_hit_count, 1);
        assert_eq!(summary.top_hits.len(), 2);
        assert_eq!(summary.top_hits[0].pvalue, 0.0);
        assert_eq!(summary.top_hits[0].source_ordinal, 0);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].pvalue, 0.0);
        assert_eq!(summary.warnings, vec!["pvalue_underflow"]);
    }

    struct FailingSink;

    impl SignificantHitSink for FailingSink {
        fn write_hit(&mut self, _row: SigHitRow) -> Result<()> {
            Err(crate::HailError::InvalidFormat(
                "injected significant-hit write failure".into(),
            ))
        }
    }

    #[test]
    fn significant_writer_errors_propagate_from_row_processing() {
        let mut writer = FailingSink;

        let mut summary = LegacyPartitionSummary::new(0).unwrap();
        let mut buffer = Vec::new();
        let mut renderer = ManhattanRenderer::new(100, 50);
        let mut chrom_renderers = HashMap::new();
        let mut chrom_layouts = HashMap::new();
        let result = process_test_row(
            &association_row(10, 1e-9),
            0,
            &mut writer,
            &mut summary,
            &mut buffer,
            &mut renderer,
            &mut chrom_renderers,
            &mut chrom_layouts,
        );

        assert!(result.is_err());
    }
}
