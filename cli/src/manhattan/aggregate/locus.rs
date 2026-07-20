//! Locus plot generation for Manhattan aggregation.
//!
//! This module handles extracting significant positions, computing locus regions,
//! reading variants from Hail tables, and rendering locus plots.

use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use genohype_core::codec::EncodedValue;
use genohype_core::error::Result;
use genohype_core::io::is_cloud_path;
use genohype_core::query::{IntervalList, QueryEngine};
use std::collections::HashMap;
use std::sync::Arc;

use crate::distributed::message::ManhattanAggregateSpec;
use crate::manhattan::config::PlotType;
use crate::manhattan::data::{
    LocusDefinitionRow, LocusVariantRow, ManifestLocus, ManifestLocusVariants, ManifestRegion,
    VariantSource,
};
use crate::manhattan::loci_writer::{LocusDefinitionWriter, LocusVariantWriter};
use crate::manhattan::locus::{LocusPlotConfig, LocusRenderer, RenderVariant};
use crate::manhattan::reference::calculate_xpos;

use super::io::{read_cloud_parquet_file, read_local_parquet_file, write_locus_file};

// =============================================================================
// Data Structures
// =============================================================================

/// A significant hit position extracted from merged parquet.
#[derive(Debug, Clone)]
pub struct SigPosition {
    pub contig: String,
    pub position: i32,
    pub pvalue: f64,
    pub source: String, // "exome" or "genome"
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

// =============================================================================
// Locus Plot Orchestration
// =============================================================================

/// Generate locus plots for significant regions (called from aggregation phase).
pub(crate) fn generate_locus_plots(
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
pub(crate) fn generate_loci_from_parquet(
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
    use crate::manhattan::genes::process_complex_gene_burden;
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Step 1: Extract significant positions from merged parquet file
    println!("    Extracting significant positions...");
    let sig_path = format!("{}/significant.parquet", output_base);

    let mut sig_positions = if std::path::Path::new(&sig_path).exists() || is_cloud_path(&sig_path)
    {
        let positions = extract_sig_positions(&sig_path)?;
        println!(
            "      Found {} significant hits from significant.parquet",
            positions.len()
        );
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
    println!(
        "    Computing locus regions (clumping window: {}bp, min_variants: {})...",
        locus_window, min_variants_per_locus
    );
    let regions = compute_locus_regions_with_genes(
        &sig_positions,
        &gene_regions,
        locus_window,
        min_variants_per_locus,
    );
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
    let results: Vec<Result<Option<(ManifestLocus, LocusDefinitionRow, Vec<LocusVariantRow>)>>> =
        pool.install(|| {
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
    let region_id = format!("{}_{}_{}", contig.replace("chr", ""), start, end);

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

// =============================================================================
// Significant Position Extraction
// =============================================================================

/// Extract significant positions from the consolidated significant.parquet file.
pub fn extract_sig_positions(parquet_path: &str) -> Result<Vec<SigPosition>> {
    let batches = if is_cloud_path(parquet_path) {
        // For cloud paths, handle 404 (file not found) gracefully -
        // phenotypes with no significant hits won't have this file
        match read_cloud_parquet_file(parquet_path) {
            Ok(b) => b,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404")
                    || err_str.contains("not found")
                    || err_str.contains("NotFound")
                {
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
        let seq_type_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "sequencing_type");

        if contig_idx.is_none()
            || position_idx.is_none()
            || pvalue_idx.is_none()
            || seq_type_idx.is_none()
        {
            continue;
        }

        let contig_col = batch
            .column(contig_idx.unwrap())
            .as_any()
            .downcast_ref::<StringArray>();
        let position_col = batch
            .column(position_idx.unwrap())
            .as_any()
            .downcast_ref::<Int32Array>();
        let pvalue_col = batch
            .column(pvalue_idx.unwrap())
            .as_any()
            .downcast_ref::<Float64Array>();
        let seq_type_col = batch
            .column(seq_type_idx.unwrap())
            .as_any()
            .downcast_ref::<StringArray>();

        if let (Some(contig_arr), Some(pos_arr), Some(pval_arr), Some(seq_arr)) =
            (contig_col, position_col, pvalue_col, seq_type_col)
        {
            for i in 0..batch.num_rows() {
                if contig_arr.is_null(i)
                    || pos_arr.is_null(i)
                    || pval_arr.is_null(i)
                    || seq_arr.is_null(i)
                {
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

// =============================================================================
// Locus Region Computation (Greedy Clumping)
// =============================================================================

/// Compute locus regions including gene bounds using Greedy P-value Clumping.
///
/// 1. Variants are sorted by p-value.
/// 2. Iteratively take the most significant unabsorbed variant to form a clump.
/// 3. Clumps containing fewer than `min_variants` are discarded to remove noise.
/// 4. Add gene regions (gene bounds expanded by window).
/// 5. Merge all overlapping regions together.
pub(crate) fn compute_locus_regions_with_genes(
    positions: &[SigPosition],
    gene_regions: &[(String, i32, i32)],
    window: i32,
    min_variants: usize,
) -> Vec<(String, i32, i32)> {
    let mut all_regions: Vec<(String, i32, i32)> = Vec::new();

    // 1. Separate variant positions (filter out "gene" source variants which are added directly)
    let mut variant_positions: Vec<SigPosition> = positions
        .iter()
        .filter(|p| p.source != "gene")
        .cloned()
        .collect();

    // 2. Greedy clumping
    if !variant_positions.is_empty() {
        // Sort by p-value ascending (best first)
        variant_positions.sort_by(|a, b| {
            a.pvalue
                .partial_cmp(&b.pvalue)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut absorbed = vec![false; variant_positions.len()];

        for i in 0..variant_positions.len() {
            if absorbed[i] {
                continue;
            }
            let lead = &variant_positions[i];

            let clump_start = (lead.position - window).max(1);
            let clump_end = lead.position + window;

            let mut clump_count = 0;

            // Absorb any variants within window
            for j in i..variant_positions.len() {
                if !absorbed[j] {
                    let candidate = &variant_positions[j];
                    if candidate.contig == lead.contig
                        && candidate.position >= clump_start
                        && candidate.position <= clump_end
                    {
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
pub(crate) fn parse_chrom_order(chrom: &str) -> (i32, String) {
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
        .min_by(|a, b| {
            a.pvalue
                .partial_cmp(&b.pvalue)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned()
}

// =============================================================================
// Hail Table Querying
// =============================================================================

/// Read variants from a Hail table for a specific genomic region.
fn read_locus_variants(
    table_path: &str,
    contig: &str,
    start: i32,
    end: i32,
    source: VariantSource,
) -> Result<Vec<RenderVariant>> {
    use std::time::Instant;

    let t0 = Instant::now();
    let engine = QueryEngine::open_path(table_path)?;
    let open_time = t0.elapsed();
    if open_time.as_secs() > 1 {
        eprintln!(
            "      [slow] QueryEngine::open_path took {:.1}s for {}",
            open_time.as_secs_f64(),
            table_path
        );
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
        eprintln!(
            "      [slow] Interval query took {:.1}s, {} rows for {}:{}-{}",
            query_time.as_secs_f64(),
            row_count,
            contig,
            start,
            end
        );
    }

    Ok(variants)
}

/// Extract position, p-value, alleles, and effect size fields from an encoded row.
fn extract_locus_info(row: &EncodedValue) -> Option<ExtractedLocusInfo> {
    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
        let mut current = value;
        for &field_name in path {
            if let EncodedValue::Struct(fields) = current {
                current = fields
                    .iter()
                    .find(|(n, _)| n == field_name)
                    .map(|(_, v)| v)?;
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
    let (ref_allele, alt_allele) =
        if let Some(EncodedValue::Array(alleles)) = get_field(row, &["alleles"]) {
            let r = alleles
                .first()
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let a = alleles
                .get(1)
                .and_then(|v| v.as_string())
                .unwrap_or_default();
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

// =============================================================================
// Locus Rendering
// =============================================================================

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

// =============================================================================
// Loci Parquet Output
// =============================================================================

/// Write loci.parquet and loci_variants.parquet files.
fn write_loci_parquet(
    output_base: &str,
    definitions: &[LocusDefinitionRow],
    variants: &[LocusVariantRow],
) -> Result<()> {
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
            println!(
                "      Wrote {} locus variants to loci_variants.parquet",
                count
            );
        }
    }

    Ok(())
}
