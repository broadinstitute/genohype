//! Gene structures and burden table processing for Manhattan plots.
//!
//! This module provides:
//! - Gene definitions loaded from gnomAD/GENCODE Hail tables
//! - Gene burden table processing for gene-level Manhattan plots
//! - Identification of significant regions of interest
//! - Parquet export for ClickHouse ingestion

use crate::manhattan::config::ResolvedStyle;
use crate::manhattan::data::{GeneAssociationRow, GenePlotPoint, PlotPoint};
use crate::manhattan::gene_writer::GeneAssociationWriter;
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::reference::{calculate_xpos, get_contig_lengths, normalize_contig_name};
use crate::manhattan::render::ManhattanRenderer;
use crate::Result;
use genohype_core::codec::EncodedValue;
use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
use genohype_core::query::{IntervalList, QueryEngine};
use serde::Serialize;
use std::collections::HashMap;

/// P-value fields to check for significance in gene burden tables.
const GENE_PVALUE_FIELDS: &[&str] = &["Pvalue", "Pvalue_SKAT", "Pvalue_Burden"];

/// A single significant test result from gene burden analysis.
#[derive(Debug, Clone, Serialize)]
pub struct SignificantTest {
    pub annotation: String,
    pub max_maf: f64,
    pub pvalue_field: String,
    pub pvalue: f64,
}

/// A gene with significant burden signal.
#[derive(Debug, Clone, Serialize)]
pub struct SignificantGene {
    pub gene_id: String,
    pub gene_symbol: String,
    pub best_pvalue: f64,
    pub best_test: String,
    pub interval: (String, i32, i32), // chrom, start, end
    pub plot_position: i32,           // TSS for manhattan plot
    pub significant_tests: Vec<SignificantTest>,
}

/// A gene definition from gnomAD/GENCODE.
#[derive(Debug, Clone, Serialize)]
pub struct Gene {
    pub id: String,
    pub symbol: String,
    pub chrom: String,
    pub start: i32,
    pub stop: i32,
    pub strand: String,
}

/// Helper to manage gene lookups.
pub struct GeneMap {
    pub genes: Vec<Gene>,
}

impl GeneMap {
    pub fn new() -> Self {
        Self { genes: Vec::new() }
    }

    /// Load genes from a gnomAD genes Hail table.
    pub fn load(path: &str) -> Result<Self> {
        let engine = QueryEngine::open_path(path)?;
        let mut genes = Vec::new();

        // Scan the table
        let iter = engine.query_iter(&[])?;

        for row_res in iter {
            let row = row_res?;
            if let EncodedValue::Struct(fields) = row {
                let get_str = |name: &str| -> Option<String> {
                    fields
                        .iter()
                        .find(|(n, _)| n == name)
                        .and_then(|(_, v)| v.as_string())
                };
                let get_int = |name: &str| -> Option<i32> {
                    fields
                        .iter()
                        .find(|(n, _)| n == name)
                        .and_then(|(_, v)| v.as_i32())
                };

                let id = get_str("gene_id");
                let symbol = get_str("gencode_symbol").or_else(|| get_str("gene_symbol"));
                let chrom = get_str("chrom").or_else(|| get_str("contig"));
                let start = get_int("start");
                let stop = get_int("stop");
                let strand = get_str("strand");

                if let (
                    Some(id),
                    Some(symbol),
                    Some(chrom),
                    Some(start),
                    Some(stop),
                    Some(strand),
                ) = (id, symbol, chrom, start, stop, strand)
                {
                    genes.push(Gene {
                        id,
                        symbol,
                        chrom,
                        start,
                        stop,
                        strand,
                    });
                }
            }
        }

        Ok(Self { genes })
    }

    /// Find a gene by symbol.
    pub fn find_by_symbol(&self, symbol: &str) -> Option<&Gene> {
        self.genes.iter().find(|g| g.symbol == symbol)
    }

    /// Query genes overlapping a region.
    #[allow(dead_code)]
    pub fn query(&self, chrom: &str, start: i32, stop: i32) -> Vec<&Gene> {
        self.genes
            .iter()
            .filter(|g| g.chrom == chrom && g.start <= stop && g.stop >= start)
            .collect()
    }
}

impl Default for GeneMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Process a gene burden table to generate plot and regions of interest.
///
/// # Arguments
/// * `path` - Path to the gene burden Hail table
/// * `pval_threshold` - P-value threshold for significance
/// * `width` - Image width in pixels
/// * `height` - Image height in pixels
/// * `gene_map` - Optional gene map for precise gene bounds
///
/// # Returns
/// A tuple of (PNG bytes, JSON sidecar string, IntervalList of significant regions)
pub fn process_gene_burden(
    path: &str,
    pval_threshold: f64,
    width: u32,
    height: u32,
    gene_map: Option<&GeneMap>,
) -> Result<(Vec<u8>, String, IntervalList)> {
    let engine = QueryEngine::open_path(path)?;

    let all_contigs = get_contig_lengths(&engine);
    // Filter to autosomes + X/Y/M (skip random/unplaced contigs)
    let contigs: Vec<(String, u32)> = all_contigs
        .into_iter()
        .filter(|(name, _)| name.len() <= 5) // Simple heuristic to skip random contigs
        .collect();

    let layout = ChromosomeLayout::new(&contigs, width, 4);
    let mut renderer = ManhattanRenderer::new(width, height);

    // Y-Scale config (cap at 50 for gene burden usually sufficient)
    let y_scale = YScale::new(height, 50.0);
    renderer.render_threshold_line(y_scale.threshold_y(pval_threshold), width);

    let mut significant_regions = IntervalList::new();
    let iter = engine.query_iter(&[])?;

    // Gene burden table fields: usually Pvalue, gene_id/symbol, genomic coords
    // We expect CHR, POS or interval
    for row_res in iter {
        let row = row_res?;

        // Extract plotting data.
        // Note: Burden tables vary. We'll look for standard fields.
        let mut point: Option<PlotPoint> = None;
        let mut gene_symbol: Option<String> = None;

        if let EncodedValue::Struct(fields) = &row {
            // Find P-value
            let pval = fields
                .iter()
                .find(|(n, _)| n == "Pvalue")
                .and_then(|(_, v)| match v {
                    EncodedValue::Float64(f) => Some(*f),
                    EncodedValue::Float32(f) => Some(*f as f64),
                    _ => None,
                });

            // Find Position (CHR/POS or interval)
            // Simplified: look for CHR and POS
            let chr = fields
                .iter()
                .find(|(n, _)| n == "CHR" || n == "chromosome")
                .and_then(|(_, v)| v.as_string());
            let pos = fields
                .iter()
                .find(|(n, _)| n == "POS" || n == "position")
                .and_then(|(_, v)| v.as_i32());

            gene_symbol = fields
                .iter()
                .find(|(n, _)| n == "gene_symbol" || n == "symbol")
                .and_then(|(_, v)| v.as_string());

            if let (Some(p), Some(c), Some(pos)) = (pval, chr, pos) {
                if p > 0.0 && p.is_finite() {
                    point = Some(PlotPoint {
                        contig: c,
                        position: pos,
                        pvalue: p,
                        neg_log10_p: -p.log10(),
                    });
                }
            }
        }

        if let Some(pt) = point {
            // Render
            // Normalize contig name (strip chr)
            let contig_name = if pt.contig.starts_with("chr") {
                pt.contig[3..].to_string()
            } else {
                pt.contig.clone()
            };

            if let Some(x) = layout.get_x(&contig_name, pt.position) {
                let y = y_scale.get_y(pt.neg_log10_p);
                let color = layout.get_color(&contig_name);
                renderer.render_point(x, y, color, 0.8);
            }

            // Check significance
            if pt.pvalue < pval_threshold {
                // Determine gene bounds
                let padding = 100_000; // 100kb padding

                // If we have a gene map and symbol, use precise bounds
                if let (Some(map), Some(sym)) = (gene_map, &gene_symbol) {
                    if let Some(gene) = map.find_by_symbol(sym) {
                        let start = (gene.start - padding).max(1);
                        let stop = gene.stop + padding;
                        significant_regions.add(gene.chrom.clone(), start, stop);
                        continue;
                    }
                }

                // Fallback: use point position +/- padding
                let start = (pt.position - padding).max(1);
                let stop = pt.position + padding;
                significant_regions.add(pt.contig.clone(), start, stop);
            }
        }
    }

    significant_regions.optimize();

    let png_data = renderer.encode_png()?;
    // Minimal sidecar for now
    let sidecar = serde_json::json!({
        "image": {"width": width, "height": height},
        "threshold": pval_threshold,
        "significant_regions": significant_regions.len()
    })
    .to_string();

    Ok((png_data, sidecar, significant_regions))
}

/// Helper to extract f64 from EncodedValue.
fn encoded_as_f64(val: &EncodedValue) -> Option<f64> {
    match val {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        EncodedValue::Int64(i) => Some(*i as f64),
        EncodedValue::Int32(i) => Some(*i as f64),
        _ => None,
    }
}

/// Process a gene burden table with compound keys and multiple p-value fields.
///
/// This handles the complex gene burden schema with:
/// - Compound key: (gene_id, gene_symbol, annotation, max_MAF)
/// - Multiple p-value fields: Pvalue, Pvalue_SKAT, Pvalue_Burden
/// - Multiple annotation types: pLoF, missenseLC, synonymous, etc.
///
/// # Arguments
/// * `path` - Path to the gene burden Hail table
/// * `pval_threshold` - P-value threshold for significance
///
/// # Returns
/// A tuple of (SignificantGenes, IntervalList of significant regions)
pub fn process_complex_gene_burden(
    path: &str,
    pval_threshold: f64,
) -> Result<(Vec<SignificantGene>, IntervalList)> {
    let engine = QueryEngine::open_path(path)?;
    let mut gene_groups: HashMap<String, Vec<EncodedValue>> = HashMap::new();

    // 1. Scan and group by gene_id
    let iter = engine.query_iter(&[])?;
    for row_res in iter {
        let row = row_res?;
        if let EncodedValue::Struct(fields) = &row {
            // Extract gene_id to group rows
            if let Some((_, val)) = fields.iter().find(|(n, _)| n == "gene_id") {
                if let Some(gene_id) = val.as_string() {
                    gene_groups.entry(gene_id).or_default().push(row);
                }
            }
        }
    }

    let mut significant_genes = Vec::new();
    let mut regions = IntervalList::new();

    // 2. Process each gene group
    for (gene_id, rows) in gene_groups {
        let mut best_pvalue = 1.0;
        let mut best_test_desc = String::new();
        let mut symbol = gene_id.clone();
        let mut interval_coords: Option<(String, i32, i32)> = None;
        let mut plot_pos = 0i32;
        let mut significant_tests = Vec::new();

        for row in &rows {
            if let EncodedValue::Struct(fields) = row {
                // Extract gene_symbol (only if not yet found)
                if symbol == gene_id {
                    if let Some((_, v)) = fields.iter().find(|(n, _)| n == "gene_symbol") {
                        if let Some(s) = v.as_string() {
                            symbol = s;
                        }
                    }
                }

                // Extract annotation and MAF for description
                let annot = fields
                    .iter()
                    .find(|(n, _)| n == "annotation")
                    .and_then(|(_, v)| v.as_string())
                    .unwrap_or_default();

                let maf = fields
                    .iter()
                    .find(|(n, _)| n == "max_MAF")
                    .and_then(|(_, v)| encoded_as_f64(v))
                    .unwrap_or(-1.0);

                // Check all p-value fields
                for &field_name in GENE_PVALUE_FIELDS {
                    if let Some((_, val)) = fields.iter().find(|(n, _)| n == field_name) {
                        if let Some(p) = encoded_as_f64(val) {
                            if p > 0.0 && p.is_finite() {
                                // Track significant tests
                                if p < pval_threshold {
                                    significant_tests.push(SignificantTest {
                                        annotation: annot.clone(),
                                        max_maf: maf,
                                        pvalue_field: field_name.to_string(),
                                        pvalue: p,
                                    });
                                }
                                // Track best pvalue
                                if p < best_pvalue {
                                    best_pvalue = p;
                                    best_test_desc =
                                        format!("{}:{}:maf{:.4}", annot, field_name, maf);
                                }
                            }
                        }
                    }
                }

                // Capture interval/position if not yet found
                if interval_coords.is_none() {
                    // Try CHR/POS fields
                    let chr = fields
                        .iter()
                        .find(|(n, _)| n == "CHR")
                        .and_then(|(_, v)| v.as_string());
                    let pos = fields
                        .iter()
                        .find(|(n, _)| n == "POS")
                        .and_then(|(_, v)| v.as_i32());

                    if let (Some(c), Some(p)) = (chr, pos) {
                        plot_pos = p;
                        // Pad by 100kb for region of interest
                        interval_coords = Some((c, (p - 100_000).max(1), p + 100_000));
                    }
                }
            }
        }

        // Only include genes with at least one significant test
        if !significant_tests.is_empty() {
            if let Some((chrom, start, end)) = interval_coords {
                significant_genes.push(SignificantGene {
                    gene_id: gene_id.clone(),
                    gene_symbol: symbol,
                    best_pvalue,
                    best_test: best_test_desc,
                    interval: (chrom.clone(), start, end),
                    plot_position: plot_pos,
                    significant_tests,
                });
                regions.add(chrom, start, end);
            }
        }
    }

    regions.optimize();
    Ok((significant_genes, regions))
}

// =============================================================================
// Gene Burden Parquet Export and Manhattan Rendering
// =============================================================================

/// Result of scanning a gene burden table to Parquet.
pub struct GeneScanResult {
    /// Total number of rows written to Parquet (all annotation × MAF combinations)
    pub total_rows: usize,
    /// Genes with at least one test passing the significance threshold
    pub significant_genes: Vec<SignificantGene>,
    /// Plot points (one per gene, using best p-value) for Manhattan rendering
    pub plot_points: Vec<GenePlotPoint>,
    /// Plot points grouped by (annotation, MAF) for combinatorial gene burden plots
    pub plot_points_by_group: HashMap<(String, String), Vec<GenePlotPoint>>,
}

/// Internal tracker for gene-level statistics during scanning.
struct GeneTracker {
    symbol: String,
    contig: String,
    start: i32,
    end: i32,
    best_pvalue: f64,
    best_test_desc: String,
    sig_tests: Vec<SignificantTest>,
    /// Best p-value per (annotation, MAF) group: maps (annotation, maf_str) -> (best_pvalue, best_test_desc)
    group_best_pvalues: HashMap<(String, String), (f64, String)>,
}

/// Scan a gene burden table, export ALL rows to Parquet, and collect plotting data.
///
/// This function:
/// 1. Streams all rows from the gene_results.ht table
/// 2. Writes every row to Parquet (full export for ClickHouse ingestion)
/// 3. Tracks the best p-value per gene (for Manhattan rendering)
/// 4. Collects genes where best_pvalue < gene_threshold (for locus regions)
///
/// # Arguments
/// * `path` - Path to the gene burden Hail table
/// * `phenotype` - Phenotype identifier for the Parquet output
/// * `ancestry` - Ancestry group (e.g., "meta", "EUR")
/// * `output_path` - Path to write the Parquet file (local or cloud)
/// * `gene_threshold` - P-value threshold for significance tracking
/// * `maf_filter` - Optional: only export rows with this max_MAF value
///
/// # Returns
/// A `GeneScanResult` containing row count, significant genes, and plot points.
pub fn scan_gene_burden_to_parquet(
    path: &str,
    phenotype: &str,
    ancestry: &str,
    output_path: &str,
    gene_threshold: f64,
    maf_filter: Option<f64>,
) -> Result<GeneScanResult> {
    let engine = QueryEngine::open_path(path)?;

    // Initialize writer based on output path type
    let mut writer: Box<dyn GeneWriterTrait> = if is_cloud_path(output_path) {
        let cloud_writer = StreamingCloudWriter::new(output_path)?;
        Box::new(CloudGeneWriter {
            writer: GeneAssociationWriter::from_writer(cloud_writer)?,
        })
    } else {
        Box::new(LocalGeneWriter {
            writer: GeneAssociationWriter::new(output_path)?,
        })
    };

    let iter = engine.query_iter(&[])?;
    let mut total_rows = 0;

    // Tracking for significance and plotting (keyed by gene_id)
    let mut gene_tracking: HashMap<String, GeneTracker> = HashMap::new();

    for row_res in iter {
        let row = row_res?;
        if let EncodedValue::Struct(fields) = row {
            // Helper closures for field extraction
            let get_str = |k: &str| -> Option<String> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| v.as_string())
            };
            let get_f64 = |k: &str| -> Option<f64> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| encoded_as_f64(v))
            };
            #[allow(dead_code)]
            let _get_i32 = |k: &str| -> Option<i32> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| v.as_i32())
            };
            let get_i64 = |k: &str| -> Option<i64> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| v.as_i64())
            };

            // Extract key fields
            let gene_id = get_str("gene_id").unwrap_or_default();
            let gene_symbol = get_str("gene_symbol").unwrap_or_default();
            let annotation = get_str("annotation").unwrap_or_default();
            let max_maf = get_f64("max_MAF").unwrap_or(0.0);

            // Apply MAF filter if specified
            if let Some(filter_maf) = maf_filter {
                if (max_maf - filter_maf).abs() > 1e-9 {
                    continue;
                }
            }

            // Extract p-values and stats
            let pvalue = get_f64("Pvalue");
            let pvalue_burden = get_f64("Pvalue_Burden");
            let pvalue_skat = get_f64("Pvalue_SKAT");
            // Try BETA_Burden first, then META_Stats_Burden (used for META ancestry)
            let beta_burden = get_f64("BETA_Burden").or_else(|| get_f64("META_Stats_Burden"));
            let mac = get_i64("MAC");

            // Extract location - try interval first, then CHR/POS
            let (contig, start, end) = extract_gene_location(&fields);

            // Build and write the row
            let row_out = GeneAssociationRow {
                gene_id: gene_id.clone(),
                gene_symbol: gene_symbol.clone(),
                annotation: annotation.clone(),
                max_maf,
                phenotype: phenotype.to_string(),
                ancestry: ancestry.to_string(),
                pvalue,
                pvalue_burden,
                pvalue_skat,
                beta_burden,
                mac,
                contig: normalize_contig_name(&contig),
                gene_start_position: start,
                xpos: calculate_xpos(&contig, start),
            };

            writer.write(row_out)?;
            total_rows += 1;

            // Track best p-value per gene for plotting
            let mut min_p = 1.0;
            let mut best_field = "";

            if let Some(p) = pvalue {
                if p > 0.0 && p.is_finite() && p < min_p {
                    min_p = p;
                    best_field = "Pvalue";
                }
            }
            if let Some(p) = pvalue_burden {
                if p > 0.0 && p.is_finite() && p < min_p {
                    min_p = p;
                    best_field = "Pvalue_Burden";
                }
            }
            if let Some(p) = pvalue_skat {
                if p > 0.0 && p.is_finite() && p < min_p {
                    min_p = p;
                    best_field = "Pvalue_SKAT";
                }
            }

            if min_p < 1.0 {
                let tracker = gene_tracking.entry(gene_id.clone()).or_insert(GeneTracker {
                    symbol: gene_symbol.clone(),
                    contig: contig.clone(),
                    start,
                    end,
                    best_pvalue: 1.0,
                    best_test_desc: String::new(),
                    sig_tests: Vec::new(),
                    group_best_pvalues: HashMap::new(),
                });

                // Update best p-value globally if this is lower
                if min_p < tracker.best_pvalue {
                    tracker.best_pvalue = min_p;
                    tracker.best_test_desc =
                        format!("{}:{}:maf{:.4}", annotation, best_field, max_maf);
                }

                // Update best p-value per (annotation, max_maf) group
                let maf_str = max_maf.to_string();
                let group_key = (annotation.clone(), maf_str.clone());
                let group_entry = tracker
                    .group_best_pvalues
                    .entry(group_key)
                    .or_insert((1.0, String::new()));
                if min_p < group_entry.0 {
                    group_entry.0 = min_p;
                    group_entry.1 = format!("{}:{}:maf{}", annotation, best_field, maf_str);
                }

                // Track significant tests
                if min_p < gene_threshold {
                    tracker.sig_tests.push(SignificantTest {
                        annotation: annotation.clone(),
                        max_maf,
                        pvalue_field: best_field.to_string(),
                        pvalue: min_p,
                    });
                }
            }
        }
    }

    // Finalize the writer
    writer.finish()?;

    // Convert tracking to results
    let mut significant_genes = Vec::new();
    let mut plot_points = Vec::new();
    let mut plot_points_by_group: HashMap<(String, String), Vec<GenePlotPoint>> = HashMap::new();

    for (gene_id, tracker) in gene_tracking {
        // Create plot point for every gene with valid p-value (combined/legacy plot)
        plot_points.push(GenePlotPoint {
            gene_id: gene_id.clone(),
            gene_symbol: tracker.symbol.clone(),
            contig: tracker.contig.clone(),
            position: tracker.start,
            best_pvalue: tracker.best_pvalue,
            best_test: tracker.best_test_desc.clone(),
        });

        // Populate grouped plot points for each (annotation, MAF) combination
        for (group_key, (best_p, best_desc)) in &tracker.group_best_pvalues {
            plot_points_by_group
                .entry(group_key.clone())
                .or_default()
                .push(GenePlotPoint {
                    gene_id: gene_id.clone(),
                    gene_symbol: tracker.symbol.clone(),
                    contig: tracker.contig.clone(),
                    position: tracker.start,
                    best_pvalue: *best_p,
                    best_test: best_desc.clone(),
                });
        }

        // Only include genes with significant tests in significant_genes list
        if !tracker.sig_tests.is_empty() {
            significant_genes.push(SignificantGene {
                gene_id,
                gene_symbol: tracker.symbol,
                best_pvalue: tracker.best_pvalue,
                best_test: tracker.best_test_desc,
                interval: (tracker.contig, tracker.start, tracker.end),
                plot_position: tracker.start,
                significant_tests: tracker.sig_tests,
            });
        }
    }

    Ok(GeneScanResult {
        total_rows,
        significant_genes,
        plot_points,
        plot_points_by_group,
    })
}

/// Extract gene location from row fields.
/// Tries interval first, then falls back to CHR/POS fields.
fn extract_gene_location(fields: &[(String, EncodedValue)]) -> (String, i32, i32) {
    // Try to extract from interval field
    if let Some((_, EncodedValue::Struct(ivl))) = fields.iter().find(|(n, _)| n == "interval") {
        let mut contig = String::new();
        let mut start = 0i32;
        let mut end = 0i32;

        // Extract start locus
        if let Some((_, EncodedValue::Struct(s))) = ivl.iter().find(|(n, _)| n == "start") {
            if let Some((_, EncodedValue::Binary(c))) = s.iter().find(|(n, _)| n == "contig") {
                contig = String::from_utf8_lossy(c).to_string();
            }
            if let Some((_, EncodedValue::Int32(p))) = s.iter().find(|(n, _)| n == "position") {
                start = *p;
            }
        }

        // Extract end locus
        if let Some((_, EncodedValue::Struct(e))) = ivl.iter().find(|(n, _)| n == "end") {
            if let Some((_, EncodedValue::Int32(p))) = e.iter().find(|(n, _)| n == "position") {
                end = *p;
            }
        }

        if !contig.is_empty() && start > 0 {
            return (contig, start, end.max(start + 1000));
        }
    }

    // Fallback to CHR/POS fields
    let chr = fields
        .iter()
        .find(|(n, _)| n == "CHR")
        .and_then(|(_, v)| v.as_string())
        .unwrap_or_default();

    let pos = fields
        .iter()
        .find(|(n, _)| n == "POS")
        .and_then(|(_, v)| v.as_i32())
        .unwrap_or(0);

    (chr, pos, pos + 1000)
}

/// Render a Manhattan plot for gene results.
///
/// # Arguments
/// * `points` - Gene plot points (one per gene, using best p-value)
/// * `width` - Image width in pixels
/// * `height` - Image height in pixels
/// * `threshold` - P-value threshold for significance line
/// * `layout` - Chromosome layout for x-axis positioning
///
/// # Returns
/// PNG image data as bytes.
pub fn render_gene_manhattan(
    points: &[GenePlotPoint],
    width: u32,
    height: u32,
    threshold: f64,
    layout: &ChromosomeLayout,
) -> Result<Vec<u8>> {
    render_gene_manhattan_styled(points, width, height, threshold, layout, None)
}

/// Render a Manhattan plot for gene results with custom styling.
pub fn render_gene_manhattan_styled(
    points: &[GenePlotPoint],
    width: u32,
    height: u32,
    threshold: f64,
    layout: &ChromosomeLayout,
    style: Option<&ResolvedStyle>,
) -> Result<Vec<u8>> {
    // Use style settings or defaults for gene burden plots
    let (background, point_radius, point_alpha, threshold_color, threshold_alpha) = match style {
        Some(s) => (
            &s.background,
            s.point_radius,
            s.point_alpha,
            &s.threshold_line_color,
            s.threshold_line_alpha,
        ),
        None => (
            &crate::manhattan::config::BackgroundStyle::White,
            5.0f32, // Larger default for genes
            0.9f32,
            &"#B71C1C".to_string(),
            0.5f32,
        ),
    };

    let mut renderer = ManhattanRenderer::new_with_background(width, height, background);

    // Y-Scale: cap at 30 or max value in data
    let max_log_p = points
        .iter()
        .filter(|p| p.best_pvalue > 0.0 && p.best_pvalue.is_finite())
        .map(|p| -p.best_pvalue.log10())
        .fold(0.0f64, |a, b| a.max(b));

    let y_scale = YScale::new(height, max_log_p.max(10.0).min(50.0));

    // Draw threshold line
    renderer.render_threshold_line_styled(
        y_scale.threshold_y(threshold),
        width,
        threshold_color,
        threshold_alpha,
    );

    // Draw points
    for pt in points {
        if pt.best_pvalue <= 0.0 || !pt.best_pvalue.is_finite() {
            continue;
        }

        // Normalize contig name (strip chr prefix for layout lookup)
        let contig = if pt.contig.starts_with("chr") {
            &pt.contig[3..]
        } else {
            &pt.contig
        };

        if let Some(x) = layout.get_x(contig, pt.position) {
            let neg_log_p = -pt.best_pvalue.log10();
            let y = y_scale.get_y(neg_log_p);
            let color = layout.get_color(contig);

            renderer.render_point_with_radius(x, y, color, point_alpha, point_radius);
        }
    }

    renderer.encode_png()
}

// =============================================================================
// Writer abstraction for local vs cloud output
// =============================================================================

/// Trait to abstract over local and cloud gene writers.
trait GeneWriterTrait {
    fn write(&mut self, row: GeneAssociationRow) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<usize>;
}

/// Local file writer wrapper.
struct LocalGeneWriter {
    writer: GeneAssociationWriter<std::fs::File>,
}

impl GeneWriterTrait for LocalGeneWriter {
    fn write(&mut self, row: GeneAssociationRow) -> Result<()> {
        self.writer.write(row)
    }

    fn finish(self: Box<Self>) -> Result<usize> {
        self.writer.finish()
    }
}

/// Cloud writer wrapper.
struct CloudGeneWriter {
    writer: GeneAssociationWriter<StreamingCloudWriter>,
}

impl GeneWriterTrait for CloudGeneWriter {
    fn write(&mut self, row: GeneAssociationRow) -> Result<()> {
        self.writer.write(row)
    }

    fn finish(self: Box<Self>) -> Result<usize> {
        let cloud_writer = self.writer.into_inner()?;
        let rows = cloud_writer.bytes_written(); // Approximate
        cloud_writer.finish()?;
        Ok(rows)
    }
}
