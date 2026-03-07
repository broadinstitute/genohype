//! Data structures for Manhattan plot points, significant hits, and the sidecar JSON.

use genohype_core::codec::EncodedValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Source of a variant (for distinguishing exome vs genome in combined analyses).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum VariantSource {
    Exome,
    Genome,
}

/// A variant buffered for locus plot generation.
/// Contains minimal data needed for rendering and JSON export.
#[derive(Debug, Clone, Serialize)]
pub struct BufferedVariant {
    pub contig: String,
    pub position: i32,
    pub alleles: Vec<String>,
    pub pvalue: f64,
    pub beta: Option<f64>,
    pub se: Option<f64>,
    pub af: Option<f64>,
    pub source: VariantSource,
    pub gene_symbol: Option<String>,
    pub consequence: Option<String>,
    // Case/control breakdown fields
    pub ac_cases: Option<f64>,
    pub ac_controls: Option<f64>,
    pub af_cases: Option<f64>,
    pub af_controls: Option<f64>,
    // Trait-level association stats
    pub association_ac: Option<f64>,
}

/// A genomic region of interest for locus plot generation.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LocusRegion {
    pub contig: String,
    pub start: i32,
    pub end: i32,
    /// Description of signals driving this region (e.g., "rs123 (exome)", "PCSK9 (burden)")
    pub signals: Vec<String>,
}

/// A single variant extracted from a table row, ready for plotting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlotPoint {
    pub contig: String,
    pub position: i32,
    pub pvalue: f64,
    pub neg_log10_p: f64,
}

/// A variant that exceeds the significance threshold. Included in the sidecar JSON.
#[derive(Debug, Serialize)]
pub struct SignificantHit {
    pub variant_id: String,
    pub pvalue: f64,
    pub x_px: f32,
    pub y_px: f32,
    pub x_normalized: f32,
    pub y_normalized: f32,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub annotations: serde_json::Value,
}

/// Per-chromosome metadata in the sidecar.
#[derive(Debug, Serialize)]
pub struct SidecarChromosome {
    pub name: String,
    pub x_start_px: f32,
    pub x_end_px: f32,
    pub color: String,
}

/// Threshold metadata in the sidecar.
#[derive(Debug, Serialize)]
pub struct SidecarThreshold {
    pub pvalue: f64,
    pub y_px: f32,
}

/// Y-axis scale metadata for the hybrid linear-log scale.
#[derive(Debug, Serialize)]
pub struct SidecarYAxis {
    /// Threshold where scale switches from linear to log (-log10(p) value, typically 10)
    pub log_threshold: f64,
    /// Fraction of plot height used for the linear portion (0 to log_threshold)
    pub linear_fraction: f64,
    /// Maximum -log10(p) value in the data (used for log portion scaling)
    pub max_neg_log_p: f64,
}

/// Top-level sidecar JSON emitted alongside the PNG.
#[derive(Debug, Serialize)]
pub struct ManhattanSidecar {
    pub image: SidecarImage,
    pub chromosomes: Vec<SidecarChromosome>,
    pub threshold: SidecarThreshold,
    pub y_axis: SidecarYAxis,
    pub significant_hits: Vec<SignificantHit>,
}

/// Image dimensions metadata.
#[derive(Debug, Serialize)]
pub struct SidecarImage {
    pub width: u32,
    pub height: u32,
}

/// Maximum -log10(p) for display (caps underflowed p-values)
const MAX_NEG_LOG10_P: f64 = 350.0;

/// Extract a `PlotPoint` from a table row.
///
/// Expects the row to have a `locus` struct with `contig` (string) and
/// `position` (i32), plus a float field named `y_field` for the p-value.
///
/// Prefers using `Pvalue_log10` from source data if available (preserves precision
/// for extremely small p-values that underflow to 0 in f64). Falls back to
/// computing from raw p-value.
pub fn extract_plot_data(row: &EncodedValue, y_field: &str) -> Option<PlotPoint> {
    let locus = get_nested_field(row, &["locus"])?;
    let contig = get_nested_field(locus, &["contig"])?.as_string()?;
    let position = get_nested_field(locus, &["position"])?.as_i32()?;

    // Try to get pre-computed Pvalue_log10 from source (preserves precision)
    let pvalue_log10_field = format!("{}_log10", y_field);
    let source_neg_log10_p = get_nested_field(row, &[&pvalue_log10_field])
        .or_else(|| get_nested_field(row, &["Pvalue_log10"]))
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        })
        .filter(|v| v.is_finite());

    // Get raw p-value
    let p_val_obj = get_nested_field(row, &[y_field])?;
    let pvalue = match p_val_obj {
        EncodedValue::Float64(v) => *v,
        EncodedValue::Float32(v) => *v as f64,
        _ => return None,
    };

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

    Some(PlotPoint {
        contig,
        position,
        pvalue,
        neg_log10_p,
    })
}

/// Navigate into a nested `EncodedValue::Struct` by field names.
fn get_nested_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
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

// =============================================================================
// Manifest types for phenotype pipeline output
// =============================================================================

/// Top-level manifest.json for a phenotype pipeline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Phenotype name (derived from output path or explicitly set)
    pub phenotype: String,
    /// Ancestry group (e.g., "META", "EUR", "AFR")
    #[serde(default)]
    pub ancestry: Option<String>,
    /// ISO timestamp when the pipeline completed
    pub created_at: String,

    /// Input table paths
    pub inputs: ManifestInputs,
    /// Manhattan plot outputs
    pub manhattans: ManifestManhattans,
    /// Per-chromosome Manhattan plot outputs
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub chrom_manhattans: HashMap<String, ManifestManhattans>,
    /// Significant hit outputs
    pub significant_hits: ManifestSignificantHits,
    /// Locus zoom regions
    pub loci: Vec<ManifestLocus>,
    /// Pipeline statistics
    pub stats: ManifestStats,
}

/// Input paths used in the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome_results: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome_results: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene_burden: Option<String>,
}

/// Manhattan plot outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestManhattans {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome: Option<ManifestManhattan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome: Option<ManifestManhattan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene: Option<ManifestManhattan>,
}

/// Info about a single Manhattan plot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestManhattan {
    /// Path to PNG file
    pub png: String,
    /// Number of variants/genes scanned
    pub count: u64,
}

/// Significant hit outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSignificantHits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome: Option<ManifestSigHits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome: Option<ManifestSigHits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene: Option<ManifestSigHits>,
}

/// Info about significant hits for one source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSigHits {
    /// Path to Parquet file with annotated hits
    pub path: String,
    /// Number of significant hits
    pub count: u64,
    /// Top hit info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_hit: Option<ManifestTopHit>,
}

/// Top hit summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestTopHit {
    /// Variant ID (e.g., "2:21234567:A:G") or gene ID
    pub id: String,
    /// P-value
    pub pvalue: f64,
    /// Gene symbol if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene: Option<String>,
    /// Consequence if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consequence: Option<String>,
}

/// A locus region with associated outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestLocus {
    /// Unique ID for the locus (e.g., "chr6_32000000_34000000")
    pub id: String,
    /// Genomic region
    pub region: ManifestRegion,
    /// Source that drove this locus (exome, genome, or gene)
    pub source: String,
    /// Lead variant ID
    pub lead_variant: String,
    /// Lead variant p-value
    pub lead_pvalue: f64,
    /// Lead variant gene (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lead_gene: Option<String>,
    /// Path to locus plot PNG
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plot: Option<String>,
    /// Exome variants in this region
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exome_variants: Option<ManifestLocusVariants>,
    /// Genome variants in this region
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genome_variants: Option<ManifestLocusVariants>,
    /// Genes overlapping this region
    pub genes: Vec<String>,
}

/// A genomic region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRegion {
    pub contig: String,
    pub start: i64,
    pub end: i64,
}

/// Locus variant file info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestLocusVariants {
    /// Path to Parquet file
    pub path: String,
    /// Number of variants
    pub count: u64,
}

/// Pipeline execution statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestStats {
    /// Duration of scan phase in seconds
    pub scan_duration_sec: f64,
    /// Duration of aggregate phase in seconds
    pub aggregate_duration_sec: f64,
    /// Total number of loci generated
    pub total_loci: usize,
    /// Total size of input Hail Tables in bytes (exome + genome + gene_burden)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_ht_size_bytes: Option<u64>,
    /// Total size of derived output directory in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_dir_size_bytes: Option<u64>,
}

/// Error manifest for tracking pipeline failures.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorManifest {
    /// Phenotype identifier
    pub phenotype: String,
    /// Pipeline status (e.g., "MANHATTAN_FAILED", "INGEST_FAILED")
    pub status: String,
    /// Error message describing the failure
    pub error: String,
    /// Unix timestamp in milliseconds when the error occurred
    pub timestamp_ms: u64,
}

// =============================================================================
// Significant hit row for Parquet output (scan phase)
// =============================================================================

/// A significant variant hit to be written to Parquet during scan phase.
/// This is the flat schema used for partition-level sig.parquet files.
#[derive(Debug, Clone)]
pub struct SigHitRow {
    // Identity fields (for ClickHouse partitioning)
    /// Phenotype identifier
    pub phenotype: String,
    /// Ancestry group (e.g., "meta", "EUR", "AFR")
    pub ancestry: String,
    /// Sequencing type ("exome" or "genome")
    pub sequencing_type: String,

    // Variant key fields
    /// Chromosome (chr-prefixed, e.g., "chr1", "chrX")
    pub contig: String,
    /// Position (1-based)
    pub position: i32,
    /// Reference allele
    pub ref_allele: String,
    /// Alternate allele
    pub alt_allele: String,
    /// Pre-computed xpos for efficient ordering (contig_num * 1e9 + position)
    pub xpos: i64,

    // Association stats
    /// P-value
    pub pvalue: f64,
    /// Effect size (beta)
    pub beta: Option<f64>,
    /// Standard error
    pub se: Option<f64>,
    /// Allele frequency
    pub af: Option<f64>,

    // Case/control breakdown fields
    /// Allele count in cases (from META: AC_case, or computed from AF_case * AN_cases)
    pub ac_cases: Option<f64>,
    /// Allele count in controls (from META: AC_ctrl, or computed from AF_ctrl * AN_controls)
    pub ac_controls: Option<f64>,
    /// Allele frequency in cases (from per-ancestry: AF_case, or computed from AC_case / AN_cases)
    pub af_cases: Option<f64>,
    /// Allele frequency in controls (from per-ancestry: AF_ctrl, or computed from AC_ctrl / AN_controls)
    pub af_controls: Option<f64>,
    /// Total allele count across all individuals (from AC_Allele2)
    pub association_ac: Option<f64>,
}

/// A locus definition row for the loci.parquet file.
/// This captures the metadata for each significant locus region.
#[derive(Debug, Clone)]
pub struct LocusDefinitionRow {
    /// Unique locus identifier (e.g., "chr6_32000000_34000000")
    pub locus_id: String,
    /// Phenotype identifier
    pub phenotype: String,
    /// Ancestry group
    pub ancestry: String,
    /// Chromosome (chr-prefixed)
    pub contig: String,
    /// Region start position
    pub start: i32,
    /// Region end position
    pub stop: i32,
    /// xpos for start position
    pub xstart: i64,
    /// xpos for end position
    pub xstop: i64,
    /// Source of the lead signal ("exome", "genome", or "both")
    pub source: String,
    /// Lead variant ID (e.g., "chr2:21234567:A:G")
    pub lead_variant: String,
    /// Lead variant p-value
    pub lead_pvalue: f64,
    /// Count of exome variants in this locus
    pub exome_count: u32,
    /// Count of genome variants in this locus
    pub genome_count: u32,
}

/// A locus variant row for the loci_variants.parquet file.
/// This captures all variants within a locus region.
#[derive(Debug, Clone)]
pub struct LocusVariantRow {
    /// Locus identifier (links to LocusDefinitionRow)
    pub locus_id: String,
    /// Phenotype identifier
    pub phenotype: String,
    /// Ancestry group
    pub ancestry: String,
    /// Sequencing type ("exome" or "genome")
    pub sequencing_type: String,
    /// Chromosome (chr-prefixed)
    pub contig: String,
    /// Pre-computed xpos
    pub xpos: i64,
    /// Position (1-based)
    pub position: i32,
    /// Reference allele
    pub ref_allele: String,
    /// Alternate allele
    pub alt_allele: String,
    /// P-value
    pub pvalue: f64,
    /// -log10(pvalue) for plotting
    pub neg_log10_p: f32,
    /// Whether this variant is significant (p < threshold)
    pub is_significant: bool,
    /// Effect size (beta coefficient)
    pub beta: Option<f64>,
    /// Standard error of beta
    pub se: Option<f64>,
    /// Allele frequency (from association)
    pub af: Option<f64>,

    // Case/control breakdown fields
    /// Allele count in cases (from META: AC_case, or computed from AF_case * AN_cases)
    pub ac_cases: Option<f64>,
    /// Allele count in controls (from META: AC_ctrl, or computed from AF_ctrl * AN_controls)
    pub ac_controls: Option<f64>,
    /// Allele frequency in cases (from per-ancestry: AF_case, or computed from AC_case / AN_cases)
    pub af_cases: Option<f64>,
    /// Allele frequency in controls (from per-ancestry: AF_ctrl, or computed from AC_ctrl / AN_controls)
    pub af_controls: Option<f64>,
    /// Total allele count across all individuals (from AC_Allele2)
    pub association_ac: Option<f64>,
}

// =============================================================================
// Gene Association Data Structures
// =============================================================================

/// A row in the gene associations Parquet file.
/// Matches the ClickHouse `gene_associations` table schema.
#[derive(Debug, Clone)]
pub struct GeneAssociationRow {
    // Key fields (match ClickHouse ORDER BY)
    pub gene_id: String,
    pub gene_symbol: String,
    pub annotation: String,
    pub max_maf: f64,
    pub phenotype: String,
    pub ancestry: String,

    // Association stats
    pub pvalue: Option<f64>,
    pub pvalue_burden: Option<f64>,
    pub pvalue_skat: Option<f64>,
    pub beta_burden: Option<f64>,
    pub mac: Option<i64>,

    // Coordinates (for Manhattan + joins)
    pub contig: String,
    pub gene_start_position: i32,
    /// Pre-computed xpos for efficient sorting/joining (contig_num * 1e9 + position)
    pub xpos: i64,
}

/// A point for the gene Manhattan plot.
/// Represents the best (lowest) p-value for a single gene across all tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenePlotPoint {
    pub gene_id: String,
    pub gene_symbol: String,
    pub contig: String,
    pub position: i32,
    pub best_pvalue: f64,
    /// Description of the test that produced the best p-value (e.g., "pLoF:Pvalue:0.001")
    pub best_test: String,
}

// =============================================================================
// QQ Plot Data Structures
// =============================================================================

/// A row in the QQ plot points Parquet file.
/// Contains observed and expected p-values for QQ plot rendering,
/// plus variant key fields for annotation joins.
#[derive(Debug, Clone)]
pub struct QQPointRow {
    // Identity fields
    pub phenotype: String,
    pub ancestry: String,
    pub sequencing_type: String,

    // Variant key fields (for annotation joins)
    pub contig: String,
    pub position: i32,
    pub ref_allele: String,
    pub alt_allele: String,

    // QQ plot values
    pub pvalue_log10: f64,
    pub pvalue_expected_log10: f64,
}

/// Lambda GC statistics extracted from QQ table globals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QQStats {
    pub lambda_gc: Option<f64>,
    pub lambda_q0_5: Option<f64>,
    pub lambda_q0_1: Option<f64>,
    pub lambda_q0_01: Option<f64>,
    pub lambda_q0_001: Option<f64>,
}
