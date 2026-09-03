//! Data structures for Manhattan plot points, significant hits, and the sidecar JSON.

use genohype_association::{normalize_pvalue, AssociationPointV1, ValidationError, WarningCode};
use genohype_core::codec::EncodedValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

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

/// Why a decoded Hail row cannot be adapted to the canonical association model.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum AssociationRowSkipReason {
    #[error("missing or invalid locus.contig")]
    MissingOrInvalidContig,
    #[error("missing or invalid locus.position")]
    MissingOrInvalidPosition,
    #[error("missing or invalid raw p-value field {0:?}")]
    MissingOrInvalidPvalue(String),
    #[error(transparent)]
    Validation(#[from] ValidationError),
}

/// Convert a decoded Hail row into a validated canonical association point.
///
/// Field aliases retain compatibility with the schemas accepted by the existing
/// integrated and locus paths. A source-provided log value is validation input,
/// not a substitute for the raw p-value: an invalid raw p-value is always rejected.
pub(crate) fn extract_association_point(
    row: &EncodedValue,
    y_field: &str,
    source_ordinal: u64,
) -> Result<(AssociationPointV1, Vec<WarningCode>), AssociationRowSkipReason> {
    let contig = get_nested_field(row, &["locus", "contig"])
        .and_then(EncodedValue::as_string)
        .ok_or(AssociationRowSkipReason::MissingOrInvalidContig)?;
    let position = get_nested_field(row, &["locus", "position"])
        .and_then(value_as_i64)
        .ok_or(AssociationRowSkipReason::MissingOrInvalidPosition)?;
    let pvalue = get_nested_field(row, &[y_field])
        .and_then(value_as_float)
        .ok_or_else(|| AssociationRowSkipReason::MissingOrInvalidPvalue(y_field.into()))?;

    let log_field = format!("{y_field}_log10");
    let supplied_neg_log10_p = optional_float(
        row,
        &[&log_field, "Pvalue_log10", "pvalue_log10", "neg_log10_p"],
    );
    let normalized_pvalue = normalize_pvalue(pvalue, supplied_neg_log10_p)
        .map_err(|error| AssociationRowSkipReason::Validation(error.into()))?;

    let (ref_allele, alt_allele) = optional_alleles(row);
    let point = AssociationPointV1 {
        contig,
        position,
        ref_allele,
        alt_allele,
        pvalue: normalized_pvalue.pvalue,
        neg_log10_p: normalized_pvalue.neg_log10_p,
        beta: optional_float(row, &["BETA", "beta", "Beta"]),
        standard_error: optional_float(row, &["SE", "se", "Se", "standard_error"]),
        allele_frequency: optional_float(row, &["AF_Allele2", "AF", "af", "allele_frequency"]),
        allele_count: optional_number(row, &["AC_Allele2", "association_ac"]),
        gene_symbol: optional_string(row, &["gene_symbol"]),
        consequence: optional_string(row, &["consequence"]),
        series: optional_string(row, &["series"]),
        source_ordinal,
    };

    // Canonical point validation also normalizes contig aliases and validates all
    // optional statistics.
    let (point, warnings) = point.normalize()?;
    Ok((point, warnings))
}

/// Extract a compatibility `PlotPoint` through canonical row validation.
///
/// `PlotPoint` retains its historical i32 position for gene and distributed
/// callers. Canonical association consumers use `extract_association_point` and
/// therefore retain the widened i64 position.
pub fn extract_plot_data(row: &EncodedValue, y_field: &str) -> Option<PlotPoint> {
    let (point, _) = extract_association_point(row, y_field, 0).ok()?;
    Some(PlotPoint {
        contig: point.contig,
        position: i32::try_from(point.position).ok()?,
        pvalue: point.pvalue,
        neg_log10_p: point.neg_log10_p,
    })
}

/// Navigate into a nested `EncodedValue::Struct` by field names.
fn get_nested_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
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

fn value_as_i64(value: &EncodedValue) -> Option<i64> {
    match value {
        EncodedValue::Int32(value) => Some(i64::from(*value)),
        EncodedValue::Int64(value) => Some(*value),
        _ => None,
    }
}

fn value_as_float(value: &EncodedValue) -> Option<f64> {
    match value {
        EncodedValue::Float32(value) => Some(f64::from(*value)),
        EncodedValue::Float64(value) => Some(*value),
        _ => None,
    }
}

fn value_as_number(value: &EncodedValue) -> Option<f64> {
    value_as_float(value).or_else(|| match value {
        EncodedValue::Int32(value) => Some(f64::from(*value)),
        EncodedValue::Int64(value) => Some(*value as f64),
        _ => None,
    })
}

fn optional_float(row: &EncodedValue, field_names: &[&str]) -> Option<f64> {
    field_names
        .iter()
        .find_map(|field_name| get_nested_field(row, &[*field_name]).and_then(value_as_float))
}

fn optional_number(row: &EncodedValue, field_names: &[&str]) -> Option<f64> {
    field_names
        .iter()
        .find_map(|field_name| get_nested_field(row, &[*field_name]).and_then(value_as_number))
}

fn optional_string(row: &EncodedValue, field_names: &[&str]) -> Option<String> {
    field_names.iter().find_map(|field_name| {
        get_nested_field(row, &[*field_name]).and_then(EncodedValue::as_string)
    })
}

fn optional_alleles(row: &EncodedValue) -> (Option<String>, Option<String>) {
    let Some(EncodedValue::Array(alleles)) = get_nested_field(row, &["alleles"]) else {
        return (None, None);
    };
    (
        alleles.first().and_then(EncodedValue::as_string),
        alleles.get(1).and_then(EncodedValue::as_string),
    )
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
    /// Minor allele count among cases. Missing/absent source fields remain null.
    pub mac_case: Option<i64>,
    /// Minor allele count among controls. Missing/absent source fields remain null.
    pub mac_control: Option<i64>,

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

#[cfg(test)]
mod tests {
    use super::*;
    use genohype_association::PValueError;

    fn string(value: &str) -> EncodedValue {
        EncodedValue::Binary(value.as_bytes().to_vec())
    }

    fn row_with_fields(
        position: EncodedValue,
        y_field: &str,
        pvalue: f64,
        fields: Vec<(&str, EncodedValue)>,
    ) -> EncodedValue {
        let mut row = vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    ("contig".to_string(), string("chrM")),
                    ("position".to_string(), position),
                ]),
            ),
            (y_field.to_string(), EncodedValue::Float64(pvalue)),
        ];
        row.extend(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_string(), value)),
        );
        EncodedValue::Struct(row)
    }

    #[test]
    fn canonical_adapter_extracts_complete_row_and_i64_position() {
        let row = row_with_fields(
            EncodedValue::Int64(3_000_000_000),
            "Pvalue",
            1e-12,
            vec![
                ("Pvalue_log10", EncodedValue::Float64(12.5)),
                (
                    "alleles",
                    EncodedValue::Array(vec![string("A"), string("T"), string("ignored")]),
                ),
                ("BETA", EncodedValue::Float64(-0.25)),
                ("se", EncodedValue::Float32(0.125)),
                ("AF_Allele2", EncodedValue::Float64(0.4)),
                ("association_ac", EncodedValue::Int64(42)),
                ("gene_symbol", string("PCSK9")),
                ("consequence", string("missense_variant")),
                ("series", string("exome")),
            ],
        );

        let (point, warnings) = extract_association_point(&row, "Pvalue", 73).unwrap();

        assert_eq!(point.contig, "MT");
        assert_eq!(point.position, 3_000_000_000);
        assert_eq!(point.ref_allele.as_deref(), Some("A"));
        assert_eq!(point.alt_allele.as_deref(), Some("T"));
        assert_eq!(point.pvalue, 1e-12);
        assert_eq!(point.neg_log10_p, 12.5);
        assert_eq!(point.beta, Some(-0.25));
        assert_eq!(point.standard_error, Some(0.125));
        assert_eq!(point.allele_frequency, Some(0.4));
        assert_eq!(point.allele_count, Some(42.0));
        assert_eq!(point.gene_symbol.as_deref(), Some("PCSK9"));
        assert_eq!(point.consequence.as_deref(), Some("missense_variant"));
        assert_eq!(point.series.as_deref(), Some("exome"));
        assert_eq!(point.source_ordinal, 73);
        assert!(warnings.is_empty());
    }

    #[test]
    fn custom_log_field_wins_and_significance_above_350_is_preserved() {
        let row = row_with_fields(
            EncodedValue::Int32(101),
            "p",
            0.0,
            vec![
                ("p_log10", EncodedValue::Float64(412.0)),
                ("Pvalue_log10", EncodedValue::Float64(399.0)),
                ("beta", EncodedValue::Float64(0.5)),
                ("SE", EncodedValue::Float64(0.1)),
                ("af", EncodedValue::Float64(0.2)),
                ("AC_Allele2", EncodedValue::Int32(7)),
            ],
        );

        let (point, warnings) = extract_association_point(&row, "p", 9).unwrap();

        assert_eq!(point.position, 101);
        assert_eq!(point.neg_log10_p, 412.0);
        assert_eq!(point.beta, Some(0.5));
        assert_eq!(point.standard_error, Some(0.1));
        assert_eq!(point.allele_frequency, Some(0.2));
        assert_eq!(point.allele_count, Some(7.0));
        assert_eq!(
            warnings,
            vec![
                WarningCode::PvalueUnderflow,
                WarningCode::NegLog10PDisplayCapped,
            ]
        );
    }

    #[test]
    fn zero_pvalue_without_supplied_log_uses_350() {
        let row = row_with_fields(EncodedValue::Int32(1), "p", 0.0, vec![]);

        let (point, warnings) = extract_association_point(&row, "p", 0).unwrap();

        assert_eq!(point.neg_log10_p, 350.0);
        assert_eq!(warnings, vec![WarningCode::PvalueUnderflow]);
        assert_eq!(point.ref_allele, None);
        assert_eq!(point.alt_allele, None);
        assert_eq!(point.beta, None);
        assert_eq!(point.standard_error, None);
        assert_eq!(point.allele_frequency, None);
        assert_eq!(point.allele_count, None);
        assert_eq!(point.gene_symbol, None);
        assert_eq!(point.consequence, None);
        assert_eq!(point.series, None);
    }

    #[test]
    fn invalid_raw_pvalue_is_skipped_even_with_finite_supplied_log() {
        for pvalue in [-0.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.000_001] {
            let row = row_with_fields(
                EncodedValue::Int32(1),
                "p",
                pvalue,
                vec![("p_log10", EncodedValue::Float64(500.0))],
            );

            assert_eq!(
                extract_association_point(&row, "p", 0),
                Err(AssociationRowSkipReason::Validation(
                    ValidationError::InvalidPvalue(PValueError::InvalidPvalue)
                ))
            );
        }
    }

    #[test]
    fn invalid_supplied_log_and_optional_statistics_are_skipped() {
        let invalid_log = row_with_fields(
            EncodedValue::Int32(10),
            "p",
            0.1,
            vec![("p_log10", EncodedValue::Float64(f64::NAN))],
        );
        assert_eq!(
            extract_association_point(&invalid_log, "p", 0),
            Err(AssociationRowSkipReason::Validation(
                ValidationError::InvalidPvalue(PValueError::InvalidNegLog10P)
            ))
        );

        let invalid_af = row_with_fields(
            EncodedValue::Int32(10),
            "p",
            0.1,
            vec![("AF_Allele2", EncodedValue::Float64(1.1))],
        );
        assert_eq!(
            extract_association_point(&invalid_af, "p", 0),
            Err(AssociationRowSkipReason::Validation(
                ValidationError::InvalidAlleleFrequency
            ))
        );
    }

    #[test]
    fn compatibility_extractor_uses_canonical_pvalue_validation() {
        let invalid = row_with_fields(
            EncodedValue::Int32(10),
            "p",
            -0.1,
            vec![("Pvalue_log10", EncodedValue::Float64(100.0))],
        );
        assert!(extract_plot_data(&invalid, "p").is_none());

        let valid = row_with_fields(
            EncodedValue::Int32(10),
            "p",
            0.0,
            vec![("Pvalue_log10", EncodedValue::Float64(400.0))],
        );
        let point = extract_plot_data(&valid, "p").unwrap();
        assert_eq!(point.contig, "MT");
        assert_eq!(point.position, 10);
        assert_eq!(point.pvalue, 0.0);
        assert_eq!(point.neg_log10_p, 400.0);
    }

    #[test]
    fn malformed_required_fields_have_typed_skip_reasons() {
        let not_a_row = EncodedValue::Null;
        assert_eq!(
            extract_association_point(&not_a_row, "p", 0),
            Err(AssociationRowSkipReason::MissingOrInvalidContig)
        );

        let missing_pvalue = EncodedValue::Struct(vec![(
            "locus".to_string(),
            EncodedValue::Struct(vec![
                ("contig".to_string(), string("1")),
                ("position".to_string(), EncodedValue::Int32(1)),
            ]),
        )]);
        assert_eq!(
            extract_association_point(&missing_pvalue, "p", 0),
            Err(AssociationRowSkipReason::MissingOrInvalidPvalue(
                "p".to_string()
            ))
        );
    }
}
