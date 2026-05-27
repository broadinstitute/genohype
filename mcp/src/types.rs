use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Variant types
// ---------------------------------------------------------------------------

/// Full variant details including frequencies, consequences, and predictions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantDetails {
    pub variant_id: String,
    pub chrom: String,
    pub pos: i64,
    pub ref_allele: String,
    pub alt_allele: String,
    pub rsids: Vec<String>,
    pub caid: Option<String>,

    /// Top-level allele count / number / frequency (across all populations).
    pub ac: Option<i64>,
    pub an: Option<i64>,
    pub af: Option<f64>,
    pub homozygote_count: Option<i64>,
    pub hemizygote_count: Option<i64>,

    /// Per-sequencing-type data (exome, genome, joint).
    pub exome: Option<SequencingTypeData>,
    pub genome: Option<SequencingTypeData>,
    pub joint: Option<JointData>,

    /// VEP transcript consequences.
    pub transcript_consequences: Vec<TranscriptConsequence>,

    /// In-silico predictor scores.
    pub in_silico_predictors: Option<InSilicoPredictors>,

    /// Flags (e.g., "lcr", "segdup", "par").
    pub flags: Vec<String>,

    /// Coverage information.
    pub coverage: Option<CoverageData>,
}

/// Concise variant summary for list views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantSummary {
    pub variant_id: String,
    pub chrom: String,
    pub pos: i64,
    pub ref_allele: String,
    pub alt_allele: String,
    pub rsids: Vec<String>,
    pub consequence: Option<String>,
    pub hgvsc: Option<String>,
    pub hgvsp: Option<String>,
    pub gene_id: Option<String>,
    pub gene_symbol: Option<String>,
    pub transcript_id: Option<String>,
    pub lof: Option<String>,
    pub ac: i64,
    pub an: i64,
    pub af: f64,
}

// ---------------------------------------------------------------------------
// Frequency types
// ---------------------------------------------------------------------------

/// Allele frequency data for a single population / ancestry group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PopulationFrequency {
    pub id: String,
    pub ac: i64,
    pub an: i64,
    pub af: f64,
    pub homozygote_count: i64,
    pub hemizygote_count: i64,
}

/// Frequency data for a sequencing type (exome or genome).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencingTypeData {
    pub ac: i64,
    pub an: i64,
    pub af: f64,
    pub homozygote_count: i64,
    pub hemizygote_count: i64,
    pub ancestry_groups: Vec<PopulationFrequency>,
    pub filters: Vec<String>,
}

/// Joint frequency data (combined exome + genome).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JointData {
    pub freq: SequencingTypeData,
    pub grpmax: Option<GrpmaxData>,
    pub fafmax: Option<FafmaxData>,
    pub flags: Vec<String>,
}

/// Group maximum allele frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpmaxData {
    pub population: String,
    pub af: f64,
    pub ac: i64,
    pub an: i64,
    pub homozygote_count: i64,
}

/// Filtering allele frequency maximum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FafmaxData {
    pub population: String,
    pub faf95: f64,
    pub faf99: f64,
}

// ---------------------------------------------------------------------------
// Transcript / consequence types
// ---------------------------------------------------------------------------

/// A VEP transcript consequence annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptConsequence {
    pub gene_id: String,
    pub gene_symbol: String,
    pub transcript_id: String,
    pub transcript_version: Option<String>,
    pub consequence_terms: Vec<String>,
    pub major_consequence: String,
    pub hgvsc: Option<String>,
    pub hgvsp: Option<String>,
    pub is_canonical: bool,
    pub is_mane_select: bool,
    pub lof: Option<String>,
    pub lof_filter: Option<String>,
    pub lof_flags: Option<String>,
    pub biotype: Option<String>,
    pub domains: Vec<String>,
    pub refseq_id: Option<String>,
}

/// In-silico variant effect predictor scores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InSilicoPredictors {
    pub revel: Option<f64>,
    pub cadd: Option<f64>,
    pub splice_ai: Option<f64>,
    pub pangolin: Option<f64>,
    pub phylop: Option<f64>,
    pub polyphen: Option<String>,
    pub sift: Option<String>,
}

// ---------------------------------------------------------------------------
// Gene types
// ---------------------------------------------------------------------------

/// Summary information for a gene.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneSummary {
    pub gene_id: String,
    pub gene_symbol: String,
    pub name: Option<String>,
    pub chrom: String,
    pub start: i64,
    pub stop: i64,
    pub strand: Option<String>,
    pub canonical_transcript_id: Option<String>,
    pub constraint: Option<GeneConstraint>,
}

/// Gene constraint metrics from gnomAD.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneConstraint {
    // Expected variant counts
    pub exp_lof: Option<f64>,
    pub exp_mis: Option<f64>,
    pub exp_syn: Option<f64>,
    // Observed variant counts
    pub obs_lof: Option<i64>,
    pub obs_mis: Option<i64>,
    pub obs_syn: Option<i64>,
    // Observed/expected ratios with confidence intervals
    pub oe_lof: Option<f64>,
    pub oe_lof_lower: Option<f64>,
    pub oe_lof_upper: Option<f64>,
    pub oe_mis: Option<f64>,
    pub oe_mis_lower: Option<f64>,
    pub oe_mis_upper: Option<f64>,
    pub oe_syn: Option<f64>,
    pub oe_syn_lower: Option<f64>,
    pub oe_syn_upper: Option<f64>,
    // Z scores
    pub lof_z: Option<f64>,
    pub mis_z: Option<f64>,
    pub syn_z: Option<f64>,
    // pLI and LOEUF (convenience aliases)
    pub pli: Option<f64>,
    pub loeuf: Option<f64>,
    // Constraint flags
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<Vec<String>>,
}

/// Tissue-level expression data for a gene.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneExpression {
    pub gene_id: String,
    pub tissues: Vec<TissueExpression>,
}

/// Expression level in a single tissue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TissueExpression {
    pub tissue: String,
    pub tpm: f64,
}

// ---------------------------------------------------------------------------
// Transcript types
// ---------------------------------------------------------------------------

/// Summary of a transcript for list views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptSummary {
    pub transcript_id: String,
    pub transcript_version: Option<String>,
    pub biotype: String,
    pub is_canonical: bool,
    pub is_mane_select: bool,
    pub refseq_id: Option<String>,
}

/// Full transcript details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptDetails {
    pub transcript_id: String,
    pub transcript_version: Option<String>,
    pub gene_id: String,
    pub gene_symbol: String,
    pub biotype: String,
    pub is_canonical: bool,
    pub is_mane_select: bool,
    pub strand: String,
    pub start: i64,
    pub stop: i64,
    pub exons: Vec<Exon>,
    pub refseq_id: Option<String>,
}

/// An exon region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exon {
    pub start: i64,
    pub stop: i64,
    pub feature_type: String,
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

/// Coverage statistics for a variant site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageData {
    pub exome: Option<SiteCoverage>,
    pub genome: Option<SiteCoverage>,
}

/// Coverage at a single site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteCoverage {
    pub mean: Option<f64>,
    pub median: Option<f64>,
    pub over_20: Option<f64>,
    pub over_30: Option<f64>,
}
