//! Parameter types for gene MCP tools.

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetGeneSummaryParams {
    /// Ensembl gene ID (e.g., "ENSG00000012048") or gene symbol (e.g., "BRCA1").
    pub gene: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetGeneVariantsParams {
    /// Ensembl gene ID (e.g., "ENSG00000012048").
    pub gene_id: String,
    /// Dataset version. Defaults to "gnomad_r4".
    pub dataset: Option<String>,
    /// Filter by consequence type (e.g., "missense_variant", "pLoF").
    pub consequence: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetGeneExpressionParams {
    /// Ensembl gene ID (e.g., "ENSG00000012048").
    pub gene_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListGeneTranscriptsParams {
    /// Ensembl gene ID (e.g., "ENSG00000012048").
    pub gene_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetTranscriptDetailsParams {
    /// Ensembl transcript ID (e.g., "ENST00000357654").
    pub transcript_id: String,
}
