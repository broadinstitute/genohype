//! Parameter types for variant MCP tools.

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetVariantDetailsParams {
    /// Variant identifier in chrom-pos-ref-alt format (e.g., "1-55039447-G-A").
    pub variant_id: String,
    /// Dataset version (e.g., "gnomad_r4", "gnomad_r2"). Defaults to "gnomad_r4".
    pub dataset: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetVariantSummaryParams {
    /// Variant identifier in chrom-pos-ref-alt format.
    pub variant_id: String,
    /// Dataset version. Defaults to "gnomad_r4".
    pub dataset: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetVariantFrequenciesParams {
    /// Variant identifier in chrom-pos-ref-alt format.
    pub variant_id: String,
    /// Dataset version. Defaults to "gnomad_r4".
    pub dataset: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetMultipleVariantDetailsParams {
    /// List of variant identifiers in chrom-pos-ref-alt format.
    pub variant_ids: Vec<String>,
    /// Dataset version. Defaults to "gnomad_r4".
    pub dataset: Option<String>,
}
