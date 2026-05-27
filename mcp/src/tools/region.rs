//! Parameter types for region MCP tools.

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetRegionVariantsParams {
    /// Chromosome (e.g., "1", "X", "chr1").
    pub chrom: String,
    /// Start position (1-based, inclusive).
    pub start: i64,
    /// End position (1-based, inclusive).
    pub end: i64,
    /// Dataset version. Defaults to "gnomad_r4".
    pub dataset: Option<String>,
}
