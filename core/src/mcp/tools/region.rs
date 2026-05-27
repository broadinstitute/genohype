use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::mcp::traits::{GenomicDataProvider, McpTool};

/// Retrieve variants in a genomic region.
pub struct GetRegionVariants;

#[async_trait]
impl McpTool for GetRegionVariants {
    fn name(&self) -> &'static str {
        "get_region_variants"
    }

    fn description(&self) -> &'static str {
        "Get variants in a genomic region defined by chromosome and start/end \
         coordinates. Returns variant summaries with consequence, frequency, \
         and gene annotation. Coordinates are 1-based, inclusive."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "chrom": {
                    "type": "string",
                    "description": "Chromosome (e.g., '1', 'X', 'chr1')"
                },
                "start": {
                    "type": "integer",
                    "description": "Start position (1-based, inclusive)"
                },
                "end": {
                    "type": "integer",
                    "description": "End position (1-based, inclusive)"
                },
                "dataset": {
                    "type": "string",
                    "description": "Dataset version",
                    "default": "gnomad_r4"
                }
            },
            "required": ["chrom", "start", "end"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let chrom = args["chrom"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("chrom is required"))?;
        let start = args["start"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("start is required"))?;
        let end = args["end"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("end is required"))?;
        let dataset = args["dataset"].as_str().unwrap_or("gnomad_r4");

        let variants = provider
            .get_region_variants(chrom, start, end, dataset)
            .await?;
        Ok(serde_json::to_value(variants)?)
    }
}
