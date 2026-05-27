use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::mcp::traits::{GenomicDataProvider, McpTool};

/// Retrieve full details for a single variant by ID.
pub struct GetVariantDetails;

#[async_trait]
impl McpTool for GetVariantDetails {
    fn name(&self) -> &'static str {
        "get_variant_details"
    }

    fn description(&self) -> &'static str {
        "Get detailed information about a specific genetic variant including \
         allele frequencies across populations, transcript consequences, \
         in silico predictor scores, and quality flags. Use variant IDs in \
         the format 'chrom-pos-ref-alt' (e.g., '1-55039447-G-A')."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "variant_id": {
                    "type": "string",
                    "description": "Variant identifier in chrom-pos-ref-alt format (e.g., '1-55039447-G-A')"
                },
                "dataset": {
                    "type": "string",
                    "description": "Dataset version (e.g., 'gnomad_r4', 'gnomad_r2')",
                    "default": "gnomad_r4"
                }
            },
            "required": ["variant_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let variant_id = args["variant_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("variant_id is required"))?;
        let dataset = args["dataset"].as_str().unwrap_or("gnomad_r4");

        match provider.get_variant_details(variant_id, dataset).await? {
            Some(details) => Ok(serde_json::to_value(details)?),
            None => Ok(json!({ "error": "variant not found", "variant_id": variant_id })),
        }
    }
}

/// Retrieve a concise summary for a variant.
pub struct GetVariantSummary;

#[async_trait]
impl McpTool for GetVariantSummary {
    fn name(&self) -> &'static str {
        "get_variant_summary"
    }

    fn description(&self) -> &'static str {
        "Get a concise summary of a variant including its consequence, \
         gene, and allele frequency. Lighter than get_variant_details."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "variant_id": {
                    "type": "string",
                    "description": "Variant identifier in chrom-pos-ref-alt format"
                },
                "dataset": {
                    "type": "string",
                    "description": "Dataset version",
                    "default": "gnomad_r4"
                }
            },
            "required": ["variant_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let variant_id = args["variant_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("variant_id is required"))?;
        let dataset = args["dataset"].as_str().unwrap_or("gnomad_r4");

        match provider.get_variant_summary(variant_id, dataset).await? {
            Some(summary) => Ok(serde_json::to_value(summary)?),
            None => Ok(json!({ "error": "variant not found", "variant_id": variant_id })),
        }
    }
}

/// Retrieve population allele frequencies for a variant.
pub struct GetVariantFrequencies;

#[async_trait]
impl McpTool for GetVariantFrequencies {
    fn name(&self) -> &'static str {
        "get_variant_frequencies"
    }

    fn description(&self) -> &'static str {
        "Get allele frequencies for a variant across all ancestry populations. \
         Returns per-population allele count, allele number, frequency, and \
         homozygote/hemizygote counts."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "variant_id": {
                    "type": "string",
                    "description": "Variant identifier in chrom-pos-ref-alt format"
                },
                "dataset": {
                    "type": "string",
                    "description": "Dataset version",
                    "default": "gnomad_r4"
                }
            },
            "required": ["variant_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let variant_id = args["variant_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("variant_id is required"))?;
        let dataset = args["dataset"].as_str().unwrap_or("gnomad_r4");

        match provider.get_variant_frequencies(variant_id, dataset).await? {
            Some(freqs) => Ok(serde_json::to_value(freqs)?),
            None => Ok(json!({ "error": "variant not found", "variant_id": variant_id })),
        }
    }
}

/// Retrieve details for multiple variants in one call.
pub struct GetMultipleVariantDetails;

#[async_trait]
impl McpTool for GetMultipleVariantDetails {
    fn name(&self) -> &'static str {
        "get_multiple_variant_details"
    }

    fn description(&self) -> &'static str {
        "Get detailed information for multiple variants in a single request. \
         More efficient than calling get_variant_details repeatedly."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "variant_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "List of variant identifiers in chrom-pos-ref-alt format"
                },
                "dataset": {
                    "type": "string",
                    "description": "Dataset version",
                    "default": "gnomad_r4"
                }
            },
            "required": ["variant_ids"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let variant_ids: Vec<String> = serde_json::from_value(
            args.get("variant_ids")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("variant_ids is required"))?,
        )?;
        let dataset = args["dataset"].as_str().unwrap_or("gnomad_r4");

        let details = provider
            .get_multiple_variant_details(&variant_ids, dataset)
            .await?;
        Ok(serde_json::to_value(details)?)
    }
}
