use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::mcp::traits::{GenomicDataProvider, McpTool};

/// Retrieve summary information for a gene.
pub struct GetGeneSummary;

#[async_trait]
impl McpTool for GetGeneSummary {
    fn name(&self) -> &'static str {
        "get_gene_summary"
    }

    fn description(&self) -> &'static str {
        "Get summary information for a gene including its genomic coordinates, \
         canonical transcript, and constraint metrics (pLI, LOEUF, missense Z). \
         Accepts Ensembl gene IDs (ENSG...) or gene symbols (e.g., BRCA1)."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "gene": {
                    "type": "string",
                    "description": "Ensembl gene ID (e.g., 'ENSG00000012048') or gene symbol (e.g., 'BRCA1')"
                }
            },
            "required": ["gene"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let gene = args["gene"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("gene is required"))?;

        match provider.get_gene_summary(gene).await? {
            Some(summary) => Ok(serde_json::to_value(summary)?),
            None => Ok(json!({ "error": "gene not found", "gene": gene })),
        }
    }
}

/// Retrieve variants within a gene.
pub struct GetGeneVariants;

#[async_trait]
impl McpTool for GetGeneVariants {
    fn name(&self) -> &'static str {
        "get_gene_variants"
    }

    fn description(&self) -> &'static str {
        "Get variants found within a gene. Optionally filter by consequence \
         type (e.g., 'missense_variant', 'stop_gained', 'pLoF'). Returns \
         variant summaries with consequence, frequency, and gene annotation."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "gene_id": {
                    "type": "string",
                    "description": "Ensembl gene ID (e.g., 'ENSG00000012048')"
                },
                "dataset": {
                    "type": "string",
                    "description": "Dataset version",
                    "default": "gnomad_r4"
                },
                "consequence": {
                    "type": "string",
                    "description": "Filter by consequence type (e.g., 'missense_variant', 'pLoF')"
                }
            },
            "required": ["gene_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let gene_id = args["gene_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("gene_id is required"))?;
        let dataset = args["dataset"].as_str().unwrap_or("gnomad_r4");
        let consequence = args["consequence"].as_str();

        let variants = provider
            .get_gene_variants(gene_id, dataset, consequence)
            .await?;
        Ok(serde_json::to_value(variants)?)
    }
}

/// Retrieve tissue expression data for a gene.
pub struct GetGeneExpressionSummary;

#[async_trait]
impl McpTool for GetGeneExpressionSummary {
    fn name(&self) -> &'static str {
        "get_gene_expression_summary"
    }

    fn description(&self) -> &'static str {
        "Get tissue-level gene expression data (TPM values from GTEx). \
         Useful for understanding where a gene is expressed and interpreting \
         the clinical relevance of variants."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "gene_id": {
                    "type": "string",
                    "description": "Ensembl gene ID (e.g., 'ENSG00000012048')"
                }
            },
            "required": ["gene_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let gene_id = args["gene_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("gene_id is required"))?;

        match provider.get_gene_expression(gene_id).await? {
            Some(expr) => Ok(serde_json::to_value(expr)?),
            None => Ok(json!({ "error": "expression data not found", "gene_id": gene_id })),
        }
    }
}

/// List transcripts for a gene.
pub struct ListGeneTranscripts;

#[async_trait]
impl McpTool for ListGeneTranscripts {
    fn name(&self) -> &'static str {
        "list_gene_transcripts"
    }

    fn description(&self) -> &'static str {
        "List all transcripts for a gene with their biotype, canonical status, \
         MANE Select status, and RefSeq ID."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "gene_id": {
                    "type": "string",
                    "description": "Ensembl gene ID (e.g., 'ENSG00000012048')"
                }
            },
            "required": ["gene_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let gene_id = args["gene_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("gene_id is required"))?;

        let transcripts = provider.list_gene_transcripts(gene_id).await?;
        Ok(serde_json::to_value(transcripts)?)
    }
}

/// Get details for a specific transcript.
pub struct GetTranscriptDetails;

#[async_trait]
impl McpTool for GetTranscriptDetails {
    fn name(&self) -> &'static str {
        "get_transcript_details"
    }

    fn description(&self) -> &'static str {
        "Get full details for a specific transcript including exon coordinates, \
         biotype, and identifiers."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "transcript_id": {
                    "type": "string",
                    "description": "Ensembl transcript ID (e.g., 'ENST00000357654')"
                }
            },
            "required": ["transcript_id"]
        })
    }

    async fn execute(
        &self,
        args: Value,
        provider: Arc<dyn GenomicDataProvider>,
    ) -> anyhow::Result<Value> {
        let transcript_id = args["transcript_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("transcript_id is required"))?;

        match provider.get_transcript_details(transcript_id).await? {
            Some(details) => Ok(serde_json::to_value(details)?),
            None => Ok(json!({ "error": "transcript not found", "transcript_id": transcript_id })),
        }
    }
}
