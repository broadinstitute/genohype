use std::sync::Arc;

use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    tool, tool_router,
};

use crate::tools::{
    gene::*,
    region::*,
    variant::*,
};
use crate::traits::GenomicDataProvider;

/// MCP server exposing generic genomic data tools.
///
/// Holds a [`GenomicDataProvider`] and a tool router with standard genomic
/// tools (variant lookup, gene summary, region query, etc.).
///
/// # Usage
///
/// ```rust,no_run
/// use genohype_mcp::{GenomicToolServer, GenomicDataProvider};
/// use rmcp::transport::io::stdio;
///
/// let provider = Arc::new(my_provider);
/// let server = GenomicToolServer::new(provider);
///
/// // Run as stdio MCP server
/// let service = server.serve(stdio()).await?;
/// service.waiting().await?;
/// ```
#[derive(Clone)]
pub struct GenomicToolServer {
    provider: Arc<dyn GenomicDataProvider>,
    #[allow(dead_code)] // read by rmcp macro-generated ServerHandler dispatch
    tool_router: ToolRouter<Self>,
}

impl GenomicToolServer {
    pub fn new(provider: Arc<dyn GenomicDataProvider>) -> Self {
        Self {
            provider,
            tool_router: Self::tool_router(),
        }
    }
}

impl std::fmt::Debug for GenomicToolServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenomicToolServer").finish()
    }
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

#[tool_router]
impl GenomicToolServer {
    // -- Variant tools --

    #[tool(description = "Get detailed information about a specific genetic variant including allele frequencies across populations, transcript consequences, in silico predictor scores, and quality flags. Use variant IDs in the format 'chrom-pos-ref-alt' (e.g., '1-55039447-G-A').")]
    async fn get_variant_details(
        &self,
        Parameters(params): Parameters<GetVariantDetailsParams>,
    ) -> String {
        let dataset = params.dataset.as_deref().unwrap_or("gnomad_r4");
        match self.provider.get_variant_details(&params.variant_id, dataset).await {
            Ok(Some(details)) => serde_json::to_string_pretty(&details).unwrap_or_default(),
            Ok(None) => format!("Variant {} not found", params.variant_id),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get a concise summary of a variant including its consequence, gene, and allele frequency. Lighter than get_variant_details.")]
    async fn get_variant_summary(
        &self,
        Parameters(params): Parameters<GetVariantSummaryParams>,
    ) -> String {
        let dataset = params.dataset.as_deref().unwrap_or("gnomad_r4");
        match self.provider.get_variant_summary(&params.variant_id, dataset).await {
            Ok(Some(summary)) => serde_json::to_string_pretty(&summary).unwrap_or_default(),
            Ok(None) => format!("Variant {} not found", params.variant_id),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get allele frequencies for a variant across all ancestry populations. Returns per-population allele count, allele number, frequency, and homozygote/hemizygote counts.")]
    async fn get_variant_frequencies(
        &self,
        Parameters(params): Parameters<GetVariantFrequenciesParams>,
    ) -> String {
        let dataset = params.dataset.as_deref().unwrap_or("gnomad_r4");
        match self.provider.get_variant_frequencies(&params.variant_id, dataset).await {
            Ok(Some(freqs)) => serde_json::to_string_pretty(&freqs).unwrap_or_default(),
            Ok(None) => format!("Variant {} not found", params.variant_id),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get detailed information for multiple variants in a single request. More efficient than calling get_variant_details repeatedly.")]
    async fn get_multiple_variant_details(
        &self,
        Parameters(params): Parameters<GetMultipleVariantDetailsParams>,
    ) -> String {
        let dataset = params.dataset.as_deref().unwrap_or("gnomad_r4");
        match self.provider.get_multiple_variant_details(&params.variant_ids, dataset).await {
            Ok(details) => serde_json::to_string_pretty(&details).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    // -- Gene tools --

    #[tool(description = "Get summary information for a gene including its genomic coordinates, canonical transcript, and constraint metrics (pLI, LOEUF, missense Z). Accepts Ensembl gene IDs (ENSG...) or gene symbols (e.g., BRCA1).")]
    async fn get_gene_summary(
        &self,
        Parameters(params): Parameters<GetGeneSummaryParams>,
    ) -> String {
        match self.provider.get_gene_summary(&params.gene).await {
            Ok(Some(summary)) => serde_json::to_string_pretty(&summary).unwrap_or_default(),
            Ok(None) => format!("Gene {} not found", params.gene),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get variants found within a gene. Optionally filter by consequence type (e.g., 'missense_variant', 'stop_gained', 'pLoF'). Returns variant summaries with consequence, frequency, and gene annotation.")]
    async fn get_gene_variants(
        &self,
        Parameters(params): Parameters<GetGeneVariantsParams>,
    ) -> String {
        let dataset = params.dataset.as_deref().unwrap_or("gnomad_r4");
        match self.provider.get_gene_variants(&params.gene_id, dataset, params.consequence.as_deref()).await {
            Ok(variants) => serde_json::to_string_pretty(&variants).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get tissue-level gene expression data (TPM values from GTEx). Useful for understanding where a gene is expressed and interpreting the clinical relevance of variants.")]
    async fn get_gene_expression_summary(
        &self,
        Parameters(params): Parameters<GetGeneExpressionParams>,
    ) -> String {
        match self.provider.get_gene_expression(&params.gene_id).await {
            Ok(Some(expr)) => serde_json::to_string_pretty(&expr).unwrap_or_default(),
            Ok(None) => format!("Expression data not found for {}", params.gene_id),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "List all transcripts for a gene with their biotype, canonical status, MANE Select status, and RefSeq ID.")]
    async fn list_gene_transcripts(
        &self,
        Parameters(params): Parameters<ListGeneTranscriptsParams>,
    ) -> String {
        match self.provider.list_gene_transcripts(&params.gene_id).await {
            Ok(transcripts) => serde_json::to_string_pretty(&transcripts).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get full details for a specific transcript including exon coordinates, biotype, and identifiers.")]
    async fn get_transcript_details(
        &self,
        Parameters(params): Parameters<GetTranscriptDetailsParams>,
    ) -> String {
        match self.provider.get_transcript_details(&params.transcript_id).await {
            Ok(Some(details)) => serde_json::to_string_pretty(&details).unwrap_or_default(),
            Ok(None) => format!("Transcript {} not found", params.transcript_id),
            Err(e) => format!("Error: {e}"),
        }
    }

    // -- Region tools --

    #[tool(description = "Get variants in a genomic region defined by chromosome and start/end coordinates. Returns variant summaries with consequence, frequency, and gene annotation. Coordinates are 1-based, inclusive.")]
    async fn get_region_variants(
        &self,
        Parameters(params): Parameters<GetRegionVariantsParams>,
    ) -> String {
        let dataset = params.dataset.as_deref().unwrap_or("gnomad_r4");
        match self.provider.get_region_variants(&params.chrom, params.start, params.end, dataset).await {
            Ok(variants) => serde_json::to_string_pretty(&variants).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// ServerHandler implementation
// ---------------------------------------------------------------------------

impl ServerHandler for GenomicToolServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(
                "Genomic data MCP server providing tools for querying variants, \
                 genes, and genomic regions across population databases."
            )
    }
}
