use async_trait::async_trait;

use crate::types::*;

/// Trait for providing genomic data to MCP tools.
///
/// Downstream applications implement this trait to bridge their specific
/// data backends (Hail tables, ClickHouse, DuckDB, etc.) into the
/// generic tool interface. Only methods needed by your registered tools
/// need real implementations — others can return `Err` or `Ok(None)`.
#[async_trait]
pub trait GenomicDataProvider: Send + Sync {
    /// Get detailed information for a single variant.
    ///
    /// `variant_id` uses gnomAD format: "chrom-pos-ref-alt" (e.g., "1-55039447-G-A").
    /// `dataset` identifies the dataset version (e.g., "gnomad_r4", "gnomad_r2").
    async fn get_variant_details(
        &self,
        variant_id: &str,
        dataset: &str,
    ) -> anyhow::Result<Option<VariantDetails>>;

    /// Get a concise summary for a variant (ID, consequence, frequencies).
    async fn get_variant_summary(
        &self,
        variant_id: &str,
        dataset: &str,
    ) -> anyhow::Result<Option<VariantSummary>>;

    /// Get population-level allele frequencies for a variant.
    async fn get_variant_frequencies(
        &self,
        variant_id: &str,
        dataset: &str,
    ) -> anyhow::Result<Option<Vec<PopulationFrequency>>>;

    /// Get details for multiple variants in a single call.
    async fn get_multiple_variant_details(
        &self,
        variant_ids: &[String],
        dataset: &str,
    ) -> anyhow::Result<Vec<VariantDetails>>;

    /// Get summary information for a gene.
    async fn get_gene_summary(
        &self,
        gene_id_or_symbol: &str,
    ) -> anyhow::Result<Option<GeneSummary>>;

    /// Get variants within a gene, optionally filtered by consequence.
    async fn get_gene_variants(
        &self,
        gene_id: &str,
        dataset: &str,
        consequence_filter: Option<&str>,
    ) -> anyhow::Result<Vec<VariantSummary>>;

    /// Get tissue-level expression data for a gene.
    async fn get_gene_expression(&self, gene_id: &str) -> anyhow::Result<Option<GeneExpression>>;

    /// Get variants in a genomic region.
    async fn get_region_variants(
        &self,
        chrom: &str,
        start: i64,
        end: i64,
        dataset: &str,
    ) -> anyhow::Result<Vec<VariantSummary>>;

    /// List transcripts for a gene.
    async fn list_gene_transcripts(&self, gene_id: &str) -> anyhow::Result<Vec<TranscriptSummary>>;

    /// Get details for a specific transcript.
    async fn get_transcript_details(
        &self,
        transcript_id: &str,
    ) -> anyhow::Result<Option<TranscriptDetails>>;
}
