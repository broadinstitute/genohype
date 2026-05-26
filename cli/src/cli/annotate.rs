//! Annotate command argument definitions.

use clap::Args;

use super::shared::{CommonExportArgs, HasCommonExportArgs};

#[derive(Args)]
pub struct AnnotateArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output file path (default: stdout)
    #[arg(short, long)]
    pub output: Option<String>,

    /// Path to GFF3 transcript annotation file
    #[arg(long)]
    pub gff3: String,

    /// Path to reference FASTA file (enables HGVS annotations)
    #[arg(long)]
    pub fasta: Option<String>,

    /// Path to supplementary annotation directory (ClinVar, gnomAD, etc.)
    #[arg(long)]
    pub sa_dir: Option<String>,

    /// Select one consequence per variant (most severe per gene)
    #[arg(long)]
    pub pick: bool,

    /// Maximum distance (bp) from transcript to annotate upstream/downstream variants
    #[arg(long, default_value = "5000")]
    pub distance: u64,

    /// Output format
    #[arg(long, default_value = "vcf", value_parser = ["vcf", "json", "tab"])]
    pub output_format: String,
}

impl HasCommonExportArgs for AnnotateArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}
