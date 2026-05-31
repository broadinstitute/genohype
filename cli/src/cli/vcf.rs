//! CLI argument definitions for VCF utilities.

use clap::Subcommand;

#[derive(Subcommand)]
pub enum VcfCommands {
    /// Build a tabix index (.tbi) for a BGZF-compressed VCF
    Index {
        /// Path to the VCF file (local or GCS)
        path: String,
        /// Output path for the .tbi file (default: <path>.tbi)
        #[arg(long)]
        output: Option<String>,
    },
}
