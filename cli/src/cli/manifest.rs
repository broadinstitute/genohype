use clap::{Args, Subcommand, ValueEnum};

#[derive(Debug, Subcommand)]
pub enum ManifestCommands {
    /// Inventory the exact immutable object closure needed to read a Hail table
    Inventory(ManifestInventoryArgs),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ManifestAccessArg {
    Sequential,
    Indexed,
}

#[derive(Debug, Args)]
pub struct ManifestInventoryArgs {
    /// Absolute local table directory, or gs:// root when supported
    #[arg(value_name = "TABLE")]
    pub table: String,

    /// Local manifest path, or - for stdout
    #[arg(long, value_name = "LOCAL|-")]
    pub output: String,

    /// Local digest receipt path (defaults to <output>.sha256)
    #[arg(long, value_name = "LOCAL")]
    pub digest_output: Option<String>,

    /// Include only full-scan objects, or also metadata-declared indexes
    #[arg(long, value_enum, default_value_t = ManifestAccessArg::Sequential)]
    pub access: ManifestAccessArg,

    /// Maximum number of objects allowed in each source observation
    #[arg(long, default_value_t = genohype_core::io::DEFAULT_MAX_OBJECTS)]
    pub max_objects: usize,
}
