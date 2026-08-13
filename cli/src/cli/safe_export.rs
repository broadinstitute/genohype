use clap::{Args, Subcommand};

#[derive(Debug, Subcommand)]
pub enum SafeExportCommands {
    /// Seal an immutable GCS Hail-to-Parquet export specification into a new local file
    Seal(SafeExportSealArgs),
    /// Independently verify the exact publication graph rooted at _SUCCESS
    Validate(SafeExportValidateArgs),
}

#[derive(Debug, Args)]
pub struct SafeExportSealArgs {
    /// Canonical gs:// Hail table URI
    #[arg(long, value_name = "GS_URI")]
    pub table: String,

    /// Local canonical source-manifest JSON produced for the exact GCS table
    #[arg(long, value_name = "LOCAL")]
    pub manifest: String,

    /// Immutable gs:// URI where the exact local manifest bytes will be uploaded
    #[arg(long, value_name = "GS_URI")]
    pub manifest_uri: String,

    /// Stable inventory time in canonical UTC form (YYYY-MM-DDTHH:MM:SSZ)
    #[arg(long, value_name = "UTC")]
    pub snapshot_timestamp: String,

    /// Canonical gs:// output root (no trailing slash)
    #[arg(long, value_name = "GS_URI")]
    pub output_root: String,

    /// Exact deployed stock Linux worker binary, read and hashed locally
    #[arg(long, value_name = "LOCAL")]
    pub worker_binary: String,

    /// Immutable gs:// URI of the exact deployed worker binary
    #[arg(long, value_name = "GS_URI")]
    pub worker_binary_uri: String,

    /// Seven-lowercase-hex commit advertised by the stock worker
    #[arg(long, value_name = "GIT7")]
    pub worker_git_commit: String,

    /// Build identity advertised by the stock worker (must be <package-version>:<git7>)
    #[arg(long, value_name = "IDENTITY")]
    pub worker_build_identity: String,

    /// Exact Cargo feature names used for the worker build; repeat for each feature
    #[arg(long = "worker-feature", value_name = "FEATURE", required = true)]
    pub worker_features: Vec<String>,

    /// Create-new local specification path
    #[arg(long, value_name = "LOCAL")]
    pub output: String,
}

#[derive(Debug, Args)]
pub struct SafeExportValidateArgs {
    /// Local sealed safe-export specification
    #[arg(long, value_name = "LOCAL")]
    pub spec: String,

    /// Create-new local deterministic report, or - for stdout (default)
    #[arg(long, value_name = "LOCAL|-", default_value = "-")]
    pub output: String,

    /// Read-only local/mock evidence bundle instead of GCS (testing and archived evidence only)
    #[arg(long, value_name = "LOCAL_JSON")]
    pub evidence_bundle: Option<String>,
}
