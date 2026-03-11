//! Pool and service command argument definitions.

use clap::Subcommand;

/// Subcommands for managing distributed worker pools.
#[derive(Subcommand)]
pub enum PoolCommands {
    /// Create a new worker pool of GCP VMs
    ///
    /// If a pool profile with the same name exists in the config file,
    /// its settings will be used as defaults. CLI arguments override config.
    Create {
        /// Name of the pool (used for tagging and identification)
        /// If a profile with this name exists in config, its settings are used as defaults
        name: String,

        /// Number of worker VMs to create (default: 4, or from config profile)
        #[arg(long)]
        workers: Option<usize>,

        /// GCP machine type (default: c3-highcpu-22, or from config profile)
        #[arg(long)]
        machine_type: Option<String>,

        /// GCP zone for the VMs (default: us-central1-a, or from config)
        #[arg(long)]
        zone: Option<String>,

        /// Use spot/preemptible instances for cost savings
        #[arg(long)]
        spot: Option<bool>,

        /// GCP project ID (defaults to gcloud config or config file)
        #[arg(long)]
        project: Option<String>,

        /// VPC network name (defaults to "default" or config file)
        #[arg(long)]
        network: Option<String>,

        /// Subnet name (required if network is specified and not using default)
        #[arg(long)]
        subnet: Option<String>,

        /// Wait for VMs to be ready (startup script complete)
        #[arg(long)]
        wait: bool,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,

        /// Create a dedicated coordinator node for distributed processing
        #[arg(long)]
        with_coordinator: bool,
    },

    /// Submit a job to run on the worker pool
    Submit {
        /// Name of the pool to submit to
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,

        /// Target cluster for this job (overrides job config with cluster's ClickHouse URL and output path)
        #[arg(long)]
        cluster: Option<String>,

        /// Path to the Linux-compiled binary (defaults to target/x86_64-unknown-linux-gnu/release/genohype)
        #[arg(long)]
        binary: Option<String>,

        /// Automatically stop VMs after job completion to save costs
        #[arg(long)]
        auto_stop: bool,

        /// Force binary redeployment even if coordinator is already running
        #[arg(long)]
        redeploy_binary: bool,

        /// Automatically scale workers up for this job and down to 0 afterwards
        #[arg(long)]
        autoscale: bool,

        /// Force submission even if a job is already running (supersedes it)
        #[arg(long)]
        force: bool,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,

        /// Number of partitions per worker batch (higher = more parallelism per worker)
        #[arg(long)]
        batch_size: Option<usize>,

        /// Hint for memory required per partition in MB (overrides default heuristics)
        #[arg(long)]
        memory_weight_mb: Option<u64>,

        /// The command to run on workers (everything after --)
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },

    /// Scale the number of workers in a pool
    Scale {
        /// Name of the pool
        name: String,

        /// Target number of workers
        #[arg(long)]
        workers: usize,

        /// GCP zone (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,

        /// Path to the Linux-compiled binary (optional)
        #[arg(long)]
        binary: Option<String>,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,
    },

    /// Destroy a worker pool and delete all VMs
    Destroy {
        /// Name of the pool to destroy
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,

        /// GCS bucket path to export metrics database before destruction (e.g., gs://my-bucket/metrics/)
        #[arg(long)]
        metrics_bucket: Option<String>,
    },

    /// List instances in a worker pool
    List {
        /// Name of the pool
        name: String,
    },

    /// Check status of a distributed job running on the pool
    Status {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,
    },

    /// Update the binary on a running pool (upload to coordinator, workers pull)
    UpdateBinary {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,

        /// Path to the Linux-compiled binary (defaults to target/x86_64-unknown-linux-gnu/release/genohype)
        #[arg(long)]
        binary: Option<String>,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,

        /// Use HTTP API instead of SSH (requires IAP tunnel to coordinator on localhost:3000)
        #[arg(long)]
        via_api: bool,

        /// Port where coordinator is accessible (default: 3000)
        #[arg(long, default_value = "3000")]
        port: u16,
    },

    /// Cancel a running job on the pool
    Cancel {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,
    },

    /// Show real-time worker activity
    Workers {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,
    },

    /// Tail the event log
    Events {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,

        /// Follow the event stream (like tail -f)
        #[arg(short, long)]
        follow: bool,
    },

    /// Show recent task failures
    Failures {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,
    },

    /// Show tail of a specific worker's logs
    Logs {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located (default: from config or us-central1-a)
        #[arg(long)]
        zone: Option<String>,

        /// Worker ID to query logs for
        #[arg(long)]
        worker: String,
    },
}

/// Subcommands for running distributed service components.
#[derive(Subcommand)]
pub enum ServiceCommands {
    /// Start the coordinator server (manages work distribution)
    StartCoordinator {
        /// Port to listen on
        #[arg(long, default_value = "3000")]
        port: u16,

        /// Local path where the active SQLite database will run
        #[arg(long, default_value = "/var/lib/genohype/ops.db")]
        db_path: String,

        /// GCS path to backup the database to (and restore from on startup)
        #[arg(long)]
        backup_path: Option<String>,

        /// Path to input Hail table (optional, can be set later via POST /api/job)
        #[arg(long)]
        input: Option<String>,

        /// Path to output directory (optional, can be set later via POST /api/job)
        #[arg(long)]
        output: Option<String>,

        /// Total number of partitions to process (optional, can be set later via POST /api/job)
        #[arg(long)]
        total_partitions: Option<usize>,

        /// Number of partitions to assign per work request
        #[arg(long, default_value = "10")]
        batch_size: usize,

        /// Timeout in seconds before rescheduling stale work
        #[arg(long, default_value = "600")]
        timeout: u64,
    },

    /// Start a worker process (connects to coordinator for work)
    StartWorker {
        /// Coordinator URL (e.g., http://10.0.0.5:3000)
        #[arg(long)]
        url: String,

        /// Unique worker ID
        #[arg(long)]
        worker_id: String,

        /// Poll interval in milliseconds when waiting for work
        #[arg(long, default_value = "2000")]
        poll_interval: u64,
    },
}

/// Re-export InitStrategy from distributed module for CLI usage.
/// This allows the CLI to use the same enum as the library code.
#[cfg(feature = "clickhouse")]
pub use crate::distributed::message::InitStrategy;
