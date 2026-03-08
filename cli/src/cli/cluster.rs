//! Cluster and ClickHouse command argument definitions.

use clap::Subcommand;

/// Subcommands for managing cluster configurations (legacy).
#[derive(Subcommand)]
pub enum ClusterCommands {
    /// List all configured clusters
    List {
        /// Filter by status (active, standby, deprecated)
        #[arg(long)]
        status: Option<String>,
    },

    /// Show details of a specific cluster
    Show {
        /// Cluster name
        name: String,
    },

    /// Verify cluster connectivity and configuration
    Verify {
        /// Cluster name
        name: String,
    },

    /// Deploy Cloud Run services to a cluster
    Deploy {
        /// Cluster name
        name: String,

        /// Docker image tag (default: latest)
        #[arg(long, default_value = "latest")]
        tag: String,

        /// Only deploy backend (skip frontend)
        #[arg(long)]
        backend_only: bool,
    },
}

/// Subcommands for managing ClickHouse instances.
#[derive(Subcommand)]
pub enum ClickHouseCommands {
    /// Create a new ClickHouse instance
    Create {
        /// Instance name (used for VM name and identification)
        name: String,

        /// Profile name from config file (e.g., "standard", "large")
        #[arg(long)]
        profile: Option<String>,

        /// GCP machine type (overrides profile)
        #[arg(long)]
        machine_type: Option<String>,

        /// Boot disk size in GB (overrides profile)
        #[arg(long)]
        disk_size_gb: Option<u32>,

        /// GCP zone (overrides profile/defaults)
        #[arg(long)]
        zone: Option<String>,
    },

    /// List ClickHouse instances
    List,

    /// Show details of a ClickHouse instance
    Show {
        /// Instance name
        name: String,
    },

    /// Destroy a ClickHouse instance
    Destroy {
        /// Instance name
        name: String,

        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// Get the internal IP of a ClickHouse instance
    Ip {
        /// Instance name
        name: String,
    },

    /// SSH into a ClickHouse instance
    Ssh {
        /// Instance name
        name: String,

        /// Command to run (optional, opens shell if not provided)
        #[arg(last = true)]
        command: Vec<String>,
    },

    /// Create an SSH tunnel to ClickHouse (port forward 8123 to localhost)
    Tunnel {
        /// Instance name
        name: String,

        /// Local port to bind (default: 8123)
        #[arg(long, default_value = "8123")]
        port: u16,
    },
}

/// Subcommands for managing environment configuration.
#[derive(Subcommand)]
pub enum EnvCommands {
    /// Initialize a new .genohype-env file
    Init {
        /// Environment name (e.g., "20260303", "dev")
        name: String,

        /// Storage path (e.g., "gs://bucket/prefix")
        #[arg(long)]
        storage: Option<String>,

        /// ClickHouse instance name or URL
        #[arg(long)]
        clickhouse: Option<String>,
    },

    /// Show current environment configuration
    Show,

    /// Verify environment connectivity (storage + ClickHouse)
    Verify,
}
