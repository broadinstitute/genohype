//! Cloud infrastructure abstraction for distributed processing.
//!
//! This module provides traits and implementations for managing cloud VMs
//! for distributed genohype workloads.

pub mod gcp;
pub mod pool;
pub mod startup;

use crate::Result;
use std::path::Path;
use std::process::Command;

/// WireGuard VPN configuration for the coordinator node.
///
/// Values can be raw strings or secret references with `secret:` prefix.
/// Secret references are resolved at VM boot time using Google Secret Manager.
#[derive(Debug, Clone)]
pub struct WireGuardConfig {
    /// WireGuard server endpoint (host:port)
    pub endpoint: String,
    /// Client IP address with CIDR (e.g., "10.10.0.50/32")
    pub client_address: String,
    /// Allowed IPs to route through VPN (e.g., "10.10.0.0/24")
    pub allowed_ips: String,
    /// Server's public key (can be `secret:name` for GSM lookup)
    pub peer_public_key: String,
    /// Client's private key (should be `secret:name` for security)
    pub client_private_key: String,
}

/// Configuration for a worker pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Name of the pool (used for tagging and identification)
    pub name: String,
    /// Number of worker VMs to create
    pub worker_count: usize,
    /// GCP machine type (e.g., "n1-standard-4")
    pub machine_type: String,
    /// GCP zone (e.g., "us-central1-a")
    pub zone: String,
    /// Use spot/preemptible instances for cost savings
    pub spot: bool,
    /// GCP project ID
    pub project_id: String,
    /// VPC network name (None = default)
    pub network: Option<String>,
    /// Subnet name (required for custom networks)
    pub subnet: Option<String>,
    /// Assign external IP addresses to VMs
    pub public_ip: bool,
    /// Create the per-pool coordinator firewall rule
    pub manage_firewall: bool,
    /// Create a dedicated coordinator node for distributed processing
    pub with_coordinator: bool,
    /// WireGuard configuration for coordinator (optional)
    pub wireguard: Option<WireGuardConfig>,
    /// GCS path for SQLite database backup/restore (e.g., "gs://bucket/pool-ops/dev-pool/ops.db")
    pub pool_db_path: Option<String>,
    /// GCS URL for the pre-staged stock coordinator binary.
    pub binary_gcs_url: Option<String>,
    /// GCS URL for the pre-staged worker binary (custom or stock).
    pub worker_binary_gcs_url: Option<String>,
    /// GCP service account email to attach to VMs
    pub service_account: Option<String>,
}

/// Information about a cloud instance.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Instance {
    /// Instance name
    pub name: String,
    /// Zone where instance is located
    pub zone: String,
    /// Network interfaces (for IP addresses)
    #[serde(rename = "networkInterfaces", default)]
    pub network_interfaces: Vec<NetworkInterface>,
    /// Instance status (RUNNING, TERMINATED, etc.)
    pub status: String,
    /// Machine type (full GCP resource URL)
    #[serde(rename = "machineType", default)]
    pub machine_type: Option<String>,
    /// Scheduling info (for spot instances)
    #[serde(default)]
    pub scheduling: Option<Scheduling>,
}

/// Scheduling information.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Scheduling {
    #[serde(rename = "provisioningModel")]
    pub provisioning_model: Option<String>,
}

/// Network interface information.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NetworkInterface {
    /// Internal IP address
    #[serde(rename = "networkIP")]
    pub network_ip: String,
    /// VPC network
    pub network: Option<String>,
    /// Subnetwork
    pub subnetwork: Option<String>,
}

impl Instance {
    /// Get the internal IP address of the instance.
    pub fn ip(&self) -> Option<&str> {
        self.network_interfaces
            .first()
            .map(|ni| ni.network_ip.as_str())
    }

    /// Check if the instance is running.
    pub fn is_running(&self) -> bool {
        self.status == "RUNNING"
    }
}

/// Configuration for creating a specific instance.
/// Used by the scale operation to create individual worker VMs.
#[derive(Debug, Clone)]
pub struct InstanceSetup {
    /// Instance name (e.g., "mypool-worker-3")
    pub name: String,
    /// GCP machine type (e.g., "n1-standard-4")
    pub machine_type: String,
    /// GCP zone (e.g., "us-central1-a")
    pub zone: String,
    /// Network tags (e.g., ["genohype-worker,pool-mypool,role-worker"])
    pub tags: Vec<String>,
    /// Startup script to run on boot
    pub startup_script: String,
    /// Use spot/preemptible instances
    pub spot: bool,
    /// VPC network name (None = default)
    pub network: Option<String>,
    /// Subnet name
    pub subnet: Option<String>,
    /// Assign an external IP address to the VM
    pub public_ip: bool,
    /// GCP project ID
    pub project_id: String,
    /// GCP service account email to attach to the VM
    pub service_account: Option<String>,
}

/// Configuration for scaling operations.
/// Contains the subset of pool configuration needed for scaling workers.
#[derive(Debug, Clone)]
pub struct ScalingConfig {
    /// GCP machine type for workers
    pub machine_type: String,
    /// Target number of workers (used for autoscale)
    pub workers: usize,
    /// Use spot instances
    pub spot: bool,
    /// VPC network name
    pub network: Option<String>,
    /// Subnet name
    pub subnet: Option<String>,
    /// Assign external IP addresses to workers
    pub public_ip: bool,
    /// Whether Genohype manages the coordinator firewall rule
    pub manage_firewall: bool,
    /// GCP project ID
    pub project: Option<String>,
    /// Whether the pool has a coordinator
    pub with_coordinator: bool,
    /// GCS path for SQLite database backup/restore
    pub pool_db_path: Option<String>,
    /// Path to a custom binary to deploy to workers instead of the coordinator binary
    pub worker_binary: Option<String>,
    /// GCP service account email to attach to VMs
    pub service_account: Option<String>,
}

/// Abstract interface for cloud infrastructure operations.
///
/// This trait allows swapping out the underlying cloud provider implementation
/// (e.g., gcloud CLI wrapper vs native SDK) without changing the orchestration logic.
pub trait CloudProvider {
    /// Stable project selected for this provider operation, if explicitly resolved.
    fn project_id(&self) -> Option<&str> {
        None
    }

    /// Provision a cluster of worker VMs.
    ///
    /// Creates `config.worker_count` VMs with names `{pool_name}-worker-{i}`.
    fn create_pool(&self, config: &PoolConfig) -> Result<()>;

    /// List active instances in the pool.
    ///
    /// Returns instances tagged with `pool-{pool_name}`.
    fn list_instances(&self, pool_name: &str) -> Result<Vec<Instance>>;

    /// Destroy all instances in the pool.
    fn destroy_pool(&self, pool_name: &str, zone: &str) -> Result<()>;

    /// Create specific instances using detailed configuration.
    /// Used for scaling up worker pools.
    fn create_instances(&self, instances: &[InstanceSetup]) -> Result<()>;

    /// Delete specific instances by name.
    /// Used for scaling down worker pools.
    fn delete_instances(&self, names: &[String], zone: &str, project_id: &str) -> Result<()>;

    /// Stop specific instances without deleting them.
    fn stop_instances(&self, names: &[String], zone: &str) -> Result<()> {
        let _ = (names, zone);
        Err(crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Stopping instances is not supported by this cloud provider",
        )))
    }

    /// Upload a file to a specific instance via SCP.
    fn upload_file(
        &self,
        local_path: &Path,
        remote_path: &str,
        instance: &str,
        zone: &str,
    ) -> Result<()>;

    /// Get a Command struct ready to execute SSH into an instance.
    ///
    /// The caller is responsible for spawning and managing I/O.
    /// This allows streaming stdout/stderr for real-time progress.
    fn get_ssh_command(&self, instance: &str, zone: &str, command: &str) -> Command;
}

/// Progress update message sent from worker to coordinator.
///
/// Workers emit this as a JSON line to stdout when `--progress-json` is enabled.
/// The coordinator parses these to update aggregate progress display.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProgressUpdate {
    /// Message type discriminator
    #[serde(rename = "type")]
    pub msg_type: String,
    /// Worker ID (from --worker-id)
    pub worker_id: usize,
    /// Number of partitions completed
    pub partitions_done: usize,
    /// Total partitions assigned to this worker
    pub partitions_total: usize,
    /// Number of rows processed so far
    pub rows: usize,
    /// Elapsed time in seconds
    pub elapsed_secs: f64,
}

impl ProgressUpdate {
    /// Create a new progress update.
    pub fn new(
        worker_id: usize,
        partitions_done: usize,
        partitions_total: usize,
        rows: usize,
        elapsed_secs: f64,
    ) -> Self {
        Self {
            msg_type: "progress".to_string(),
            worker_id,
            partitions_done,
            partitions_total,
            rows,
            elapsed_secs,
        }
    }

    /// Serialize to a JSON line (no trailing newline).
    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}
