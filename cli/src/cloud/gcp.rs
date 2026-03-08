//! Google Cloud Platform client using gcloud CLI.
//!
//! This module wraps the `gcloud` command-line tool to provide VM management
//! operations. Using the CLI instead of native SDKs has key advantages:
//!
//! - **IAP Tunneling**: `gcloud compute ssh` handles Identity-Aware Proxy tunneling
//!   automatically, allowing SSH to VMs without public IPs.
//! - **Authentication**: Leverages existing `gcloud auth` credentials.
//! - **Key Management**: Handles ephemeral SSH key generation and metadata updates.
//!
//! The trade-off is requiring `gcloud` to be installed and configured.

use super::{CloudProvider, Instance, PoolConfig};
use crate::{HailError, Result};
use rayon::prelude::*;
use std::path::Path;
use std::process::Command;

/// GCP client using the gcloud CLI.
pub struct GcpClient {
    /// Optional project override (uses gcloud default if None)
    project: Option<String>,
}

impl GcpClient {
    /// Create a new GCP client using gcloud defaults.
    pub fn new() -> Self {
        Self { project: None }
    }

    /// Create a new GCP client with a specific project.
    #[allow(dead_code)]
    pub fn with_project(project: String) -> Self {
        Self {
            project: Some(project),
        }
    }

    /// Check that gcloud CLI is installed and accessible.
    fn check_gcloud_installed(&self) -> Result<()> {
        let output = Command::new("gcloud").arg("--version").output();
        match output {
            Ok(o) if o.status.success() => Ok(()),
            Ok(_) => Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "gcloud command failed. Please check your Google Cloud SDK installation.",
            ))),
            Err(_) => Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "gcloud CLI not found. Please install Google Cloud SDK: https://cloud.google.com/sdk/docs/install",
            ))),
        }
    }

    /// Get the current project from gcloud config.
    pub fn get_current_project(&self) -> Result<String> {
        if let Some(ref project) = self.project {
            return Ok(project.clone());
        }

        let output = Command::new("gcloud")
            .args(["config", "get-value", "project"])
            .output()
            .map_err(HailError::Io)?;

        if !output.status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to get current GCP project. Run: gcloud config set project PROJECT_ID",
            )));
        }

        let project = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if project.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No GCP project configured. Run: gcloud config set project PROJECT_ID",
            )));
        }

        Ok(project)
    }

    /// Get the default zone from gcloud config.
    #[allow(dead_code)]
    pub fn get_default_zone(&self) -> Result<String> {
        let output = Command::new("gcloud")
            .args(["config", "get-value", "compute/zone"])
            .output()
            .map_err(HailError::Io)?;

        let zone = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if zone.is_empty() {
            // Default to us-central1-a if not configured
            Ok("us-central1-a".to_string())
        } else {
            Ok(zone)
        }
    }

    /// Wait for an instance to be in RUNNING state.
    #[allow(dead_code)]
    pub fn wait_for_instance(&self, instance: &str, zone: &str, timeout_secs: u64) -> Result<()> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timeout waiting for instance {} to be ready", instance),
                )));
            }

            let output = Command::new("gcloud")
                .args([
                    "compute",
                    "instances",
                    "describe",
                    instance,
                    "--zone",
                    zone,
                    "--format",
                    "value(status)",
                ])
                .output()
                .map_err(HailError::Io)?;

            let status = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if status == "RUNNING" {
                return Ok(());
            }

            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    /// Wait for the startup script to complete (marker file exists).
    #[allow(dead_code)]
    pub fn wait_for_startup_complete(
        &self,
        instance: &str,
        zone: &str,
        timeout_secs: u64,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timeout waiting for startup script on instance {}",
                        instance
                    ),
                )));
            }

            // Check if the ready marker file exists
            let mut cmd = self.get_ssh_command(instance, zone, "test -f /tmp/genohype-ready");
            let status = cmd.status();

            if let Ok(s) = status {
                if s.success() {
                    return Ok(());
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(3));
        }
    }
}

impl Default for GcpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudProvider for GcpClient {
    fn create_pool(&self, config: &PoolConfig) -> Result<()> {
        self.check_gcloud_installed()?;

        // Generate startup scripts (with optional binary download from GCS)
        // Workers auto-start and connect to coordinator via internal DNS
        let worker_script = super::startup::generate_worker_startup_script(
            config.binary_gcs_url.as_deref(),
            &config.name,
        );
        // Coordinator auto-starts if binary is provided
        let coordinator_script = super::startup::generate_coordinator_startup_script(
            config.wireguard.as_ref(),
            config.binary_gcs_url.as_deref(),
            config.pool_db_path.as_deref(),
        );

        // Build list of instances to create: coordinator (optional) + workers
        let mut instance_configs: Vec<(String, String, String)> = Vec::new(); // (name, tags, script)

        // Add coordinator if requested
        if config.with_coordinator {
            instance_configs.push((
                format!("{}-coordinator", config.name),
                format!("genohype-coordinator,pool-{},role-coordinator", config.name),
                coordinator_script,
            ));
        }

        // Add workers (always use standard script)
        for i in 0..config.worker_count {
            instance_configs.push((
                format!("{}-worker-{}", config.name, i),
                format!("genohype-worker,pool-{},role-worker", config.name),
                worker_script.clone(),
            ));
        }

        // Auto-create firewall rule for coordinator port if needed
        if config.with_coordinator {
            let firewall_name = format!("allow-hail-coord-int-{}", config.name);
            let network = config.network.as_deref().unwrap_or("default");
            // Try to create firewall rule (ignore error if it already exists)
            let _ = Command::new("gcloud")
                .args([
                    "compute",
                    "firewall-rules",
                    "create",
                    &firewall_name,
                    "--network",
                    network,
                    "--allow",
                    "tcp:3000",
                    "--source-ranges",
                    "10.0.0.0/8",
                    "--project",
                    &config.project_id,
                    "--quiet",
                ])
                .output();
        }

        // Create instances in parallel using rayon
        let results: Vec<Result<()>> = instance_configs
            .into_par_iter()
            .map(|(instance_name, tags, startup_script)| {
                let mut cmd = Command::new("gcloud");

                // Use a smaller machine for coordinator (it just routes work)
                let machine_type = if instance_name.ends_with("-coordinator") {
                    "e2-standard-2"
                } else {
                    &config.machine_type
                };

                cmd.args([
                    "compute",
                    "instances",
                    "create",
                    &instance_name,
                    "--project",
                    &config.project_id,
                    "--zone",
                    &config.zone,
                    "--machine-type",
                    machine_type,
                    "--image-family",
                    "ubuntu-2204-lts",
                    "--image-project",
                    "ubuntu-os-cloud",
                    "--tags",
                    &tags,
                    "--metadata",
                    &format!("startup-script={}", startup_script),
                    "--scopes",
                    "cloud-platform", // Allow access to GCS and Secret Manager
                ]);

                // Use Spot for workers if requested, but keep coordinator on standard VM
                if config.spot && !instance_name.ends_with("-coordinator") {
                    cmd.arg("--provisioning-model=SPOT");
                    cmd.arg("--instance-termination-action=STOP");
                }

                // Network configuration
                if let Some(ref network) = config.network {
                    cmd.args(["--network", network]);
                }
                if let Some(ref subnet) = config.subnet {
                    cmd.args(["--subnet", subnet]);
                }

                let output = cmd.output().map_err(HailError::Io)?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create instance {}: {}", instance_name, stderr),
                    )));
                }

                Ok(())
            })
            .collect();

        // Check if any creation failed
        for result in results {
            result?;
        }

        Ok(())
    }

    fn list_instances(&self, pool_name: &str) -> Result<Vec<Instance>> {
        let output = Command::new("gcloud")
            .args([
                "compute",
                "instances",
                "list",
                "--filter",
                &format!("tags.items:pool-{}", pool_name),
                "--format",
                "json(name,zone,status,networkInterfaces[].networkIP)",
            ])
            .output()
            .map_err(HailError::Io)?;

        if !output.status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to list instances: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            )));
        }

        let instances: Vec<Instance> = serde_json::from_slice(&output.stdout).map_err(|e| {
            HailError::ParseError(format!("Failed to parse gcloud output: {}", e))
        })?;

        Ok(instances)
    }

    fn destroy_pool(&self, pool_name: &str, zone: &str) -> Result<()> {
        let instances = self.list_instances(pool_name)?;
        if instances.is_empty() {
            return Ok(());
        }

        let instance_names: Vec<&str> = instances.iter().map(|i| i.name.as_str()).collect();

        // gcloud supports bulk delete
        let status = Command::new("gcloud")
            .args(["compute", "instances", "delete"])
            .args(&instance_names)
            .args(["--zone", zone, "--quiet"])
            .status()
            .map_err(HailError::Io)?;

        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to delete instances",
            )));
        }

        Ok(())
    }

    fn create_instances(&self, instances: &[super::InstanceSetup]) -> Result<()> {
        self.check_gcloud_installed()?;

        if instances.is_empty() {
            return Ok(());
        }

        // Create instances in parallel using rayon
        let results: Vec<Result<()>> = instances
            .par_iter()
            .map(|setup| {
                let mut cmd = Command::new("gcloud");

                cmd.args([
                    "compute",
                    "instances",
                    "create",
                    &setup.name,
                    "--project",
                    &setup.project_id,
                    "--zone",
                    &setup.zone,
                    "--machine-type",
                    &setup.machine_type,
                    "--image-family",
                    "ubuntu-2204-lts",
                    "--image-project",
                    "ubuntu-os-cloud",
                    "--tags",
                    &setup.tags.join(","),
                    "--metadata",
                    &format!("startup-script={}", setup.startup_script),
                    "--scopes",
                    "cloud-platform",
                ]);

                if setup.spot {
                    cmd.arg("--provisioning-model=SPOT");
                    cmd.arg("--instance-termination-action=STOP");
                }

                if let Some(ref network) = setup.network {
                    cmd.args(["--network", network]);
                }
                if let Some(ref subnet) = setup.subnet {
                    cmd.args(["--subnet", subnet]);
                }

                let output = cmd.output().map_err(HailError::Io)?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create instance {}: {}", setup.name, stderr),
                    )));
                }

                Ok(())
            })
            .collect();

        // Check for any creation failures
        for result in results {
            result?;
        }

        Ok(())
    }

    fn delete_instances(&self, names: &[String], zone: &str, project_id: &str) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }

        let status = Command::new("gcloud")
            .args(["compute", "instances", "delete"])
            .args(names)
            .args(["--zone", zone, "--project", project_id, "--quiet"])
            .status()
            .map_err(HailError::Io)?;

        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to delete instances",
            )));
        }

        Ok(())
    }

    fn upload_file(
        &self,
        local_path: &Path,
        remote_path: &str,
        instance: &str,
        zone: &str,
    ) -> Result<()> {
        let local_str = local_path
            .to_str()
            .ok_or_else(|| HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid local path",
            )))?;

        let status = Command::new("gcloud")
            .args([
                "compute",
                "scp",
                local_str,
                &format!("{}:{}", instance, remote_path),
                "--zone",
                zone,
                "--tunnel-through-iap",
                "--quiet",
            ])
            .status()
            .map_err(HailError::Io)?;

        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to upload file to {}", instance),
            )));
        }

        Ok(())
    }

    fn get_ssh_command(&self, instance: &str, zone: &str, command: &str) -> Command {
        let mut cmd = Command::new("gcloud");
        cmd.args([
            "compute",
            "ssh",
            instance,
            "--zone",
            zone,
            "--tunnel-through-iap",
            "--command",
            command,
            "--quiet",
        ]);
        cmd
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::NetworkInterface;

    #[test]
    fn test_gcp_client_creation() {
        let client = GcpClient::new();
        assert!(client.project.is_none());

        let client = GcpClient::with_project("my-project".to_string());
        assert_eq!(client.project, Some("my-project".to_string()));
    }

    #[test]
    fn test_instance_helpers() {
        let instance = Instance {
            name: "test-worker-0".to_string(),
            zone: "us-central1-a".to_string(),
            network_interfaces: vec![NetworkInterface {
                network_ip: "10.0.0.1".to_string(),
            }],
            status: "RUNNING".to_string(),
        };

        assert_eq!(instance.ip(), Some("10.0.0.1"));
        assert!(instance.is_running());

        let stopped = Instance {
            status: "TERMINATED".to_string(),
            ..instance
        };
        assert!(!stopped.is_running());
    }
}
