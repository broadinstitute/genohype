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

use super::{CloudProvider, Instance, InstanceSetup, PoolConfig};
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

    fn add_project_arg(&self, command: &mut Command) {
        if let Some(project) = &self.project {
            command.args(["--project", project]);
        }
    }

    fn list_instances_command(&self, pool_name: &str) -> Command {
        let mut command = Command::new("gcloud");
        command.args([
            "compute",
            "instances",
            "list",
            "--filter",
            &format!("tags.items:pool-{}", pool_name),
            "--format",
            "json(name,zone,status,networkInterfaces[].networkIP,networkInterfaces[].network,networkInterfaces[].subnetwork,machineType,scheduling.provisioningModel)",
        ]);
        self.add_project_arg(&mut command);
        command
    }

    fn firewall_create_command(config: &PoolConfig) -> Option<Command> {
        if !config.with_coordinator || !config.manage_firewall {
            return None;
        }

        let mut command = Command::new("gcloud");
        command.args([
            "compute",
            "firewall-rules",
            "create",
            &format!("allow-hail-coord-int-{}", config.name),
            "--network",
            config.network.as_deref().unwrap_or("default"),
            "--allow",
            "tcp:3000",
            "--source-ranges",
            "10.0.0.0/8",
            "--project",
            &config.project_id,
            "--quiet",
        ]);
        Some(command)
    }

    fn instance_service_account(config: &PoolConfig, is_coordinator: bool) -> Option<String> {
        if is_coordinator {
            config
                .coordinator_service_account
                .clone()
                .or_else(|| config.service_account.clone())
        } else {
            config.service_account.clone()
        }
    }

    fn instance_create_command(setup: &InstanceSetup) -> Command {
        let mut command = Command::new("gcloud");
        command.args([
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
            command.arg("--provisioning-model=SPOT");
            command.arg("--instance-termination-action=STOP");
        }
        if let Some(network) = &setup.network {
            command.args(["--network", network]);
        }
        if let Some(subnet) = &setup.subnet {
            command.args(["--subnet", subnet]);
        }
        if !setup.public_ip {
            command.arg("--no-address");
        }
        if let Some(service_account) = &setup.service_account {
            command.arg(format!("--service-account={}", service_account));
        }
        command
    }

    fn scp_command(
        &self,
        local_path: &str,
        remote_path: &str,
        instance: &str,
        zone: &str,
    ) -> Command {
        let mut command = Command::new("gcloud");
        command.args([
            "compute",
            "scp",
            local_path,
            &format!("{}:{}", instance, remote_path),
            "--zone",
            zone,
            "--tunnel-through-iap",
            "--quiet",
        ]);
        self.add_project_arg(&mut command);
        command
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

            let mut command = Command::new("gcloud");
            command.args([
                "compute",
                "instances",
                "describe",
                instance,
                "--zone",
                zone,
                "--format",
                "value(status)",
            ]);
            self.add_project_arg(&mut command);
            let output = command.output().map_err(HailError::Io)?;

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
    fn project_id(&self) -> Option<&str> {
        self.project.as_deref()
    }

    fn create_pool(&self, config: &PoolConfig) -> Result<()> {
        self.check_gcloud_installed()?;

        // Generate startup scripts (with optional binary download from GCS)
        // Workers auto-start and connect to coordinator via internal DNS
        let worker_script = super::startup::generate_worker_startup_script(
            config
                .worker_binary_gcs_url
                .as_deref()
                .or(config.binary_gcs_url.as_deref()),
            &config.name,
        );
        // Coordinator auto-starts if binary is provided
        let cluster_cfg = super::startup::CoordinatorClusterConfig {
            pool_name: Some(&config.name),
            project: Some(&config.project_id),
            zone: Some(&config.zone),
            machine_type: Some(&config.machine_type),
            spot: Some(config.spot),
            network: config.network.as_deref(),
            subnet: config.subnet.as_deref(),
            public_ip: Some(config.public_ip),
            manage_firewall: Some(config.manage_firewall),
            worker_service_account: config.service_account.as_deref(),
        };
        let coordinator_script = super::startup::generate_coordinator_startup_script_with_cluster(
            config.wireguard.as_ref(),
            config.binary_gcs_url.as_deref(),
            config.pool_db_path.as_deref(),
            Some(&cluster_cfg),
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

        // Auto-create firewall rule for coordinator port unless infrastructure manages it.
        if let Some(mut command) = Self::firewall_create_command(config) {
            // Preserve legacy best-effort behavior for existing users.
            let _ = command.output();
        }

        // Create instances in parallel using rayon
        let results: Vec<Result<()>> = instance_configs
            .into_par_iter()
            .map(|(instance_name, tags, startup_script)| {
                let is_coordinator = instance_name.ends_with("-coordinator");
                let setup = InstanceSetup {
                    name: instance_name.clone(),
                    machine_type: if is_coordinator {
                        "e2-standard-2".to_string()
                    } else {
                        config.machine_type.clone()
                    },
                    zone: config.zone.clone(),
                    tags: vec![tags],
                    startup_script,
                    spot: config.spot && !is_coordinator,
                    network: config.network.clone(),
                    subnet: config.subnet.clone(),
                    public_ip: config.public_ip,
                    project_id: config.project_id.clone(),
                    service_account: Self::instance_service_account(config, is_coordinator),
                };
                let output = Self::instance_create_command(&setup)
                    .output()
                    .map_err(HailError::Io)?;

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
        let output = self
            .list_instances_command(pool_name)
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

        let instances: Vec<Instance> = serde_json::from_slice(&output.stdout)
            .map_err(|e| HailError::ParseError(format!("Failed to parse gcloud output: {}", e)))?;

        Ok(instances)
    }

    fn destroy_pool(&self, pool_name: &str, zone: &str) -> Result<()> {
        let instances = self.list_instances(pool_name)?;
        if instances.is_empty() {
            return Ok(());
        }

        let instance_names: Vec<&str> = instances.iter().map(|i| i.name.as_str()).collect();

        // gcloud supports bulk delete
        let mut command = Command::new("gcloud");
        command
            .args(["compute", "instances", "delete"])
            .args(&instance_names)
            .args(["--zone", zone, "--quiet"]);
        self.add_project_arg(&mut command);
        let status = command.status().map_err(HailError::Io)?;

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
                let output = Self::instance_create_command(setup)
                    .output()
                    .map_err(HailError::Io)?;

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

    fn stop_instances(&self, names: &[String], zone: &str) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }

        let mut command = Command::new("gcloud");
        command
            .args(["compute", "instances", "stop"])
            .args(names)
            .args(["--zone", zone, "--quiet"]);
        self.add_project_arg(&mut command);
        let status = command.status().map_err(HailError::Io)?;
        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to stop instances",
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
        let local_str = local_path.to_str().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid local path",
            ))
        })?;

        let status = self
            .scp_command(local_str, remote_path, instance, zone)
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
        self.add_project_arg(&mut cmd);
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

    fn args(command: &Command) -> Vec<String> {
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn assert_project_arg(command: &Command, expected: &str) {
        let args = args(command);
        let position = args.iter().position(|arg| arg == "--project").unwrap();
        assert_eq!(args.get(position + 1).map(String::as_str), Some(expected));
    }

    #[test]
    fn configured_project_is_added_to_discovery_ssh_and_scp() {
        let client = GcpClient::with_project("configured-project".to_string());

        assert_project_arg(&client.list_instances_command("demo"), "configured-project");
        assert_project_arg(
            &client.get_ssh_command("demo-coordinator", "us-central1-a", "true"),
            "configured-project",
        );
        assert_project_arg(
            &client.scp_command(
                "/tmp/local",
                "/tmp/remote",
                "demo-worker-0",
                "us-central1-a",
            ),
            "configured-project",
        );
    }

    #[test]
    fn ambient_project_fallback_does_not_add_an_override() {
        let client = GcpClient::new();
        assert!(!args(&client.list_instances_command("demo"))
            .iter()
            .any(|arg| arg == "--project"));
    }

    #[test]
    fn test_custom_worker_and_stock_coordinator_use_separate_artifacts() {
        let config = PoolConfig {
            name: "demo".into(),
            worker_count: 1,
            machine_type: "e2-standard-2".into(),
            zone: "us-central1-a".into(),
            spot: true,
            project_id: "project".into(),
            network: None,
            subnet: None,
            public_ip: true,
            manage_firewall: true,
            with_coordinator: true,
            wireguard: None,
            pool_db_path: None,
            binary_gcs_url: Some("gs://bucket/stock-coordinator".into()),
            worker_binary_gcs_url: Some("gs://bucket/custom-worker".into()),
            service_account: None,
            coordinator_service_account: None,
        };

        let worker = super::super::startup::generate_worker_startup_script(
            config.worker_binary_gcs_url.as_deref(),
            &config.name,
        );
        let coordinator = super::super::startup::generate_coordinator_startup_script(
            None,
            config.binary_gcs_url.as_deref(),
            None,
        );
        assert!(worker.contains("gs://bucket/custom-worker"));
        assert!(!worker.contains("stock-coordinator"));
        assert!(coordinator.contains("gs://bucket/stock-coordinator"));
        assert!(!coordinator.contains("custom-worker"));
    }

    fn pool_config(public_ip: bool, manage_firewall: bool) -> PoolConfig {
        PoolConfig {
            name: "demo".into(),
            worker_count: 1,
            machine_type: "e2-standard-2".into(),
            zone: "us-central1-a".into(),
            spot: false,
            project_id: "project".into(),
            network: Some("network".into()),
            subnet: Some("subnet".into()),
            public_ip,
            manage_firewall,
            with_coordinator: true,
            wireguard: None,
            pool_db_path: None,
            binary_gcs_url: None,
            worker_binary_gcs_url: None,
            service_account: None,
            coordinator_service_account: None,
        }
    }

    fn instance_create_args_for(config: &PoolConfig, is_coordinator: bool) -> Vec<String> {
        let setup = InstanceSetup {
            name: if is_coordinator {
                "demo-coordinator".into()
            } else {
                "demo-worker-0".into()
            },
            machine_type: config.machine_type.clone(),
            zone: config.zone.clone(),
            tags: vec![],
            startup_script: "true".into(),
            spot: config.spot,
            network: config.network.clone(),
            subnet: config.subnet.clone(),
            public_ip: config.public_ip,
            project_id: config.project_id.clone(),
            service_account: GcpClient::instance_service_account(config, is_coordinator),
        };
        args(&GcpClient::instance_create_command(&setup))
    }

    #[test]
    fn legacy_service_account_is_attached_to_coordinator_and_worker_commands() {
        let mut config = pool_config(false, false);
        config.service_account = Some("legacy@project.iam.gserviceaccount.com".into());

        for is_coordinator in [true, false] {
            assert!(instance_create_args_for(&config, is_coordinator)
                .iter()
                .any(|arg| arg == "--service-account=legacy@project.iam.gserviceaccount.com"));
        }
    }

    #[test]
    fn coordinator_and_worker_commands_use_separate_service_accounts() {
        let mut config = pool_config(false, false);
        config.service_account = Some("worker@project.iam.gserviceaccount.com".into());
        config.coordinator_service_account =
            Some("coordinator@project.iam.gserviceaccount.com".into());

        let worker = instance_create_args_for(&config, false);
        assert!(worker
            .iter()
            .any(|arg| arg == "--service-account=worker@project.iam.gserviceaccount.com"));
        let coordinator = instance_create_args_for(&config, true);
        assert!(coordinator
            .iter()
            .any(|arg| arg == "--service-account=coordinator@project.iam.gserviceaccount.com"));
        assert!(!coordinator
            .iter()
            .any(|arg| arg == "--service-account=worker@project.iam.gserviceaccount.com"));
    }

    #[test]
    fn private_instances_use_no_address_for_create_and_scale_paths() {
        let config = pool_config(false, false);
        let setup = InstanceSetup {
            name: "demo-worker-0".into(),
            machine_type: config.machine_type.clone(),
            zone: config.zone.clone(),
            tags: vec!["genohype-worker".into()],
            startup_script: "true".into(),
            spot: false,
            network: config.network.clone(),
            subnet: config.subnet.clone(),
            public_ip: config.public_ip,
            project_id: config.project_id.clone(),
            service_account: None,
        };

        let command_args = args(&GcpClient::instance_create_command(&setup));
        assert!(command_args.iter().any(|arg| arg == "--no-address"));
        assert!(command_args
            .windows(2)
            .any(|args| args == ["--network", "network"]));
        assert!(command_args
            .windows(2)
            .any(|args| args == ["--subnet", "subnet"]));
    }

    #[test]
    fn preconfigured_networking_skips_firewall_command() {
        assert!(GcpClient::firewall_create_command(&pool_config(false, false)).is_none());
    }

    #[test]
    fn legacy_networking_defaults_keep_address_and_firewall_management() {
        let config = pool_config(true, true);
        let setup = InstanceSetup {
            name: "demo-worker-0".into(),
            machine_type: config.machine_type.clone(),
            zone: config.zone.clone(),
            tags: vec![],
            startup_script: "true".into(),
            spot: false,
            network: None,
            subnet: None,
            public_ip: config.public_ip,
            project_id: config.project_id.clone(),
            service_account: None,
        };
        assert!(!args(&GcpClient::instance_create_command(&setup))
            .iter()
            .any(|arg| arg == "--no-address"));
        assert!(GcpClient::firewall_create_command(&config).is_some());
    }

    #[test]
    fn test_instance_helpers() {
        let instance = Instance {
            name: "test-worker-0".to_string(),
            zone: "us-central1-a".to_string(),
            network_interfaces: vec![NetworkInterface {
                network_ip: "10.0.0.1".to_string(),
                network: None,
                subnetwork: None,
            }],
            status: "RUNNING".to_string(),
            machine_type: None,
            scheduling: None,
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
