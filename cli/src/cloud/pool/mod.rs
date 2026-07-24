//! Worker pool management for distributed processing.
//!
//! This module provides the `PoolManager` which orchestrates:
//! - Creating worker VMs
//! - Deploying the genohype binary
//! - Submitting distributed jobs
//! - Streaming logs and aggregating metrics
//! - Cleaning up resources

pub mod binary;
pub mod client;
pub mod lifecycle;
pub mod parser;
pub mod submit;

// Re-export helper types
pub use submit::{list_completed_markers, read_completed_checkpoint, WorkerMessage};

use crate::cloud::CloudProvider;

/// Manages distributed worker pools for parallel processing.
pub struct PoolManager<P: CloudProvider> {
    pub(crate) provider: P,
}

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Create a new pool manager with the given cloud provider.
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    pub(crate) fn coordinator_identity_args(&self, pool_name: &str, zone: &str) -> String {
        let mut args = format!(" --pool-name {} --gcp-zone {}", pool_name, zone);
        if let Some(project) = self.provider.project_id() {
            args.push_str(&format!(" --gcp-project {}", project));
        }
        args
    }

    pub(crate) fn coordinator_scaling_args(
        &self,
        pool_name: &str,
        zone: &str,
        config: Option<&crate::cloud::ScalingConfig>,
    ) -> String {
        let mut args = self.coordinator_identity_args(pool_name, zone);
        if let Some(config) = config {
            args.push_str(&format!(
                " --cluster-machine-type {} --cluster-spot {} --cluster-public-ip {} --cluster-manage-firewall {}",
                config.machine_type, config.spot, config.public_ip, config.manage_firewall
            ));
            if let Some(network) = &config.network {
                args.push_str(&format!(" --cluster-network {}", network));
            }
            if let Some(subnet) = &config.subnet {
                args.push_str(&format!(" --cluster-subnet {}", subnet));
            }
        }
        args
    }

    pub(crate) fn coordinator_pool_args(&self, config: &crate::cloud::PoolConfig) -> String {
        let mut args = self.coordinator_identity_args(&config.name, &config.zone);
        args.push_str(&format!(
            " --cluster-machine-type {} --cluster-spot {} --cluster-public-ip {} --cluster-manage-firewall {}",
            config.machine_type, config.spot, config.public_ip, config.manage_firewall
        ));
        if let Some(network) = &config.network {
            args.push_str(&format!(" --cluster-network {}", network));
        }
        if let Some(subnet) = &config.subnet {
            args.push_str(&format!(" --cluster-subnet {}", subnet));
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::{Instance, PoolConfig};
    use crate::Result;
    use std::path::Path;

    #[test]
    fn test_locate_binary_not_found() {
        struct MockProvider;
        impl CloudProvider for MockProvider {
            fn create_pool(&self, _: &PoolConfig) -> Result<()> {
                Ok(())
            }
            fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
                Ok(vec![])
            }
            fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn create_instances(&self, _: &[super::super::InstanceSetup]) -> Result<()> {
                Ok(())
            }
            fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn get_ssh_command(&self, _: &str, _: &str, _: &str) -> std::process::Command {
                std::process::Command::new("echo")
            }
        }

        let manager = PoolManager::new(MockProvider);
        let result = manager.locate_binary(Some("/nonexistent/path".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn coordinator_service_rewrites_preserve_private_networking() {
        struct ProjectProvider;
        impl CloudProvider for ProjectProvider {
            fn project_id(&self) -> Option<&str> {
                Some("project")
            }
            fn create_pool(&self, _: &PoolConfig) -> Result<()> {
                Ok(())
            }
            fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
                Ok(vec![])
            }
            fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn create_instances(&self, _: &[super::super::InstanceSetup]) -> Result<()> {
                Ok(())
            }
            fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn get_ssh_command(&self, _: &str, _: &str, _: &str) -> std::process::Command {
                std::process::Command::new("echo")
            }
        }

        let config = crate::cloud::ScalingConfig {
            machine_type: "e2-standard-2".into(),
            workers: 2,
            spot: true,
            network: Some("vpc".into()),
            subnet: Some("subnet".into()),
            public_ip: false,
            manage_firewall: false,
            project: Some("project".into()),
            with_coordinator: true,
            pool_db_path: None,
            worker_binary: None,
            service_account: None,
        };
        let args = PoolManager::new(ProjectProvider).coordinator_scaling_args(
            "demo",
            "us-central1-a",
            Some(&config),
        );
        assert!(args.contains("--cluster-public-ip false"));
        assert!(args.contains("--cluster-manage-firewall false"));
        assert!(args.contains("--cluster-network vpc"));
        assert!(args.contains("--cluster-subnet subnet"));
    }

    #[test]
    fn worker_service_install_enables_restarts_and_verifies() {
        #[derive(Clone)]
        struct RecordingProvider(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
        impl CloudProvider for RecordingProvider {
            fn create_pool(&self, _: &PoolConfig) -> Result<()> {
                Ok(())
            }
            fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
                Ok(vec![])
            }
            fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn create_instances(&self, _: &[super::super::InstanceSetup]) -> Result<()> {
                Ok(())
            }
            fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn get_ssh_command(&self, _: &str, _: &str, command: &str) -> std::process::Command {
                self.0.lock().unwrap().push(command.to_string());
                let mut cmd = std::process::Command::new("sh");
                cmd.args(["-c", "true"]);
                cmd
            }
        }

        let commands = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let manager = PoolManager::new(RecordingProvider(commands.clone()));
        let worker = Instance {
            name: "demo-worker-0".into(),
            zone: "us-central1-a".into(),
            network_interfaces: vec![],
            status: "RUNNING".into(),
            machine_type: None,
            scheduling: None,
        };
        manager
            .start_worker_services(&[worker], "10.0.0.2", "us-central1-a")
            .unwrap();

        let command = commands.lock().unwrap().join("\n");
        assert!(command.contains("systemctl enable genohype-worker"));
        assert!(command.contains("systemctl restart genohype-worker"));
        assert!(command.contains("systemctl is-active --quiet genohype-worker"));
    }
}
