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
pub use submit::{read_completed_checkpoint, WorkerMessage};

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
            fn get_ssh_command(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> std::process::Command {
                std::process::Command::new("echo")
            }
        }

        let manager = PoolManager::new(MockProvider);
        let result = manager.locate_binary(Some("/nonexistent/path".to_string()));
        assert!(result.is_err());
    }
}
