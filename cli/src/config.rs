//! Configuration file support for genohype.
//!
//! Loads configuration from TOML files, supporting:
//! - Global defaults (project, zone, network)
//! - Export defaults (bigquery staging bucket, clickhouse URL)
//! - Named pool profiles with optional WireGuard configuration
//!
//! Configuration is loaded from (in order of precedence):
//! 1. Path specified via `--config` CLI flag
//! 2. `$XDG_CONFIG_HOME/genohype/config.toml`
//! 3. `~/.config/genohype/config.toml`
//! 4. `./genohype.toml` (current directory)

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// Root configuration structure.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct Config {
    /// Global default settings
    #[serde(default)]
    pub defaults: Defaults,

    /// Export command defaults
    #[allow(dead_code)]
    #[serde(default)]
    pub export: ExportDefaults,

    /// Cache settings for remote metadata
    #[serde(default)]
    pub cache: CacheConfig,

    /// Named pool profiles
    #[serde(default)]
    pub pools: HashMap<String, PoolProfile>,

    /// ClickHouse instance profiles
    #[serde(default)]
    pub clickhouse: HashMap<String, ClickHouseProfile>,

    /// Cluster configurations for multi-environment deployments (legacy)
    #[serde(default)]
    pub clusters: ClustersConfig,
}

/// Cache configuration for remote metadata files.
#[derive(Debug, Deserialize, Clone)]
pub struct CacheConfig {
    /// Cache TTL in hours (default: 24)
    #[serde(default = "default_ttl_hours")]
    pub ttl_hours: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_hours: default_ttl_hours(),
        }
    }
}

fn default_ttl_hours() -> u64 {
    24
}

/// Global default settings.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct Defaults {
    /// GCP project ID
    pub project: Option<String>,
    /// GCP zone
    pub zone: Option<String>,
    /// GCP region (e.g., "us-central1")
    pub region: Option<String>,
    /// Artifact Registry URL (e.g., "us-central1-docker.pkg.dev/project/repo")
    pub registry: Option<String>,
    /// VPC network name
    pub network: Option<String>,
    /// Subnet name
    pub subnet: Option<String>,
    /// SSH user (defaults to current user)
    pub ssh_user: Option<String>,
}

/// Export command defaults.
#[allow(dead_code)]
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ExportDefaults {
    /// BigQuery export settings
    #[serde(default)]
    pub bigquery: BigQueryDefaults,
    /// ClickHouse export settings
    #[serde(default)]
    pub clickhouse: ClickHouseDefaults,
}

/// BigQuery export defaults.
#[allow(dead_code)]
#[derive(Debug, Deserialize, Default, Clone)]
pub struct BigQueryDefaults {
    /// GCS bucket for staging parquet files
    pub staging_bucket: Option<String>,
    /// Dataset prefix for auto-generated table names
    pub dataset_prefix: Option<String>,
}

/// ClickHouse export defaults.
#[allow(dead_code)]
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ClickHouseDefaults {
    /// ClickHouse HTTP URL
    pub url: Option<String>,
    /// ClickHouse user
    pub user: Option<String>,
}

/// A named pool profile with machine and network configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct PoolProfile {
    /// GCP machine type (e.g., "n1-standard-4")
    pub machine_type: Option<String>,
    /// Number of workers to create initially (default: 0)
    pub starting_workers: Option<usize>,
    /// Target number of workers for autoscaling (default: 4)
    pub workers: Option<usize>,
    /// Use spot/preemptible instances
    pub spot: Option<bool>,
    /// GCP zone (overrides defaults)
    pub zone: Option<String>,
    /// VPC network name (overrides defaults)
    pub network: Option<String>,
    /// Subnet name (overrides defaults)
    pub subnet: Option<String>,
    /// Create a dedicated coordinator node
    pub with_coordinator: Option<bool>,
    /// WireGuard configuration for coordinator
    pub wireguard: Option<WireGuardConfig>,
    /// GCS path for SQLite database backup/restore (e.g., "gs://bucket/pool-ops/dev-pool/ops.db")
    pub pool_db_path: Option<String>,
    /// Use HTTP API for binary updates instead of SSH (requires IAP tunnel)
    pub update_via_api: Option<bool>,
    /// Port for API updates (default: 3000)
    pub update_api_port: Option<u16>,
    /// Path to a custom binary to deploy to workers instead of the coordinator binary
    pub worker_binary: Option<String>,
}

/// A named ClickHouse instance profile.
#[derive(Debug, Deserialize, Clone)]
pub struct ClickHouseProfile {
    /// GCP machine type (default: "n2-standard-8")
    pub machine_type: Option<String>,
    /// Boot disk size in GB (default: 200)
    pub disk_size_gb: Option<u32>,
    /// Boot disk type (default: "pd-ssd")
    pub disk_type: Option<String>,
    /// GCP zone (overrides defaults)
    pub zone: Option<String>,
    /// VPC network name (overrides defaults)
    pub network: Option<String>,
    /// Subnet name (overrides defaults)
    pub subnet: Option<String>,
    /// Service account email (e.g., "my-sa@project.iam.gserviceaccount.com")
    pub service_account: Option<String>,
    /// Use spot/preemptible instance
    pub spot: Option<bool>,
}

impl Default for ClickHouseProfile {
    fn default() -> Self {
        Self {
            machine_type: None,
            disk_size_gb: None,
            disk_type: None,
            zone: None,
            network: None,
            subnet: None,
            service_account: None,
            spot: None,
        }
    }
}

/// WireGuard VPN configuration for the coordinator node.
///
/// Values can be:
/// - Raw strings (not recommended for private keys)
/// - `secret:NAME` - fetched from Google Secret Manager at VM boot
/// - `env:VAR_NAME` - read from environment variable (source USB secrets.sh first)
///
/// # Example with Google Secret Manager
/// ```toml
/// [pools.dev.wireguard]
/// endpoint = "vpn.example.com:51820"
/// client_address = "10.10.0.50/32"
/// peer_public_key = "secret:wg-peer-pubkey"
/// client_private_key = "secret:wg-coord-privkey"
/// ```
///
/// # Example with environment variables (USB stick)
/// ```toml
/// [pools.dev.wireguard]
/// endpoint = "env:GCP_WG_ENDPOINT"
/// client_address = "env:GCP_WG_ADDRESS_1"
/// peer_public_key = "env:GCP_WG_PEER_PUBLIC_KEY"
/// client_private_key = "env:GCP_WG_PRIVATE_KEY_1"
/// ```
///
/// Then source your USB secrets before running:
/// ```bash
/// source /Volumes/USB_NAME/bootstrap/secrets.sh
/// genohype pool create dev --with-coordinator
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct WireGuardConfig {
    /// WireGuard server endpoint (host:port)
    pub endpoint: String,
    /// Client IP address with CIDR (e.g., "10.10.0.50/32")
    pub client_address: String,
    /// Allowed IPs to route through VPN (default: "10.10.0.0/24")
    #[serde(default = "default_allowed_ips")]
    pub allowed_ips: String,
    /// Server's public key (can be `secret:name` or `env:VAR`)
    pub peer_public_key: String,
    /// Client's private key (should be `secret:name` or `env:VAR`)
    pub client_private_key: String,
}

impl WireGuardConfig {
    /// Resolve all `env:VAR_NAME` references in the config.
    /// Returns a new config with environment variables expanded.
    /// Called at pool creation time (on the local machine).
    pub fn resolve_env_vars(&self) -> Result<WireGuardConfig, String> {
        Ok(WireGuardConfig {
            endpoint: resolve_env_value(&self.endpoint)?,
            client_address: resolve_env_value(&self.client_address)?,
            allowed_ips: resolve_env_value(&self.allowed_ips)?,
            peer_public_key: resolve_env_value(&self.peer_public_key)?,
            client_private_key: resolve_env_value(&self.client_private_key)?,
        })
    }
}

/// Resolve a value that may have an `env:` prefix.
/// - `env:VAR_NAME` -> reads from environment variable
/// - `secret:name` -> passed through (resolved at VM boot)
/// - other -> passed through as-is
pub fn resolve_env_value(value: &str) -> Result<String, String> {
    if let Some(var_name) = value.strip_prefix("env:") {
        std::env::var(var_name).map_err(|_| {
            format!(
                "Environment variable '{}' not set. Source your USB secrets.sh first.",
                var_name
            )
        })
    } else {
        Ok(value.to_string())
    }
}

fn default_allowed_ips() -> String {
    "10.10.0.0/24".to_string()
}

impl Config {
    /// Load configuration from the default search paths.
    ///
    /// Searches in order:
    /// 1. `$XDG_CONFIG_HOME/genohype/config.toml`
    /// 2. `~/.config/genohype/config.toml`
    /// 3. `./genohype.toml`
    ///
    /// Returns default config if no file is found.
    #[allow(dead_code)]
    pub fn load() -> Self {
        Self::load_from_path(None)
    }

    /// Load configuration from a specific path, or search default paths if None.
    ///
    /// Resolution order:
    /// 1. If explicit path provided, use it
    /// 2. Walk up from current dir looking for `genohype.toml` or `.genohype/config.toml`
    /// 3. Load user config from `~/.config/genohype/config.toml` and merge (project takes precedence)
    pub fn load_from_path(path: Option<&str>) -> Self {
        // If explicit path provided, use it
        if let Some(p) = path {
            return Self::load_file(p).unwrap_or_else(|e| {
                eprintln!("Warning: Failed to load config from {}: {}", p, e);
                Config::default()
            });
        }

        // 1. Try to find project config by walking up directories
        let mut config = Self::find_project_config()
            .and_then(|p| Self::load_file(p.to_str().unwrap()).ok())
            .unwrap_or_default();

        // 2. Try to load user config
        let user_paths = [
            dirs::home_dir().map(|h| h.join(".config").join("genohype").join("config.toml")),
            dirs::config_dir().map(|c| c.join("genohype").join("config.toml")),
        ];

        let user_config = user_paths
            .into_iter()
            .flatten()
            .find(|p| p.exists())
            .and_then(|p| {
                let path_str = p.to_str().unwrap();
                match Self::load_file(path_str) {
                    Ok(cfg) => Some(cfg),
                    Err(e) => {
                        eprintln!("Warning: Failed to parse {}: {}", path_str, e);
                        None
                    }
                }
            });

        // 3. Merge user config into project config (project takes precedence for shared keys)
        if let Some(user) = user_config {
            // Defaults: project overrides user
            if config.defaults.project.is_none() {
                config.defaults.project = user.defaults.project;
            }
            if config.defaults.zone.is_none() {
                config.defaults.zone = user.defaults.zone;
            }
            if config.defaults.region.is_none() {
                config.defaults.region = user.defaults.region;
            }
            if config.defaults.registry.is_none() {
                config.defaults.registry = user.defaults.registry;
            }
            if config.defaults.network.is_none() {
                config.defaults.network = user.defaults.network;
            }
            if config.defaults.subnet.is_none() {
                config.defaults.subnet = user.defaults.subnet;
            }
            if config.defaults.ssh_user.is_none() {
                config.defaults.ssh_user = user.defaults.ssh_user;
            }

            // Merge pools (project entries override user entries with same name)
            for (k, v) in user.pools {
                config.pools.entry(k).or_insert(v);
            }

            // Merge clusters (project entries override user entries with same name)
            for (k, v) in user.clusters.clusters {
                config.clusters.clusters.entry(k).or_insert(v);
            }
            if config.clusters.default.is_none() {
                config.clusters.default = user.clusters.default;
            }
        }

        config
    }

    /// Find project config by walking up from current directory.
    fn find_project_config() -> Option<PathBuf> {
        let mut dir = std::env::current_dir().ok()?;
        loop {
            let candidates = [
                dir.join("genohype.toml"),
                dir.join(".genohype").join("config.toml"),
            ];
            for candidate in candidates {
                if candidate.exists() {
                    return Some(candidate);
                }
            }
            if !dir.pop() {
                break;
            }
        }
        None
    }

    /// Get the list of paths to search for config files.
    #[allow(dead_code)]
    fn config_search_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        // ~/.config/genohype/config.toml (Linux/macOS convention)
        if let Some(home) = dirs::home_dir() {
            paths.push(home.join(".config").join("genohype").join("config.toml"));
        }

        // XDG config home (~/Library/Application Support on macOS)
        if let Some(config_dir) = dirs::config_dir() {
            paths.push(config_dir.join("genohype").join("config.toml"));
        }

        // Current directory
        paths.push(PathBuf::from("genohype.toml"));

        paths
    }

    /// Load configuration from a specific file path.
    fn load_file(path: &str) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read file: {}", e))?;
        toml::from_str(&content).map_err(|e| format!("Failed to parse TOML: {}", e))
    }

    /// Get a pool profile by name, with defaults applied.
    pub fn get_pool(&self, name: &str) -> Option<ResolvedPoolConfig> {
        self.pools.get(name).map(|profile| {
            // Auto-enable coordinator if WireGuard is configured
            let with_coordinator = profile.with_coordinator.unwrap_or(false)
                || profile.wireguard.is_some();

            ResolvedPoolConfig {
                name: name.to_string(),
                machine_type: profile
                    .machine_type
                    .clone()
                    .unwrap_or_else(|| "n1-standard-4".to_string()),
                starting_workers: profile.starting_workers.unwrap_or(0),
                workers: profile.workers.unwrap_or(4),
                spot: profile.spot.unwrap_or(false),
                zone: profile
                    .zone
                    .clone()
                    .or_else(|| self.defaults.zone.clone())
                    .unwrap_or_else(|| "us-central1-a".to_string()),
                network: profile.network.clone().or_else(|| self.defaults.network.clone()),
                subnet: profile.subnet.clone().or_else(|| self.defaults.subnet.clone()),
                project: self.defaults.project.clone(),
                with_coordinator,
                wireguard: profile.wireguard.clone(),
                pool_db_path: profile.pool_db_path.clone(),
                update_via_api: profile.update_via_api.unwrap_or(false),
                update_api_port: profile.update_api_port.unwrap_or(3000),
                worker_binary: profile.worker_binary.clone(),
            }
        })
    }

    /// Get a cluster configuration by name, or the default if name is None.
    pub fn get_cluster(&self, name: Option<&str>) -> Option<&ClusterConfig> {
        let cluster_name = name.or(self.clusters.default.as_deref())?;
        self.clusters.clusters.get(cluster_name)
    }

    /// Get a ClickHouse profile by name, with defaults applied.
    pub fn get_clickhouse(&self, name: &str) -> ResolvedClickHouseConfig {
        let profile = self.clickhouse.get(name);
        ResolvedClickHouseConfig {
            name: name.to_string(),
            machine_type: profile
                .and_then(|p| p.machine_type.clone())
                .unwrap_or_else(|| "n2-standard-8".to_string()),
            disk_size_gb: profile.and_then(|p| p.disk_size_gb).unwrap_or(200),
            disk_type: profile
                .and_then(|p| p.disk_type.clone())
                .unwrap_or_else(|| "pd-ssd".to_string()),
            zone: profile
                .and_then(|p| p.zone.clone())
                .or_else(|| self.defaults.zone.clone())
                .unwrap_or_else(|| "us-central1-a".to_string()),
            network: profile
                .and_then(|p| p.network.clone())
                .or_else(|| self.defaults.network.clone()),
            subnet: profile
                .and_then(|p| p.subnet.clone())
                .or_else(|| self.defaults.subnet.clone()),
            project: self.defaults.project.clone(),
            service_account: profile.and_then(|p| p.service_account.clone()),
            spot: profile.and_then(|p| p.spot).unwrap_or(false),
        }
    }
}

/// A fully resolved ClickHouse instance configuration.
#[derive(Debug, Clone)]
pub struct ResolvedClickHouseConfig {
    /// Instance name
    pub name: String,
    /// GCP machine type
    pub machine_type: String,
    /// Boot disk size in GB
    pub disk_size_gb: u32,
    /// Boot disk type
    pub disk_type: String,
    /// GCP zone
    pub zone: String,
    /// VPC network
    pub network: Option<String>,
    /// Subnet
    pub subnet: Option<String>,
    /// GCP project
    pub project: Option<String>,
    /// Service account email
    pub service_account: Option<String>,
    /// Use spot instance
    pub spot: bool,
}

/// A fully resolved pool configuration with all defaults applied.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ResolvedPoolConfig {
    /// Pool name
    pub name: String,
    /// Machine type
    pub machine_type: String,
    /// Number of workers to create initially (default: 0)
    pub starting_workers: usize,
    /// Target number of workers for autoscaling (default: 4)
    pub workers: usize,
    /// Use spot instances
    pub spot: bool,
    /// GCP zone
    pub zone: String,
    /// VPC network
    pub network: Option<String>,
    /// Subnet
    pub subnet: Option<String>,
    /// GCP project (may need CLI override)
    pub project: Option<String>,
    /// Create a dedicated coordinator node
    pub with_coordinator: bool,
    /// WireGuard configuration
    pub wireguard: Option<WireGuardConfig>,
    /// GCS path for SQLite database backup/restore
    pub pool_db_path: Option<String>,
    /// Use HTTP API for binary updates instead of SSH
    pub update_via_api: bool,
    /// Port for API updates
    pub update_api_port: u16,
    /// Path to a custom binary to deploy to workers instead of the coordinator binary
    pub worker_binary: Option<String>,
}

/// Status of a cluster deployment.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClusterStatus {
    /// Active production cluster
    Active,
    /// Standby cluster (ready but not primary)
    #[default]
    Standby,
    /// Deprecated cluster (scheduled for removal)
    Deprecated,
}

/// Configuration for a single cluster deployment target.
#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    /// Human-readable description of this cluster
    pub description: Option<String>,
    /// Cluster lifecycle status
    #[serde(default)]
    pub status: ClusterStatus,
    /// ClickHouse VM instance name (for SSH/management)
    pub clickhouse_vm: Option<String>,
    /// ClickHouse HTTP URL (e.g., "http://10.0.0.5:8123")
    pub clickhouse_url: String,
    /// Cloud Run backend service name
    pub backend_service: Option<String>,
    /// Cloud Run frontend service name
    pub frontend_service: Option<String>,
    /// GCS bucket for output data
    pub output_bucket: String,
    /// Path prefix within the output bucket
    pub output_prefix: String,
}

impl ClusterConfig {
    /// Generate a timestamped output directory path.
    pub fn output_dir(&self, timestamp: &str) -> String {
        let prefix = self.output_prefix.trim_matches('/');
        let bucket = self.output_bucket.trim_end_matches('/');
        format!("{}/{}/analyses/{}", bucket, prefix, timestamp)
    }

    /// Resolve environment variable references in the config.
    pub fn resolve_env_vars(&self) -> Result<ClusterConfig, String> {
        Ok(ClusterConfig {
            clickhouse_url: resolve_env_value(&self.clickhouse_url)?,
            ..self.clone()
        })
    }
}

/// Container for cluster configurations with an optional default.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ClustersConfig {
    /// Name of the default cluster to use when --cluster is not specified
    pub default: Option<String>,
    /// Named cluster configurations
    #[serde(flatten)]
    pub clusters: HashMap<String, ClusterConfig>,
}

// =============================================================================
// Environment Configuration (.genohype-env)
// =============================================================================

/// Environment configuration loaded from `.genohype-env` file.
///
/// This file ties together storage location and ClickHouse instance for a
/// specific working environment. It's auto-discovered by walking up from
/// the current directory.
///
/// # Example
/// ```toml
/// name = "20260303"
/// storage = "gs://axaou-central/browserv2/20260303"
/// clickhouse = "my-ch"  # instance name or URL
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct HailEnv {
    /// Environment name (e.g., "20260303", "dev", "prod")
    pub name: String,
    /// GCS storage path for outputs (e.g., "gs://bucket/prefix")
    pub storage: String,
    /// ClickHouse instance name or URL
    /// - If it looks like a URL (starts with http), use directly
    /// - Otherwise, resolve as instance name to get internal IP
    pub clickhouse: String,
}

impl HailEnv {
    /// Load .genohype-env from current directory or walk up to find it.
    pub fn load() -> Option<Self> {
        Self::find_env_file().and_then(|path| Self::load_file(&path).ok())
    }

    /// Load .genohype-env from a specific path.
    pub fn load_file(path: &PathBuf) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read .genohype-env: {}", e))?;
        toml::from_str(&content).map_err(|e| format!("Failed to parse .genohype-env: {}", e))
    }

    /// Find .genohype-env by walking up from current directory.
    fn find_env_file() -> Option<PathBuf> {
        let mut dir = std::env::current_dir().ok()?;
        loop {
            let candidate = dir.join(".genohype-env");
            if candidate.exists() {
                return Some(candidate);
            }
            if !dir.pop() {
                break;
            }
        }
        None
    }

    /// Check if clickhouse field is a URL or an instance name.
    pub fn clickhouse_is_url(&self) -> bool {
        self.clickhouse.starts_with("http://") || self.clickhouse.starts_with("https://")
    }

    /// Get the ClickHouse URL.
    /// If clickhouse is already a URL, return it directly.
    /// Otherwise, this returns None and caller should resolve instance name to IP.
    #[allow(dead_code)]
    pub fn clickhouse_url(&self) -> Option<&str> {
        if self.clickhouse_is_url() {
            Some(&self.clickhouse)
        } else {
            None
        }
    }

    /// Get the ClickHouse instance name (if not a URL).
    #[allow(dead_code)]
    pub fn clickhouse_instance(&self) -> Option<&str> {
        if self.clickhouse_is_url() {
            None
        } else {
            Some(&self.clickhouse)
        }
    }

    /// Generate output directory path with timestamp.
    pub fn output_dir(&self, timestamp: &str) -> String {
        let storage = self.storage.trim_end_matches('/');
        format!("{}/analyses/{}", storage, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.pools.is_empty());
        assert!(config.defaults.project.is_none());
    }

    #[test]
    fn test_parse_config() {
        let toml = r#"
[defaults]
project = "my-project"
zone = "us-central1-a"

[pools.dev]
machine_type = "n1-standard-4"
workers = 2
spot = true

[pools.dev.wireguard]
endpoint = "vpn.example.com:51820"
client_address = "10.10.0.50/32"
peer_public_key = "secret:wg-peer-pubkey"
client_private_key = "secret:wg-coord-privkey"
"#;
        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.defaults.project, Some("my-project".to_string()));
        assert_eq!(config.defaults.zone, Some("us-central1-a".to_string()));

        let dev = config.pools.get("dev").unwrap();
        assert_eq!(dev.machine_type, Some("n1-standard-4".to_string()));
        assert_eq!(dev.workers, Some(2));
        assert_eq!(dev.spot, Some(true));

        let wg = dev.wireguard.as_ref().unwrap();
        assert_eq!(wg.endpoint, "vpn.example.com:51820");
        assert_eq!(wg.client_address, "10.10.0.50/32");
        assert_eq!(wg.peer_public_key, "secret:wg-peer-pubkey");
        assert_eq!(wg.client_private_key, "secret:wg-coord-privkey");
        assert_eq!(wg.allowed_ips, "10.10.0.0/24"); // default
    }

    #[test]
    fn test_resolved_pool_config() {
        let toml = r#"
[defaults]
project = "my-project"
zone = "us-east1-b"

[pools.test]
starting_workers = 2
workers = 8
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let resolved = config.get_pool("test").unwrap();

        assert_eq!(resolved.name, "test");
        assert_eq!(resolved.machine_type, "n1-standard-4"); // default
        assert_eq!(resolved.starting_workers, 2);
        assert_eq!(resolved.workers, 8); // autoscale target
        assert!(!resolved.spot); // default
        assert_eq!(resolved.zone, "us-east1-b"); // from defaults
        assert_eq!(resolved.project, Some("my-project".to_string()));
    }

    #[test]
    fn test_resolved_pool_config_defaults() {
        let toml = r#"
[pools.minimal]
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let resolved = config.get_pool("minimal").unwrap();

        assert_eq!(resolved.starting_workers, 0); // default: coordinator-only
        assert_eq!(resolved.workers, 4); // default autoscale target
    }

    #[test]
    fn test_wireguard_resolve_env_vars() {
        // Set test environment variables
        std::env::set_var("TEST_WG_ENDPOINT", "vpn.test.com:51820");
        std::env::set_var("TEST_WG_PRIVKEY", "test-private-key");

        let wg = WireGuardConfig {
            endpoint: "env:TEST_WG_ENDPOINT".to_string(),
            client_address: "10.10.0.50/32".to_string(), // raw value
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "raw-public-key".to_string(), // raw value
            client_private_key: "env:TEST_WG_PRIVKEY".to_string(),
        };

        let resolved = wg.resolve_env_vars().unwrap();

        assert_eq!(resolved.endpoint, "vpn.test.com:51820");
        assert_eq!(resolved.client_address, "10.10.0.50/32");
        assert_eq!(resolved.peer_public_key, "raw-public-key");
        assert_eq!(resolved.client_private_key, "test-private-key");

        // Clean up
        std::env::remove_var("TEST_WG_ENDPOINT");
        std::env::remove_var("TEST_WG_PRIVKEY");
    }

    #[test]
    fn test_wireguard_missing_env_var() {
        let wg = WireGuardConfig {
            endpoint: "env:NONEXISTENT_VAR_12345".to_string(),
            client_address: "10.10.0.50/32".to_string(),
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "key".to_string(),
            client_private_key: "key".to_string(),
        };

        let result = wg.resolve_env_vars();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("NONEXISTENT_VAR_12345"));
    }

    #[test]
    fn test_wireguard_secret_prefix_passthrough() {
        let wg = WireGuardConfig {
            endpoint: "vpn.example.com:51820".to_string(),
            client_address: "10.10.0.50/32".to_string(),
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "secret:wg-peer-pubkey".to_string(),
            client_private_key: "secret:wg-privkey".to_string(),
        };

        // secret: prefix should pass through unchanged (resolved at VM boot)
        let resolved = wg.resolve_env_vars().unwrap();
        assert_eq!(resolved.peer_public_key, "secret:wg-peer-pubkey");
        assert_eq!(resolved.client_private_key, "secret:wg-privkey");
    }
}
