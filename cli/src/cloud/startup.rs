//! VM startup script generation for worker nodes.
//!
//! This module generates the bash script that runs on VM boot to install
//! dependencies required by the genohype binary.

use super::WireGuardConfig;

/// Generate the startup script for worker VMs.
///
/// The script installs minimal dependencies needed to run the Rust binary:
/// - libssl-dev (for TLS/HTTPS support)
/// - ca-certificates (for certificate verification)
/// - curl (for health checks and API calls)
///
/// If `binary_gcs_url` is provided, the binary is downloaded from GCS during startup
/// and the worker service is automatically started, connecting to the coordinator
/// via internal DNS (`{pool_name}-coordinator:3000`). This achieves "zero-SSH"
/// deployment for distributed jobs.
pub fn generate_worker_startup_script(binary_gcs_url: Option<&str>, pool_name: &str) -> String {
    let binary_download = if let Some(gcs_url) = binary_gcs_url {
        format!(
            r#"
# Download binary from GCS (pre-staged)
echo "Downloading binary from GCS..."
gsutil cp {} /tmp/genohype
chmod +x /tmp/genohype
mv /tmp/genohype /usr/local/bin/genohype
echo "Binary installed at /usr/local/bin/genohype"

# Create and start worker systemd service
WORKER_ID=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")
echo "Creating systemd service for worker $WORKER_ID connecting to {}-coordinator..."
cat > /etc/systemd/system/genohype-worker.service << 'SVCEOF'
[Unit]
Description=Genohype Worker
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-worker --url http://{}-coordinator:3000 --worker-id WORKER_ID_PLACEHOLDER
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
SVCEOF

# Replace placeholder with actual worker ID
sed -i "s/WORKER_ID_PLACEHOLDER/$WORKER_ID/" /etc/systemd/system/genohype-worker.service

systemctl daemon-reload
systemctl enable --now genohype-worker
echo "Worker service started via systemd"
"#,
            gcs_url, pool_name, pool_name
        )
    } else {
        String::new()
    };

    format!(
        r#"#!/bin/bash
set -e

# Log startup
echo "=== genohype worker VM startup ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Install dependencies required for the Rust binary
apt-get update -qq
apt-get install -y -qq libssl-dev ca-certificates curl

# Create directory for the binary
mkdir -p /usr/local/bin

# Create directory for the active SQLite database
mkdir -p /var/lib/genohype
chmod 777 /var/lib/genohype
{}
# Create a marker file to indicate VM is ready
touch /tmp/genohype-ready

echo "=== Worker VM initialized ==="
"#,
        binary_download
    )
}

/// Generate the startup script for the coordinator VM with optional WireGuard VPN.
///
/// This script includes:
/// - All worker dependencies
/// - WireGuard installation and configuration (if provided)
/// - Secret resolution via Google Secret Manager (for `secret:` prefixed values)
/// - Binary download from GCS (if `binary_gcs_url` is provided)
/// - Auto-start coordinator service (if binary is provided)
///
/// # Security
/// The `secret:` prefix pattern allows storing sensitive values (like private keys)
/// in Google Secret Manager. The VM uses its service account identity to fetch
/// secrets at boot time, avoiding exposure in metadata or logs.
pub fn generate_coordinator_startup_script(
    wireguard: Option<&WireGuardConfig>,
    binary_gcs_url: Option<&str>,
    pool_db_path: Option<&str>,
) -> String {
    let binary_download = if let Some(gcs_url) = binary_gcs_url {
        let db_arg = match pool_db_path {
            Some(path) => format!("--backup-path {}", path),
            None => String::new(),
        };

        format!(
            r#"
# Download binary from GCS (pre-staged)
echo "Downloading binary from GCS..."
gsutil cp {} /tmp/genohype
chmod +x /tmp/genohype
mv /tmp/genohype /usr/local/bin/genohype
echo "Binary installed at /usr/local/bin/genohype"

# Create and start coordinator systemd service
echo "Creating systemd service for coordinator..."
cat > /etc/systemd/system/genohype-coordinator.service << 'SVCEOF'
[Unit]
Description=Genohype Coordinator
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db {}
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable --now genohype-coordinator
echo "Coordinator service started via systemd"
"#,
            gcs_url, db_arg
        )
    } else {
        String::new()
    };

    let wg_install = if let Some(wg) = wireguard {
        format!(
            r#"
# Install WireGuard
echo "Installing WireGuard..."
apt-get install -y -qq wireguard

# Resolve WireGuard configuration values
WG_PEER_PUBKEY=$(resolve_secret "{}")
WG_PRIVATE_KEY=$(resolve_secret "{}")

# Create WireGuard configuration
echo "Configuring WireGuard..."
cat > /etc/wireguard/wg0.conf << WGEOF
[Interface]
PrivateKey = $WG_PRIVATE_KEY
Address = {}

[Peer]
PublicKey = $WG_PEER_PUBKEY
Endpoint = {}
AllowedIPs = {}
PersistentKeepalive = 25
WGEOF

# Secure the config file
chmod 600 /etc/wireguard/wg0.conf

# Enable and start WireGuard
echo "Starting WireGuard..."
systemctl enable wg-quick@wg0
systemctl start wg-quick@wg0

# Verify WireGuard is running
if wg show wg0 > /dev/null 2>&1; then
    echo "WireGuard interface wg0 is up"
    wg show wg0
else
    echo "WARNING: WireGuard interface wg0 failed to start"
fi
"#,
            wg.peer_public_key,
            wg.client_private_key,
            wg.client_address,
            wg.endpoint,
            wg.allowed_ips
        )
    } else {
        String::new()
    };

    format!(
        r#"#!/bin/bash
set -e

# Log startup
echo "=== genohype coordinator VM startup ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Helper function to resolve secret references
# Values prefixed with "secret:" are fetched from Google Secret Manager
resolve_secret() {{
    local val="$1"
    if [[ "$val" == secret:* ]]; then
        local secret_name="${{val#secret:}}"
        echo "Resolving secret: $secret_name" >&2
        gcloud secrets versions access latest --secret="$secret_name" --quiet
    else
        echo "$val"
    fi
}}

# Install dependencies required for the Rust binary
apt-get update -qq
apt-get install -y -qq libssl-dev ca-certificates curl
{}

# Create directory for the binary
mkdir -p /usr/local/bin

# Create directory for the active SQLite database
mkdir -p /var/lib/genohype
chmod 777 /var/lib/genohype
{}

# Create a marker file to indicate VM is ready
touch /tmp/genohype-ready

echo "=== Coordinator VM initialized ==="
"#,
        wg_install,
        binary_download
    )
}

/// Generate a startup script with custom commands appended.
///
/// Useful for adding project-specific setup steps.
#[allow(dead_code)]
pub fn generate_startup_script_with_extras(extra_commands: &str) -> String {
    let mut script = generate_worker_startup_script(None, "default");
    // Remove the final echo and add custom commands before it
    script = script.trim_end_matches("echo \"=== Worker VM initialized ===\"\n").to_string();
    script.push_str("\n# Custom setup\n");
    script.push_str(extra_commands);
    script.push_str("\necho \"=== Worker VM initialized ===\"\n");
    script
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_script_contains_essentials() {
        let script = generate_worker_startup_script(None, "testpool");
        assert!(script.contains("#!/bin/bash"));
        assert!(script.contains("apt-get"));
        assert!(script.contains("libssl-dev"));
        assert!(script.contains("ca-certificates"));
        assert!(script.contains("/usr/local/bin"));
        assert!(script.contains("genohype-ready"));
    }

    #[test]
    fn test_startup_script_with_binary_url() {
        let script = generate_worker_startup_script(Some("gs://bucket/path/genohype"), "testpool");
        assert!(script.contains("gsutil cp gs://bucket/path/genohype"));
        assert!(script.contains("/usr/local/bin/genohype"));
        assert!(script.contains("start-worker"));
        assert!(script.contains("testpool-coordinator:3000"));
        // Check systemd service creation
        assert!(script.contains("genohype-worker.service"));
        assert!(script.contains("Restart=always"));
        assert!(script.contains("systemctl daemon-reload"));
        assert!(script.contains("systemctl enable --now genohype-worker"));
    }

    #[test]
    fn test_startup_script_with_extras() {
        let script = generate_startup_script_with_extras("echo 'Custom step'");
        assert!(script.contains("Custom step"));
        assert!(script.contains("# Custom setup"));
    }

    #[test]
    fn test_coordinator_startup_script_with_wireguard() {
        let wg_config = WireGuardConfig {
            endpoint: "vpn.example.com:51820".to_string(),
            client_address: "10.10.0.50/32".to_string(),
            allowed_ips: "10.10.0.0/24".to_string(),
            peer_public_key: "secret:wg-peer-pubkey".to_string(),
            client_private_key: "secret:wg-coord-privkey".to_string(),
        };

        let script = generate_coordinator_startup_script(Some(&wg_config), None, None);

        // Check essential components
        assert!(script.contains("#!/bin/bash"));
        assert!(script.contains("apt-get install -y -qq wireguard"));
        assert!(script.contains("resolve_secret"));
        assert!(script.contains("/etc/wireguard/wg0.conf"));
        assert!(script.contains("systemctl enable wg-quick@wg0"));
        assert!(script.contains("systemctl start wg-quick@wg0"));

        // Check config values are embedded
        assert!(script.contains("vpn.example.com:51820"));
        assert!(script.contains("10.10.0.50/32"));
        assert!(script.contains("10.10.0.0/24"));
        assert!(script.contains("secret:wg-peer-pubkey"));
        assert!(script.contains("secret:wg-coord-privkey"));

        // Check it still has worker essentials
        assert!(script.contains("libssl-dev"));
        assert!(script.contains("genohype-ready"));
    }

    #[test]
    fn test_coordinator_startup_script_with_binary_autostart() {
        let script = generate_coordinator_startup_script(
            None,
            Some("gs://bucket/path/genohype"),
            Some("gs://bucket/backup/ops.db"),
        );

        // Check binary download and auto-start
        assert!(script.contains("gsutil cp gs://bucket/path/genohype"));
        assert!(script.contains("start-coordinator"));
        assert!(script.contains("--port 3000"));
        assert!(script.contains("--backup-path gs://bucket/backup/ops.db"));
        // Check systemd service creation
        assert!(script.contains("genohype-coordinator.service"));
        assert!(script.contains("Restart=always"));
        assert!(script.contains("systemctl daemon-reload"));
        assert!(script.contains("systemctl enable --now genohype-coordinator"));

        // No WireGuard
        assert!(!script.contains("wireguard"));
    }

    #[test]
    fn test_coordinator_startup_script_no_wireguard_no_binary() {
        let script = generate_coordinator_startup_script(None, None, None);

        // Basic essentials only
        assert!(script.contains("#!/bin/bash"));
        assert!(script.contains("libssl-dev"));
        assert!(script.contains("genohype-ready"));

        // No WireGuard, no auto-start
        assert!(!script.contains("wireguard"));
        assert!(!script.contains("start-coordinator"));
    }
}
