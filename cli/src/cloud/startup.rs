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
///
/// The script is designed to be minimal and fast, avoiding unnecessary packages.
pub fn generate_startup_script() -> String {
    r#"#!/bin/bash
set -e

# Log startup
echo "=== genohype worker VM startup ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Install dependencies required for the Rust binary
apt-get update -qq
apt-get install -y -qq libssl-dev ca-certificates

# Create directory for the binary
mkdir -p /usr/local/bin

# Create directory for the active SQLite database
mkdir -p /var/lib/genohype
chmod 777 /var/lib/genohype

# Create a marker file to indicate VM is ready
touch /tmp/genohype-ready

echo "=== Worker VM initialized ==="
"#
    .to_string()
}

/// Generate the startup script for the coordinator VM with WireGuard VPN.
///
/// This script includes:
/// - All worker dependencies
/// - WireGuard installation and configuration
/// - Secret resolution via Google Secret Manager (for `secret:` prefixed values)
///
/// # Security
/// The `secret:` prefix pattern allows storing sensitive values (like private keys)
/// in Google Secret Manager. The VM uses its service account identity to fetch
/// secrets at boot time, avoiding exposure in metadata or logs.
pub fn generate_coordinator_startup_script(wireguard: &WireGuardConfig) -> String {
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
apt-get install -y -qq libssl-dev ca-certificates

# Install WireGuard
echo "Installing WireGuard..."
apt-get install -y -qq wireguard

# Resolve WireGuard configuration values
WG_PEER_PUBKEY=$(resolve_secret "{peer_public_key}")
WG_PRIVATE_KEY=$(resolve_secret "{client_private_key}")

# Create WireGuard configuration
echo "Configuring WireGuard..."
cat > /etc/wireguard/wg0.conf << WGEOF
[Interface]
PrivateKey = $WG_PRIVATE_KEY
Address = {client_address}

[Peer]
PublicKey = $WG_PEER_PUBKEY
Endpoint = {endpoint}
AllowedIPs = {allowed_ips}
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

# Create directory for the binary
mkdir -p /usr/local/bin

# Create directory for the active SQLite database
mkdir -p /var/lib/genohype
chmod 777 /var/lib/genohype

# Create a marker file to indicate VM is ready
touch /tmp/genohype-ready

echo "=== Coordinator VM initialized ==="
"#,
        peer_public_key = wireguard.peer_public_key,
        client_private_key = wireguard.client_private_key,
        client_address = wireguard.client_address,
        endpoint = wireguard.endpoint,
        allowed_ips = wireguard.allowed_ips,
    )
}

/// Generate a startup script with custom commands appended.
///
/// Useful for adding project-specific setup steps.
#[allow(dead_code)]
pub fn generate_startup_script_with_extras(extra_commands: &str) -> String {
    let mut script = generate_startup_script();
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
        let script = generate_startup_script();
        assert!(script.contains("#!/bin/bash"));
        assert!(script.contains("apt-get"));
        assert!(script.contains("libssl-dev"));
        assert!(script.contains("ca-certificates"));
        assert!(script.contains("/usr/local/bin"));
        assert!(script.contains("genohype-ready"));
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

        let script = generate_coordinator_startup_script(&wg_config);

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
}
