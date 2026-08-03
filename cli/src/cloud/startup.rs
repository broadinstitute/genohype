//! VM startup script generation for worker nodes.
//!
//! This module generates the bash script that runs on VM boot to install
//! dependencies required by the genohype binary.

use super::attestation::ExpectedWorkerBootstrap;
use super::WireGuardConfig;
use crate::{HailError, Result};

fn invalid_bootstrap(message: impl Into<String>) -> HailError {
    HailError::InvalidFormat(format!("invalid worker bootstrap: {}", message.into()))
}

/// Generate an integrity-bound custom-worker startup script.
///
/// The Ubuntu image is not assumed to contain `gsutil` or `gcloud`; the script
/// installs the Google Cloud CLI from Google's apt repository before using the
/// VM service account to download the private GCS object.
pub fn generate_attested_worker_startup_script(
    bootstrap: &ExpectedWorkerBootstrap,
    pool_name: &str,
) -> Result<String> {
    let staged = &bootstrap.artifact;
    let attestation = &staged.attestation;
    let safe_pool = !pool_name.is_empty()
        && pool_name.len() <= 63
        && pool_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !safe_pool {
        return Err(invalid_bootstrap(
            "pool name must contain only lowercase letters, digits, and hyphens",
        ));
    }
    if !staged.gcs_url.starts_with("gs://")
        || !staged.gcs_url.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'/' | b'.' | b'_' | b'-')
        })
    {
        return Err(invalid_bootstrap("artifact URL must be a safe gs:// URL"));
    }
    if bootstrap.deployment_id.is_empty()
        || !bootstrap
            .deployment_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(invalid_bootstrap(
            "deployment ID contains unsafe characters",
        ));
    }

    let marker = serde_json::json!({
        "schema_version": 1,
        "deployment_id": bootstrap.deployment_id,
        "artifact_url": staged.gcs_url,
        "sha256": attestation.sha256,
        "size": attestation.size,
        "revision": attestation.revision,
        "build_identity": attestation.build_identity,
        "build_version": attestation.build_version,
        "platform": attestation.platform,
        "arch": attestation.arch,
        "service_active": true,
    });

    Ok(format!(
        r#"#!/bin/bash
set -euo pipefail

readonly ARTIFACT_URL='{url}'
readonly EXPECTED_SHA256='{sha}'
readonly EXPECTED_SIZE='{size}'
readonly DEPLOYMENT_ID='{deployment}'
readonly INSTALL_PATH='/usr/local/bin/genohype'
readonly TEMP_PATH="${{INSTALL_PATH}}.${{DEPLOYMENT_ID}}.tmp"
readonly METADATA_PATH="/var/lib/genohype/deployments/${{DEPLOYMENT_ID}}.json"
trap 'rm -f "$TEMP_PATH" /var/lib/genohype/bootstrap-ready.json.tmp' EXIT

apt-get update -qq
apt-get install -y -qq libssl-dev ca-certificates curl gnupg apt-transport-https
if ! command -v gcloud >/dev/null 2>&1; then
  curl -fsSL --retry 5 https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor --yes -o /usr/share/keyrings/cloud.google.gpg
  echo 'deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main' \
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  apt-get update -qq
  apt-get install -y -qq google-cloud-cli
fi

mkdir -p /usr/local/bin /var/lib/genohype/deployments
rm -f "$TEMP_PATH" /tmp/genohype-ready /var/lib/genohype/bootstrap-ready.json
for attempt in 1 2 3 4 5 6 7 8; do
  if gcloud storage cp "$ARTIFACT_URL" "$TEMP_PATH"; then
    break
  fi
  rm -f "$TEMP_PATH"
  if [ "$attempt" = 8 ]; then
    echo 'custom worker download failed after 8 attempts' >&2
    exit 1
  fi
  sleep $((attempt * 3))
done

[ "$(stat -c %s "$TEMP_PATH")" = "$EXPECTED_SIZE" ]
printf '%s  %s\n' "$EXPECTED_SHA256" "$TEMP_PATH" | sha256sum --check --strict -
chmod 0755 "$TEMP_PATH"
chown root:root "$TEMP_PATH"
mv -f "$TEMP_PATH" "$INSTALL_PATH"

WORKER_ID=$(curl -fsS 'http://metadata.google.internal/computeMetadata/v1/instance/name' \
  -H 'Metadata-Flavor: Google')
cat > /etc/systemd/system/genohype-worker.service << SVCEOF
[Unit]
Description=Genohype Attested Custom Worker
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=root
Environment=GENOHYPE_WORKER_DEPLOYMENT_ID={deployment}
Environment=GENOHYPE_WORKER_ARTIFACT_SHA256={sha}
Environment=GENOHYPE_WORKER_BUILD_IDENTITY={build_identity}
Environment=GENOHYPE_WORKER_REVISION={revision}
ExecStart=/usr/local/bin/genohype service start-worker --url http://{pool}-coordinator:3000 --worker-id $WORKER_ID
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
SVCEOF
systemctl daemon-reload
systemctl enable --now genohype-worker
systemctl is-active --quiet genohype-worker

# The generation-scoped deployment record is create-only and read-only.
set -o noclobber
cat > "$METADATA_PATH" << 'MARKEREOF'
{marker}
MARKEREOF
set +o noclobber
chmod 0444 "$METADATA_PATH"
cp "$METADATA_PATH" /var/lib/genohype/bootstrap-ready.json.tmp
mv -f /var/lib/genohype/bootstrap-ready.json.tmp /var/lib/genohype/bootstrap-ready.json
ln -sfn /var/lib/genohype/bootstrap-ready.json /tmp/genohype-ready
trap - EXIT
"#,
        url = staged.gcs_url,
        sha = attestation.sha256,
        size = attestation.size,
        deployment = bootstrap.deployment_id,
        build_identity = attestation.build_identity,
        revision = attestation.revision,
        pool = pool_name,
        marker = marker,
    ))
}

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
    let binary_download = if let Some(binary_url) = binary_gcs_url {
        let download_cmd = if binary_url.starts_with("gs://") {
            format!("gsutil cp {} /tmp/genohype", binary_url)
        } else {
            // HTTP URL (e.g., from coordinator /api/binary) - retry since coordinator may still be booting
            format!(
                "curl -sL --retry 10 --retry-delay 5 --retry-all-errors -o /tmp/genohype {}",
                binary_url
            )
        };
        format!(
            r#"
# Download binary
echo "Downloading binary from {}..."
{}
chmod +x /tmp/genohype
mv /tmp/genohype /usr/local/bin/genohype
echo "Binary installed at /usr/local/bin/genohype"

# Create and start worker systemd service
WORKER_ID=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")
echo "Creating systemd service for worker $WORKER_ID connecting to {}-coordinator..."
cat > /etc/systemd/system/genohype-worker.service << 'SVCEOF'
[Unit]
Description=Genohype Worker
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-worker --url http://{}-coordinator:3000 --worker-id WORKER_ID_PLACEHOLDER
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
SVCEOF

# Replace placeholder with actual worker ID
sed -i "s/WORKER_ID_PLACEHOLDER/$WORKER_ID/" /etc/systemd/system/genohype-worker.service

systemctl daemon-reload
systemctl enable --now genohype-worker
echo "Worker service started via systemd"
"#,
            binary_url, download_cmd, pool_name, pool_name
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
/// Optional cluster configuration to pass to the coordinator startup.
pub struct CoordinatorClusterConfig<'a> {
    pub pool_name: Option<&'a str>,
    pub project: Option<&'a str>,
    pub zone: Option<&'a str>,
    pub machine_type: Option<&'a str>,
    pub spot: Option<bool>,
    pub network: Option<&'a str>,
    pub subnet: Option<&'a str>,
    pub public_ip: Option<bool>,
    pub manage_firewall: Option<bool>,
    pub worker_service_account: Option<&'a str>,
}

pub fn generate_coordinator_startup_script(
    wireguard: Option<&WireGuardConfig>,
    binary_gcs_url: Option<&str>,
    pool_db_path: Option<&str>,
) -> String {
    generate_coordinator_startup_script_with_cluster(wireguard, binary_gcs_url, pool_db_path, None)
}

pub fn generate_coordinator_startup_script_with_cluster(
    wireguard: Option<&WireGuardConfig>,
    binary_gcs_url: Option<&str>,
    pool_db_path: Option<&str>,
    cluster_config: Option<&CoordinatorClusterConfig>,
) -> String {
    let binary_download = if let Some(gcs_url) = binary_gcs_url {
        let db_arg = match pool_db_path {
            Some(path) => format!("--backup-path {}", path),
            None => String::new(),
        };

        // Build cluster flags
        let mut cluster_args = String::new();
        if let Some(cc) = cluster_config {
            if let Some(pool_name) = cc.pool_name {
                cluster_args.push_str(&format!(" --pool-name {}", pool_name));
            }
            if let Some(project) = cc.project {
                cluster_args.push_str(&format!(" --gcp-project {}", project));
            }
            if let Some(zone) = cc.zone {
                cluster_args.push_str(&format!(" --gcp-zone {}", zone));
            }
            if let Some(mt) = cc.machine_type {
                cluster_args.push_str(&format!(" --cluster-machine-type {}", mt));
            }
            if let Some(spot) = cc.spot {
                cluster_args.push_str(&format!(" --cluster-spot {}", spot));
            }
            if let Some(net) = cc.network {
                cluster_args.push_str(&format!(" --cluster-network {}", net));
            }
            if let Some(sub) = cc.subnet {
                cluster_args.push_str(&format!(" --cluster-subnet {}", sub));
            }
            if let Some(public_ip) = cc.public_ip {
                cluster_args.push_str(&format!(" --cluster-public-ip {}", public_ip));
            }
            if let Some(manage_firewall) = cc.manage_firewall {
                cluster_args.push_str(&format!(" --cluster-manage-firewall {}", manage_firewall));
            }
            if let Some(service_account) = cc.worker_service_account {
                cluster_args.push_str(&format!(
                    " --cluster-worker-service-account {}",
                    service_account
                ));
            }
        }

        format!(
            r#"
# Download binary from GCS (pre-staged). Minimal GCE images do not
# consistently include gsutil/google-cloud-cli, so install it fail-closed.
if ! command -v gcloud >/dev/null 2>&1; then
  curl -fsSL --retry 5 https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor --yes -o /usr/share/keyrings/cloud.google.gpg
  echo 'deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main' \
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  apt-get update -qq
  apt-get install -y -qq google-cloud-cli
fi
echo "Downloading binary from GCS..."
gcloud storage cp {} /tmp/genohype
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
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db {}{}
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
            gcs_url, db_arg, cluster_args
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
apt-get install -y -qq libssl-dev ca-certificates curl gnupg apt-transport-https
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
        wg_install, binary_download
    )
}

/// Generate a startup script with custom commands appended.
///
/// Useful for adding project-specific setup steps.
#[allow(dead_code)]
pub fn generate_startup_script_with_extras(extra_commands: &str) -> String {
    let mut script = generate_worker_startup_script(None, "default");
    // Remove the final echo and add custom commands before it
    script = script
        .trim_end_matches("echo \"=== Worker VM initialized ===\"\n")
        .to_string();
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
    fn attested_startup_verifies_before_atomic_install_and_marks_ready_last() {
        use crate::cloud::attestation::{
            ExpectedWorkerBootstrap, StagedWorkerArtifact, WorkerArtifactAttestation,
        };
        let revision = "a".repeat(40);
        let bootstrap = ExpectedWorkerBootstrap {
            artifact: StagedWorkerArtifact {
                gcs_url: format!(
                    "gs://bucket/pool/bin/custom-workers/{}/{}//genohype-worker",
                    revision,
                    "b".repeat(64)
                ),
                attestation: WorkerArtifactAttestation {
                    schema_version: 1,
                    sha256: "b".repeat(64),
                    size: 123,
                    revision: revision.clone(),
                    build_identity: format!("e2e:{revision}:fixture-a"),
                    build_version: format!("e2e:{revision}:fixture-a"),
                    platform: "linux".into(),
                    arch: "x86_64".into(),
                },
            },
            deployment_id: "deployment-1".into(),
        };
        let script = generate_attested_worker_startup_script(&bootstrap, "testpool").unwrap();
        assert!(script.contains("apt-get install -y -qq google-cloud-cli"));
        assert!(script.contains("gcloud storage cp \"$ARTIFACT_URL\" \"$TEMP_PATH\""));
        let verify = script.find("sha256sum --check --strict").unwrap();
        let install = script
            .find("mv -f \"$TEMP_PATH\" \"$INSTALL_PATH\"")
            .unwrap();
        let active = script
            .find("systemctl is-active --quiet genohype-worker")
            .unwrap();
        let marker = script
            .find("mv -f /var/lib/genohype/bootstrap-ready.json.tmp")
            .unwrap();
        assert!(verify < install && install < active && active < marker);
        assert!(script.contains("\"deployment_id\":\"deployment-1\""));
        assert!(script.contains("/var/lib/genohype/deployments/${DEPLOYMENT_ID}.json"));
        assert!(script.contains("chmod 0444 \"$METADATA_PATH\""));
        assert!(script.contains("Environment=GENOHYPE_WORKER_DEPLOYMENT_ID=deployment-1"));
        assert!(script.contains("\"arch\":\"x86_64\""));
        assert!(!script.contains("touch /tmp/genohype-ready"));
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
    fn test_coordinator_startup_script_propagates_cluster_project() {
        let cluster = CoordinatorClusterConfig {
            pool_name: Some("demo"),
            project: Some("configured-project"),
            zone: Some("us-central1-a"),
            machine_type: Some("e2-standard-2"),
            spot: Some(true),
            network: None,
            subnet: None,
            public_ip: Some(false),
            manage_firewall: Some(false),
            worker_service_account: Some("worker@project.iam.gserviceaccount.com"),
        };
        let script = generate_coordinator_startup_script_with_cluster(
            None,
            Some("gs://bucket/genohype"),
            None,
            Some(&cluster),
        );

        assert!(script.contains("--pool-name demo"));
        assert!(script.contains("--gcp-project configured-project"));
        assert!(script.contains("--gcp-zone us-central1-a"));
        assert!(script.contains("--cluster-public-ip false"));
        assert!(script.contains("--cluster-manage-firewall false"));
        assert!(script
            .contains("--cluster-worker-service-account worker@project.iam.gserviceaccount.com"));
    }

    #[test]
    fn test_coordinator_startup_script_with_binary_autostart() {
        let script = generate_coordinator_startup_script(
            None,
            Some("gs://bucket/path/genohype"),
            Some("gs://bucket/backup/ops.db"),
        );

        // Check binary download and auto-start
        assert!(script.contains("gcloud storage cp gs://bucket/path/genohype"));
        assert!(script.contains("apt-get install -y -qq google-cloud-cli"));
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
