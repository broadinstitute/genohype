//! ClickHouse instance management.
//!
//! Provides commands for creating, listing, and destroying ClickHouse
//! instances on GCP. Similar to pool management but for database VMs.

use crate::config::Config;
use crate::Result;
use owo_colors::OwoColorize;
use std::process::Command;

/// Create a new ClickHouse instance.
pub fn create_instance(
    config: &Config,
    name: &str,
    profile: Option<&str>,
    machine_type_override: Option<&str>,
    disk_size_override: Option<u32>,
    zone_override: Option<&str>,
) -> Result<()> {
    // Start with profile or defaults
    let profile_name = profile.unwrap_or("default");
    let mut resolved = config.get_clickhouse(profile_name);
    resolved.name = name.to_string();

    // Apply CLI overrides
    if let Some(mt) = machine_type_override {
        resolved.machine_type = mt.to_string();
    }
    if let Some(ds) = disk_size_override {
        resolved.disk_size_gb = ds;
    }
    if let Some(z) = zone_override {
        resolved.zone = z.to_string();
    }

    let project = resolved.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    println!(
        "{} Creating ClickHouse instance '{}'",
        "ClickHouse:".green().bold(),
        name.cyan()
    );
    println!("  Machine type: {}", resolved.machine_type);
    println!(
        "  Disk: {} GB ({})",
        resolved.disk_size_gb, resolved.disk_type
    );
    println!("  Zone: {}", resolved.zone);
    if resolved.spot {
        println!("  Spot: {}", "yes".yellow());
    }

    // Build gcloud command
    let mut args = vec![
        "compute".to_string(),
        "instances".to_string(),
        "create".to_string(),
        name.to_string(),
        "--project".to_string(),
        project.clone(),
        "--zone".to_string(),
        resolved.zone.clone(),
        "--machine-type".to_string(),
        resolved.machine_type.clone(),
        "--boot-disk-size".to_string(),
        format!("{}GB", resolved.disk_size_gb),
        "--boot-disk-type".to_string(),
        resolved.disk_type.clone(),
        "--image-family".to_string(),
        "debian-12".to_string(),
        "--image-project".to_string(),
        "debian-cloud".to_string(),
        "--scopes".to_string(),
        "cloud-platform".to_string(),
        "--labels".to_string(),
        "purpose=clickhouse".to_string(),
    ];

    // Add network/subnet if specified
    if let Some(ref network) = resolved.network {
        args.push("--network".to_string());
        args.push(network.clone());
    }
    if let Some(ref subnet) = resolved.subnet {
        args.push("--subnet".to_string());
        args.push(subnet.clone());
    }

    // Add spot flag
    if resolved.spot {
        args.push("--provisioning-model".to_string());
        args.push("SPOT".to_string());
    }

    // Add service account if specified
    if let Some(ref sa) = resolved.service_account {
        args.push("--service-account".to_string());
        args.push(sa.clone());
    }

    // Add startup script to install ClickHouse
    let startup_script = r#"#!/bin/bash
set -e

# Install ClickHouse
apt-get update
apt-get install -y apt-transport-https ca-certificates curl gnupg

curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-server clickhouse-client

# Configure to listen on all interfaces
sed -i 's/<!-- <listen_host>::<\/listen_host> -->/<listen_host>::<\/listen_host>/' /etc/clickhouse-server/config.xml

# Start ClickHouse
systemctl enable clickhouse-server
systemctl start clickhouse-server

echo "ClickHouse installation complete"
"#;

    args.push("--metadata".to_string());
    args.push(format!("startup-script={}", startup_script));

    // Run gcloud
    let status = Command::new("gcloud").args(&args).status()?;

    if !status.success() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to create ClickHouse instance",
        )));
    }

    // Get internal IP
    let ip = get_internal_ip(project, name, &resolved.zone)?;
    println!(
        "\n{} Instance created: {}",
        "OK".green(),
        name.bright_white()
    );
    println!("  Internal IP: {}", ip.cyan());
    println!("  ClickHouse URL: http://{}:8123", ip);
    println!(
        "\n{} ClickHouse will be available in ~2-3 minutes.",
        "Note:".cyan()
    );
    println!("  Check status: genohype clickhouse show {}", name);

    Ok(())
}

/// List all ClickHouse instances.
pub fn list_instances(config: &Config) -> Result<()> {
    let project = config.defaults.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    println!("{}", "ClickHouse instances:\n".bold());

    let output = Command::new("gcloud")
        .args([
            "compute",
            "instances",
            "list",
            "--project",
            project,
            "--filter",
            "labels.purpose=clickhouse",
            "--format",
            "table(name,zone.basename(),machineType.basename(),networkInterfaces[0].networkIP,status)",
        ])
        .output()?;

    if output.status.success() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    } else {
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    }

    Ok(())
}

/// Show details of a ClickHouse instance.
pub fn show_instance(config: &Config, name: &str) -> Result<()> {
    let project = config.defaults.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    // First, find the zone by listing instances
    let zone = find_instance_zone(project, name)?;

    // Get instance details
    let output = Command::new("gcloud")
        .args([
            "compute",
            "instances",
            "describe",
            name,
            "--project",
            project,
            "--zone",
            &zone,
            "--format",
            "yaml(name,zone,machineType,networkInterfaces[0].networkIP,status,disks[0].diskSizeGb)",
        ])
        .output()?;

    if !output.status.success() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Instance '{}' not found", name),
        )));
    }

    let details = String::from_utf8_lossy(&output.stdout);

    // Parse basic info
    let mut ip = String::new();
    for line in details.lines() {
        if line.contains("networkIP:") {
            ip = line.split(':').nth(1).unwrap_or("").trim().to_string();
            break;
        }
    }

    println!("{} {}", "ClickHouse Instance:".green(), name.bright_white());
    println!("\n{}", details);

    if !ip.is_empty() {
        println!("{}", "Connection:".cyan());
        println!("  URL: http://{}:8123", ip);

        // Check if ClickHouse is responding
        print!("  Status: ");
        std::io::Write::flush(&mut std::io::stdout()).ok();

        let check = Command::new("curl")
            .args([
                "-s",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                "--connect-timeout",
                "2",
                &format!("http://{}:8123", ip),
            ])
            .output();

        match check {
            Ok(o) if o.status.success() => {
                let code = String::from_utf8_lossy(&o.stdout);
                if code.starts_with('2') {
                    println!("{}", "responding".green());
                } else {
                    println!("{} (HTTP {})", "not ready".yellow(), code);
                }
            }
            _ => println!("{}", "not reachable".red()),
        }
    }

    Ok(())
}

/// Destroy a ClickHouse instance.
pub fn destroy_instance(config: &Config, name: &str, skip_confirm: bool) -> Result<()> {
    let project = config.defaults.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    // Find the zone
    let zone = find_instance_zone(project, name)?;

    if !skip_confirm {
        println!(
            "{} This will permanently delete ClickHouse instance '{}'",
            "Warning:".red().bold(),
            name.cyan()
        );
        print!("Continue? [y/N] ");
        std::io::Write::flush(&mut std::io::stdout()).ok();

        let mut input = String::new();
        std::io::stdin().read_line(&mut input).ok();
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    println!("Deleting instance '{}'...", name.cyan());

    let status = Command::new("gcloud")
        .args([
            "compute",
            "instances",
            "delete",
            name,
            "--project",
            project,
            "--zone",
            &zone,
            "--quiet",
        ])
        .status()?;

    if status.success() {
        println!("{} Instance '{}' deleted", "OK".green(), name);
    } else {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to delete instance",
        )));
    }

    Ok(())
}

/// Get the internal IP of a ClickHouse instance.
pub fn get_instance_ip(config: &Config, name: &str) -> Result<()> {
    let project = config.defaults.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    let zone = find_instance_zone(project, name)?;
    let ip = get_internal_ip(project, name, &zone)?;
    println!("{}", ip);

    Ok(())
}

/// SSH into a ClickHouse instance.
pub fn ssh_instance(config: &Config, name: &str, command: &[String]) -> Result<()> {
    let project = config.defaults.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    let zone = find_instance_zone(project, name)?;

    let mut args = vec![
        "compute".to_string(),
        "ssh".to_string(),
        name.to_string(),
        "--project".to_string(),
        project.clone(),
        "--zone".to_string(),
        zone.to_string(),
    ];

    if !command.is_empty() {
        args.push("--command".to_string());
        args.push(command.join(" "));
    }

    let status = Command::new("gcloud").args(&args).status()?;

    if !status.success() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "SSH command failed",
        )));
    }

    Ok(())
}

/// Create an SSH tunnel to forward ClickHouse port to localhost.
pub fn tunnel_instance(config: &Config, name: &str, local_port: u16) -> Result<()> {
    let project = config.defaults.project.as_ref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    let zone = find_instance_zone(project, name)?;

    println!(
        "{} Tunneling to ClickHouse instance '{}'",
        "Tunnel:".green().bold(),
        name.cyan()
    );
    println!("  Local:  http://localhost:{}", local_port);
    println!("  Remote: localhost:8123 on {}", name);
    println!("\n{} Press Ctrl+C to stop the tunnel\n", "Note:".cyan());

    let status = Command::new("gcloud")
        .args([
            "compute",
            "ssh",
            name,
            "--project",
            project,
            "--zone",
            &zone,
            "--tunnel-through-iap",
            "--",
            "-L",
            &format!("{}:localhost:8123", local_port),
            "-N",
        ])
        .status()?;

    if !status.success() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "SSH tunnel failed",
        )));
    }

    Ok(())
}

// Helper functions

/// Find the zone of an instance by name.
fn find_instance_zone(project: &str, name: &str) -> Result<String> {
    let output = Command::new("gcloud")
        .args([
            "compute",
            "instances",
            "list",
            "--project",
            project,
            "--filter",
            &format!("name={}", name),
            "--format",
            "value(zone)",
        ])
        .output()?;

    if !output.status.success() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Instance '{}' not found", name),
        )));
    }

    let zone = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if zone.is_empty() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Instance '{}' not found", name),
        )));
    }

    // Extract just the zone name from the full path
    let zone = zone.rsplit('/').next().unwrap_or(&zone).to_string();
    Ok(zone)
}

fn get_internal_ip(project: &str, name: &str, zone: &str) -> Result<String> {
    let output = Command::new("gcloud")
        .args([
            "compute",
            "instances",
            "describe",
            name,
            "--project",
            project,
            "--zone",
            zone,
            "--format",
            "value(networkInterfaces[0].networkIP)",
        ])
        .output()?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not get instance IP",
        )))
    }
}
