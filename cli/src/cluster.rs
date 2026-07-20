//! Cluster management operations.
//!
//! Provides commands for listing, inspecting, verifying, and deploying
//! to cluster configurations defined in the config file.

use crate::config::{ClusterStatus, Config};
use crate::Result;
use owo_colors::OwoColorize;
use std::process::Command;

/// List all configured clusters with optional status filtering.
pub fn list_clusters(config: &Config, status_filter: Option<&String>) -> Result<()> {
    println!("{}", "Available clusters:\n".bold());

    let filter = status_filter.map(|s| match s.as_str() {
        "active" => ClusterStatus::Active,
        "deprecated" => ClusterStatus::Deprecated,
        _ => ClusterStatus::Standby,
    });

    let default = config.clusters.default.as_deref();

    if config.clusters.clusters.is_empty() {
        println!("  No clusters configured.");
        println!(
            "\n  Add clusters to your {} or project config file.",
            "~/.config/genohype/config.toml".cyan()
        );
        return Ok(());
    }

    for (name, cluster) in &config.clusters.clusters {
        if let Some(ref f) = filter {
            if &cluster.status != f {
                continue;
            }
        }

        let is_default = default == Some(name.as_str());

        let status_display = match cluster.status {
            ClusterStatus::Active => format!("{}", "active".green()),
            ClusterStatus::Standby => format!("{}", "standby".yellow()),
            ClusterStatus::Deprecated => format!("{}", "deprecated".red()),
        };

        println!(
            "  {}{} [{}] - {}",
            name.cyan(),
            if is_default { " (default)" } else { "" },
            status_display,
            cluster.description.as_deref().unwrap_or("")
        );
    }

    Ok(())
}

/// Show detailed information about a specific cluster.
pub fn show_cluster(config: &Config, name: &str) -> Result<()> {
    let cluster = config.get_cluster(Some(name)).ok_or_else(|| {
        genohype_core::HailError::InvalidFormat(format!("Unknown cluster: {}", name))
    })?;

    println!("{} {}", "Cluster:".green(), name.bright_white());

    if let Some(desc) = &cluster.description {
        println!("  Description: {}", desc);
    }
    println!("  Status: {:?}", cluster.status);

    println!("\n  {}", "ClickHouse:".cyan());
    if let Some(vm) = &cluster.clickhouse_vm {
        println!("    VM:  {}", vm);
    }
    println!("    URL: {}", cluster.clickhouse_url);

    println!("\n  {}", "Cloud Run:".cyan());
    if let Some(backend) = &cluster.backend_service {
        println!("    Backend:  {}", backend);
    }
    if let Some(frontend) = &cluster.frontend_service {
        println!("    Frontend: {}", frontend);
    }

    println!("\n  {}", "Storage:".cyan());
    println!("    Bucket: {}", cluster.output_bucket);
    println!("    Prefix: {}", cluster.output_prefix);

    Ok(())
}

/// Verify cluster connectivity and configuration.
pub fn verify_cluster(config: &Config, name: &str) -> Result<()> {
    let cluster = config
        .get_cluster(Some(name))
        .ok_or_else(|| {
            genohype_core::HailError::InvalidFormat(format!("Unknown cluster: {}", name))
        })?
        .resolve_env_vars()
        .map_err(genohype_core::HailError::Config)?;

    println!("Verifying cluster '{}'...\n", name.cyan());

    // 1. Check ClickHouse connectivity (using curl to avoid reqwest dependency)
    print!("  ClickHouse at {}... ", cluster.clickhouse_url);
    std::io::Write::flush(&mut std::io::stdout()).ok();

    let output = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--connect-timeout",
            "5",
            &cluster.clickhouse_url,
        ])
        .output();

    match output {
        Ok(o) if o.status.success() => {
            let status_code = String::from_utf8_lossy(&o.stdout);
            if status_code.starts_with('2') {
                println!("{}", "OK".green());
            } else {
                println!("{} (HTTP {})", "WARN".yellow(), status_code);
            }
        }
        Ok(_) => println!("{}", "FAIL".red()),
        Err(e) => println!("{} ({})", "ERROR".red(), e),
    }

    // 2. Check Backend Service (if configured)
    if let Some(backend) = &cluster.backend_service {
        print!("  Backend service '{}'... ", backend);
        std::io::Write::flush(&mut std::io::stdout()).ok();

        if let Some(project) = &config.defaults.project {
            let region = config.defaults.region.as_deref().unwrap_or("us-central1");
            let output = Command::new("gcloud")
                .args([
                    "run",
                    "services",
                    "describe",
                    backend,
                    "--project",
                    project,
                    "--region",
                    region,
                    "--format=value(status.url)",
                ])
                .output();

            match output {
                Ok(o) if o.status.success() => {
                    let url = String::from_utf8_lossy(&o.stdout);
                    println!("{} ({})", "OK".green(), url.trim());
                }
                Ok(_) => println!("{}", "NOT FOUND".red()),
                Err(e) => println!("{} ({})", "ERROR".red(), e),
            }
        } else {
            println!("{} (no project in defaults)", "SKIP".yellow());
        }
    }

    // 3. Check Frontend Service (if configured)
    if let Some(frontend) = &cluster.frontend_service {
        print!("  Frontend service '{}'... ", frontend);
        std::io::Write::flush(&mut std::io::stdout()).ok();

        if let Some(project) = &config.defaults.project {
            let region = config.defaults.region.as_deref().unwrap_or("us-central1");
            let output = Command::new("gcloud")
                .args([
                    "run",
                    "services",
                    "describe",
                    frontend,
                    "--project",
                    project,
                    "--region",
                    region,
                    "--format=value(status.url)",
                ])
                .output();

            match output {
                Ok(o) if o.status.success() => {
                    let url = String::from_utf8_lossy(&o.stdout);
                    println!("{} ({})", "OK".green(), url.trim());
                }
                Ok(_) => println!("{}", "NOT FOUND".red()),
                Err(e) => println!("{} ({})", "ERROR".red(), e),
            }
        } else {
            println!("{} (no project in defaults)", "SKIP".yellow());
        }
    }

    // 4. Check GCS bucket
    print!("  GCS bucket {}... ", cluster.output_bucket);
    std::io::Write::flush(&mut std::io::stdout()).ok();

    let output = Command::new("gsutil")
        .args(["ls", &cluster.output_bucket])
        .output();

    match output {
        Ok(o) if o.status.success() => println!("{}", "OK".green()),
        Ok(_) => println!("{}", "NOT ACCESSIBLE".red()),
        Err(e) => println!("{} ({})", "ERROR".red(), e),
    }

    Ok(())
}

/// Deploy Cloud Run services to a cluster.
pub fn deploy_cluster(config: &Config, name: &str, tag: &str, backend_only: bool) -> Result<()> {
    let cluster = config
        .get_cluster(Some(name))
        .ok_or_else(|| {
            genohype_core::HailError::InvalidFormat(format!("Unknown cluster: {}", name))
        })?
        .resolve_env_vars()
        .map_err(genohype_core::HailError::Config)?;

    let project = config.defaults.project.as_deref().ok_or_else(|| {
        genohype_core::HailError::Config("No project configured in defaults".into())
    })?;

    let region = config.defaults.region.as_deref().unwrap_or("us-central1");
    let registry = config
        .defaults
        .registry
        .as_deref()
        .unwrap_or("us-central1-docker.pkg.dev");

    // Standardize registry URL formatting
    let full_registry = if registry.contains('/') {
        registry.to_string()
    } else {
        format!("{}/{}", registry, project)
    };

    // Deploy backend service
    if let Some(backend) = &cluster.backend_service {
        println!(
            "Deploying {} with CLICKHOUSE_URL={}",
            backend.cyan(),
            cluster.clickhouse_url
        );

        let image = format!("{}/axaou-backend:{}", full_registry, tag);
        let env_var = format!("CLICKHOUSE_URL={}", cluster.clickhouse_url);

        let status = Command::new("gcloud")
            .args([
                "run",
                "deploy",
                backend,
                "--image",
                &image,
                "--set-env-vars",
                &env_var,
                "--region",
                region,
                "--project",
                project,
                "--quiet",
            ])
            .status()?;

        if !status.success() {
            return Err(genohype_core::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Backend deployment failed",
            )));
        }
        println!("{} Backend deployed: {}", "OK".green(), backend);
    } else {
        println!("{} No backend service configured", "SKIP".yellow());
    }

    // Deploy frontend service (unless --backend-only)
    if !backend_only {
        if let Some(frontend) = &cluster.frontend_service {
            println!("Deploying {}...", frontend.cyan());

            let image = format!("{}/axaou-frontend:{}", full_registry, tag);

            let status = Command::new("gcloud")
                .args([
                    "run",
                    "deploy",
                    frontend,
                    "--image",
                    &image,
                    "--region",
                    region,
                    "--project",
                    project,
                    "--quiet",
                ])
                .status()?;

            if !status.success() {
                return Err(genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Frontend deployment failed",
                )));
            }
            println!("{} Frontend deployed: {}", "OK".green(), frontend);
        } else {
            println!("{} No frontend service configured", "SKIP".yellow());
        }
    }

    Ok(())
}
