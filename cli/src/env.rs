//! Environment configuration management (.genohype-env).
//!
//! Provides commands for initializing, showing, and verifying environment
//! configurations that tie together storage locations and ClickHouse instances.

use crate::config::{Config, HailEnv};
use crate::Result;
use owo_colors::OwoColorize;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

/// Initialize a new .genohype-env file.
pub fn init_env(
    _config: &Config,
    name: &str,
    storage: Option<&str>,
    clickhouse: Option<&str>,
) -> Result<()> {
    let env_path = PathBuf::from(".genohype-env");

    if env_path.exists() {
        return Err(genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            ".genohype-env already exists in current directory",
        )));
    }

    // Generate defaults if not provided
    let storage = storage
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("gs://YOUR_BUCKET/{}", name));

    let clickhouse = clickhouse
        .map(|s| s.to_string())
        .unwrap_or_else(|| "YOUR_CLICKHOUSE_INSTANCE".to_string());

    let content = format!(
        r#"# Environment configuration for genohype
# This file ties together storage location and ClickHouse instance.

name = "{}"
storage = "{}"
clickhouse = "{}"
"#,
        name, storage, clickhouse
    );

    std::fs::write(&env_path, &content)?;

    println!("{} Created .genohype-env", "OK".green());
    println!("\n{}", content);
    println!(
        "{} Edit the file to set your storage path and ClickHouse instance.",
        "Next:".cyan()
    );

    Ok(())
}

/// Show current environment configuration.
pub fn show_env(config: &Config) -> Result<()> {
    match HailEnv::load() {
        Some(env) => {
            println!("{} {}", "Environment:".green(), env.name.bright_white());
            println!("  Storage: {}", env.storage.cyan());

            if env.clickhouse_is_url() {
                println!("  ClickHouse: {} (URL)", env.clickhouse);
            } else {
                println!("  ClickHouse: {} (instance)", env.clickhouse.cyan());

                // Try to resolve instance to IP
                if let Some(project) = &config.defaults.project {
                    let zone = config.defaults.zone.as_deref().unwrap_or("us-central1-a");
                    let output = Command::new("gcloud")
                        .args([
                            "compute",
                            "instances",
                            "describe",
                            &env.clickhouse,
                            "--project",
                            project,
                            "--zone",
                            zone,
                            "--format",
                            "value(networkInterfaces[0].networkIP)",
                        ])
                        .output();

                    if let Ok(o) = output {
                        if o.status.success() {
                            let ip = String::from_utf8_lossy(&o.stdout).trim().to_string();
                            println!("    -> http://{}:8123", ip);
                        }
                    }
                }
            }

            // Show example output path
            let timestamp = "YYYYMMDD-HHMM";
            println!("\n{} {}", "Output dir:".cyan(), env.output_dir(timestamp));
        }
        None => {
            println!("{} No .genohype-env found", "Warning:".yellow());
            println!("\nCreate one with:");
            println!(
                "  genohype env init <name> --storage gs://bucket/prefix --clickhouse <instance>"
            );
        }
    }

    Ok(())
}

/// Verify environment connectivity.
pub fn verify_env(config: &Config) -> Result<()> {
    let env = HailEnv::load().ok_or_else(|| {
        genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No .genohype-env found. Run 'genohype env init' first.",
        ))
    })?;

    println!("Verifying environment '{}'...\n", env.name.cyan());

    // 1. Check GCS storage
    print!("  Storage {}... ", env.storage);
    std::io::stdout().flush().ok();

    let output = Command::new("gsutil").args(["ls", &env.storage]).output();

    match output {
        Ok(o) if o.status.success() => println!("{}", "OK".green()),
        Ok(_) => println!("{} (not accessible or doesn't exist)", "WARN".yellow()),
        Err(e) => println!("{} ({})", "ERROR".red(), e),
    }

    // 2. Check ClickHouse
    let clickhouse_url = if env.clickhouse_is_url() {
        env.clickhouse.clone()
    } else {
        // Resolve instance name to URL
        let project = config
            .defaults
            .project
            .as_ref()
            .ok_or_else(|| genohype_core::HailError::Config("No project in defaults".into()))?;
        let zone = config.defaults.zone.as_deref().unwrap_or("us-central1-a");

        let output = Command::new("gcloud")
            .args([
                "compute",
                "instances",
                "describe",
                &env.clickhouse,
                "--project",
                project,
                "--zone",
                zone,
                "--format",
                "value(networkInterfaces[0].networkIP)",
            ])
            .output()?;

        if !output.status.success() {
            println!(
                "  ClickHouse {}... {} (instance not found)",
                env.clickhouse,
                "FAIL".red()
            );
            return Ok(());
        }

        let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
        format!("http://{}:8123", ip)
    };

    print!("  ClickHouse {}... ", clickhouse_url);
    std::io::stdout().flush().ok();

    let output = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--connect-timeout",
            "5",
            &clickhouse_url,
        ])
        .output();

    match output {
        Ok(o) if o.status.success() => {
            let code = String::from_utf8_lossy(&o.stdout);
            if code.starts_with('2') {
                println!("{}", "OK".green());
            } else {
                println!("{} (HTTP {})", "WARN".yellow(), code);
            }
        }
        Ok(_) => println!("{}", "FAIL".red()),
        Err(e) => println!("{} ({})", "ERROR".red(), e),
    }

    Ok(())
}
