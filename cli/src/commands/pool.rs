//! Pool management commands for distributed processing.

use crate::cli::PoolCommands;
use crate::cloud::gcp::GcpClient;
use crate::cloud::pool::PoolManager;
use crate::cloud::PoolConfig;
use crate::config;
use genohype_core::Result;
use owo_colors::OwoColorize;

/// Resolve zone: CLI arg > pool profile > config defaults > fallback
fn resolve_zone(zone: Option<String>, pool_name: &str, app_config: &config::Config) -> String {
    zone.or_else(|| app_config.get_pool(pool_name).map(|p| p.zone.clone()))
        .or_else(|| app_config.defaults.zone.clone())
        .unwrap_or_else(|| "us-central1-a".to_string())
}

fn resolve_worker_binary(
    cli: Option<String>,
    profile: Option<&config::ResolvedPoolConfig>,
) -> Option<String> {
    cli.or_else(|| profile.and_then(|p| p.worker_binary.clone()))
}

fn resolve_service_accounts(
    worker: Option<String>,
    coordinator: Option<String>,
    profile: Option<&config::ResolvedPoolConfig>,
) -> (Option<String>, Option<String>) {
    let worker = worker.or_else(|| profile.and_then(|p| p.service_account.clone()));
    let coordinator = coordinator
        .or_else(|| profile.and_then(|p| p.coordinator_service_account.clone()))
        .or_else(|| worker.clone());
    (worker, coordinator)
}

fn command_pool_name_and_project(command: &PoolCommands) -> (&str, Option<&str>) {
    match command {
        PoolCommands::Create { name, project, .. } => (name, project.as_deref()),
        PoolCommands::Submit { name, .. }
        | PoolCommands::Scale { name, .. }
        | PoolCommands::Destroy { name, .. }
        | PoolCommands::List { name }
        | PoolCommands::Status { name, .. }
        | PoolCommands::UpdateBinary { name, .. }
        | PoolCommands::Cancel { name, .. }
        | PoolCommands::Workers { name, .. }
        | PoolCommands::Events { name, .. }
        | PoolCommands::Failures { name, .. }
        | PoolCommands::Logs { name, .. } => (name, None),
    }
}

fn configured_project(
    cli_project: Option<&str>,
    pool_name: &str,
    app_config: &config::Config,
) -> Option<String> {
    cli_project
        .map(String::from)
        .or_else(|| app_config.get_pool(pool_name).and_then(|p| p.project))
        .or_else(|| app_config.defaults.project.clone())
}

/// Run pool management commands
pub fn run_pool_command(command: PoolCommands, app_config: &config::Config) -> Result<()> {
    // Resolve the project once for the whole command. GcpClient then adds it to
    // discovery, SSH/SCP, and lifecycle calls so an operation cannot switch to
    // a different ambient gcloud project after provisioning.
    let (pool_name, cli_project) = command_pool_name_and_project(&command);
    let project_id = configured_project(cli_project, pool_name, app_config)
        .map(Ok)
        .unwrap_or_else(|| GcpClient::new().get_current_project())?;
    let manager = PoolManager::new(GcpClient::with_project(project_id.clone()));

    match command {
        PoolCommands::Create {
            name,
            workers,
            machine_type,
            zone,
            spot,
            project: _,
            network,
            subnet,
            public_ip,
            manage_firewall,
            wait,
            skip_build,
            worker_binary,
            with_coordinator,
            service_account,
            coordinator_service_account,
        } => {
            // Try to load pool profile from config (if exists)
            let profile = app_config.get_pool(&name);

            // Resolve values: CLI args > profile > defaults
            // starting_workers defaults to 0 (coordinator-only pool)
            let resolved_workers = workers
                .or_else(|| profile.as_ref().map(|p| p.starting_workers))
                .unwrap_or(0);
            let resolved_machine_type = machine_type
                .or_else(|| profile.as_ref().map(|p| p.machine_type.clone()))
                .unwrap_or_else(|| "n1-standard-4".to_string());
            let resolved_zone = resolve_zone(zone, &name, app_config);
            let resolved_spot = spot
                .or_else(|| profile.as_ref().map(|p| p.spot))
                .unwrap_or(false);
            let resolved_network = network
                .or_else(|| profile.as_ref().and_then(|p| p.network.clone()))
                .or_else(|| app_config.defaults.network.clone());
            let resolved_subnet = subnet
                .or_else(|| profile.as_ref().and_then(|p| p.subnet.clone()))
                .or_else(|| app_config.defaults.subnet.clone());
            let resolved_public_ip = public_ip
                .or_else(|| profile.as_ref().map(|p| p.public_ip))
                .or(app_config.defaults.public_ip)
                .unwrap_or(true);
            let resolved_manage_firewall = manage_firewall
                .or_else(|| profile.as_ref().map(|p| p.manage_firewall))
                .or(app_config.defaults.manage_firewall)
                .unwrap_or(true);

            // Convert WireGuard config from config module to cloud module
            // Resolve env: prefixes (for USB-sourced secrets) at this point
            let wireguard = match profile.as_ref().and_then(|p| p.wireguard.as_ref()) {
                Some(wg) => {
                    // Resolve env:VAR_NAME references from environment
                    let resolved = wg.resolve_env_vars().map_err(|e| {
                        genohype_core::HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            e,
                        ))
                    })?;
                    Some(crate::cloud::WireGuardConfig {
                        endpoint: resolved.endpoint,
                        client_address: resolved.client_address,
                        allowed_ips: resolved.allowed_ips,
                        peer_public_key: resolved.peer_public_key,
                        client_private_key: resolved.client_private_key,
                    })
                }
                None => None,
            };

            // Resolve with_coordinator: CLI flag > config profile > auto-enable if WireGuard
            let resolved_with_coordinator = with_coordinator
                || profile
                    .as_ref()
                    .map(|p| p.with_coordinator)
                    .unwrap_or(false)
                || wireguard.is_some();

            if wireguard.is_some()
                && !with_coordinator
                && !profile
                    .as_ref()
                    .map(|p| p.with_coordinator)
                    .unwrap_or(false)
            {
                println!(
                    "{} WireGuard config found, enabling coordinator node",
                    "Note:".cyan()
                );
            }

            let (resolved_service_account, resolved_coordinator_service_account) =
                resolve_service_accounts(
                    service_account,
                    coordinator_service_account,
                    profile.as_ref(),
                );
            // Custom worker selection follows the same precedence as other pool settings.
            let resolved_worker_binary = resolve_worker_binary(worker_binary, profile.as_ref());

            let pool_config = PoolConfig {
                name,
                worker_count: resolved_workers,
                machine_type: resolved_machine_type,
                zone: resolved_zone,
                spot: resolved_spot,
                project_id,
                network: resolved_network,
                subnet: resolved_subnet,
                public_ip: resolved_public_ip,
                manage_firewall: resolved_manage_firewall,
                with_coordinator: resolved_with_coordinator,
                wireguard,
                pool_db_path: profile.as_ref().and_then(|p| p.pool_db_path.clone()),
                binary_gcs_url: None,        // Set by create() after staging
                worker_binary_gcs_url: None, // Set independently by create()
                service_account: resolved_service_account,
                coordinator_service_account: resolved_coordinator_service_account,
            };

            manager.create(&pool_config, wait, skip_build, resolved_worker_binary)?;
        }
        PoolCommands::Submit {
            name,
            zone,
            cluster,
            binary,
            worker_binary,
            auto_stop,
            redeploy_binary,
            force,
            autoscale,
            skip_build,
            batch_size,
            memory_weight_mb,
            mut command,
        } => {
            // Intercept: Apply cluster config overrides if requested
            if let Some(cluster_name) = cluster {
                let cluster_conf = app_config
                    .get_cluster(Some(&cluster_name))
                    .ok_or_else(|| {
                        genohype_core::HailError::InvalidFormat(format!(
                            "Unknown cluster: {}",
                            cluster_name
                        ))
                    })?
                    .resolve_env_vars()
                    .map_err(genohype_core::HailError::Config)?;

                // We only override if it's a manhattan-batch command with a --config flag
                if command.len() >= 2 && command[0] == "manhattan-batch" {
                    if let Some(config_idx) = command.iter().position(|a| a == "--config") {
                        if let Some(config_path) = command.get(config_idx + 1).cloned() {
                            // Load the local job config
                            let mut job_config =
                                crate::manhattan::config::ManhattanJobConfig::load(
                                    std::path::Path::new(&config_path),
                                )?;

                            // Override specific fields with cluster details
                            job_config.ingest.clickhouse_url =
                                Some(cluster_conf.clickhouse_url.clone());

                            // Generate timestamp (YYYYMMDD-HHMM format)
                            let timestamp = {
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                // Convert to approximate date/time (simplified, not TZ-aware)
                                let days = secs / 86400;
                                let time_of_day = secs % 86400;
                                let hours = time_of_day / 3600;
                                let minutes = (time_of_day % 3600) / 60;
                                // Approximate date calculation (from 1970-01-01)
                                let mut year = 1970u64;
                                let mut remaining_days = days;
                                loop {
                                    let days_in_year =
                                        if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                                            366
                                        } else {
                                            365
                                        };
                                    if remaining_days < days_in_year {
                                        break;
                                    }
                                    remaining_days -= days_in_year;
                                    year += 1;
                                }
                                let month_days = [
                                    31,
                                    28 + if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                                        1
                                    } else {
                                        0
                                    },
                                    31,
                                    30,
                                    31,
                                    30,
                                    31,
                                    31,
                                    30,
                                    31,
                                    30,
                                    31,
                                ];
                                let mut month = 1u64;
                                for days_in_month in month_days {
                                    if remaining_days < days_in_month {
                                        break;
                                    }
                                    remaining_days -= days_in_month;
                                    month += 1;
                                }
                                let day = remaining_days + 1;
                                format!(
                                    "{:04}{:02}{:02}-{:02}{:02}",
                                    year, month, day, hours, minutes
                                )
                            };
                            let new_output_dir = cluster_conf.output_dir(&timestamp);
                            job_config.job.output_dir = Some(new_output_dir.clone());

                            // Write modified config to temp file
                            let temp_path =
                                std::env::temp_dir().join(format!("job-config-{}.toml", timestamp));
                            let content = toml::to_string_pretty(&job_config)
                                .map_err(|e| genohype_core::HailError::Config(e.to_string()))?;
                            std::fs::write(&temp_path, content)?;

                            // Replace config path in the command vector to point to the new temp file
                            command[config_idx + 1] = temp_path.to_string_lossy().to_string();

                            println!(
                                "{} Using cluster '{}' ({})",
                                "Cluster:".green().bold(),
                                cluster_name.cyan(),
                                cluster_conf.clickhouse_url
                            );
                            println!(
                                "{} {}",
                                "Output:".green().bold(),
                                new_output_dir.bright_white()
                            );
                        }
                    }
                }
            }

            let resolved_zone = resolve_zone(zone, &name, app_config);

            // Convert ResolvedPoolConfig to ScalingConfig if available
            let scaling_config = app_config
                .get_pool(&name)
                .map(|p| crate::cloud::ScalingConfig {
                    machine_type: p.machine_type.clone(),
                    workers: p.workers,
                    spot: p.spot,
                    network: p.network.clone(),
                    subnet: p.subnet.clone(),
                    public_ip: p.public_ip,
                    manage_firewall: p.manage_firewall,
                    project: p.project.clone(),
                    with_coordinator: p.with_coordinator,
                    pool_db_path: p.pool_db_path.clone(),
                    worker_binary: p.worker_binary.clone(),
                    service_account: p.service_account.clone(),
                });
            // Resolve worker binary: CLI flag > config profile
            let resolved_worker_binary = worker_binary.or_else(|| {
                scaling_config
                    .as_ref()
                    .and_then(|c| c.worker_binary.clone())
            });

            manager.submit(
                &name,
                &resolved_zone,
                binary,
                resolved_worker_binary,
                auto_stop,
                redeploy_binary,
                force,
                autoscale,
                skip_build,
                batch_size,
                memory_weight_mb,
                scaling_config.as_ref(),
                &command,
            )?;
        }
        PoolCommands::Scale {
            name,
            workers,
            zone,
            public_ip,
            manage_firewall,
            binary,
            worker_binary,
            skip_build,
        } => {
            let pool_config = app_config.get_pool(&name).ok_or_else(|| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Pool '{}' not found in config. Required for scaling to know machine type/network.",
                        name
                    ),
                ))
            })?;

            let scaling_config = crate::cloud::ScalingConfig {
                machine_type: pool_config.machine_type.clone(),
                workers: pool_config.workers,
                spot: pool_config.spot,
                network: pool_config.network.clone(),
                subnet: pool_config.subnet.clone(),
                public_ip: public_ip.unwrap_or(pool_config.public_ip),
                manage_firewall: manage_firewall.unwrap_or(pool_config.manage_firewall),
                project: pool_config.project.clone(),
                with_coordinator: pool_config.with_coordinator,
                pool_db_path: pool_config.pool_db_path.clone(),
                worker_binary: pool_config.worker_binary.clone(),
                service_account: pool_config.service_account.clone(),
            };

            let resolved_zone = resolve_zone(zone, &name, app_config);
            // Resolve worker binary: CLI flag > config profile
            let resolved_worker_binary = worker_binary.or(scaling_config.worker_binary.clone());
            manager.scale(
                &name,
                workers,
                &resolved_zone,
                binary,
                resolved_worker_binary,
                skip_build,
                &scaling_config,
            )?;
        }
        PoolCommands::Destroy {
            name,
            zone,
            metrics_bucket,
        } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.destroy(&name, &resolved_zone, metrics_bucket.as_deref())?;
        }
        PoolCommands::List { name } => {
            manager.list(&name)?;
        }
        PoolCommands::Status { name, zone } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.status(&name, &resolved_zone)?;
        }
        PoolCommands::UpdateBinary {
            name,
            zone,
            binary,
            worker_binary,
            skip_build,
            via_api,
            port,
        } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            let pool_config = app_config.get_pool(&name);
            let pool_db_path = pool_config.as_ref().and_then(|p| p.pool_db_path.clone());
            let scaling_config = pool_config.as_ref().map(|p| crate::cloud::ScalingConfig {
                machine_type: p.machine_type.clone(),
                workers: p.workers,
                spot: p.spot,
                network: p.network.clone(),
                subnet: p.subnet.clone(),
                public_ip: p.public_ip,
                manage_firewall: p.manage_firewall,
                project: p.project.clone(),
                with_coordinator: p.with_coordinator,
                pool_db_path: p.pool_db_path.clone(),
                worker_binary: p.worker_binary.clone(),
                service_account: p.service_account.clone(),
            });
            // Resolve worker binary: CLI flag > config profile
            let resolved_worker_binary = worker_binary
                .or_else(|| pool_config.as_ref().and_then(|p| p.worker_binary.clone()));
            // CLI flag overrides config, config defaults to false
            let use_api = via_api
                || pool_config
                    .as_ref()
                    .map(|p| p.update_via_api)
                    .unwrap_or(false);
            // CLI port overrides config port
            let api_port = if via_api {
                port // CLI explicitly set, use CLI port
            } else {
                pool_config
                    .as_ref()
                    .map(|p| p.update_api_port)
                    .unwrap_or(port)
            };
            if use_api {
                manager.update_binary_via_api(
                    binary,
                    resolved_worker_binary,
                    skip_build,
                    pool_db_path.as_deref(),
                    api_port,
                )?;
            } else {
                manager.update_binary(
                    &name,
                    &resolved_zone,
                    binary,
                    resolved_worker_binary,
                    skip_build,
                    pool_db_path.as_deref(),
                    scaling_config.as_ref(),
                )?;
            }
        }
        PoolCommands::Cancel { name, zone } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.cancel(&name, &resolved_zone)?;
        }
        PoolCommands::Workers { name, zone } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.workers(&name, &resolved_zone)?;
        }
        PoolCommands::Events { name, zone, follow } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.events(&name, &resolved_zone, follow)?;
        }
        PoolCommands::Failures { name, zone } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.failures(&name, &resolved_zone)?;
        }
        PoolCommands::Logs { name, zone, worker } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.logs(&name, &resolved_zone, &worker)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_project_prefers_cli_then_config_and_ignores_ambient() {
        let config: config::Config = toml::from_str(
            r#"
[defaults]
project = "configured-project"
[pools.demo]
"#,
        )
        .unwrap();

        assert_eq!(
            configured_project(Some("cli-project"), "demo", &config).as_deref(),
            Some("cli-project")
        );
        assert_eq!(
            configured_project(None, "demo", &config).as_deref(),
            Some("configured-project")
        );
    }

    #[test]
    fn custom_worker_resolution_prefers_cli_then_profile() {
        let config: config::Config = toml::from_str(
            r#"
[pools.demo]
worker_binary = "/profile/worker"
"#,
        )
        .unwrap();
        let profile = config.get_pool("demo").unwrap();

        assert_eq!(
            resolve_worker_binary(Some("/cli/worker".into()), Some(&profile)).as_deref(),
            Some("/cli/worker")
        );
        assert_eq!(
            resolve_worker_binary(None, Some(&profile)).as_deref(),
            Some("/profile/worker")
        );
    }

    #[test]
    fn create_account_resolution_preserves_legacy_and_separate_identities() {
        let legacy_config: config::Config = toml::from_str(
            r#"
[pools.legacy]
service_account = "legacy@project.iam.gserviceaccount.com"
"#,
        )
        .unwrap();
        let legacy = legacy_config.get_pool("legacy").unwrap();
        let (worker, coordinator) = resolve_service_accounts(None, None, Some(&legacy));
        assert_eq!(worker, coordinator);
        assert_eq!(
            coordinator.as_deref(),
            Some("legacy@project.iam.gserviceaccount.com")
        );

        let (worker, coordinator) = resolve_service_accounts(
            Some("worker@project.iam.gserviceaccount.com".into()),
            Some("coordinator@project.iam.gserviceaccount.com".into()),
            Some(&legacy),
        );
        assert_eq!(
            worker.as_deref(),
            Some("worker@project.iam.gserviceaccount.com")
        );
        assert_eq!(
            coordinator.as_deref(),
            Some("coordinator@project.iam.gserviceaccount.com")
        );
    }
}
