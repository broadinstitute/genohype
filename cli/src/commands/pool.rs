//! Pool management commands for distributed processing.

use crate::cli::PoolCommands;
use crate::cloud::gcp::GcpClient;
use crate::cloud::pool::PoolManager;
use crate::cloud::PoolConfig;
use crate::config;
use genohype_core::Result;
use owo_colors::OwoColorize;

/// Resolve zone: CLI arg > pool profile > config defaults > fallback
fn resolve_zone(
    zone: Option<String>,
    pool_name: &str,
    app_config: &config::Config,
) -> String {
    zone.or_else(|| app_config.get_pool(pool_name).map(|p| p.zone.clone()))
        .or_else(|| app_config.defaults.zone.clone())
        .unwrap_or_else(|| "us-central1-a".to_string())
}

/// Run pool management commands
pub fn run_pool_command(command: PoolCommands, app_config: &config::Config) -> Result<()> {
    let client = GcpClient::new();
    let manager = PoolManager::new(client);

    match command {
        PoolCommands::Create {
            name,
            workers,
            machine_type,
            zone,
            spot,
            project,
            network,
            subnet,
            wait,
            skip_build,
            with_coordinator,
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

            // Resolve project ID: CLI > config > gcloud default
            let project_id = project
                .or_else(|| profile.as_ref().and_then(|p| p.project.clone()))
                .or_else(|| app_config.defaults.project.clone())
                .map(Ok)
                .unwrap_or_else(|| GcpClient::new().get_current_project())?;

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

            let pool_config = PoolConfig {
                name,
                worker_count: resolved_workers,
                machine_type: resolved_machine_type,
                zone: resolved_zone,
                spot: resolved_spot,
                project_id,
                network: resolved_network,
                subnet: resolved_subnet,
                with_coordinator: resolved_with_coordinator,
                wireguard,
                pool_db_path: profile.as_ref().and_then(|p| p.pool_db_path.clone()),
                binary_gcs_url: None, // Set by create() after staging
            };

            manager.create(&pool_config, wait, skip_build)?;
        }
        PoolCommands::Submit {
            name,
            zone,
            cluster,
            binary,
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
                            let temp_path = std::env::temp_dir()
                                .join(format!("job-config-{}.toml", timestamp));
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
            let scaling_config = app_config.get_pool(&name).map(|p| {
                crate::cloud::ScalingConfig {
                    machine_type: p.machine_type.clone(),
                    workers: p.workers,
                    spot: p.spot,
                    network: p.network.clone(),
                    subnet: p.subnet.clone(),
                    project: p.project.clone(),
                    with_coordinator: p.with_coordinator,
                    pool_db_path: p.pool_db_path.clone(),
                }
            });
            manager.submit(
                &name,
                &resolved_zone,
                binary,
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
            binary,
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
                project: pool_config.project.clone(),
                with_coordinator: pool_config.with_coordinator,
                pool_db_path: pool_config.pool_db_path.clone(),
            };

            let resolved_zone = resolve_zone(zone, &name, app_config);
            manager.scale(&name, workers, &resolved_zone, binary, skip_build, &scaling_config)?;
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
            skip_build,
            via_api,
            port,
        } => {
            let resolved_zone = resolve_zone(zone, &name, app_config);
            let pool_config = app_config.get_pool(&name);
            let pool_db_path = pool_config.as_ref().and_then(|p| p.pool_db_path.clone());
            // CLI flag overrides config, config defaults to false
            let use_api =
                via_api || pool_config.as_ref().map(|p| p.update_via_api).unwrap_or(false);
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
                    skip_build,
                    pool_db_path.as_deref(),
                    api_port,
                )?;
            } else {
                manager.update_binary(&name, &resolved_zone, binary, skip_build, pool_db_path.as_deref())?;
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
