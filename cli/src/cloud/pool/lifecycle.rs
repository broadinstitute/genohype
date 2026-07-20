//! Pool lifecycle management (create, destroy, scale, list).

use super::PoolManager;
use crate::cloud::startup;
use crate::cloud::{CloudProvider, Instance, PoolConfig};
use crate::HailError;
use crate::Result;
use owo_colors::OwoColorize;

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Create a new worker pool.
    ///
    /// Provisions `config.worker_count` VMs in parallel.
    /// If `wait` is true, polls until all VMs have completed their startup scripts.
    /// Automatically builds Linux binary if on macOS (unless `skip_build` is true).
    /// If `with_coordinator` is true, also starts the coordinator in idle mode.
    pub(crate) fn create(&self, config: &PoolConfig, wait: bool, skip_build: bool) -> Result<()> {
        // Determine if we should build
        // 1. Explicit skip_build -> skip
        // 2. We have a bundled binary -> skip (Release mode)
        // 3. Otherwise -> build (Dev mode)
        let should_build = if skip_build {
            println!("{}", "Skipping binary build (--skip-build)".dimmed());
            false
        } else if self.has_bundled_binary() {
            println!(
                "{}",
                "Found bundled worker binary, skipping build...".dimmed()
            );
            false
        } else {
            true
        };

        if should_build {
            Self::build_linux_binary(&[])?;
        }

        // Stage binary to GCS BEFORE creating VMs (so startup script can download it)
        let mut config = config.clone();
        if let Some(ref db_path) = config.pool_db_path {
            let binary = self.locate_binary(None)?;
            match self.stage_binary_to_gcs(&binary, db_path) {
                Ok(gcs_url) => {
                    config.binary_gcs_url = Some(gcs_url);
                }
                Err(e) => {
                    println!(
                        "{} GCS staging failed ({}), will deploy via SSH",
                        "Warning:".yellow(),
                        e
                    );
                }
            }
        }

        println!(
            "{} pool '{}' with {} workers ({}, {})...",
            "Creating".green(),
            config.name.bright_white(),
            config.worker_count.to_string().bright_white(),
            config.machine_type.dimmed(),
            if config.spot { "spot" } else { "on-demand" }
        );

        self.provider.create_pool(&config)?;

        println!(
            "{} Pool '{}' created successfully.",
            "OK".green().bold(),
            config.name
        );

        // Always wait for pool ready when with_coordinator, so we can deploy and start
        let should_wait = wait || config.with_coordinator;

        if should_wait {
            println!("{}", "Waiting for VMs to be ready...".dimmed());
            self.wait_for_pool_ready(&config.name, &config.zone, 300)?;
            println!("{} All workers are ready.", "OK".green().bold());
        } else {
            println!("   Workers will be ready in ~60 seconds (or use --wait)");
        }

        // If with_coordinator, start coordinator in idle mode
        // Binary was already deployed via startup script if GCS staging worked
        if config.with_coordinator {
            self.start_idle_coordinator(
                &config.name,
                &config.zone,
                skip_build,
                config.pool_db_path.as_deref(),
                config.binary_gcs_url.is_some(), // skip_binary_deploy if already via startup
            )?;
        }

        Ok(())
    }

    /// Wait for all instances in a pool to complete their startup scripts.
    pub(crate) fn wait_for_pool_ready(
        &self,
        pool_name: &str,
        zone: &str,
        timeout_secs: u64,
    ) -> Result<()> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        // Get list of instances
        let instances = self.provider.list_instances(pool_name)?;
        if instances.is_empty() {
            return Ok(());
        }

        let total = instances.len();
        let mut ready_count = 0;

        println!("   Waiting for {} instances...", total);

        while ready_count < total {
            if start.elapsed() > timeout {
                return Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timeout waiting for pool '{}' to be ready", pool_name),
                )));
            }

            ready_count = 0;
            for inst in &instances {
                // Check if the ready marker file exists
                let mut cmd = self.provider.get_ssh_command(
                    &inst.name,
                    zone,
                    "test -f /tmp/genohype-ready && echo ready",
                );
                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::null());

                if let Ok(output) = cmd.output() {
                    if output.status.success() {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        if stdout.contains("ready") {
                            ready_count += 1;
                        }
                    }
                }
            }

            println!("   {}/{} ready", ready_count, total);

            if ready_count < total {
                std::thread::sleep(Duration::from_secs(5));
            }
        }

        Ok(())
    }

    /// Destroy a worker pool.
    ///
    /// Deletes all VMs tagged with the pool name.
    /// If `metrics_bucket` is provided, exports metrics to GCS before deletion.
    pub(crate) fn destroy(
        &self,
        name: &str,
        zone: &str,
        metrics_bucket: Option<&str>,
    ) -> Result<()> {
        println!("{} pool '{}'...", "Destroying".red(), name.bright_white());

        // First list to show what we're deleting
        let instances = self.provider.list_instances(name)?;
        if instances.is_empty() {
            println!("   No instances found for pool '{}'", name);
            return Ok(());
        }

        println!("   Found {} instances to delete", instances.len());
        for inst in &instances {
            println!("   - {}", inst.name.dimmed());
        }

        // Export metrics database to GCS before destroying (if bucket provided)
        if let Some(bucket) = metrics_bucket {
            if let Some(coordinator) = instances.iter().find(|i| i.name.ends_with("-coordinator")) {
                self.export_metrics_to_gcs(name, coordinator, zone, bucket);
            }
        }

        self.provider.destroy_pool(name, zone)?;

        println!("{} Pool '{}' destroyed.", "OK".green().bold(), name);

        Ok(())
    }

    /// List instances in a pool.
    pub(crate) fn list(&self, name: &str) -> Result<Vec<Instance>> {
        let instances = self.provider.list_instances(name)?;

        if instances.is_empty() {
            println!("No instances found for pool '{}'", name);
        } else {
            println!(
                "Pool '{}' has {} instances:",
                name.bright_white(),
                instances.len().to_string().bright_white()
            );
            for inst in &instances {
                let status_color = if inst.is_running() {
                    inst.status.green().to_string()
                } else {
                    inst.status.yellow().to_string()
                };
                println!(
                    "  {} {} ({})",
                    inst.name.cyan(),
                    status_color,
                    inst.ip().unwrap_or("no IP")
                );
            }
        }

        Ok(instances)
    }

    /// Scale the number of workers in a pool.
    ///
    /// This will:
    /// - Scale up: create new worker VMs, wait for startup, deploy binary
    /// - Scale down: delete workers with highest indices
    ///
    /// The coordinator is never affected by scaling operations.
    pub(crate) fn scale(
        &self,
        name: &str,
        target_workers: usize,
        zone: &str,
        binary_path: Option<String>,
        worker_binary_path: Option<String>,
        skip_build: bool,
        config: &crate::cloud::ScalingConfig,
    ) -> Result<()> {
        println!(
            "{} pool '{}' to {} workers...",
            "Scaling".green(),
            name.bright_white(),
            target_workers.to_string().bright_white()
        );

        // 1. Get current status
        let instances = self.provider.list_instances(name)?;
        let workers: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.name.contains("-worker-"))
            .collect();

        let current_count = workers.len();

        if current_count == target_workers {
            println!(
                "{} Pool already has {} workers.",
                "OK".green().bold(),
                current_count
            );
            return Ok(());
        }

        // 2. Identify coordinator (needed for deploying binary to new workers)
        let coordinator = instances.iter().find(|i| i.name.ends_with("-coordinator"));
        if coordinator.is_none() && config.with_coordinator {
            println!(
                "{} Coordinator not found, but configuration expects one.",
                "Warning:".yellow()
            );
        }

        // Get project ID for operations
        let project_id = config
            .project
            .clone()
            .unwrap_or_else(|| "default".to_string());

        if target_workers > current_count {
            // SCALE UP
            let to_add = target_workers - current_count;
            println!(
                "{} Adding {} workers...",
                "Scaling up:".cyan(),
                to_add.to_string().bright_white()
            );

            // Determine if we should build (fail fast before creating VMs)
            let should_build = if skip_build {
                false
            } else if self.has_bundled_binary() {
                println!(
                    "{}",
                    "Found bundled worker binary, skipping build...".dimmed()
                );
                false
            } else {
                true
            };

            if should_build {
                Self::build_linux_binary(&[])?;
            }
            let binary = self.locate_binary(binary_path.clone())?;

            // Resolve worker binary if specified
            let worker_bin = if let Some(ref wb_path) = worker_binary_path {
                let path = std::path::PathBuf::from(wb_path);
                if !path.exists() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Worker binary not found at: {}", wb_path),
                    )));
                }
                Some(path)
            } else {
                None
            };

            // Determine indices for new workers
            // Find existing indices and create new workers at gaps or at the end
            let mut existing_indices: Vec<usize> = workers
                .iter()
                .filter_map(|w| w.name.split("-worker-").nth(1).and_then(|s| s.parse().ok()))
                .collect();
            existing_indices.sort();

            let mut new_instances = Vec::new();
            let mut next_idx = 0;

            for _ in 0..to_add {
                // Find the next available index
                while existing_indices.contains(&next_idx) {
                    next_idx += 1;
                }

                let instance_name = format!("{}-worker-{}", name, next_idx);
                let tags = format!("genohype-worker,pool-{},role-worker", name);

                new_instances.push(crate::cloud::InstanceSetup {
                    name: instance_name,
                    machine_type: config.machine_type.clone(),
                    zone: zone.to_string(),
                    tags: vec![tags],
                    startup_script: startup::generate_worker_startup_script(None, name),
                    spot: config.spot,
                    network: config.network.clone(),
                    subnet: config.subnet.clone(),
                    project_id: project_id.clone(),
                    service_account: config.service_account.clone(),
                });

                existing_indices.push(next_idx);
            }

            // Create instances
            self.provider.create_instances(&new_instances)?;
            println!("{} Created {} new instances.", "OK".green().bold(), to_add);

            // Wait for readiness
            println!("{}", "Waiting for new instances to be ready...".dimmed());
            for inst in &new_instances {
                self.wait_for_startup_complete(&inst.name, zone, 300)?;
            }

            // Get updated instance list to get IPs
            let updated_instances = self.provider.list_instances(name)?;
            let new_worker_instances: Vec<Instance> = updated_instances
                .into_iter()
                .filter(|i| new_instances.iter().any(|n| n.name == i.name))
                .collect();

            // Deploy binary to new workers
            // If a custom worker binary is specified, always deploy it directly via SCP
            // (can't use coordinator propagation since coordinator serves a different binary)
            let deploy_bin = worker_bin.as_ref().unwrap_or(&binary);
            if worker_bin.is_some() {
                println!("{}", "Deploying custom worker binary via SCP...".dimmed());
                self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
            } else if let Some(coord) = coordinator {
                if let Some(coord_ip) = coord.ip() {
                    // Coordinator exists, check if it's running to serve binary
                    if self.check_coordinator_status(coord, zone) {
                        println!("{}", "Deploying binary via coordinator...".dimmed());
                        self.propagate_binary_from_coordinator(
                            coord_ip,
                            &new_worker_instances,
                            zone,
                        )?;
                    } else {
                        // Coordinator not running, deploy via SCP
                        println!(
                            "{}",
                            "Coordinator not running, deploying via SCP...".dimmed()
                        );
                        self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
                    }
                } else {
                    self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
                }
            } else {
                // No coordinator, direct SCP
                self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
            }

            println!(
                "{} Scaled up to {} workers.",
                "OK".green().bold(),
                target_workers
            );
        } else {
            // SCALE DOWN
            let to_remove = current_count - target_workers;
            println!(
                "{} Removing {} workers...",
                "Scaling down:".cyan(),
                to_remove.to_string().bright_white()
            );

            // Sort workers by index descending to remove highest indices first
            let mut sorted_workers: Vec<(usize, &Instance)> = workers
                .iter()
                .filter_map(|w| {
                    w.name
                        .split("-worker-")
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .map(|idx| (idx, *w))
                })
                .collect();

            // Sort descending by index
            sorted_workers.sort_by(|a, b| b.0.cmp(&a.0));

            let instances_to_delete: Vec<String> = sorted_workers
                .iter()
                .take(to_remove)
                .map(|(_, w)| w.name.clone())
                .collect();

            // Show which instances are being deleted
            for name in &instances_to_delete {
                println!("  {} {}", "Deleting:".dimmed(), name.yellow());
            }

            self.provider
                .delete_instances(&instances_to_delete, zone, &project_id)?;

            println!(
                "{} Scaled down to {} workers.",
                "OK".green().bold(),
                target_workers
            );
        }

        Ok(())
    }

    /// Wait for startup script to complete on a specific instance.
    pub(crate) fn wait_for_startup_complete(
        &self,
        instance_name: &str,
        zone: &str,
        timeout_secs: u64,
    ) -> Result<()> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timeout waiting for startup script on instance {}",
                        instance_name
                    ),
                )));
            }

            // Check if the ready marker file exists
            let mut cmd = self.provider.get_ssh_command(
                instance_name,
                zone,
                "test -f /tmp/genohype-ready && echo ready",
            );
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::null());

            if let Ok(output) = cmd.output() {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    if stdout.contains("ready") {
                        println!("  {} {}", "Ready:".dimmed(), instance_name.cyan());
                        return Ok(());
                    }
                }
            }

            std::thread::sleep(Duration::from_secs(5));
        }
    }
}
