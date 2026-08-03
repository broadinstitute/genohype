//! Pool lifecycle management (create, destroy, scale, list).

use super::PoolManager;
use crate::cloud::startup;
use crate::cloud::{CloudProvider, Instance, PoolConfig};
use crate::HailError;
use crate::Result;
use owo_colors::OwoColorize;

const ATTESTED_WORKER_FRESHNESS_SECS: f64 = 15.0;

#[derive(Debug, serde::Deserialize)]
struct GuestStartupStatus {
    status: String,
    instance_id: String,
    deployment_id: Option<String>,
    sha256: Option<String>,
    service_active: bool,
}

fn startup_status_is_ready(
    instance: &Instance,
    value: &str,
    bootstrap: Option<&crate::cloud::attestation::ExpectedWorkerBootstrap>,
) -> std::result::Result<bool, String> {
    let status: GuestStartupStatus =
        serde_json::from_str(value).map_err(|error| format!("invalid startup status: {error}"))?;
    if status.status != "ready" || status.instance_id != instance.id {
        return Ok(false);
    }
    if let Some(bootstrap) = bootstrap {
        Ok(status.service_active
            && status.deployment_id.as_deref() == Some(bootstrap.deployment_id.as_str())
            && status.sha256.as_deref() == Some(bootstrap.artifact.attestation.sha256.as_str()))
    } else {
        Ok(status.deployment_id.is_none() && status.sha256.is_none())
    }
}

fn attested_dashboard_worker_is_ready(
    worker: &crate::distributed::message::DashboardWorker,
    expected_build_version: &str,
    generation_age: std::time::Duration,
) -> bool {
    matches!(worker.status.as_str(), "active" | "idle")
        && worker.last_seen_secs >= 0.0
        && worker.last_seen_secs <= ATTESTED_WORKER_FRESHNESS_SECS
        // A heartbeat inferred to predate create_instances is from an old VM
        // generation, even when the worker name and build are unchanged.
        && worker.last_seen_secs < generation_age.as_secs_f64()
        && worker.build_version.as_deref() == Some(expected_build_version)
}

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Create a new worker pool.
    ///
    /// Provisions `config.worker_count` VMs in parallel.
    /// If `wait` is true, polls until all VMs have completed their startup scripts.
    /// Automatically builds Linux binary if on macOS (unless `skip_build` is true).
    /// If `with_coordinator` is true, also starts the coordinator in idle mode.
    pub(crate) fn create(
        &self,
        config: &PoolConfig,
        wait: bool,
        skip_build: bool,
        worker_binary_path: Option<String>,
        worker_attestation_path: Option<String>,
    ) -> Result<()> {
        if worker_binary_path.is_some() && !config.with_coordinator {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Custom workers require a coordinator; enable --with-coordinator or set with_coordinator = true",
            )));
        }

        // Validate a custom worker and its exact bytes before build, staging, or
        // any VM creation. An attestation without a custom binary is ambiguous.
        if worker_attestation_path.is_some() && worker_binary_path.is_none() {
            return Err(HailError::InvalidFormat(
                "--worker-attestation requires a resolved custom worker binary".into(),
            ));
        }
        let worker_binary = worker_binary_path
            .map(std::path::PathBuf::from)
            .map(|path| {
                if path.is_file() {
                    Ok(path)
                } else {
                    Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Worker binary not found at: {}", path.display()),
                    )))
                }
            })
            .transpose()?;
        let attested_worker = match (&worker_binary, worker_attestation_path.as_deref()) {
            (Some(binary), Some(sidecar)) => {
                Some(crate::cloud::attestation::validate_worker_artifact(
                    binary,
                    std::path::Path::new(sidecar),
                )?)
            }
            _ => None,
        };
        if attested_worker.is_some() && config.pool_db_path.is_none() {
            return Err(HailError::InvalidFormat(
                "attested custom workers require pool_db_path for fail-closed GCS staging".into(),
            ));
        }
        if worker_binary.is_some() && attested_worker.is_none() {
            println!(
                "{} Unattested custom-worker create is legacy compatibility; pool scale will require worker_attestation",
                "Warning:".yellow()
            );
        }

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
            match self.stage_binary_to_gcs(&binary, db_path, "genohype-coordinator") {
                Ok(gcs_url) => config.binary_gcs_url = Some(gcs_url),
                Err(e) => println!(
                    "{} Coordinator staging failed ({}), will deploy via SSH",
                    "Warning:".yellow(),
                    e
                ),
            }

            if let Some(ref artifact) = attested_worker {
                // No fallback: an attested deployment must use the exact bytes
                // validated above and a single content-addressed GCS object.
                let staged = self.stage_attested_worker_to_gcs(artifact, db_path)?;
                config.worker_binary_gcs_url = Some(staged.gcs_url.clone());
                config.worker_bootstrap = Some(
                    crate::cloud::attestation::ExpectedWorkerBootstrap::new(staged),
                );
            } else if let Some(ref custom_worker) = worker_binary {
                match self.stage_binary_to_gcs(custom_worker, db_path, "genohype-worker-custom") {
                    Ok(gcs_url) => config.worker_binary_gcs_url = Some(gcs_url),
                    Err(e) => println!(
                        "{} Custom worker staging failed ({}), will deploy via SSH",
                        "Warning:".yellow(),
                        e
                    ),
                }
            } else {
                config.worker_binary_gcs_url = config.binary_gcs_url.clone();
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

        let create_started = std::time::Instant::now();
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
            self.wait_for_pool_ready(
                &config.name,
                &config.zone,
                config.worker_bootstrap.as_ref(),
                300,
            )?;
            println!("{} All workers are ready.", "OK".green().bold());
        } else {
            println!("   Workers will be ready in ~60 seconds (or use --wait)");
        }

        // A fully staged pool has already installed and started both services.
        // Validate that path instead of immediately rewriting/restarting units via SSH.
        if config.with_coordinator
            && config.binary_gcs_url.is_some()
            && config.worker_binary_gcs_url.is_some()
        {
            let instances = self.provider.list_instances(&config.name)?;
            let coordinator = instances
                .iter()
                .find(|instance| instance.name.ends_with("-coordinator"))
                .ok_or_else(|| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "coordinator missing after zero-SSH pool creation",
                    ))
                })?;
            let api_started = std::time::Instant::now();
            while !self.check_coordinator_status(coordinator, &config.zone) {
                if api_started.elapsed() > std::time::Duration::from_secs(60) {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "coordinator API did not become ready after startup bootstrap",
                    )));
                }
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
            if let Some(bootstrap) = config.worker_bootstrap.as_ref() {
                let expected_names: Vec<String> = (0..config.worker_count)
                    .map(|index| format!("{}-worker-{}", config.name, index))
                    .collect();
                self.wait_for_attested_workers_ready(
                    coordinator,
                    &expected_names,
                    bootstrap,
                    &config.zone,
                    create_started,
                    300,
                )?;
            }
            println!(
                "{} Startup-installed coordinator and worker services validated without restart",
                "OK".green().bold()
            );
        } else if config.with_coordinator {
            // Explicit legacy fallback when staging was unavailable.
            self.start_idle_coordinator(
                &config.name,
                &config.zone,
                skip_build,
                config.pool_db_path.as_deref(),
                config.binary_gcs_url.is_some(),
                worker_binary.as_deref(),
                config.worker_binary_gcs_url.is_some(),
                &config,
            )?;
        }

        Ok(())
    }

    /// Wait for all instances in a pool to complete their startup scripts.
    pub(crate) fn wait_for_pool_ready(
        &self,
        pool_name: &str,
        zone: &str,
        worker_bootstrap: Option<&crate::cloud::attestation::ExpectedWorkerBootstrap>,
        timeout_secs: u64,
    ) -> Result<()> {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let instances = self.provider.list_instances(pool_name)?;
        if instances.is_empty() {
            return Ok(());
        }
        if let Some(instance) = instances.iter().find(|instance| instance.id.is_empty()) {
            return Err(HailError::InvalidFormat(format!(
                "instance {} has no immutable generation ID",
                instance.name
            )));
        }

        let total = instances.len();
        let mut ready_count = 0;
        let mut last_diagnostic = "no guest startup status published".to_string();
        println!("   Waiting for {} instances...", total);

        while ready_count < total {
            if start.elapsed() > timeout {
                return Err(crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timeout waiting for pool '{}' startup status: {}",
                        pool_name, last_diagnostic
                    ),
                )));
            }

            ready_count = 0;
            let mut diagnostics = Vec::new();
            for instance in &instances {
                let expected = instance
                    .name
                    .contains("-worker-")
                    .then_some(worker_bootstrap)
                    .flatten();
                match self.provider.get_startup_status(instance, zone) {
                    Ok(Some(value)) => match startup_status_is_ready(instance, &value, expected) {
                        Ok(true) => ready_count += 1,
                        Ok(false) => diagnostics.push(format!(
                            "{} (ID {}) published non-matching status {}",
                            instance.name, instance.id, value
                        )),
                        Err(error) => diagnostics
                            .push(format!("{} (ID {}): {}", instance.name, instance.id, error)),
                    },
                    Ok(None) => diagnostics.push(format!(
                        "{} (ID {}) has no startup status",
                        instance.name, instance.id
                    )),
                    Err(error) => diagnostics.push(format!(
                        "{} (ID {}) status query failed: {}",
                        instance.name, instance.id, error
                    )),
                }
            }
            if !diagnostics.is_empty() {
                last_diagnostic = diagnostics.join("; ");
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

        // 1. Get current status. Stopped Spot VMs remain in GCE and must be
        // deleted before their indices can be reused; they are not capacity.
        let instances = self.provider.list_instances(name)?;
        let project_id = config
            .project
            .clone()
            .or_else(|| self.provider.project_id().map(String::from))
            .unwrap_or_else(|| "default".to_string());
        let all_workers: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.name.contains("-worker-"))
            .collect();
        let stale_workers: Vec<String> = all_workers
            .iter()
            .filter(|worker| !worker.is_running())
            .map(|worker| worker.name.clone())
            .collect();

        let workers: Vec<&Instance> = all_workers
            .into_iter()
            .filter(|worker| worker.is_running())
            .collect();
        let current_count = workers.len();

        // Scale-down/no-op cleanup does not depend on custom artifact bytes.
        // Scale-up defers this mutation until after fail-closed validation below.
        if target_workers <= current_count && !stale_workers.is_empty() {
            println!(
                "{} Removing {} non-running worker(s) before scaling...",
                "Cleanup:".cyan(),
                stale_workers.len()
            );
            self.provider
                .delete_instances(&stale_workers, zone, &project_id)?;
        }

        if current_count == target_workers {
            println!(
                "{} Pool already has {} running workers.",
                "OK".green().bold(),
                current_count
            );
            return Ok(());
        }

        // 2. Identify a running coordinator (needed for worker services).
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator") && i.is_running());
        if coordinator.is_none() && config.with_coordinator {
            println!(
                "{} Running coordinator not found, but configuration expects one.",
                "Warning:".yellow()
            );
        }

        if target_workers > current_count {
            // SCALE UP
            let to_add = target_workers - current_count;
            println!(
                "{} Adding {} workers...",
                "Scaling up:".cyan(),
                to_add.to_string().bright_white()
            );

            // A TaskHandler-based custom worker has no legacy execution mode.
            // Fail before creating VMs if its stock coordinator is unavailable.
            if worker_binary_path.is_some() {
                let coordinator = coordinator.ok_or_else(|| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Custom workers require a running coordinator",
                    ))
                })?;
                if !self.check_coordinator_status(coordinator, zone) {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "Custom workers require a healthy coordinator",
                    )));
                }
            }

            if config.worker_attestation.is_some() && worker_binary_path.is_none() {
                return Err(HailError::InvalidFormat(
                    "worker_attestation requires a resolved custom worker binary".into(),
                ));
            }

            // Resolve and attest the exact custom bytes before staging or VM creation.
            let worker_bin = if let Some(ref wb_path) = worker_binary_path {
                let path = std::path::PathBuf::from(wb_path);
                if !path.is_file() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Worker binary not found at: {}", wb_path),
                    )));
                }
                Some(path)
            } else {
                None
            };
            let attested_worker = match (&worker_bin, config.worker_attestation.as_deref()) {
                (Some(binary), Some(sidecar)) => {
                    Some(crate::cloud::attestation::validate_worker_artifact(
                        binary,
                        std::path::Path::new(sidecar),
                    )?)
                }
                (Some(_), None) => {
                    return Err(HailError::InvalidFormat(
                        "custom worker scale requires --worker-attestation or worker_attestation in the pool profile".into(),
                    ));
                }
                _ => None,
            };

            // Stock and legacy custom paths retain their build/deployment behavior.
            let binary = if attested_worker.is_none() {
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
                Some(self.locate_binary(binary_path.clone())?)
            } else {
                None
            };

            let worker_bootstrap = if let Some(ref artifact) = attested_worker {
                let db_path = config.pool_db_path.as_deref().ok_or_else(|| {
                    HailError::InvalidFormat(
                        "attested custom workers require pool_db_path for fail-closed GCS staging"
                            .into(),
                    )
                })?;
                let staged = self.stage_attested_worker_to_gcs(artifact, db_path)?;
                Some(crate::cloud::attestation::ExpectedWorkerBootstrap::new(
                    staged,
                ))
            } else {
                None
            };

            // Only mutate stale infrastructure after validation and immutable
            // custom-worker staging have both succeeded.
            if !stale_workers.is_empty() {
                println!(
                    "{} Removing {} non-running worker(s) before scaling...",
                    "Cleanup:".cyan(),
                    stale_workers.len()
                );
                self.provider
                    .delete_instances(&stale_workers, zone, &project_id)?;
            }

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
                    startup_script: if let Some(bootstrap) = worker_bootstrap.as_ref() {
                        startup::generate_attested_worker_startup_script(bootstrap, name)?
                    } else {
                        startup::generate_worker_startup_script(None, name)
                    },
                    spot: config.spot,
                    network: config.network.clone(),
                    subnet: config.subnet.clone(),
                    public_ip: config.public_ip,
                    project_id: project_id.clone(),
                    service_account: config.service_account.clone(),
                });

                existing_indices.push(next_idx);
            }

            // Capture the generation boundary before VM creation. A same-name
            // registry entry from a deleted VM cannot satisfy readiness.
            let create_started = std::time::Instant::now();
            self.provider.create_instances(&new_instances)?;
            println!("{} Created {} new instances.", "OK".green().bold(), to_add);

            // Discover the immutable generations created for these names before
            // accepting any completion signal. Name and internal IP can both be
            // reused immediately after deletion.
            let updated_instances = self.provider.list_instances(name)?;
            let new_worker_instances: Vec<Instance> = new_instances
                .iter()
                .map(|requested| {
                    updated_instances
                        .iter()
                        .find(|instance| instance.name == requested.name)
                        .cloned()
                        .ok_or_else(|| {
                            HailError::Io(std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                format!(
                                    "new instance {} was not visible after creation",
                                    requested.name
                                ),
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(instance) = new_worker_instances
                .iter()
                .find(|instance| instance.id.is_empty())
            {
                return Err(HailError::InvalidFormat(format!(
                    "new instance {} has no immutable generation ID",
                    instance.name
                )));
            }

            println!("{}", "Waiting for new instances to be ready...".dimmed());
            for instance in &new_worker_instances {
                self.wait_for_startup_complete(instance, zone, worker_bootstrap.as_ref(), 300)?;
            }

            // Attested workers are fully installed by their startup scripts.
            // The successful path performs no per-worker SCP or service rewrite.
            if let Some(ref bootstrap) = worker_bootstrap {
                let coordinator = coordinator.expect("custom coordinator checked above");
                let expected_names: Vec<String> = new_instances
                    .iter()
                    .map(|instance| instance.name.clone())
                    .collect();
                self.wait_for_attested_workers_ready(
                    coordinator,
                    &expected_names,
                    bootstrap,
                    zone,
                    create_started,
                    300,
                )?;
            } else if let Some(ref deploy_bin) = worker_bin {
                println!("{}", "Deploying custom worker binary via SCP...".dimmed());
                self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
                let coord_ip = coordinator.and_then(|coord| coord.ip()).ok_or_else(|| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Custom workers require a running coordinator",
                    ))
                })?;
                self.start_worker_services(&new_worker_instances, coord_ip, zone)?;
            } else if let Some(coord) = coordinator {
                let deploy_bin = binary.as_ref().expect("stock binary resolved");
                if let Some(coord_ip) = coord.ip() {
                    if self.check_coordinator_status(coord, zone) {
                        println!("{}", "Deploying binary via coordinator...".dimmed());
                        self.propagate_binary_from_coordinator(
                            coord_ip,
                            &new_worker_instances,
                            zone,
                        )?;
                    } else {
                        println!(
                            "{}",
                            "Coordinator not running, deploying via SCP...".dimmed()
                        );
                        self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
                        self.start_worker_services(&new_worker_instances, coord_ip, zone)?;
                    }
                } else {
                    self.deploy_binary(deploy_bin, &new_worker_instances, zone)?;
                }
            } else {
                self.deploy_binary(
                    binary.as_ref().expect("stock binary resolved"),
                    &new_worker_instances,
                    zone,
                )?;
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

    /// Wait until the coordinator and systemd both attest the exact new fleet.
    fn wait_for_attested_workers_ready(
        &self,
        coordinator: &Instance,
        expected_names: &[String],
        bootstrap: &crate::cloud::attestation::ExpectedWorkerBootstrap,
        zone: &str,
        create_started: std::time::Instant,
        timeout_secs: u64,
    ) -> Result<()> {
        use crate::distributed::message::DashboardWorker;
        use std::time::{Duration, Instant};

        let started = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let mut last_diagnostic = "coordinator dashboard has not responded".to_string();

        loop {
            if started.elapsed() > timeout {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "timed out waiting for attested workers {:?}: {}",
                        expected_names, last_diagnostic
                    ),
                )));
            }

            match self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/workers", 3000) {
                Ok(json) => match serde_json::from_str::<Vec<DashboardWorker>>(&json) {
                    Ok(workers) => {
                        let generation_age = create_started.elapsed();
                        let missing: Vec<&str> = expected_names
                            .iter()
                            .filter(|expected| {
                                !workers.iter().any(|worker| {
                                    worker.worker_id == expected.as_str()
                                        && attested_dashboard_worker_is_ready(
                                            worker,
                                            &bootstrap.artifact.attestation.build_version,
                                            generation_age,
                                        )
                                })
                            })
                            .map(String::as_str)
                            .collect();
                        if missing.is_empty() {
                            // The numeric-ID startup record already proved the
                            // exact deployment, artifact SHA, and active service.
                            println!(
                                "{} Exact attested worker generation is active ({})",
                                "OK".green().bold(),
                                bootstrap.deployment_id
                            );
                            return Ok(());
                        } else {
                            last_diagnostic = format!(
                                "missing fresh post-create build_version {:?} for {:?}",
                                bootstrap.artifact.attestation.build_version, missing
                            );
                        }
                    }
                    Err(error) => last_diagnostic = format!("invalid dashboard response: {error}"),
                },
                Err(error) => last_diagnostic = error.to_string(),
            }

            std::thread::sleep(Duration::from_secs(3));
        }
    }

    /// Wait for generation-scoped startup completion on a specific instance.
    pub(crate) fn wait_for_startup_complete(
        &self,
        instance: &Instance,
        zone: &str,
        bootstrap: Option<&crate::cloud::attestation::ExpectedWorkerBootstrap>,
        timeout_secs: u64,
    ) -> Result<()> {
        use std::time::{Duration, Instant};

        if instance.id.is_empty() {
            return Err(HailError::InvalidFormat(format!(
                "instance {} has no immutable generation ID",
                instance.name
            )));
        }
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let mut last_diagnostic = "no guest startup status published".to_string();

        loop {
            if start.elapsed() > timeout {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timeout waiting for startup status on instance {} (ID {}): {}",
                        instance.name, instance.id, last_diagnostic
                    ),
                )));
            }

            match self.provider.get_startup_status(instance, zone) {
                Ok(Some(value)) => match startup_status_is_ready(instance, &value, bootstrap) {
                    Ok(true) => {
                        println!(
                            "  {} {} (ID {})",
                            "Ready:".dimmed(),
                            instance.name.cyan(),
                            instance.id
                        );
                        return Ok(());
                    }
                    Ok(false) => last_diagnostic = format!("non-matching startup status {value}"),
                    Err(error) => last_diagnostic = error,
                },
                Ok(None) => last_diagnostic = "no guest startup status published".to_string(),
                Err(error) => last_diagnostic = error.to_string(),
            }

            std::thread::sleep(Duration::from_secs(5));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::{InstanceSetup, NetworkInterface, ScalingConfig};
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct RecordingProvider {
        instances: Vec<Instance>,
        deleted: Arc<Mutex<Vec<String>>>,
    }

    impl CloudProvider for RecordingProvider {
        fn create_pool(&self, _: &PoolConfig) -> Result<()> {
            panic!("create_pool must not be called")
        }

        fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
            Ok(self.instances.clone())
        }

        fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }

        fn create_instances(&self, _: &[InstanceSetup]) -> Result<()> {
            panic!("create_instances must not be called")
        }

        fn delete_instances(&self, names: &[String], _: &str, _: &str) -> Result<()> {
            self.deleted.lock().unwrap().extend_from_slice(names);
            Ok(())
        }

        fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
            Ok(())
        }

        fn get_ssh_command(&self, _: &str, _: &str, _: &str) -> std::process::Command {
            let mut command = std::process::Command::new("sh");
            command.args(["-c", "true"]);
            command
        }
    }

    fn worker(name: &str, status: &str) -> Instance {
        Instance {
            id: format!("id-{name}"),
            name: name.into(),
            zone: "us-central1-a".into(),
            network_interfaces: vec![NetworkInterface {
                network_ip: "10.0.0.2".into(),
                network: None,
                subnetwork: None,
            }],
            status: status.into(),
            machine_type: None,
            scheduling: None,
        }
    }

    fn scaling_config() -> ScalingConfig {
        ScalingConfig {
            machine_type: "e2-standard-2".into(),
            workers: 1,
            spot: true,
            network: None,
            subnet: None,
            public_ip: true,
            manage_firewall: true,
            project: Some("project".into()),
            with_coordinator: true,
            pool_db_path: None,
            worker_binary: None,
            worker_attestation: None,
            service_account: None,
        }
    }

    #[test]
    fn scale_removes_terminated_workers_instead_of_counting_them() {
        let deleted = Arc::new(Mutex::new(Vec::new()));
        let manager = PoolManager::new(RecordingProvider {
            instances: vec![
                worker("demo-worker-0", "RUNNING"),
                worker("demo-worker-1", "TERMINATED"),
            ],
            deleted: deleted.clone(),
        });

        manager
            .scale(
                "demo",
                1,
                "us-central1-a",
                None,
                None,
                true,
                &scaling_config(),
            )
            .unwrap();

        assert_eq!(*deleted.lock().unwrap(), vec!["demo-worker-1"]);
    }

    #[test]
    fn readiness_rejects_stale_generation_wrong_build_and_dead_status() {
        let worker = crate::distributed::message::DashboardWorker {
            worker_id: "demo-worker-0".into(),
            status: "idle".into(),
            current_batch_size: None,
            max_batch_capacity: None,
            last_seen_secs: 1.0,
            telemetry: None,
            total_items: 0,
            tasks_completed: 0,
            current_task: None,
            build_version: Some("build-a".into()),
            effective_status: None,
        };
        assert!(attested_dashboard_worker_is_ready(
            &worker,
            "build-a",
            std::time::Duration::from_secs(10),
        ));
        assert!(!attested_dashboard_worker_is_ready(
            &worker,
            "build-b",
            std::time::Duration::from_secs(10),
        ));
        assert!(!attested_dashboard_worker_is_ready(
            &worker,
            "build-a",
            std::time::Duration::from_millis(500),
        ));
        let mut dead = worker;
        dead.status = "suspected_dead".into();
        assert!(!attested_dashboard_worker_is_ready(
            &dead,
            "build-a",
            std::time::Duration::from_secs(10),
        ));
    }

    #[test]
    fn same_name_startup_status_rejects_old_instance_and_deployment_generations() {
        let (_, _, attestation) = write_attested_fixture();
        let bootstrap = crate::cloud::attestation::ExpectedWorkerBootstrap {
            artifact: crate::cloud::attestation::StagedWorkerArtifact {
                gcs_url: "gs://bucket/fresh-worker".into(),
                attestation,
            },
            deployment_id: "fresh-generation".into(),
        };
        let mut replacement = worker("demo-worker-0", "RUNNING");
        replacement.id = "222".into();

        let old_instance = serde_json::json!({
            "status": "ready",
            "instance_id": "111",
            "deployment_id": "stale-generation",
            "sha256": bootstrap.artifact.attestation.sha256,
            "service_active": true,
        });
        assert!(!startup_status_is_ready(
            &replacement,
            &old_instance.to_string(),
            Some(&bootstrap),
        )
        .unwrap());

        let old_deployment = serde_json::json!({
            "status": "ready",
            "instance_id": "222",
            "deployment_id": "stale-generation",
            "sha256": bootstrap.artifact.attestation.sha256,
            "service_active": true,
        });
        assert!(!startup_status_is_ready(
            &replacement,
            &old_deployment.to_string(),
            Some(&bootstrap),
        )
        .unwrap());

        let fresh = serde_json::json!({
            "status": "ready",
            "instance_id": "222",
            "deployment_id": "fresh-generation",
            "sha256": bootstrap.artifact.attestation.sha256,
            "service_active": true,
        });
        assert!(
            startup_status_is_ready(&replacement, &fresh.to_string(), Some(&bootstrap),).unwrap()
        );
    }

    #[derive(Clone)]
    struct CustomScaleProvider {
        list_calls: Arc<std::sync::atomic::AtomicUsize>,
        staged: Arc<std::sync::atomic::AtomicUsize>,
        setups: Arc<Mutex<Vec<InstanceSetup>>>,
        ssh_commands: Arc<Mutex<Vec<String>>>,
        build_version: String,
    }

    impl CloudProvider for CustomScaleProvider {
        fn create_pool(&self, _: &PoolConfig) -> Result<()> {
            panic!("create_pool must not be called")
        }
        fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
            let call = self
                .list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut instances = vec![worker("demo-coordinator", "RUNNING")];
            if call > 0 {
                instances.push(worker("demo-worker-0", "RUNNING"));
            }
            Ok(instances)
        }
        fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn create_instances(&self, instances: &[InstanceSetup]) -> Result<()> {
            self.setups.lock().unwrap().extend_from_slice(instances);
            Ok(())
        }
        fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn stage_worker_artifact(
            &self,
            artifact: &crate::cloud::attestation::ValidatedWorkerArtifact,
            _: &str,
        ) -> Result<crate::cloud::attestation::StagedWorkerArtifact> {
            self.staged
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::cloud::attestation::StagedWorkerArtifact {
                gcs_url: format!(
                    "gs://bucket/pool/bin/custom-workers/{}/{}/genohype-worker",
                    artifact.attestation.revision, artifact.attestation.sha256
                ),
                attestation: artifact.attestation.clone(),
            })
        }
        fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
            panic!("attested scale must not upload to workers")
        }
        fn get_ssh_command(&self, _: &str, _: &str, command: &str) -> std::process::Command {
            self.ssh_commands.lock().unwrap().push(command.to_string());
            let output = if command.contains("/api/dashboard/workers") {
                format!(
                    r#"[{{"worker_id":"demo-worker-0","status":"idle","current_batch_size":null,"max_batch_capacity":null,"last_seen_secs":0.0,"telemetry":null,"total_items":0,"tasks_completed":0,"current_task":null,"build_version":"{}"}}]"#,
                    self.build_version
                )
            } else if command.contains("/status") {
                r#"{"pending_tasks":0}"#.to_string()
            } else {
                String::new()
            };
            let mut shell = std::process::Command::new("sh");
            shell.args(["-c", &format!("printf '%s' '{}'", output)]);
            shell
        }
        fn get_startup_status(&self, instance: &Instance, _: &str) -> Result<Option<String>> {
            let setups = self.setups.lock().unwrap();
            let script = setups
                .iter()
                .find(|setup| setup.name == instance.name)
                .map(|setup| setup.startup_script.as_str())
                .ok_or_else(|| HailError::InvalidFormat("missing recorded setup".into()))?;
            let extract = |prefix: &str| {
                script
                    .lines()
                    .find_map(|line| line.strip_prefix(prefix))
                    .and_then(|value| value.strip_suffix('\''))
                    .map(str::to_string)
            };
            let deployment_id = extract("readonly DEPLOYMENT_ID='")
                .ok_or_else(|| HailError::InvalidFormat("missing deployment ID".into()))?;
            let sha256 = extract("readonly EXPECTED_SHA256='")
                .ok_or_else(|| HailError::InvalidFormat("missing artifact SHA".into()))?;
            Ok(Some(
                serde_json::json!({
                    "status": "ready",
                    "instance_id": instance.id,
                    "deployment_id": deployment_id,
                    "sha256": sha256,
                    "service_active": true,
                })
                .to_string(),
            ))
        }
    }

    fn write_attested_fixture() -> (
        tempfile::NamedTempFile,
        tempfile::NamedTempFile,
        crate::cloud::attestation::WorkerArtifactAttestation,
    ) {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let revision = "a".repeat(40);
        let build_identity = format!("e2e:{revision}:fixture-a");
        let build_version = build_identity.clone();
        let mut bytes = vec![0_u8; 20];
        bytes[..6].copy_from_slice(b"\x7fELF\x02\x01");
        bytes[18..20].copy_from_slice(&62_u16.to_le_bytes());
        bytes.extend_from_slice(build_version.as_bytes());
        let attestation = crate::cloud::attestation::WorkerArtifactAttestation {
            schema_version: 1,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            size: bytes.len() as u64,
            revision,
            build_identity,
            build_version,
            platform: "linux".into(),
            arch: "x86_64".into(),
        };
        let mut binary = tempfile::NamedTempFile::new().unwrap();
        binary.write_all(&bytes).unwrap();
        let mut sidecar = tempfile::NamedTempFile::new().unwrap();
        serde_json::to_writer(&mut sidecar, &attestation).unwrap();
        (binary, sidecar, attestation)
    }

    #[test]
    fn create_rejects_attestation_byte_mismatch_before_provisioning() {
        use std::io::Write;

        let (mut binary, sidecar, _) = write_attested_fixture();
        binary.write_all(b"changed-after-attestation").unwrap();
        let manager = PoolManager::new(RecordingProvider {
            instances: Vec::new(),
            deleted: Arc::new(Mutex::new(Vec::new())),
        });
        let config = PoolConfig {
            name: "demo".into(),
            worker_count: 1,
            machine_type: "e2-standard-2".into(),
            zone: "us-central1-a".into(),
            spot: false,
            project_id: "project".into(),
            network: None,
            subnet: None,
            public_ip: true,
            manage_firewall: true,
            with_coordinator: true,
            wireguard: None,
            pool_db_path: Some("gs://bucket/pool/ops.db".into()),
            binary_gcs_url: None,
            worker_binary_gcs_url: None,
            worker_bootstrap: None,
            service_account: None,
            coordinator_service_account: None,
        };
        let error = manager
            .create(
                &config,
                false,
                true,
                Some(binary.path().to_string_lossy().into_owned()),
                Some(sidecar.path().to_string_lossy().into_owned()),
            )
            .unwrap_err();
        assert!(error.to_string().contains("size mismatch"));
    }

    #[test]
    fn custom_scale_stages_once_and_performs_no_worker_upload_or_service_rewrite() {
        let (binary, sidecar, attestation) = write_attested_fixture();
        let provider = CustomScaleProvider {
            list_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            setups: Arc::new(Mutex::new(Vec::new())),
            ssh_commands: Arc::new(Mutex::new(Vec::new())),
            build_version: attestation.build_version.clone(),
        };
        let staged = provider.staged.clone();
        let setups = provider.setups.clone();
        let ssh_commands = provider.ssh_commands.clone();
        let manager = PoolManager::new(provider);
        let mut config = scaling_config();
        config.pool_db_path = Some("gs://bucket/pool/ops.db".into());
        config.worker_attestation = Some(sidecar.path().to_string_lossy().into_owned());

        manager
            .scale(
                "demo",
                1,
                "us-central1-a",
                None,
                Some(binary.path().to_string_lossy().into_owned()),
                true,
                &config,
            )
            .unwrap();

        assert_eq!(staged.load(std::sync::atomic::Ordering::SeqCst), 1);
        let setups = setups.lock().unwrap();
        assert_eq!(setups.len(), 1);
        assert!(setups[0].startup_script.contains(&attestation.sha256));
        assert!(setups[0].startup_script.contains("gcloud storage cp"));
        let commands = ssh_commands.lock().unwrap().join("\n");
        assert!(!commands.contains("systemctl restart genohype-worker"));
        assert!(!commands.contains("cat > /etc/systemd/system/genohype-worker"));
        assert!(!commands.contains("/var/lib/genohype/deployments/"));
    }

    #[test]
    fn create_rejects_custom_worker_without_coordinator_before_provisioning() {
        let manager = PoolManager::new(RecordingProvider {
            instances: Vec::new(),
            deleted: Arc::new(Mutex::new(Vec::new())),
        });
        let config = PoolConfig {
            name: "demo".into(),
            worker_count: 1,
            machine_type: "e2-standard-2".into(),
            zone: "us-central1-a".into(),
            spot: true,
            project_id: "project".into(),
            network: None,
            subnet: None,
            public_ip: true,
            manage_firewall: true,
            with_coordinator: false,
            wireguard: None,
            pool_db_path: None,
            binary_gcs_url: None,
            worker_binary_gcs_url: None,
            worker_bootstrap: None,
            service_account: None,
            coordinator_service_account: None,
        };

        let error = manager
            .create(&config, false, true, Some("custom-worker".into()), None)
            .unwrap_err();
        assert!(error.to_string().contains("require a coordinator"));
    }
}
