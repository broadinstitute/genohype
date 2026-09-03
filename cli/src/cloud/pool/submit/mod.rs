//! Job submission orchestration.

mod legacy_runner;
mod preflight;
mod readiness;

pub use legacy_runner::WorkerMessage;
pub use preflight::{list_completed_markers, read_completed_checkpoint};

use self::readiness::require_worker_readiness;
use super::PoolManager;
use crate::benchmark::BenchmarkReport;
use crate::cloud::{CloudProvider, Instance};
use crate::HailError;
use crate::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

const WORKER_FRESHNESS_SECS: f64 = 15.0;
const LOG_TAIL_UNHEALTHY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn worker_is_fresh(worker: &crate::distributed::message::DashboardWorker) -> bool {
    matches!(worker.status.as_str(), "active" | "idle")
        && worker.last_seen_secs.is_finite()
        && worker.last_seen_secs >= 0.0
        && worker.last_seen_secs <= WORKER_FRESHNESS_SECS
}

fn validate_submit_contract(
    force_redeploy: bool,
    worker_binary_path: Option<&str>,
    detach: bool,
    output: crate::cli::PoolSubmitOutput,
) -> Result<()> {
    if output == crate::cli::PoolSubmitOutput::Json && !detach {
        return Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--output json requires explicit --detach so stdout remains exactly one JSON acknowledgement object",
        )));
    }
    if force_redeploy && worker_binary_path.is_some() {
        return Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Custom-worker redeployment is not supported by 'pool submit' because submit cannot safely replace the attested artifact. Run 'genohype pool update-binary' with the configured --worker-attestation, verify readiness, then submit without --redeploy-binary.",
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmissionMode {
    Standard,
    ZeroWorkerCustom,
}

fn classify_submission(
    pool_name: &str,
    use_distributed: bool,
    total_workers: usize,
    is_custom_job: bool,
) -> Result<SubmissionMode> {
    if use_distributed && total_workers == 0 {
        if is_custom_job {
            return Ok(SubmissionMode::ZeroWorkerCustom);
        }
        return Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "No workers available for pool '{}'. Either:\n\
                 - Scale up workers: genohype pool scale {} --workers N\n\
                 - Use --autoscale to automatically scale workers for the job",
                pool_name, pool_name
            ),
        )));
    }

    Ok(SubmissionMode::Standard)
}

fn require_idle_summary(summary_json: &str) -> Result<()> {
    let summary: crate::distributed::message::DashboardSummary = serde_json::from_str(summary_json)
        .map_err(|error| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid coordinator summary: {error}"),
            ))
        })?;
    if summary.idle {
        Ok(())
    } else {
        Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Coordinator already has a job running. Use --force to supersede.",
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailHealth {
    Healthy,
    NoWorkers,
    CoordinatorUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailFailure {
    NoWorkers,
    CoordinatorUnavailable,
    StreamEnded,
}

#[derive(Default)]
struct TailWatchdog {
    unhealthy_since: Option<Instant>,
}

impl TailWatchdog {
    fn observe(&mut self, health: TailHealth, now: Instant) -> Option<TailFailure> {
        if health == TailHealth::Healthy {
            self.unhealthy_since = None;
            return None;
        }

        let since = self.unhealthy_since.get_or_insert(now);
        if now.duration_since(*since) < LOG_TAIL_UNHEALTHY_TIMEOUT {
            return None;
        }

        Some(match health {
            TailHealth::NoWorkers => TailFailure::NoWorkers,
            TailHealth::CoordinatorUnavailable => TailFailure::CoordinatorUnavailable,
            TailHealth::Healthy => unreachable!(),
        })
    }
}

impl<P: CloudProvider + Sync> PoolManager<P> {
    fn ensure_coordinator_idle(&self, coordinator: &Instance, zone: &str) -> Result<()> {
        let summary_json = self
            .fetch_coordinator_api(coordinator, zone, "/api/dashboard/summary", 3000)
            .map_err(|error| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!("Could not verify that the coordinator is idle: {error}"),
                ))
            })?;
        require_idle_summary(&summary_json)
    }
    /// Start coordinator in idle mode (binary already deployed via startup script or needs deployment).
    pub(crate) fn start_idle_coordinator(
        &self,
        pool_name: &str,
        zone: &str,
        _skip_build: bool,
        pool_db_path: Option<&str>,
        binary_deployed_via_startup: bool,
        worker_binary: Option<&std::path::Path>,
        worker_deployed_via_startup: bool,
        pool_config: &crate::cloud::PoolConfig,
    ) -> Result<()> {
        // Get coordinator instance
        let instances = self.provider.list_instances(pool_name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "No coordinator instance found for pool '{}'. Pool was not created with --with-coordinator?",
                        pool_name
                    ),
                ))
            })?;

        let coord_ip = coordinator.ip().unwrap_or("localhost");

        // Binary deployment and coordinator startup: different paths for startup-script vs SSH
        if binary_deployed_via_startup {
            println!(
                "{} Binary deployed and service started via startup script (zero SSH)",
                "OK".green().bold()
            );

            // Wait for coordinator API to be reachable (it started from startup script)
            print!(
                "{}",
                "  Waiting for coordinator API to be reachable".dimmed()
            );
            use std::io::Write;
            let mut ready = false;
            for _ in 0..15 {
                std::io::stdout().flush().ok();
                if self.check_coordinator_status(coordinator, zone) {
                    ready = true;
                    break;
                }
                print!(".");
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
            println!();

            if !ready {
                println!(
                    "{} Coordinator API did not become reachable. It may still be starting.",
                    "Warning:".yellow()
                );
            } else {
                println!("{} Coordinator is running", "OK".green().bold());
            }
        } else {
            // Fallback: deploy via SCP
            let binary = self.locate_binary(None)?;
            println!(
                "{} Deploying binary to coordinator via SCP...",
                "Setup:".cyan()
            );
            self.deploy_binary(&binary, &[coordinator.clone()], zone)?;

            // Start coordinator in idle mode via SSH
            println!(
                "{} Starting coordinator in idle mode on {}...",
                "Setup:".cyan(),
                coordinator.name.cyan()
            );

            let backup_arg = pool_db_path
                .map(|b| format!(" --backup-path {}", b))
                .unwrap_or_default();
            let identity_args = self.coordinator_pool_args(pool_config);
            let coord_cmd = format!(
                "sudo bash -c 'cat > /etc/systemd/system/genohype-coordinator.service << EOF
[Unit]
Description=Genohype Coordinator
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db{}{}
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
EOF
' && sudo systemctl daemon-reload && sudo systemctl enable --now genohype-coordinator",
                backup_arg, identity_args
            );

            let status = self
                .provider
                .get_ssh_command(&coordinator.name, zone, &coord_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to start coordinator service in idle mode",
                )));
            }

            std::thread::sleep(std::time::Duration::from_secs(2));
            println!("{} Coordinator started in idle mode", "OK".green().bold());
        }

        println!("  Dashboard: http://{}:3000/dashboard", coord_ip);
        println!(
            "  Submit jobs with: genohype pool submit {} -- <command>",
            pool_name
        );

        // Workers: binary already deployed via startup script if GCS staging worked
        let workers: Vec<_> = instances
            .iter()
            .filter(|i| i.name.contains("-worker-"))
            .cloned()
            .collect();

        if !workers.is_empty() {
            if let Some(custom_worker) = worker_binary {
                if pool_config.worker_bootstrap.is_some() {
                    // The fail-closed attested startup path has already verified,
                    // atomically installed, and enabled the compatible service.
                    // Rewriting it here would discard deployment identity.
                    println!(
                        "{} {} attested custom worker(s) started by VM bootstrap",
                        "OK".green().bold(),
                        workers.len()
                    );
                } else {
                    if !worker_deployed_via_startup {
                        println!("{}", "Deploying custom worker via SCP...".dimmed());
                        self.deploy_binary(custom_worker, &workers, zone)?;
                    }
                    // Preserve the legacy custom-worker recovery behavior.
                    self.start_worker_services(&workers, coord_ip, zone)?;
                    println!(
                        "{} Custom binary deployed and {} worker(s) started",
                        "OK".green().bold(),
                        workers.len()
                    );
                }
            } else if worker_deployed_via_startup {
                self.start_worker_services(&workers, coord_ip, zone)?;
                println!(
                    "{} {} workers verified and restarted",
                    "OK".green().bold(),
                    workers.len()
                );
            } else {
                println!(
                    "{}",
                    "Deploying binary and starting workers via coordinator...".dimmed()
                );
                self.propagate_binary_from_coordinator(coord_ip, &workers, zone)?;
                println!(
                    "{} Binary deployed and {} worker(s) started",
                    "OK".green().bold(),
                    workers.len()
                );
            }
        }

        Ok(())
    }

    ///
    /// This will:
    /// 1. Locate or validate the Linux binary
    /// 2. Upload the binary to all workers in parallel
    /// 3. Execute the command on each worker with partition slicing
    /// 4. Stream logs and aggregate benchmark results
    ///
    /// Automatically uses coordinator/worker pattern when a coordinator exists,
    /// providing resilient distributed processing with automatic retry on Spot
    /// instance preemption.
    ///
    /// If `autoscale` is true and `config` is provided, automatically scales
    /// workers up before the job and down to 0 after the job completes.
    pub(crate) fn submit(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        worker_binary_path: Option<String>,
        auto_stop: bool,
        force_redeploy: bool,
        force: bool,
        detach: bool,
        output: crate::cli::PoolSubmitOutput,
        autoscale: bool,
        skip_build: bool,
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        config: Option<&crate::cloud::ScalingConfig>,
        command: &[String],
    ) -> Result<()> {
        let human_output = output == crate::cli::PoolSubmitOutput::Human;
        macro_rules! println {
            () => {{
                if human_output {
                    std::println!();
                }
            }};
            ($($arg:tt)*) => {{
                if human_output {
                    std::println!($($arg)*);
                }
            }};
        }

        // Validate all machine-output and lifecycle authorization contracts
        // before parsing local inputs, building, discovery, or mutation.
        validate_submit_contract(
            force_redeploy,
            worker_binary_path.as_deref(),
            detach,
            output,
        )?;

        // Validate the command before any explicitly requested lifecycle mutation.
        // The fully enriched JobSpec is prepared later, after read-only discovery.
        Self::parse_command_to_job_spec(command)?;

        if detach && (autoscale || auto_stop) {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--detach cannot be combined with --autoscale or --auto-stop because those cleanups are client-owned",
            )));
        }

        // A normal coordinator-backed submit never builds. Build is authorized
        // only by an explicit lifecycle option.
        if (force_redeploy || autoscale) && !skip_build && !self.has_bundled_binary() {
            let features: Vec<&str> =
                if command.len() >= 2 && command[0] == "export" && command[1] == "clickhouse" {
                    vec!["clickhouse"]
                } else if command.len() >= 2 && command[0] == "ingest" && command[1] == "manhattan"
                {
                    vec!["clickhouse"]
                } else {
                    vec![]
                };
            Self::build_linux_binary(&features)?;
        }

        // Handle autoscaling
        if autoscale {
            let pool_config = config.ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Autoscaling requires pool configuration. Ensure pool is defined in config.toml",
                ))
            })?;

            // Scale up to target workers. Autoscale-to-zero is not a durable
            // registration request because its lifecycle would immediately run
            // the autoscale cleanup path.
            let target = pool_config.workers;
            if target == 0 {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Autoscaling requires a positive worker target",
                )));
            }
            println!(
                "{} Autoscaling up to {} workers...",
                "Setup:".cyan(),
                target.to_string().bright_white()
            );

            // Pass skip_build=true because we handled the build logic above
            self.scale(
                name,
                target,
                zone,
                binary_path.clone(),
                worker_binary_path.clone(),
                true,
                pool_config,
            )?;
        }

        // Run the actual job
        let result = self.submit_internal(
            name,
            zone,
            binary_path.clone(),
            worker_binary_path.clone(),
            auto_stop,
            force_redeploy,
            force,
            detach,
            output,
            skip_build,
            batch_size,
            memory_weight_mb,
            config,
            command,
        );

        // Handle autoscaling down
        if autoscale {
            if let Some(pool_config) = config {
                println!("\n{} Autoscaling down to 0 workers...", "Cleanup:".cyan());
                // Ignore errors during scale down to ensure we return the job result
                if let Err(e) = self.scale(
                    name,
                    0,
                    zone,
                    binary_path,
                    worker_binary_path,
                    true,
                    pool_config,
                ) {
                    eprintln!("{} Failed to scale down: {}", "Warning:".yellow(), e);
                }
            }
        }

        result
    }

    /// Internal submit implementation (called by submit, handles the actual job).
    pub(crate) fn submit_internal(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        worker_binary_path: Option<String>,
        auto_stop: bool,
        force_redeploy: bool,
        force: bool,
        detach: bool,
        output: crate::cli::PoolSubmitOutput,
        skip_build: bool,
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        config: Option<&crate::cloud::ScalingConfig>,
        command: &[String],
    ) -> Result<()> {
        let human_output = output == crate::cli::PoolSubmitOutput::Human;
        macro_rules! println {
            () => {{
                if human_output {
                    std::println!();
                }
            }};
            ($($arg:tt)*) => {{
                if human_output {
                    std::println!($($arg)*);
                }
            }};
        }

        let (_, parsed_job_spec, _, _) = Self::parse_command_to_job_spec(command)?;
        let is_custom_job = matches!(
            parsed_job_spec,
            crate::distributed::message::JobSpec::Custom { .. }
        );
        let has_custom_worker = worker_binary_path.is_some();

        // 1. Get running instances using read-only provider discovery.
        println!("{}", "Fetching instance list...".dimmed());
        let instances = self.provider.list_instances(name)?;
        let running: Vec<_> = instances.into_iter().filter(|i| i.is_running()).collect();

        if running.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No running instances found for pool '{}'. Create with: genohype pool create {}",
                    name, name
                ),
            )));
        }

        // Separate coordinator from workers
        let (coordinators, workers): (Vec<_>, Vec<_>) = running
            .into_iter()
            .partition(|i| i.name.ends_with("-coordinator"));

        let coordinator = coordinators.into_iter().next();
        let total_workers = workers.len();

        // Auto-detect distributed mode: use coordinator/worker pattern when coordinator exists.
        // A configured coordinator-backed pool must never silently fall back to legacy SSH.
        let use_distributed = coordinator.is_some();
        if !use_distributed && config.map(|value| value.with_coordinator).unwrap_or(false) {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Coordinator instance is missing; submit did not create or repair it",
            )));
        }

        let submission_mode =
            classify_submission(name, use_distributed, total_workers, is_custom_job)?;
        let zero_worker_custom = submission_mode == SubmissionMode::ZeroWorkerCustom;

        println!(
            "{} {} running worker(s){}",
            "Found".green(),
            total_workers.to_string().bright_white(),
            if let Some(ref c) = coordinator {
                format!(", 1 coordinator ({})", c.name.cyan())
            } else {
                String::new()
            }
        );

        let coordinator_running = coordinator
            .as_ref()
            .map(|coord| self.check_coordinator_status(coord, zone))
            .unwrap_or(false);

        // Coordinator-backed normal submit is registration-only. An unavailable
        // coordinator fails closed instead of authorizing implicit repair.
        if use_distributed && !coordinator_running && !force_redeploy {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "Coordinator is not healthy; submit did not repair or restart it. Use --redeploy-binary explicitly after verifying lifecycle impact.",
            )));
        }

        // Zero-worker custom registration is deliberately narrower than deployment.
        if zero_worker_custom {
            if force {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Zero-worker custom submission does not allow --force",
                )));
            }
            if force_redeploy {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Zero-worker custom submission does not allow --redeploy-binary",
                )));
            }
            if auto_stop {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Zero-worker custom submission does not allow --auto-stop",
                )));
            }
            self.ensure_coordinator_idle(coordinator.as_ref().unwrap(), zone)?;
        } else if use_distributed && force_redeploy && !force {
            if !coordinator_running {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Cannot prove the coordinator is idle before --redeploy-binary; retry when healthy or pair with --force",
                )));
            }
            self.ensure_coordinator_idle(coordinator.as_ref().unwrap(), zone)?;
        }

        let should_deploy = !use_distributed || force_redeploy;
        if !use_distributed {
            println!(
                "{} Coordinator-less legacy submit explicitly deploys binaries and is not lifecycle-free.",
                "Legacy mode:".yellow().bold()
            );
            if !skip_build && !self.has_bundled_binary() {
                let features: Vec<&str> = if (command.len() >= 2
                    && command[0] == "export"
                    && command[1] == "clickhouse")
                    || (command.len() >= 2 && command[0] == "ingest" && command[1] == "manhattan")
                {
                    vec!["clickhouse"]
                } else {
                    vec![]
                };
                Self::build_linux_binary(&features)?;
            }
        }

        // Local artifact paths are inspected only after lifecycle deployment was
        // explicitly authorized (or for the explicit legacy compatibility path).
        let binary = if should_deploy {
            Some(self.locate_binary(binary_path)?)
        } else {
            None
        };
        let worker_binary = if should_deploy {
            if let Some(ref wb_path) = worker_binary_path {
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
            }
        } else {
            None
        };

        if should_deploy {
            let binary = binary.as_ref().expect("deployment path resolves binary");

            // Try to stage binary to GCS for fast updates
            let pool_db_path = config.and_then(|c| c.pool_db_path.as_deref());
            let staging_url = if let Some(db_path) = pool_db_path {
                Some(self.stage_binary_to_gcs(binary, db_path, "genohype-coordinator")?)
            } else {
                None
            };

            if use_distributed {
                let coord = coordinator.as_ref().unwrap();
                let coord_ip = coord.ip().ok_or_else(|| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Coordinator {} has no internal IP", coord.name),
                    ))
                })?;

                // Stop any existing coordinator first
                println!("{}", "Stopping existing coordinator...".dimmed());
                let stop_cmd = "sudo systemctl stop genohype-coordinator";
                let stop_status = self
                    .provider
                    .get_ssh_command(&coord.name, zone, stop_cmd)
                    .status()
                    .map_err(HailError::Io)?;
                if !stop_status.success() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to stop the coordinator before explicit redeployment",
                    )));
                }
                std::thread::sleep(std::time::Duration::from_secs(1));

                // Deploy coordinator binary (always the stock genohype binary)
                if let Some(ref gcs_url) = staging_url {
                    println!("{}", "Deploying coordinator binary via GCS...".dimmed());
                    let update_coord_cmd = format!(
                        "gsutil cp {} /tmp/genohype && chmod +x /tmp/genohype && sudo mv /tmp/genohype /usr/local/bin/genohype",
                        gcs_url
                    );
                    let status = self
                        .provider
                        .get_ssh_command(&coord.name, zone, &update_coord_cmd)
                        .status()
                        .map_err(HailError::Io)?;
                    if !status.success() {
                        return Err(HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Failed to install the explicitly redeployed coordinator binary",
                        )));
                    }
                } else {
                    println!("{}", "Deploying coordinator binary via SCP...".dimmed());
                    self.deploy_binary(binary, &[coord.clone()], zone)?;
                }

                // Start coordinator service
                println!(
                    "{}",
                    "Starting coordinator service to serve binary/API...".dimmed()
                );
                let backup_arg = pool_db_path
                    .map(|b| format!(" --backup-path {}", b))
                    .unwrap_or_default();
                let identity_args = self.coordinator_scaling_args(name, zone, config);
                let coord_cmd = format!(
                    "sudo bash -c 'cat > /etc/systemd/system/genohype-coordinator.service << EOF
[Unit]
Description=Genohype Coordinator
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db{}{}
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
EOF
' && sudo systemctl daemon-reload && sudo systemctl restart genohype-coordinator",
                    backup_arg, identity_args
                );
                let status = self
                    .provider
                    .get_ssh_command(&coord.name, zone, &coord_cmd)
                    .status()
                    .map_err(HailError::Io)?;

                if !status.success() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to start coordinator service",
                    )));
                }

                std::thread::sleep(std::time::Duration::from_secs(2));

                if !self.check_coordinator_status(coord, zone) {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Coordinator failed to start",
                    )));
                }

                // Custom-worker redeploy is rejected by validate_submit_contract
                // before any artifact access or mutation. The remaining shared
                // redeploy path is therefore stock-only.
                if worker_binary.is_some() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Custom-worker redeploy must use 'genohype pool update-binary'",
                    )));
                }
                if let Some(ref gcs_url) = staging_url {
                    println!(
                        "{} Instructing workers to self-update via GCS...",
                        "API:".cyan()
                    );
                    let req = serde_json::json!({ "gcs_url": gcs_url });
                    let curl_cmd = format!(
                        "curl --fail-with-body --show-error --silent -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/update-fleet",
                        req
                    );
                    let status = self
                        .provider
                        .get_ssh_command(&coord.name, zone, &curl_cmd)
                        .status()
                        .map_err(HailError::Io)?;
                    if !status.success() {
                        return Err(HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Coordinator rejected the stock worker fleet update",
                        )));
                    }
                    println!(
                        "{} Binary update triggered for workers.",
                        "OK".green().bold()
                    );
                } else {
                    println!("{}", "Workers pulling binary from coordinator...".dimmed());
                    self.propagate_binary_from_coordinator(coord_ip, &workers, zone)?;
                    println!(
                        "{} Binary propagated to {} workers.",
                        "OK".green().bold(),
                        workers.len()
                    );
                }
            } else {
                // Non-distributed mode
                let all_nodes: Vec<_> = if let Some(ref c) = coordinator {
                    let mut nodes = workers.clone();
                    nodes.push(c.clone());
                    nodes
                } else {
                    workers.clone()
                };

                println!("{}", "Deploying binary to nodes via SCP...".dimmed());
                self.deploy_binary(binary, &all_nodes, zone)?;
                println!("{} Binary deployed to all nodes.", "OK".green().bold());
            }
        }

        // Redeployment explicitly authorizes service repair. Keep that mutation
        // inside the opt-in path; normal submit never reaches this block.
        if use_distributed && force_redeploy && !workers.is_empty() {
            let coord = coordinator.as_ref().expect("distributed coordinator");
            let coord_ip = coord.ip().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Coordinator {} has no internal IP", coord.name),
                ))
            })?;
            self.start_worker_services(&workers, coord_ip, zone)?;
            std::thread::sleep(std::time::Duration::from_secs(3));
        }

        // 4. Branch based on mode - use coordinator/worker pattern when coordinator exists
        if use_distributed {
            return self.submit_distributed(
                name,
                zone,
                coordinator.as_ref().unwrap(),
                &workers,
                command,
                auto_stop,
                force,
                detach,
                output,
                has_custom_worker,
                batch_size,
                memory_weight_mb,
                config,
            );
        }

        // Legacy mode: submit jobs with progress tracking
        println!("{}", "Submitting jobs (legacy mode)...".dimmed());
        let base_args = command.join(" ");
        let start_time = Instant::now();

        // Setup multi-progress display
        let multi_progress = MultiProgress::new();
        let progress_style = ProgressStyle::default_bar()
            .template("{prefix:.cyan} [{bar:30.cyan/blue}] {pos}/{len} partitions ({eta})")
            .unwrap()
            .progress_chars("█▓░");

        // Create progress bars for each worker (initially with length 0, updated on first progress)
        let worker_bars: Vec<ProgressBar> = (0..total_workers)
            .map(|i| {
                let pb = multi_progress.add(ProgressBar::new(0));
                pb.set_style(progress_style.clone());
                pb.set_prefix(format!("worker-{}", i));
                pb
            })
            .collect();

        // Total progress bar
        let total_bar = multi_progress.add(ProgressBar::new(0));
        total_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{prefix:.green.bold} [{bar:30.green/white}] {pos}/{len} partitions | {msg}",
                )
                .unwrap()
                .progress_chars("█▓░"),
        );
        total_bar.set_prefix("TOTAL");

        // Atomic counters for aggregate tracking
        let total_rows = Arc::new(AtomicUsize::new(0));
        let total_partitions_done = Arc::new(AtomicUsize::new(0));
        let total_partitions_expected = Arc::new(AtomicUsize::new(0));

        // Channel for receiving results from workers
        let (tx, rx) = mpsc::channel();

        // Spawn threads for each worker (legacy mode uses workers list)
        let handles: Vec<_> = workers
            .iter()
            .enumerate()
            .map(|(worker_id, inst)| {
                let inst_name = inst.name.clone();
                let inst_zone = inst.zone.clone();
                // Add --progress-json flag for machine-readable progress
                let args = format!(
                    "{} --worker-id {} --total-workers {} --progress-json",
                    base_args, worker_id, total_workers
                );
                let tx = tx.clone();

                // Build SSH command
                let remote_cmd = format!("/usr/local/bin/genohype {}", args);
                let mut cmd = self
                    .provider
                    .get_ssh_command(&inst_name, &inst_zone, &remote_cmd);
                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());

                std::thread::spawn(move || {
                    let result = Self::run_worker_job(worker_id, cmd, &tx);
                    if let Err(e) = result {
                        let _ = tx.send(WorkerMessage::Error {
                            worker_id,
                            message: e.to_string(),
                        });
                    }
                })
            })
            .collect();

        // Drop our sender so the channel closes when all workers are done
        drop(tx);

        // Process messages from workers
        let mut aggregate_report = BenchmarkReport::empty();
        let mut completed = 0;
        let mut errors = 0;
        let mut worker_partition_totals: Vec<usize> = vec![0; total_workers];

        for msg in rx {
            match msg {
                WorkerMessage::Log { worker_id, line } => {
                    // Use suspend to avoid interfering with progress bars
                    multi_progress.suspend(|| {
                        println!("[worker-{}] {}", worker_id, line.dimmed());
                    });
                }
                WorkerMessage::Progress { worker_id, update } => {
                    // Update worker's progress bar
                    if worker_id < worker_bars.len() {
                        let pb = &worker_bars[worker_id];
                        // Set total on first update (partitions_total might not be known initially)
                        if pb.length() != Some(update.partitions_total as u64) {
                            pb.set_length(update.partitions_total as u64);
                            // Track totals for overall progress
                            let old_total = worker_partition_totals[worker_id];
                            worker_partition_totals[worker_id] = update.partitions_total;
                            total_partitions_expected.fetch_add(
                                update.partitions_total.saturating_sub(old_total),
                                Ordering::Relaxed,
                            );
                            // Update total bar length
                            total_bar.set_length(
                                total_partitions_expected.load(Ordering::Relaxed) as u64
                            );
                        }
                        pb.set_position(update.partitions_done as u64);
                    }

                    // Update totals
                    total_rows.store(
                        total_rows.load(Ordering::Relaxed).max(update.rows),
                        Ordering::Relaxed,
                    );
                    total_partitions_done.store(
                        worker_bars.iter().map(|pb| pb.position() as usize).sum(),
                        Ordering::Relaxed,
                    );
                    total_bar.set_position(total_partitions_done.load(Ordering::Relaxed) as u64);

                    // Update throughput message
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rows = total_rows.load(Ordering::Relaxed);
                    if elapsed > 0.0 && rows > 0 {
                        total_bar.set_message(format!("{:.0} rows/sec", rows as f64 / elapsed));
                    }
                }
                WorkerMessage::Report { worker_id, report } => {
                    // Mark worker's bar as finished
                    if worker_id < worker_bars.len() {
                        worker_bars[worker_id].finish_with_message("done");
                    }
                    aggregate_report.merge(report);
                    completed += 1;
                }
                WorkerMessage::Error { worker_id, message } => {
                    if worker_id < worker_bars.len() {
                        worker_bars[worker_id].abandon_with_message("error");
                    }
                    multi_progress.suspend(|| {
                        eprintln!("[worker-{}] {} {}", worker_id, "Error:".red(), message);
                    });
                    errors += 1;
                }
                WorkerMessage::Complete { worker_id } => {
                    if worker_id < worker_bars.len() {
                        worker_bars[worker_id].finish_with_message("done");
                    }
                    completed += 1;
                }
            }
        }

        // Wait for all threads to finish
        for handle in handles {
            let _ = handle.join();
        }

        // Finish total bar
        total_bar.finish_with_message("complete");

        let elapsed = start_time.elapsed();

        // Print summary
        println!();
        println!("{}", "Cluster Job Complete".green().bold());
        println!("  {} {:.1}s", "Duration:".cyan(), elapsed.as_secs_f64());
        println!(
            "  {} {}/{}",
            "Workers:".cyan(),
            completed.to_string().green(),
            total_workers
        );
        if errors > 0 {
            println!("  {} {}", "Errors:".cyan(), errors.to_string().red());
        }

        // Print aggregate metrics if available
        if aggregate_report.total_rows > 0 {
            println!();
            println!("{}", "Aggregate Results:".green().bold());
            println!(
                "  {} {}",
                "Total rows:".cyan(),
                aggregate_report.total_rows.to_string().bright_white()
            );
            println!(
                "  {} {}",
                "Total partitions:".cyan(),
                aggregate_report.total_partitions.to_string().bright_white()
            );
            println!(
                "  {} {:.0} rows/sec",
                "Throughput:".cyan(),
                aggregate_report.total_rows as f64 / elapsed.as_secs_f64()
            );
        }

        if errors > 0 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{} workers failed", errors),
            )));
        }

        Ok(())
    }

    /// Submit a distributed job using the coordinator/worker pattern.
    ///
    /// This method parses and enriches the job, verifies the already-running
    /// coordinator and exact fresh worker registry without mutation, submits via
    /// the API, prints the durable acknowledgement, and optionally monitors it.
    pub(crate) fn submit_distributed(
        &self,
        pool_name: &str,
        zone: &str,
        coordinator: &Instance,
        workers: &[Instance],
        command: &[String],
        auto_stop: bool,
        force: bool,
        detach: bool,
        output: crate::cli::PoolSubmitOutput,
        has_custom_worker: bool,
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        config: Option<&crate::cloud::ScalingConfig>,
    ) -> Result<()> {
        use genohype_core::query::QueryEngine;

        let human_output = output == crate::cli::PoolSubmitOutput::Human;
        macro_rules! println {
            () => {{
                if human_output {
                    std::println!();
                }
            }};
            ($($arg:tt)*) => {{
                if human_output {
                    std::println!($($arg)*);
                }
            }};
        }

        println!("{}", "Preparing distributed job...".dimmed());

        // Parse command into JobSpec
        // Supported formats:
        //   export parquet <input> <output> [--where ...]
        //   export json <input> <output> [--where ...]
        let (input_path, mut job_spec, filters, intervals) =
            Self::parse_command_to_job_spec(command)?;

        // For IngestManhattan jobs, we don't read Hail table metadata
        // The coordinator discovers phenotypes at runtime
        let is_idle_batch =
            if let crate::distributed::message::JobSpec::ManhattanBatch { ref config, .. } =
                job_spec
            {
                config.as_ref().map(|c| c.job.idle).unwrap_or(false)
            } else {
                false
            };

        let (total_partitions, engine) = if is_idle_batch {
            println!("Idle batch: skipping metadata read, coordinator will load catalog");
            (0, None)
        } else if matches!(
            job_spec,
            crate::distributed::message::JobSpec::IngestManhattan { .. }
        ) {
            println!("Ingestion job: phenotypes will be discovered by coordinator");
            (0, None) // Coordinator will set this after discovering phenotypes
        } else if let crate::distributed::message::JobSpec::Stress(ref spec) = job_spec {
            println!(
                "Stress job: queuing {} synthetic partitions",
                spec.partitions
            );
            (spec.partitions, None)
        } else if let crate::distributed::message::JobSpec::SafeExport(ref spec) = job_spec {
            spec.validate()?;
            if !filters.is_empty()
                || !intervals.is_empty()
                || batch_size.is_some_and(|size| size != 1)
            {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "safe export forbids filters/intervals and requires batch size 1",
                )));
            }
            let manifest = genohype_core::io::SourceManifest::from_path(&spec.source_manifest_uri)?;
            let tasks = spec.part_tasks(&manifest)?;
            let engine = QueryEngine::open_path_with_manifest(
                &spec.canonical_table_uri,
                &spec.source_manifest_uri,
                &spec.source_manifest_sha256,
            )?;
            let mut actual_table = engine.table_descriptor()?;
            for (actual, expected) in actual_table
                .partitions
                .iter_mut()
                .zip(&spec.table.partitions)
            {
                actual.output_part_ids = expected.output_part_ids.clone();
                actual.row_count = expected.row_count;
                actual.index_objects = expected.index_objects.clone();
            }
            if actual_table != spec.table
                || genohype_core::release::GlobalsPayload::from_value(&engine.globals()?)
                    != spec.globals
                || tasks.iter().any(|task| {
                    match engine.row_partition_source_object(task.partition_index) {
                        Ok(source) => source != task.source_object,
                        Err(_) => true,
                    }
                })
            {
                return Err(HailError::InvalidFormat(
                    "safe-export pinned preflight does not match the sealed descriptor".to_string(),
                ));
            }
            println!(
                "Safe export: verified {} pinned source partitions",
                tasks.len()
            );
            (tasks.len(), Some(engine))
        } else if let crate::distributed::message::JobSpec::Custom {
            ref manifest,
            tasks,
            ..
        } = job_spec
        {
            let count = manifest.as_ref().map(|m| m.len()).unwrap_or(tasks);
            if manifest.is_some() {
                println!("Custom job: queuing {} tasks from manifest", count);
            } else {
                println!("Custom job: queuing {} generic tasks", count);
            }
            (count, None)
        } else {
            // Calculate total partitions by reading metadata locally
            println!("Reading metadata from {}...", input_path.bright_white());
            let engine = QueryEngine::open_path(&input_path)?;
            let partitions = engine.num_partitions();
            (partitions, Some(engine))
        };
        println!(
            "  {} {} partitions to process",
            "Found".green(),
            total_partitions.to_string().bright_white()
        );
        println!(
            "  {} {}",
            "Job type:".cyan(),
            job_spec.description().bright_white()
        );
        if let Some(out) = job_spec.output_path() {
            println!("  {} {}", "Output:".cyan(), out.bright_white());
        }

        // For Manhattan jobs, compute the layout and partition counts for all tables
        if let crate::distributed::message::JobSpec::Manhattan { ref mut spec, .. } = job_spec {
            use crate::manhattan::layout::{ChromosomeLayout, YScale};

            println!("  {} Computing chromosome layout...", "Setup:".cyan());
            let contigs: Vec<(String, u32)> = genohype_association::canonical_contig_lengths(
                genohype_association::Assembly::Grch38,
            )
            .into_iter()
            .map(|(name, length)| (name, length as u32))
            .collect();
            let layout = ChromosomeLayout::new(&contigs, spec.width, 4);
            let y_scale = YScale::new(spec.height, 350.0);
            spec.layout = Some(layout);
            spec.y_scale = Some(y_scale);
            // Add contig lengths for per-chromosome plot generation
            spec.contig_lengths = Some(contigs.into_iter().collect());

            // Count partitions for each table
            if let Some(ref exome_path) = spec.exome {
                if let Ok(exome_engine) = QueryEngine::open_path(exome_path) {
                    let exome_parts = exome_engine.num_partitions();
                    println!("  {} {} exome partitions", "Found".green(), exome_parts);
                    spec.exome_partitions = Some(exome_parts);
                }
            }
            if let Some(ref genome_path) = spec.genome {
                if let Ok(genome_engine) = QueryEngine::open_path(genome_path) {
                    let genome_parts = genome_engine.num_partitions();
                    println!("  {} {} genome partitions", "Found".green(), genome_parts);
                    spec.genome_partitions = Some(genome_parts);
                }
            }
        }

        // For ManhattanBatch jobs, compute partition counts for all unique tables
        // (Layout enrichment is handled by the coordinator via enrich_specs)
        if let crate::distributed::message::JobSpec::ManhattanBatch {
            ref mut specs,
            ref config,
            ..
        } = job_spec
        {
            use std::collections::HashMap;

            let is_idle_batch = config.as_ref().map(|c| c.job.idle).unwrap_or(false);

            if !is_idle_batch {
                if specs.is_empty() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "ManhattanBatch has no specs",
                    )));
                }

                // Collect all unique table paths
                let mut exome_paths: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let mut genome_paths: std::collections::HashSet<String> =
                    std::collections::HashSet::new();

                for spec in specs.iter() {
                    if let Some(ref path) = spec.exome {
                        exome_paths.insert(path.clone());
                    }
                    if let Some(ref path) = spec.genome {
                        genome_paths.insert(path.clone());
                    }
                }

                // Cache partition counts by path (avoid re-opening same tables)
                let mut partition_cache: HashMap<String, usize> = HashMap::new();

                println!(
                    "  {} Counting partitions for {} unique exome tables...",
                    "Setup:".cyan(),
                    exome_paths.len()
                );
                for path in exome_paths {
                    if let Ok(table_engine) = QueryEngine::open_path(&path) {
                        let parts = table_engine.num_partitions();
                        partition_cache.insert(path, parts);
                    }
                }

                println!(
                    "  {} Counting partitions for {} unique genome tables...",
                    "Setup:".cyan(),
                    genome_paths.len()
                );
                for path in genome_paths {
                    if let Ok(table_engine) = QueryEngine::open_path(&path) {
                        let parts = table_engine.num_partitions();
                        partition_cache.insert(path, parts);
                    }
                }

                // Apply partition counts to all specs
                for spec in specs.iter_mut() {
                    if let Some(ref exome_path) = spec.exome {
                        if let Some(&parts) = partition_cache.get(exome_path) {
                            spec.exome_partitions = Some(parts);
                        }
                    }
                    if let Some(ref genome_path) = spec.genome {
                        if let Some(&parts) = partition_cache.get(genome_path) {
                            spec.genome_partitions = Some(parts);
                        }
                    }
                }

                // Log summary
                let with_exome = specs
                    .iter()
                    .filter(|s| s.exome_partitions.is_some())
                    .count();
                let with_genome = specs
                    .iter()
                    .filter(|s| s.genome_partitions.is_some())
                    .count();
                println!(
                    "  {} {} phenotypes ({} with exome, {} with genome partitions)",
                    "Prepared".green(),
                    specs.len(),
                    with_exome,
                    with_genome
                );

                // Check for already-completed phenotypes via checkpoint file
                let total_before = specs.len();
                // Derive checkpoint path from first spec's output_path (clone to avoid borrow issues)
                // output_path is like gs://bucket/manhattans/meta/1234
                // checkpoint is at gs://bucket/manhattans/.completed
                let base_dir: Option<String> = specs
                    .first()
                    .and_then(|s| s.output_path.rsplit_once('/'))
                    .and_then(|(parent, _)| parent.rsplit_once('/'))
                    .map(|(base, _)| base.to_string());

                if let Some(ref base_dir) = base_dir {
                    let checkpoint_path = format!("{}/.completed", base_dir);
                    println!(
                        "  {} Checking for completed phenotypes...",
                        "Resume:".cyan()
                    );

                    match read_completed_checkpoint(&checkpoint_path) {
                        Ok(completed) => {
                            if !completed.is_empty() {
                                let before = specs.len();
                                specs.retain(|s| {
                                    // Extract relative path (ancestry/id) from output_path
                                    let rel_path = s
                                        .output_path
                                        .strip_prefix(base_dir)
                                        .map(|p| p.trim_start_matches('/'))
                                        .unwrap_or(&s.output_path);
                                    !completed.contains(rel_path)
                                });
                                let skipped = before - specs.len();
                                if skipped > 0 {
                                    println!(
                                        "  {} {} phenotypes already complete, {} remaining",
                                        "Skipped".yellow(),
                                        skipped,
                                        specs.len()
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            // No checkpoint file or error reading - that's fine, process all
                            println!("  {} No checkpoint file ({})", "Note:".dimmed(), e);
                        }
                    }
                }

                // If all phenotypes are complete, exit early
                if specs.is_empty() {
                    println!(
                        "{} All {} phenotypes already complete!",
                        "Done".green().bold(),
                        total_before
                    );
                    return Ok(());
                }
            } // End of if !is_idle_batch
        }

        drop(engine); // Drop the QueryEngine if it exists (Option<QueryEngine>)

        // Get coordinator's internal IP
        let coord_ip = coordinator.ip().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Coordinator {} has no internal IP", coordinator.name),
            ))
        })?;

        if !self.check_coordinator_status(coordinator, zone) {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "Coordinator became unavailable before registration; no repair was attempted",
            )));
        }
        println!(
            "{} Healthy coordinator on {} ({})",
            "Found".green(),
            coordinator.name.cyan(),
            coord_ip
        );

        if !force {
            self.ensure_coordinator_idle(coordinator, zone)?;
        }

        let workers_json =
            self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/workers", 3000)?;
        let registrations: Vec<crate::distributed::message::DashboardWorker> =
            serde_json::from_str(&workers_json).map_err(|error| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid coordinator worker registry: {error}"),
                ))
            })?;
        let expected_names: Vec<_> = workers.iter().map(|worker| worker.name.clone()).collect();
        // Avoid inspecting, hashing, staging, or building a mutable operator-local
        // worker binary. Stock workers must match the coordinator build. Custom
        // workers bind to the configured build-produced attestation identity.
        //
        // Durable-state limitation: the expected custom identity is not yet
        // persisted by the pool/coordinator. Until it is, normal submit relies on
        // the configured operator-local sidecar path remaining available and
        // trusted; absence is therefore a fail-closed readiness error.
        let expected_build = if !has_custom_worker {
            let summary_json =
                self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/summary", 3000)?;
            let summary: crate::distributed::message::DashboardSummary =
                serde_json::from_str(&summary_json).map_err(|error| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid coordinator summary: {error}"),
                    ))
                })?;
            Some(summary.build_version.ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Coordinator did not report its build identity",
                ))
            })?)
        } else {
            let attestation_path = config
                .and_then(|config| config.worker_attestation.as_deref())
                .ok_or_else(|| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Custom-worker submit requires worker_attestation in the pool profile to bind readiness to the intended build. This read-only check does not hash, stage, or build the local binary.",
                    ))
                })?;
            let attestation = crate::cloud::attestation::read_worker_attestation_intent(
                std::path::Path::new(attestation_path),
            )?;
            Some(attestation.build_version)
        };
        require_worker_readiness(&expected_names, &registrations, expected_build.as_deref())?;
        println!(
            "{} {}/{} exact managed workers have fresh, healthy build identities",
            "OK".green(),
            registrations
                .iter()
                .filter(|worker| expected_names.contains(&worker.worker_id))
                .count(),
            workers.len()
        );

        println!("{}", "Submitting job via API...".dimmed());
        let acknowledgement = self.submit_job_via_api(
            coordinator,
            zone,
            &input_path,
            &job_spec,
            total_partitions,
            force,
            batch_size,
            memory_weight_mb,
            &filters,
            &intervals,
        )?;

        let job_id = match acknowledgement {
            crate::distributed::message::JobSubmissionAck::Job { job_id } => {
                match output {
                    crate::cli::PoolSubmitOutput::Human => {
                        println!();
                        println!("{} Submission acknowledged", "OK".green().bold());
                        println!("  {} {}", "Job ID:".cyan(), job_id.bright_white().bold());
                        println!("  {} {}", "Coordinator:".cyan(), coordinator.name);
                        println!("  {} {}", "Workers:".cyan(), workers.len());
                        println!("  {} {}", "Total partitions:".cyan(), total_partitions);
                        println!();
                    }
                    crate::cli::PoolSubmitOutput::Json => std::println!(
                        "{}",
                        serde_json::json!({
                            "acknowledged": true,
                            "job_id": job_id,
                            "pool": pool_name,
                            "coordinator": coordinator.name,
                            "workers": workers.len(),
                            "total_partitions": total_partitions,
                        })
                    ),
                }
                job_id
            }
            crate::distributed::message::JobSubmissionAck::Configuration => {
                match output {
                    crate::cli::PoolSubmitOutput::Human => println!(
                        "{} Idle catalog configuration acknowledged (no executable job)",
                        "OK".green().bold()
                    ),
                    crate::cli::PoolSubmitOutput::Json => std::println!(
                        "{}",
                        serde_json::json!({
                            "acknowledged": true,
                            "job_id": null,
                            "pool": pool_name,
                            "configuration_only": true,
                        })
                    ),
                }
                return Ok(());
            }
        };
        if workers.is_empty() {
            println!(
                "{}",
                "Job is durably registered with zero workers; scale explicitly to begin processing."
                    .yellow()
            );
            return Ok(());
        }
        if detach {
            if output == crate::cli::PoolSubmitOutput::Human {
                println!(
                    "Detached after durable acknowledgement. Remote job {} continues running.",
                    job_id
                );
                println!("Monitor with: genohype pool status {}", pool_name);
            }
            return Ok(());
        }
        println!(
            "{}",
            "Streaming coordinator logs (interrupting monitoring does not cancel the remote job)..."
                .dimmed()
        );
        println!();

        // Stream coordinator logs, detecting job completion to exit automatically
        let mut log_cmd = self.provider.get_ssh_command(
            &coordinator.name,
            zone,
            "journalctl -u genohype-coordinator -f --no-pager",
        );

        log_cmd.stdout(std::process::Stdio::piped());
        log_cmd.stderr(std::process::Stdio::null());

        let mut child = log_cmd.spawn().map_err(|e| {
            HailError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to start log streaming: {}", e),
            ))
        })?;

        let (job_failed, tail_failure) = if let Some(stdout) = child.stdout.take() {
            use std::io::BufRead;
            use std::sync::mpsc::RecvTimeoutError;
            use std::time::Duration;

            let (line_tx, line_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(stdout).lines() {
                    if line_tx.send(line).is_err() {
                        break;
                    }
                }
            });

            let mut failed = false;
            let mut complete = false;
            let mut failure = None;
            let mut watchdog = TailWatchdog::default();
            let mut last_health_check = Instant::now() - Duration::from_secs(10);

            while !complete {
                match line_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(Ok(line)) => {
                        println!("{}", line);
                        complete =
                            line.contains("Job complete. Coordinator returning to idle mode");
                        failed |= line.contains("Job finished with") && line.contains("failed");
                    }
                    Ok(Err(_)) | Err(RecvTimeoutError::Disconnected) => {
                        failure = Some(TailFailure::StreamEnded);
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                }

                if complete {
                    break;
                }

                if last_health_check.elapsed() >= Duration::from_secs(5) {
                    last_health_check = Instant::now();
                    let health = match self.fetch_coordinator_api(
                        coordinator,
                        zone,
                        "/api/dashboard/workers",
                        3000,
                    ) {
                        Ok(json) => match serde_json::from_str::<
                            Vec<crate::distributed::message::DashboardWorker>,
                        >(&json)
                        {
                            Ok(worker_list) if worker_list.iter().any(worker_is_fresh) => {
                                TailHealth::Healthy
                            }
                            Ok(_) => TailHealth::NoWorkers,
                            Err(_) => TailHealth::CoordinatorUnavailable,
                        },
                        Err(_) => TailHealth::CoordinatorUnavailable,
                    };

                    if let Some(reason) = watchdog.observe(health, Instant::now()) {
                        failure = Some(reason);
                        break;
                    }
                }
            }

            let _ = child.kill();
            let _ = child.wait();
            if complete {
                println!();
                println!("{} Job complete, exiting log stream.", "OK".green().bold());
                failure = None;
            }
            (failed, failure)
        } else {
            let _ = child.kill();
            let _ = child.wait();
            (false, Some(TailFailure::StreamEnded))
        };

        if let Some(failure) = tail_failure {
            let message = match failure {
                TailFailure::NoWorkers => {
                    "All workers stopped heartbeating for 30 seconds; aborting log tail"
                }
                TailFailure::CoordinatorUnavailable => {
                    "Coordinator monitoring was unavailable for 30 seconds; aborting log tail"
                }
                TailFailure::StreamEnded => "Coordinator log stream ended before job completion",
            };
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("{message}; remote job {job_id} continues running and was not cancelled"),
            )));
        }

        if job_failed {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Job completed with failed partitions",
            )));
        }

        // Fetch and display aggregated results for Summary jobs
        if matches!(job_spec, crate::distributed::message::JobSpec::Summary) {
            println!();
            println!("{}", "Fetching aggregated results...".dimmed());
            if let Err(e) = self.fetch_and_display_summary_results(coordinator, zone) {
                eprintln!("{} Failed to fetch results: {}", "Warning:".yellow(), e);
            }
        }

        if auto_stop {
            println!(
                "{}",
                "Job finished. Stopping pool instances (--auto-stop)...".yellow()
            );
            let mut instance_names = vec![coordinator.name.clone()];
            instance_names.extend(workers.iter().map(|worker| worker.name.clone()));

            self.provider.stop_instances(&instance_names, zone)?;
            println!("{} Instances stopped.", "OK".green().bold());
        }

        Ok(())
    }

    /// Start worker services on the given instances.
    pub(crate) fn start_worker_services(
        &self,
        workers: &[Instance],
        coord_ip: &str,
        zone: &str,
    ) -> Result<()> {
        use rayon::prelude::*;

        let worker_results: Vec<Result<()>> = workers
            .par_iter()
            .map(|worker| {
                let worker_cmd = format!(
                    "sudo bash -c 'cat > /etc/systemd/system/genohype-worker.service << EOF
[Unit]
Description=Genohype Worker
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-worker --url http://{}:3000 --worker-id {}
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
' && sudo systemctl daemon-reload && sudo systemctl enable genohype-worker && sudo systemctl restart genohype-worker && sudo systemctl is-active --quiet genohype-worker",
                    coord_ip, worker.name
                );

                let status = self
                    .provider
                    .get_ssh_command(&worker.name, zone, &worker_cmd)
                    .status()
                    .map_err(HailError::Io)?;

                if !status.success() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to start worker on {}", worker.name),
                    )));
                }

                println!("  {} started on {}", "Worker".dimmed(), worker.name.cyan());
                Ok(())
            })
            .collect();

        // Check for any startup failures
        for result in worker_results {
            result?;
        }

        Ok(())
    }

    /// Fetch aggregated summary results from coordinator and display them.
    pub(crate) fn fetch_and_display_summary_results(
        &self,
        coordinator: &Instance,
        zone: &str,
    ) -> Result<()> {
        use genohype_core::summary::stats::StatsAccumulator;

        // Fetch results file saved by coordinator before exit
        let fetch_cmd = "cat /tmp/job_result.json";
        let output = self
            .provider
            .get_ssh_command(&coordinator.name, zone, fetch_cmd)
            .output()
            .map_err(HailError::Io)?;

        if !output.status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to fetch results file from coordinator",
            )));
        }

        // Parse the response
        let response: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse result JSON: {}", e),
            ))
        })?;

        // Check if results are available
        if !response
            .get("available")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let error = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Results not available: {}", error),
            )));
        }

        // Get the array of partial results from workers
        let results = response
            .get("result")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "No result array in response",
                ))
            })?;

        // Merge all partial StatsAccumulators
        let mut merged = StatsAccumulator::new();
        let mut total_rows = 0usize;

        for partial in results {
            if let Ok(acc) = serde_json::from_value::<StatsAccumulator>(partial.clone()) {
                total_rows += acc.stats.values().map(|s| s.count).max().unwrap_or(0);
                merged.merge(acc);
            }
        }

        // Display results
        println!();
        println!(
            "{} {}",
            "Row Count:".green(),
            total_rows.to_string().bright_white().bold()
        );
        println!();

        // Print field statistics
        println!("{}", "Field Statistics:".green().bold());
        println!(
            "{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
            "Field".cyan(),
            "Count".cyan(),
            "Nulls".cyan(),
            "Min".cyan(),
            "Max".cyan()
        );
        println!("{}", "-".repeat(120).dimmed());

        for key in merged.sorted_fields() {
            let s = &merged.stats[key];

            // Truncate field name if too long
            let field_display = if key.len() > 48 {
                format!("...{}", &key[key.len() - 45..])
            } else {
                key.clone()
            };

            // Truncate min/max if too long
            let min_display = match &s.min {
                Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
                Some(m) => m.clone(),
                None => String::new(),
            };
            let max_display = match &s.max {
                Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
                Some(m) => m.clone(),
                None => String::new(),
            };

            println!(
                "{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
                field_display, s.count, s.null_count, min_display, max_display
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    const TEST_REVISION: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TEST_BUILD: &str =
        "fixture/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/x86_64-linux-release";

    fn dashboard_worker(
        status: &str,
        last_seen_secs: f64,
    ) -> crate::distributed::message::DashboardWorker {
        crate::distributed::message::DashboardWorker {
            worker_id: "worker-0".into(),
            status: status.into(),
            current_batch_size: None,
            max_batch_capacity: None,
            last_seen_secs,
            telemetry: None,
            total_items: 0,
            tasks_completed: 0,
            current_task: None,
            build_version: Some(TEST_BUILD.into()),
            effective_status: None,
        }
    }

    #[test]
    fn zero_worker_mode_is_based_on_custom_job_not_binary_path_intent() {
        assert_eq!(
            classify_submission("pool", true, 0, true).unwrap(),
            SubmissionMode::ZeroWorkerCustom
        );
        assert!(classify_submission("pool", true, 0, false).is_err());
        assert_eq!(
            classify_submission("pool", true, 1, true).unwrap(),
            SubmissionMode::Standard
        );
        assert_eq!(
            classify_submission("pool", false, 0, true).unwrap(),
            SubmissionMode::Standard
        );
    }

    #[test]
    fn stale_registry_entry_is_not_treated_as_connected() {
        assert!(worker_is_fresh(&dashboard_worker("idle", 1.0)));
        assert!(!worker_is_fresh(&dashboard_worker("idle", 16.0)));
        assert!(!worker_is_fresh(&dashboard_worker("suspected_dead", 1.0)));
    }

    fn summary_json(idle: bool) -> String {
        serde_json::json!({
            "progress_percent": 0.0,
            "total_tasks": 0,
            "completed_tasks": 0,
            "processing_tasks": 0,
            "pending_tasks": 0,
            "failed_tasks": 0,
            "total_items": 0,
            "cluster_items_per_sec": 0.0,
            "elapsed_secs": 0.0,
            "eta_secs": null,
            "is_complete": false,
            "input_path": "",
            "job_spec": null,
            "idle": idle,
            "build_version": TEST_BUILD
        })
        .to_string()
    }

    #[test]
    fn deployment_preflight_requires_verified_idle_coordinator() {
        assert!(require_idle_summary(&summary_json(true)).is_ok());
        assert!(require_idle_summary(&summary_json(false))
            .unwrap_err()
            .to_string()
            .contains("already has a job running"));
        assert!(require_idle_summary("not json").is_err());
    }

    #[test]
    fn healthy_detached_submits_never_issue_lifecycle_operations() {
        use crate::cloud::{CloudProvider, Instance, InstanceSetup, NetworkInterface, PoolConfig};
        use std::path::Path;
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct Calls {
            ssh: Vec<String>,
            uploads: usize,
            creates: usize,
            deletes: usize,
            stops: usize,
        }

        #[derive(Clone)]
        struct RecordingProvider(Arc<Mutex<Calls>>);

        impl CloudProvider for RecordingProvider {
            fn create_pool(&self, _: &PoolConfig) -> Result<()> {
                self.0.lock().unwrap().creates += 1;
                Ok(())
            }

            fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
                let instance = |name: &str, ip: &str| Instance {
                    id: format!("id-{name}"),
                    name: name.into(),
                    zone: "us-central1-a".into(),
                    network_interfaces: vec![NetworkInterface {
                        network_ip: ip.into(),
                        network: None,
                        subnetwork: None,
                    }],
                    status: "RUNNING".into(),
                    machine_type: None,
                    scheduling: None,
                };
                Ok(vec![
                    instance("pool-coordinator", "10.0.0.1"),
                    instance("pool-worker-0", "10.0.0.2"),
                ])
            }

            fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
                self.0.lock().unwrap().deletes += 1;
                Ok(())
            }

            fn create_instances(&self, _: &[InstanceSetup]) -> Result<()> {
                self.0.lock().unwrap().creates += 1;
                Ok(())
            }

            fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
                self.0.lock().unwrap().deletes += 1;
                Ok(())
            }

            fn stop_instances(&self, _: &[String], _: &str) -> Result<()> {
                self.0.lock().unwrap().stops += 1;
                Ok(())
            }

            fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
                self.0.lock().unwrap().uploads += 1;
                Ok(())
            }

            fn get_ssh_command(&self, _: &str, _: &str, command: &str) -> std::process::Command {
                self.0.lock().unwrap().ssh.push(command.to_string());
                let response = if command.contains("/api/dashboard/summary") {
                    summary_json(true)
                } else if command.contains("/api/dashboard/workers") {
                    let mut worker = dashboard_worker("idle", 1.0);
                    worker.worker_id = "pool-worker-0".into();
                    serde_json::to_string(&vec![worker]).unwrap()
                } else if command.contains("/api/job") {
                    r#"{"acknowledged":true,"job_id":"job-exact","error":null}"#.into()
                } else {
                    r#"{"pending_tasks":0,"completed_tasks":0}"#.into()
                };
                let quoted = response.replace('\'', "'\\''");
                let script = if command.contains("/api/job") {
                    format!("cat >/dev/null; printf '%s' '{quoted}'")
                } else {
                    format!("printf '%s' '{quoted}'")
                };
                let mut process = std::process::Command::new("sh");
                process.args(["-c", &script]);
                process
            }
        }

        let cases = [
            (
                vec!["custom".into(), "--tasks".into(), "1".into()],
                false,
                Some("/profile/path/that/does-not-exist".to_string()),
            ),
            (
                vec![
                    "stress".into(),
                    "--tasks".into(),
                    "1".into(),
                    "--cpu-secs".into(),
                    "0".into(),
                ],
                false,
                None,
            ),
            (
                vec![
                    "stress".into(),
                    "--tasks".into(),
                    "1".into(),
                    "--cpu-secs".into(),
                    "0".into(),
                ],
                true,
                None,
            ),
        ];

        for (command, force, worker_binary) in cases {
            let mut sidecar = tempfile::NamedTempFile::new().unwrap();
            serde_json::to_writer(
                &mut sidecar,
                &crate::cloud::attestation::WorkerArtifactAttestation {
                    schema_version: 1,
                    sha256: "b".repeat(64),
                    size: 1,
                    revision: TEST_REVISION.into(),
                    build_identity: TEST_BUILD.into(),
                    build_version: TEST_BUILD.into(),
                    platform: "linux".into(),
                    arch: "x86_64".into(),
                },
            )
            .unwrap();
            let config = worker_binary
                .as_ref()
                .map(|worker_binary| crate::cloud::ScalingConfig {
                    machine_type: "e2-standard-2".into(),
                    workers: 1,
                    spot: false,
                    network: None,
                    subnet: None,
                    public_ip: false,
                    manage_firewall: false,
                    project: Some("project".into()),
                    with_coordinator: true,
                    pool_db_path: Some("gs://bucket/pool/ops.db".into()),
                    worker_binary: Some(worker_binary.clone()),
                    worker_attestation: Some(sidecar.path().to_string_lossy().into_owned()),
                    service_account: None,
                });
            let calls = Arc::new(Mutex::new(Calls::default()));
            let manager = PoolManager::new(RecordingProvider(calls.clone()));
            manager
                .submit(
                    "pool",
                    "us-central1-a",
                    None,
                    worker_binary,
                    false,
                    false,
                    force,
                    true,
                    crate::cli::PoolSubmitOutput::Human,
                    false,
                    true,
                    None,
                    None,
                    config.as_ref(),
                    &command,
                )
                .unwrap();

            let calls = calls.lock().unwrap();
            assert_eq!(calls.uploads, 0);
            assert_eq!(calls.creates, 0);
            assert_eq!(calls.deletes, 0);
            assert_eq!(calls.stops, 0);
            let ssh = calls.ssh.join("\n");
            for forbidden in [
                "systemctl",
                "fuser",
                "update-fleet",
                "gsutil cp",
                "/etc/systemd/system",
            ] {
                assert!(!ssh.contains(forbidden), "forbidden {forbidden}: {ssh}");
            }
        }
    }

    #[test]
    fn machine_json_requires_explicit_detach() {
        let error =
            validate_submit_contract(false, None, false, crate::cli::PoolSubmitOutput::Json)
                .unwrap_err()
                .to_string();
        assert!(error.contains("requires explicit --detach"), "{error}");
        assert!(
            validate_submit_contract(false, None, true, crate::cli::PoolSubmitOutput::Json).is_ok()
        );
    }

    #[test]
    fn custom_submit_redeploy_fails_closed_before_artifact_access() {
        let error = validate_submit_contract(
            true,
            Some("/does/not/need/to/exist"),
            true,
            crate::cli::PoolSubmitOutput::Human,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("pool update-binary"), "{error}");
    }

    #[test]
    fn log_tail_watchdog_bounds_worker_and_api_loss() {
        use std::time::Duration;

        let start = Instant::now();
        let mut watchdog = TailWatchdog::default();
        assert_eq!(
            watchdog.observe(TailHealth::CoordinatorUnavailable, start),
            None
        );
        assert_eq!(
            watchdog.observe(
                TailHealth::CoordinatorUnavailable,
                start + LOG_TAIL_UNHEALTHY_TIMEOUT
            ),
            Some(TailFailure::CoordinatorUnavailable)
        );

        assert_eq!(
            watchdog.observe(TailHealth::Healthy, start + Duration::from_secs(31)),
            None
        );
        assert_eq!(
            watchdog.observe(TailHealth::NoWorkers, start + Duration::from_secs(32)),
            None
        );
        assert_eq!(
            watchdog.observe(
                TailHealth::NoWorkers,
                start + Duration::from_secs(32) + LOG_TAIL_UNHEALTHY_TIMEOUT
            ),
            Some(TailFailure::NoWorkers)
        );
    }
}
