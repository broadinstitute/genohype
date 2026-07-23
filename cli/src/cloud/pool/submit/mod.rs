//! Job submission orchestration.

mod legacy_runner;
mod preflight;

pub use legacy_runner::WorkerMessage;
pub use preflight::{list_completed_markers, read_completed_checkpoint};

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

fn worker_is_fresh(worker: &crate::distributed::message::DashboardWorker) -> bool {
    worker.status != "suspected_dead" && worker.last_seen_secs <= WORKER_FRESHNESS_SECS
}

impl<P: CloudProvider + Sync> PoolManager<P> {
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
            let coord_cmd = format!(
                "sudo bash -c 'cat > /etc/systemd/system/genohype-coordinator.service << EOF
[Unit]
Description=Genohype Coordinator
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db{}
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
EOF
' && sudo systemctl daemon-reload && sudo systemctl enable --now genohype-coordinator",
                backup_arg
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
                if !worker_deployed_via_startup {
                    println!("{}", "Deploying custom worker via SCP...".dimmed());
                    self.deploy_binary(custom_worker, &workers, zone)?;
                }
                // Always rewrite, enable, and restart the unit. This also recovers a
                // service that exited between VM readiness and the first submission.
                self.start_worker_services(&workers, coord_ip, zone)?;
                println!(
                    "{} Custom binary deployed and {} worker(s) started",
                    "OK".green().bold(),
                    workers.len()
                );
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
        autoscale: bool,
        skip_build: bool,
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        config: Option<&crate::cloud::ScalingConfig>,
        command: &[String],
    ) -> Result<()> {
        // Determine required features based on command
        let features: Vec<&str> =
            if command.len() >= 2 && command[0] == "export" && command[1] == "clickhouse" {
                vec!["clickhouse"]
            } else if command.len() >= 2 && command[0] == "ingest" && command[1] == "manhattan" {
                vec!["clickhouse"] // Ingest manhattan requires clickhouse feature
            } else {
                vec![]
            };

        // Determine if we should build
        // 1. Explicit skip_build -> skip
        // 2. We have a bundled binary -> skip (Release mode)
        // 3. Otherwise -> build (Dev mode)
        let should_build_base = if skip_build {
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

        // Optimize: check if coordinator is already running before building
        // If coordinator is running and we're not force redeploying or autoscaling,
        // we can skip the build entirely since binary is already deployed
        let should_build = if should_build_base && !force_redeploy && !autoscale {
            // Peek at coordinator status to intelligently skip local build
            let instances = self.provider.list_instances(name).unwrap_or_default();
            let coordinator = instances.iter().find(|i| i.name.ends_with("-coordinator"));
            if let Some(coord) = coordinator {
                if self.check_coordinator_status(coord, zone) {
                    println!(
                        "{}",
                        "Coordinator already running, skipping build...".dimmed()
                    );
                    false
                } else {
                    should_build_base
                }
            } else {
                should_build_base
            }
        } else {
            should_build_base
        };

        // Build binary if redeploying (ensures latest code is used) or if needed
        if force_redeploy || should_build {
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

            // Scale up to target workers
            let target = pool_config.workers;
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
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        config: Option<&crate::cloud::ScalingConfig>,
        command: &[String],
    ) -> Result<()> {
        // 1. Locate the Linux binary (will check if needed after seeing coordinator status)
        let binary = self.locate_binary(binary_path).ok();

        // Resolve worker binary (if --worker-binary specified, validate it exists)
        let worker_binary = if let Some(ref wb_path) = worker_binary_path {
            let path = std::path::PathBuf::from(wb_path);
            if !path.exists() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Worker binary not found at: {}", wb_path),
                )));
            }
            println!(
                "{} {}",
                "Worker binary:".cyan(),
                path.display().to_string().bright_white()
            );
            Some(path)
        } else {
            None
        };

        // 2. Get running instances
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

        // Auto-detect distributed mode: use coordinator/worker pattern when coordinator exists
        let use_distributed = coordinator.is_some();

        // Validate we have workers for distributed mode
        if use_distributed && total_workers == 0 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "No workers available for pool '{}'. Either:\n\
                     - Scale up workers: genohype pool scale {} --workers N\n\
                     - Use --autoscale to automatically scale workers for the job",
                    name, name
                ),
            )));
        }

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

        // 3. Deploy binary. A custom worker is independent of the stock
        // coordinator and must be deployed even when that coordinator is healthy.
        let coordinator_running = coordinator
            .as_ref()
            .map(|coord| self.check_coordinator_status(coord, zone))
            .unwrap_or(false);
        if use_distributed && coordinator_running && !force_redeploy && worker_binary.is_some() {
            let coord = coordinator.as_ref().unwrap();
            let coord_ip = coord.ip().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Coordinator {} has no internal IP", coord.name),
                ))
            })?;
            let custom_worker = worker_binary.as_ref().unwrap();
            println!(
                "{} Coordinator is healthy; deploying custom worker binary only...",
                "Setup:".cyan()
            );
            self.deploy_binary(custom_worker, &workers, zone)?;
            self.start_worker_services(&workers, coord_ip, zone)?;
        }

        let should_deploy = if use_distributed {
            let coord_running = coordinator_running;
            if coord_running && !force_redeploy {
                // Coordinator already running = binary already deployed
                println!(
                    "{} Coordinator already running, skipping binary deployment",
                    "Note:".cyan()
                );
                println!(
                    "{}",
                    "      (use --redeploy-binary or 'pool update-binary' to redeploy)".dimmed()
                );
                false
            } else {
                true // Deploy if coordinator not running, or if force_redeploy
            }
        } else {
            true // Non-distributed always deploys
        };

        if should_deploy {
            let binary = binary.as_ref().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Linux binary not found. Build with: cargo linux --release",
                ))
            })?;

            // Try to stage binary to GCS for fast updates
            let pool_db_path = config.and_then(|c| c.pool_db_path.as_deref());
            let staging_url = if let Some(db_path) = pool_db_path {
                self.stage_binary_to_gcs(binary, db_path, "genohype-coordinator")
                    .ok()
            } else {
                None
            };

            // Stage worker binary to GCS separately if it differs from coordinator binary
            let worker_staging_url = if let Some(ref wb) = worker_binary {
                if let Some(db_path) = pool_db_path {
                    self.stage_binary_to_gcs(wb, db_path, "genohype-worker-custom")
                        .ok()
                } else {
                    None
                }
            } else {
                staging_url.clone() // Same binary for both
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
                let stop_cmd = "sudo systemctl stop genohype-coordinator 2>/dev/null || true";
                let _ = self
                    .provider
                    .get_ssh_command(&coord.name, zone, stop_cmd)
                    .status();
                std::thread::sleep(std::time::Duration::from_secs(1));

                // Deploy coordinator binary (always the stock genohype binary)
                if let Some(ref gcs_url) = staging_url {
                    println!("{}", "Deploying coordinator binary via GCS...".dimmed());
                    let update_coord_cmd = format!(
                        "gsutil cp {} /tmp/genohype && chmod +x /tmp/genohype && sudo mv /tmp/genohype /usr/local/bin/genohype",
                        gcs_url
                    );
                    self.provider
                        .get_ssh_command(&coord.name, zone, &update_coord_cmd)
                        .status()
                        .map_err(HailError::Io)?;
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
                let coord_cmd = format!(
                    "sudo bash -c 'cat > /etc/systemd/system/genohype-coordinator.service << EOF
[Unit]
Description=Genohype Coordinator
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db{}
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
EOF
' && sudo systemctl daemon-reload && sudo systemctl restart genohype-coordinator",
                    backup_arg
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

                // Deploy worker binary (custom binary if specified, otherwise same as coordinator)
                if worker_binary.is_some() {
                    // Custom worker binary: deploy separately to workers
                    let wb = worker_binary.as_ref().unwrap();
                    if let Some(ref gcs_url) = worker_staging_url {
                        println!(
                            "{} Deploying custom worker binary to {} workers via GCS...",
                            "API:".cyan(),
                            workers.len()
                        );
                        let req = serde_json::json!({ "gcs_url": gcs_url });
                        let curl_cmd = format!(
                            "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/update-fleet",
                            req.to_string()
                        );
                        let _ = self
                            .provider
                            .get_ssh_command(&coord.name, zone, &curl_cmd)
                            .status();
                        println!(
                            "{} Custom worker binary update triggered.",
                            "OK".green().bold()
                        );
                    } else {
                        // Fallback: SCP the custom worker binary directly to workers
                        println!(
                            "{}",
                            "Deploying custom worker binary to workers via SCP...".dimmed()
                        );
                        self.deploy_binary(wb, &workers, zone)?;
                        println!(
                            "{} Custom worker binary deployed to {} workers.",
                            "OK".green().bold(),
                            workers.len()
                        );
                    }
                    // Whether the binary arrived through fleet update or SCP, make
                    // the unit deterministic and active before accepting a job.
                    self.start_worker_services(&workers, coord_ip, zone)?;
                } else {
                    // Same binary for coordinator and workers
                    if let Some(ref gcs_url) = staging_url {
                        println!(
                            "{} Instructing workers to self-update via GCS...",
                            "API:".cyan()
                        );
                        let req = serde_json::json!({ "gcs_url": gcs_url });
                        let curl_cmd = format!(
                            "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/update-fleet",
                            req.to_string()
                        );
                        let _ = self
                            .provider
                            .get_ssh_command(&coord.name, zone, &curl_cmd)
                            .status();
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
    /// This method:
    /// 1. Parses the command to extract input/output paths and job type
    /// 2. Calculates total partitions from the input table
    /// 3. Checks if coordinator is already running (idle mode), submits via API
    /// 4. Or starts the coordinator service on the coordinator VM (legacy)
    /// 5. Starts worker services on all worker VMs
    /// 6. Streams coordinator logs for progress monitoring
    pub(crate) fn submit_distributed(
        &self,
        _pool_name: &str,
        zone: &str,
        coordinator: &Instance,
        workers: &[Instance],
        command: &[String],
        auto_stop: bool,
        force: bool,
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        config: Option<&crate::cloud::ScalingConfig>,
    ) -> Result<()> {
        use genohype_core::query::QueryEngine;

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
            use crate::manhattan::reference::get_contig_lengths;

            println!("  {} Computing chromosome layout...", "Setup:".cyan());
            let contigs = get_contig_lengths(engine.as_ref().unwrap());
            let layout = ChromosomeLayout::new(&contigs, spec.width, 4);
            // Use a reasonable max -log10(p) for initial Y scale (will cover most GWAS hits)
            // Use high max to avoid cutting off extreme p-values (height can have -log10(p) > 100)
            let y_scale = YScale::new(spec.height, 300.0);
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

        // Check if coordinator is already running (started in idle mode during pool create)
        let coordinator_already_running = self.check_coordinator_status(coordinator, zone);

        if coordinator_already_running {
            println!(
                "{} Coordinator already running on {} ({})",
                "Found".green(),
                coordinator.name.cyan(),
                coord_ip
            );

            // Do not disrupt an existing job merely to discover that submission
            // needs --force. Forced replacement intentionally restarts workers.
            if !force {
                if let Ok(summary_json) =
                    self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/summary", 3000)
                {
                    if let Ok(summary) = serde_json::from_str::<
                        crate::distributed::message::DashboardSummary,
                    >(&summary_json)
                    {
                        if !summary.idle {
                            return Err(HailError::Io(std::io::Error::new(
                                std::io::ErrorKind::AlreadyExists,
                                "Coordinator already has a job running. Use --force to supersede.",
                            )));
                        }
                    }
                }
            }

            // Reinstall/enable/restart every unit before submission. The
            // coordinator registry can look healthy for 30 seconds after a worker
            // exits, which previously left a release smoke at 0/N indefinitely.
            self.start_worker_services(workers, coord_ip, zone)?;
            std::thread::sleep(std::time::Duration::from_secs(3));

            // Only count recent heartbeats; a non-dead status alone can be stale.
            let mut connected_count = 0;
            if let Ok(workers_json) =
                self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/workers", 3000)
            {
                if let Ok(worker_list) = serde_json::from_str::<
                    Vec<crate::distributed::message::DashboardWorker>,
                >(&workers_json)
                {
                    connected_count = worker_list.iter().filter(|w| worker_is_fresh(w)).count();
                }
            }

            if connected_count < workers.len() {
                // Some workers are not connected - start missing ones via SSH
                println!(
                    "{} {}/{} workers connected. Starting missing workers via SSH...",
                    "Warning:".yellow(),
                    connected_count,
                    workers.len()
                );
                self.start_worker_services(workers, coord_ip, zone)?;

                // Give workers a moment to connect to coordinator
                println!("{}", "Waiting for workers to connect...".dimmed());
                std::thread::sleep(std::time::Duration::from_secs(3));
            } else {
                println!(
                    "{} {}/{} workers already connected",
                    "OK".green(),
                    connected_count,
                    workers.len()
                );
            }

            // For ClickHouse export jobs, create the target table before submitting
            // Workers will INSERT into this table, so it must exist first
            #[cfg(feature = "clickhouse")]
            if let crate::distributed::message::JobSpec::ExportClickhouse {
                ref clickhouse_url,
                ref table_name,
            } = job_spec
            {
                use genohype_core::export::clickhouse::{generate_create_table, ClickHouseClient};
                use genohype_core::query::QueryEngine;

                println!(
                    "{} Creating ClickHouse table '{}'...",
                    "Setup:".cyan(),
                    table_name.bright_white()
                );

                // Read schema from the input Hail table
                let engine = QueryEngine::open_path(&input_path)?;
                let row_type = engine.row_type();
                let key_fields = engine.key_fields();

                // Generate CREATE TABLE IF NOT EXISTS DDL
                let ddl = generate_create_table(table_name, row_type, key_fields).map_err(|e| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to generate ClickHouse DDL: {}", e),
                    ))
                })?;

                // Execute DDL on ClickHouse
                let client = ClickHouseClient::new(clickhouse_url);
                client.execute(&ddl).map_err(|e| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create ClickHouse table: {}", e),
                    ))
                })?;

                println!("  {} Table '{}' ready", "OK".green(), table_name);
            }

            // Submit job via API
            println!("{}", "Submitting job via API...".dimmed());
            if !self.submit_job_via_api(
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
            )? {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to submit job to coordinator via API",
                )));
            }
            println!("{} Job submitted via API", "OK".green().bold());
        } else {
            // Start coordinator in idle/API mode (no pre-configured job)
            println!(
                "Starting coordinator on {} ({})...",
                coordinator.name.cyan(),
                coord_ip
            );

            // First ensure port is free
            let cleanup_cmd = "fuser -k 3000/tcp 2>/dev/null; true";
            let _ = self
                .provider
                .get_ssh_command(&coordinator.name, zone, cleanup_cmd)
                .status();
            std::thread::sleep(std::time::Duration::from_millis(500));

            // Start coordinator service in idle mode (accepts jobs via API)
            let backup_arg = config
                .and_then(|c| c.pool_db_path.as_deref())
                .map(|b| format!(" --backup-path {}", b))
                .unwrap_or_default();
            let coord_cmd = format!(
                "sudo bash -c 'cat > /etc/systemd/system/genohype-coordinator.service << EOF
[Unit]
Description=Genohype Coordinator
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/genohype service start-coordinator --port 3000 --db-path /var/lib/genohype/ops.db{}
Restart=always
RestartSec=3
StartLimitIntervalSec=0

[Install]
WantedBy=multi-user.target
EOF
' && sudo systemctl daemon-reload && sudo systemctl enable --now genohype-coordinator",
                backup_arg
            );

            let status = self
                .provider
                .get_ssh_command(&coordinator.name, zone, &coord_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to start coordinator service",
                )));
            }

            // Give coordinator a moment to bind its port
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Verify coordinator started successfully
            if !self.check_coordinator_status(coordinator, zone) {
                // Show log to help debug
                let log_cmd = "journalctl -u genohype-coordinator -n 50 --no-pager 2>/dev/null || echo '(no log available)'";
                if let Ok(output) = self
                    .provider
                    .get_ssh_command(&coordinator.name, zone, log_cmd)
                    .output()
                {
                    let log_content = String::from_utf8_lossy(&output.stdout);
                    eprintln!("\n{}", "Coordinator log:".red().bold());
                    eprintln!("{}", log_content);
                }
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Coordinator failed to start. See log above.",
                )));
            }

            // Check if any workers are already connected (may have started from startup script)
            let mut connected_count = 0;
            if let Ok(workers_json) =
                self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/workers", 3000)
            {
                if let Ok(worker_list) = serde_json::from_str::<
                    Vec<crate::distributed::message::DashboardWorker>,
                >(&workers_json)
                {
                    connected_count = worker_list.iter().filter(|w| worker_is_fresh(w)).count();
                }
            }

            if connected_count < workers.len() {
                // Start missing workers via SSH
                println!(
                    "Starting {} worker(s) ({} already connected)...",
                    workers.len().to_string().bright_white(),
                    connected_count
                );
                self.start_worker_services(workers, coord_ip, zone)?;

                // Give workers a moment to connect to coordinator
                println!("{}", "Waiting for workers to connect...".dimmed());
                std::thread::sleep(std::time::Duration::from_secs(3));
            } else {
                println!(
                    "{} {}/{} workers already connected",
                    "OK".green(),
                    connected_count,
                    workers.len()
                );
            }

            // Submit job via API (same as the "coordinator already running" path)
            println!("{}", "Submitting job via API...".dimmed());
            if !self.submit_job_via_api(
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
            )? {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to submit job to coordinator via API",
                )));
            }
            println!("{} Job submitted via API", "OK".green().bold());
        }

        println!();
        println!("{} Distributed job submitted!", "OK".green().bold());
        println!("  {} {}", "Coordinator:".cyan(), coordinator.name);
        println!("  {} {}", "Workers:".cyan(), workers.len());
        println!("  {} {}", "Total partitions:".cyan(), total_partitions);
        println!();
        println!(
            "{}",
            "Streaming coordinator logs (will exit on job completion)...".dimmed()
        );
        println!();

        // Stream coordinator logs, detecting job completion to exit automatically
        let mut log_cmd = self.provider.get_ssh_command(
            &coordinator.name,
            zone,
            "journalctl -u genohype-coordinator -f --no-pager",
        );

        log_cmd.stdout(std::process::Stdio::piped());
        log_cmd.stderr(std::process::Stdio::piped());

        let mut child = log_cmd.spawn().map_err(|e| {
            HailError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to start log streaming: {}", e),
            ))
        })?;

        let (job_failed, workers_lost) = if let Some(stdout) = child.stdout.take() {
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
            let mut no_worker_since = None;
            let mut last_health_check = Instant::now() - Duration::from_secs(10);

            while !complete {
                match line_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(Ok(line)) => {
                        println!("{}", line);
                        complete =
                            line.contains("Job complete. Coordinator returning to idle mode");
                        failed |= line.contains("Job finished with") && line.contains("failed");
                    }
                    Ok(Err(_)) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {}
                }

                if last_health_check.elapsed() >= Duration::from_secs(5) {
                    last_health_check = Instant::now();
                    if let Ok(json) = self.fetch_coordinator_api(
                        coordinator,
                        zone,
                        "/api/dashboard/workers",
                        3000,
                    ) {
                        if let Ok(worker_list) = serde_json::from_str::<
                            Vec<crate::distributed::message::DashboardWorker>,
                        >(&json)
                        {
                            let healthy = worker_list.iter().filter(|w| worker_is_fresh(w)).count();
                            if healthy == 0 {
                                let since = no_worker_since.get_or_insert_with(Instant::now);
                                if since.elapsed() >= Duration::from_secs(30) {
                                    break;
                                }
                            } else {
                                no_worker_since = None;
                            }
                        }
                    }
                }
            }

            let workers_lost = !complete
                && no_worker_since
                    .map(|since| since.elapsed() >= Duration::from_secs(30))
                    .unwrap_or(false);
            let _ = child.kill();
            let _ = child.wait();
            if complete {
                println!();
                println!("{} Job complete, exiting log stream.", "OK".green().bold());
            }
            (failed, workers_lost)
        } else {
            let _ = child.wait();
            (false, false)
        };

        if workers_lost {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "All workers stopped heartbeating for 30 seconds; aborting log tail",
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
            let mut stop_cmd = std::process::Command::new("gcloud");
            stop_cmd.args(["compute", "instances", "stop"]);

            let mut instance_names = vec![coordinator.name.as_str()];
            for w in workers {
                instance_names.push(&w.name);
            }

            stop_cmd.args(&instance_names);
            stop_cmd.args(["--zone", zone, "--quiet"]);

            match stop_cmd.status() {
                Ok(s) if s.success() => {
                    println!("{} Instances stopped.", "OK".green().bold())
                }
                _ => eprintln!("{} Failed to stop instances.", "Error:".red()),
            }
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
            build_version: None,
            effective_status: None,
        }
    }

    #[test]
    fn stale_registry_entry_is_not_treated_as_connected() {
        assert!(worker_is_fresh(&dashboard_worker("idle", 1.0)));
        assert!(!worker_is_fresh(&dashboard_worker("idle", 16.0)));
        assert!(!worker_is_fresh(&dashboard_worker("suspected_dead", 1.0)));
    }
}
