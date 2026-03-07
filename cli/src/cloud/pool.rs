//! Worker pool management for distributed processing.
//!
//! This module provides the `PoolManager` which orchestrates:
//! - Creating worker VMs
//! - Deploying the genohype binary
//! - Submitting distributed jobs
//! - Streaming logs and aggregating metrics
//! - Cleaning up resources

use crate::benchmark::BenchmarkReport;
use crate::cloud::{CloudProvider, Instance, PoolConfig, ProgressUpdate};
use crate::HailError;
use crate::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

/// Manages distributed worker pools for parallel processing.
pub struct PoolManager<P: CloudProvider> {
    provider: P,
}

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Create a new pool manager with the given cloud provider.
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    /// Stage the genohype binary to GCS for fast worker pulls.
    fn stage_binary_to_gcs(&self, binary: &Path, pool_db_path: &str) -> Result<String> {
        use genohype_core::io::CloudWriter;
        use std::io::Write;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Derive staging path (e.g. gs://bucket/path/ops.db -> gs://bucket/path)
        let base_dir = if pool_db_path.ends_with(".db") {
            let parts: Vec<&str> = pool_db_path.split('/').collect();
            parts[..parts.len().saturating_sub(1)].join("/")
        } else {
            pool_db_path.trim_end_matches('/').to_string()
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let staging_url = format!("{}/bin/genohype-worker-{}", base_dir, timestamp);

        println!(
            "{} Staging binary to {}...",
            "Setup:".cyan(),
            staging_url.dimmed()
        );

        let binary_data = std::fs::read(binary).map_err(HailError::Io)?;
        let mut writer = CloudWriter::new(&staging_url)?;
        writer.write_all(&binary_data).map_err(HailError::Io)?;
        writer.finish()?;

        Ok(staging_url)
    }

    /// Create a new worker pool.
    ///
    /// Provisions `config.worker_count` VMs in parallel.
    /// If `wait` is true, polls until all VMs have completed their startup scripts.
    /// Automatically builds Linux binary if on macOS (unless `skip_build` is true).
    /// If `with_coordinator` is true, also starts the coordinator in idle mode.
    pub fn create(&self, config: &PoolConfig, wait: bool, skip_build: bool) -> Result<()> {
        // Determine if we should build
        // 1. Explicit skip_build -> skip
        // 2. We have a bundled binary -> skip (Release mode)
        // 3. Otherwise -> build (Dev mode)
        let should_build = if skip_build {
            println!("{}", "Skipping binary build (--skip-build)".dimmed());
            false
        } else if self.has_bundled_binary() {
            println!("{}", "Found bundled worker binary, skipping build...".dimmed());
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

    /// Start coordinator in idle mode (binary already deployed via startup script or needs deployment).
    fn start_idle_coordinator(
        &self,
        pool_name: &str,
        zone: &str,
        _skip_build: bool,
        pool_db_path: Option<&str>,
        binary_deployed_via_startup: bool,
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

        // Binary deployment: skip if already done via startup script
        if binary_deployed_via_startup {
            println!(
                "{} Binary deployed via startup script (zero SSH)",
                "OK".green().bold()
            );
        } else {
            // Fallback: deploy via SCP
            let binary = self.locate_binary(None)?;
            println!(
                "{} Deploying binary to coordinator via SCP...",
                "Setup:".cyan()
            );
            self.deploy_binary(&binary, &[coordinator.clone()], zone)?;
        }

        // Start coordinator in idle mode
        println!(
            "{} Starting coordinator in idle mode on {}...",
            "Setup:".cyan(),
            coordinator.name.cyan()
        );

        let mut coord_cmd = String::from(
            "nohup /usr/local/bin/genohype service start-coordinator \
             --port 3000 \
             --db-path /var/lib/genohype/ops.db",
        );
        if let Some(backup) = pool_db_path {
            coord_cmd.push_str(&format!(" --backup-path {}", backup));
        }
        coord_cmd.push_str(" > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid");

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

        let coord_ip = coordinator.ip().unwrap_or("localhost");
        println!("{} Coordinator started in idle mode", "OK".green().bold());
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
            if binary_deployed_via_startup {
                println!(
                    "{} {} workers have binary via startup script",
                    "OK".green().bold(),
                    workers.len()
                );
            } else {
                // Fallback: workers pull from coordinator
                println!("{}", "Pre-deploying binary to workers...".dimmed());
                self.pre_deploy_binary_to_workers(coord_ip, &workers, zone)?;
                println!(
                    "{} Binary pre-deployed to {} worker(s)",
                    "OK".green().bold(),
                    workers.len()
                );
            }
        }

        Ok(())
    }

    /// Check if coordinator service is already running and reachable.
    fn check_coordinator_status(&self, coordinator: &Instance, zone: &str) -> bool {
        // Try to reach the coordinator's /status endpoint via SSH
        let mut cmd = self.provider.get_ssh_command(
            &coordinator.name,
            zone,
            "curl -s --connect-timeout 2 http://localhost:3000/status",
        );
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());

        if let Ok(output) = cmd.output() {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                // Check if it looks like a valid JSON response (new field names)
                return stdout.contains("\"pending_tasks\"") || stdout.contains("\"completed_tasks\"");
            }
        }
        false
    }

    /// Fetch data from coordinator API, trying local tunnel first (fast), then SSH (slow).
    ///
    /// If you have an IAP tunnel running (`gcloud compute ssh ... -L 3000:localhost:3000`),
    /// this will use it directly, avoiding the overhead of SSH per-request.
    fn fetch_coordinator_api(
        &self,
        coordinator: &Instance,
        zone: &str,
        endpoint: &str,
        port: u16,
    ) -> Result<String> {
        // Fast path: try local tunnel first (sub-second)
        let local_url = format!("http://localhost:{}{}", port, endpoint);
        let mut local_cmd = std::process::Command::new("curl");
        local_cmd.args(["-s", "--connect-timeout", "1", &local_url]);
        local_cmd.stdout(std::process::Stdio::piped());
        local_cmd.stderr(std::process::Stdio::null());

        if let Ok(output) = local_cmd.output() {
            if output.status.success() && !output.stdout.is_empty() {
                return Ok(String::from_utf8_lossy(&output.stdout).to_string());
            }
        }

        // Slow path: SSH through IAP (can take 5-30+ seconds)
        let remote_curl = format!("curl -s http://localhost:3000{}", endpoint);
        let mut cmd = self.provider.get_ssh_command(&coordinator.name, zone, &remote_curl);
        cmd.stdout(std::process::Stdio::piped());

        let output = cmd.output().map_err(HailError::Io)?;
        if !output.status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch {} from coordinator", endpoint),
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Submit job configuration to an already-running coordinator via its API.
    fn submit_job_via_api(
        &self,
        coordinator: &Instance,
        zone: &str,
        input_path: &str,
        job_spec: &crate::distributed::message::JobSpec,
        total_partitions: usize,
        force: bool,
        batch_size: Option<usize>,
        memory_weight_mb: Option<u64>,
        filters: &[String],
        intervals: &[String],
    ) -> Result<bool> {
        use crate::distributed::message::{JobConfigRequest, JobConfigResponse};

        // Use provided batch_size or fall back to sensible defaults per job type
        // Larger batches let workers parallelize with rayon
        let batch_size = batch_size.or_else(|| match job_spec {
            crate::distributed::message::JobSpec::Manhattan { .. } => Some(40),
            crate::distributed::message::JobSpec::ExportParquet { .. } => Some(100),
            _ => Some(50),
        });

        // Phase 3: Job-type memory weight hints (MB per partition)
        // Use CLI override if provided, otherwise fall back to job-type heuristics
        let memory_weight_mb = memory_weight_mb.or_else(|| match job_spec {
            crate::distributed::message::JobSpec::Manhattan { .. }
            | crate::distributed::message::JobSpec::ManhattanBatch { .. }
            | crate::distributed::message::JobSpec::ManhattanScan(_) => Some(1024), // 1GB per partition
            crate::distributed::message::JobSpec::ExportParquet { .. }
            | crate::distributed::message::JobSpec::ExportJson { .. } => Some(256), // 256MB
            crate::distributed::message::JobSpec::Summary => Some(64), // 64MB, very light
            _ => None,
        });

        let request = JobConfigRequest {
            input_path: input_path.to_string(),
            job_spec: job_spec.clone(),
            total_tasks: total_partitions,
            batch_size,
            force,
            filters: filters.to_vec(),
            intervals: intervals.to_vec(),
            memory_weight_mb,
        };

        let json_payload = serde_json::to_string(&request)
            .map_err(|e| HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize job config: {}", e),
            )))?;

        // Submit via curl through SSH
        // Determine submission method based on payload size
        // 4KB is a safe limit for command line arguments across most systems
        let curl_cmd = if json_payload.len() < 4096 {
            format!(
                "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/job",
                json_payload.replace('\'', "'\\''") // Escape single quotes for shell
            )
        } else {
            use std::io::Write;
            use std::time::{SystemTime, UNIX_EPOCH};

            println!("{}", "  Payload large, uploading job config file...".dimmed());

            // Create local temp file
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let local_filename = format!("hail_job_{}.json", timestamp);
            let mut local_path = std::env::temp_dir();
            local_path.push(&local_filename);

            {
                let mut file = std::fs::File::create(&local_path).map_err(HailError::Io)?;
                file.write_all(json_payload.as_bytes())
                    .map_err(HailError::Io)?;
            }

            let remote_path = format!("/tmp/{}", local_filename);

            // Upload to coordinator
            self.provider
                .upload_file(&local_path, &remote_path, &coordinator.name, zone)?;

            // Clean up local file
            let _ = std::fs::remove_file(&local_path);

            // Construct curl command using @file syntax
            // Also remove the remote file after successful submission
            format!(
                "curl -s -X POST -H 'Content-Type: application/json' -d @{} http://localhost:3000/api/job && rm {}",
                remote_path, remote_path
            )
        };

        let mut cmd = self.provider.get_ssh_command(&coordinator.name, zone, &curl_cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let output = cmd.output().map_err(HailError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("Failed to submit job via API: {}", stderr);
            return Ok(false);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse response
        match serde_json::from_str::<JobConfigResponse>(&stdout) {
            Ok(response) => {
                if response.acknowledged {
                    Ok(true)
                } else {
                    if let Some(err) = response.error {
                        eprintln!("Coordinator rejected job: {}", err);
                    }
                    Ok(false)
                }
            }
            Err(e) => {
                eprintln!("Failed to parse coordinator response: {} (raw: {})", e, stdout);
                Ok(false)
            }
        }
    }

    /// Wait for all instances in a pool to complete their startup scripts.
    fn wait_for_pool_ready(&self, pool_name: &str, zone: &str, timeout_secs: u64) -> Result<()> {
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
    pub fn destroy(&self, name: &str, zone: &str, metrics_bucket: Option<&str>) -> Result<()> {
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

    /// Export metrics database to GCS via coordinator API.
    /// Best-effort: failures are logged but don't block pool destruction.
    fn export_metrics_to_gcs(
        &self,
        pool_name: &str,
        coordinator: &Instance,
        zone: &str,
        bucket_path: &str,
    ) {
        use crate::distributed::message::{ExportMetricsRequest, ExportMetricsResponse};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate timestamp for unique filename
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Build the destination path
        let bucket_path = bucket_path.trim_end_matches('/');
        let destination = format!("{}/{}-{}-metrics.db", bucket_path, pool_name, timestamp);

        println!(
            "{} Exporting metrics to GCS...",
            "Saving:".cyan()
        );

        let request = ExportMetricsRequest {
            destination: destination.clone(),
        };

        let json_payload = match serde_json::to_string(&request) {
            Ok(j) => j,
            Err(e) => {
                println!(
                    "   {} Failed to serialize request: {}",
                    "Warning:".yellow(),
                    e
                );
                return;
            }
        };

        // Call the API via SSH curl
        let curl_cmd = format!(
            "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/export-metrics",
            json_payload.replace('\'', "'\\''")
        );

        let mut cmd = self.provider.get_ssh_command(&coordinator.name, zone, &curl_cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => {
                println!(
                    "   {} Failed to call export API: {}",
                    "Warning:".yellow(),
                    e
                );
                return;
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            println!(
                "   {} Export API call failed: {}",
                "Warning:".yellow(),
                stderr.trim()
            );
            return;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        match serde_json::from_str::<ExportMetricsResponse>(&stdout) {
            Ok(response) => {
                if response.success {
                    println!(
                        "   {} Metrics exported to {}",
                        "OK".green().bold(),
                        response.path.unwrap_or(destination).bright_white()
                    );
                } else {
                    println!(
                        "   {} {}",
                        "Warning:".yellow(),
                        response.error.unwrap_or_else(|| "Unknown error".to_string())
                    );
                }
            }
            Err(e) => {
                println!(
                    "   {} Failed to parse response: {} (raw: {})",
                    "Warning:".yellow(),
                    e,
                    stdout.trim()
                );
            }
        }
    }

    /// Cancel a distributed job running on the pool.
    pub fn cancel(&self, name: &str, zone: &str) -> Result<()> {
        let instances = self.provider.list_instances(name)?;
        let coordinator = instances.iter().find(|i| i.name.ends_with("-coordinator"));

        if let Some(coord) = coordinator {
            println!("Sending cancel request to {}...", coord.name);

            use crate::distributed::message::{CancelRequest, CancelResponse};
            let request = CancelRequest {
                reason: Some("CLI cancel command".to_string()),
            };
            let json_payload = serde_json::to_string(&request).unwrap();

            let cmd_str = format!(
                "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/cancel",
                json_payload
            );

            let mut cmd = self
                .provider
                .get_ssh_command(&coord.name, zone, &cmd_str);
            cmd.stdout(std::process::Stdio::piped());

            if let Ok(output) = cmd.output() {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    if let Ok(response) = serde_json::from_str::<CancelResponse>(&stdout) {
                        if response.success {
                            println!("{} {}", "Success:".green().bold(), response.message);
                        } else {
                            println!("{} {}", "Failed:".red().bold(), response.message);
                        }
                        return Ok(());
                    }
                }
            }
            println!("Failed to communicate with coordinator");
        } else {
            println!("No coordinator found for pool '{}'", name);
        }
        Ok(())
    }

    /// Get status of a distributed job running on the pool.
    pub fn status(&self, name: &str, zone: &str) -> Result<()> {
        let instances = self.provider.list_instances(name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No coordinator found for pool '{}'", name),
                ))
            })?;

        let json_str = self.fetch_coordinator_api(coordinator, zone, "/status", 3000)?;

        if let Ok(status) =
            serde_json::from_str::<crate::distributed::message::StatusResponse>(&json_str)
        {
            println!();
            println!("{}", "Job Status".bold().underline());
            println!(
                "  Progress:    {}/{} tasks ({:.1}%)",
                status.completed_tasks,
                status.total_tasks,
                if status.total_tasks > 0 {
                    (status.completed_tasks as f64 / status.total_tasks as f64) * 100.0
                } else {
                    0.0
                }
            );
            println!("  Processing:  {} workers active", status.processing_tasks);
            println!("  Pending:     {} tasks", status.pending_tasks);
            if status.failed_tasks > 0 {
                println!("  {} {} tasks", "Failed:".red(), status.failed_tasks);
            }
            println!("  Rows:        {}", status.total_items);
        } else {
            println!("Could not parse status response. Is the job running?");
        }
        Ok(())
    }

    /// List instances in a pool.
    pub fn list(&self, name: &str) -> Result<Vec<Instance>> {
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
    pub fn scale(
        &self,
        name: &str,
        target_workers: usize,
        zone: &str,
        binary_path: Option<String>,
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
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"));
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
                println!("{}", "Found bundled worker binary, skipping build...".dimmed());
                false
            } else {
                true
            };

            if should_build {
                Self::build_linux_binary(&[])?;
            }
            let binary = self.locate_binary(binary_path.clone())?;

            // Determine indices for new workers
            // Find existing indices and create new workers at gaps or at the end
            let mut existing_indices: Vec<usize> = workers
                .iter()
                .filter_map(|w| {
                    w.name
                        .split("-worker-")
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                })
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
                let tags = format!(
                    "genohype-worker,pool-{},role-worker",
                    name
                );

                new_instances.push(crate::cloud::InstanceSetup {
                    name: instance_name,
                    machine_type: config.machine_type.clone(),
                    zone: zone.to_string(),
                    tags: vec![tags],
                    startup_script: super::startup::generate_startup_script(None),
                    spot: config.spot,
                    network: config.network.clone(),
                    subnet: config.subnet.clone(),
                    project_id: project_id.clone(),
                });

                existing_indices.push(next_idx);
            }

            // Create instances
            self.provider.create_instances(&new_instances)?;
            println!(
                "{} Created {} new instances.",
                "OK".green().bold(),
                to_add
            );

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

            // Deploy binary
            if let Some(coord) = coordinator {
                if let Some(coord_ip) = coord.ip() {
                    // Coordinator exists, check if it's running to serve binary
                    if self.check_coordinator_status(coord, zone) {
                        println!(
                            "{}",
                            "Deploying binary via coordinator...".dimmed()
                        );
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
                        self.deploy_binary(&binary, &new_worker_instances, zone)?;
                    }
                } else {
                    self.deploy_binary(&binary, &new_worker_instances, zone)?;
                }
            } else {
                // No coordinator, direct SCP
                self.deploy_binary(&binary, &new_worker_instances, zone)?;
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
    fn wait_for_startup_complete(
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

    /// Update the binary on a running pool.
    ///
    /// This will:
    /// 1. Build the Linux binary (unless skip_build is true)
    /// 2. Upload the binary to the coordinator
    /// 3. Ensure coordinator is running (to serve /api/binary)
    /// 4. Have all workers pull the new binary from the coordinator
    ///
    /// This is useful for updating code on a long-running pool without
    /// destroying and recreating it.
    pub fn update_binary(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
        skip_build: bool,
        pool_db_path: Option<&str>,
    ) -> Result<()> {
        // Determine if we should build
        // Note: update_binary doesn't know about job features, defaulting to none.
        // Users needing features should use pool create or manual build.
        let should_build = if skip_build {
            println!("{}", "Skipping binary build (--skip-build)".dimmed());
            false
        } else if self.has_bundled_binary() {
            println!("{}", "Found bundled worker binary, skipping build...".dimmed());
            false
        } else {
            true
        };

        if should_build {
            Self::build_linux_binary(&[])?;
        }

        // Locate the binary
        let binary = self.locate_binary(binary_path)?;
        println!(
            "{} {}",
            "Binary:".cyan(),
            binary.display().to_string().bright_white()
        );

        // Get running instances
        println!("{}", "Fetching instance list...".dimmed());
        let instances = self.provider.list_instances(name)?;
        let running: Vec<_> = instances.into_iter().filter(|i| i.is_running()).collect();

        if running.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No running instances found for pool '{}'. Is the pool running?",
                    name
                ),
            )));
        }

        // Separate coordinator from workers
        let (coordinators, workers): (Vec<_>, Vec<_>) = running
            .into_iter()
            .partition(|i| i.name.ends_with("-coordinator"));

        let coordinator = coordinators.into_iter().next().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No coordinator found for pool '{}'. This command requires a coordinator.\n\
                     Create pool with: genohype pool create {} --with-coordinator",
                    name, name
                ),
            ))
        })?;

        let coord_ip = coordinator.ip().ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Coordinator {} has no internal IP", coordinator.name),
            ))
        })?;

        println!(
            "{} coordinator: {} ({}), {} workers",
            "Found".green(),
            coordinator.name.cyan(),
            coord_ip,
            workers.len().to_string().bright_white()
        );

        // Try to stage binary to GCS for fast updates
        let pool_db_path = pool_db_path.map(|s| s.to_string());
        let staging_url = if let Some(ref db_path) = pool_db_path {
            println!("{}", "Using fast GCS staging for update...".dimmed());
            self.stage_binary_to_gcs(&binary, db_path).ok()
        } else {
            None
        };

        // Always stop any running coordinator before updating (to avoid "Address already in use")
        println!(
            "{}",
            "Stopping any running coordinator service...".dimmed()
        );
        let stop_cmd = "pkill -9 -f 'genohype service start-coordinator' 2>/dev/null; \
                        pkill -9 -f 'genohype-worker' 2>/dev/null; \
                        fuser -k 3000/tcp 2>/dev/null; \
                        true";
        let _ = self
            .provider
            .get_ssh_command(&coordinator.name, zone, stop_cmd)
            .status();

        std::thread::sleep(std::time::Duration::from_secs(2));

        // Update coordinator binary via GCS (fast) or SCP (fallback)
        if let Some(ref gcs_url) = staging_url {
            println!(
                "{}",
                "Updating coordinator binary via GCS pull...".dimmed()
            );
            let update_coord_cmd = format!(
                "gsutil cp {} /tmp/genohype && chmod +x /tmp/genohype && sudo mv /tmp/genohype /usr/local/bin/genohype",
                gcs_url
            );
            let status = self
                .provider
                .get_ssh_command(&coordinator.name, zone, &update_coord_cmd)
                .status()
                .map_err(HailError::Io)?;
            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to update coordinator binary via GCS",
                )));
            }
        } else {
            println!("{}", "Uploading binary to coordinator via SCP...".dimmed());
            self.deploy_binary(&binary, &[coordinator.clone()], zone)?;
        }

        println!("{} Coordinator binary updated.", "OK".green().bold());

        // Start/restart coordinator service
        println!(
            "{}",
            "Starting coordinator service with new binary...".dimmed()
        );
        let mut coord_cmd = String::from(
            "nohup /usr/local/bin/genohype service start-coordinator \
             --port 3000 \
             --db-path /var/lib/genohype/ops.db",
        );
        if let Some(ref backup) = pool_db_path {
            coord_cmd.push_str(&format!(" --backup-path {}", backup));
        }
        coord_cmd.push_str(" > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid");
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

        // Wait for coordinator to be ready
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Verify coordinator is back up
        if !self.check_coordinator_status(&coordinator, zone) {
            let log_cmd = "tail -50 /tmp/coordinator.log 2>/dev/null || echo '(no log file)'";
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
                "Coordinator failed to start after binary update",
            )));
        }

        // Update workers
        if !workers.is_empty() {
            if let Some(ref gcs_url) = staging_url {
                // Zero-SSH fleet update: call /api/update-fleet
                println!(
                    "{} Instructing {} workers to self-update...",
                    "API:".cyan(),
                    workers.len()
                );
                let req = serde_json::json!({ "gcs_url": gcs_url });
                let curl_cmd = format!(
                    "curl -s -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/api/update-fleet",
                    req.to_string()
                );
                let status = self
                    .provider
                    .get_ssh_command(&coordinator.name, zone, &curl_cmd)
                    .status()
                    .map_err(HailError::Io)?;
                if !status.success() {
                    println!("{}", "Warning: Failed to call /api/update-fleet".yellow());
                }
            } else {
                // Fallback: workers pull from coordinator
                println!(
                    "{}",
                    format!(
                        "Workers pulling binary from coordinator ({})...",
                        coord_ip
                    )
                    .dimmed()
                );
                self.propagate_binary_from_coordinator(coord_ip, &workers, zone)?;
            }
        }

        println!();
        println!(
            "{} Binary updated on pool '{}'",
            "Done!".green().bold(),
            name.bright_white()
        );

        Ok(())
    }

    /// Update the binary via HTTP API (zero SSH).
    ///
    /// Requires an IAP tunnel to the coordinator on localhost:port.
    /// Uses /api/update-coordinator and /api/update-fleet endpoints.
    pub fn update_binary_via_api(
        &self,
        binary_path: Option<String>,
        skip_build: bool,
        pool_db_path: Option<&str>,
        port: u16,
    ) -> Result<()> {
        // Build if needed
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

        // Locate binary
        let binary = self.locate_binary(binary_path)?;
        println!(
            "{} {}",
            "Binary:".cyan(),
            binary.display().to_string().bright_white()
        );

        // Stage to GCS
        let pool_db_path = pool_db_path.ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "pool_db_path is required for --via-api (set in config or use pool create --pool-db-path)",
            ))
        })?;

        let gcs_url = self.stage_binary_to_gcs(&binary, pool_db_path)?;

        let base_url = format!("http://localhost:{}", port);
        let payload = serde_json::json!({ "gcs_url": gcs_url }).to_string();

        // Helper to call curl
        let curl_post = |endpoint: &str| -> Result<()> {
            let output = std::process::Command::new("curl")
                .args([
                    "-s",
                    "-w",
                    "\n%{http_code}", // Append the HTTP status code to stdout
                    "-X",
                    "POST",
                    "-H",
                    "Content-Type: application/json",
                    "-d",
                    &payload,
                    &format!("{}{}", base_url, endpoint),
                ])
                .output()
                .map_err(HailError::Io)?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            // Parse the output which should be "<body...>\n<http_code>"
            let mut parts = stdout.rsplitn(2, '\n');
            let http_code_str = parts.next().unwrap_or("").trim();
            let body = parts.next().unwrap_or("").trim();

            if let Ok(code) = http_code_str.parse::<u16>() {
                // HTTP code 0 means no response received (connection failed)
                if code == 0 {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!(
                            "Failed to connect to coordinator at {}. Is the IAP tunnel running?\n\
                             Start with: gcloud compute ssh <coordinator> --zone=<zone> --tunnel-through-iap -- -L {}:localhost:3000 -N",
                            base_url, port
                        ),
                    )));
                }
                if code >= 400 {
                    if code == 404 {
                        return Err(HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!(
                                "API call to {} returned 404 Not Found.\n\
                                 The running coordinator might be too old to support API updates.\n\
                                 Try updating via SSH first (set update_via_api = false in config temporarily).",
                                endpoint
                            ),
                        )));
                    }
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("API call to {} failed (HTTP {}): {}", endpoint, code, body),
                    )));
                }
            }

            if !output.status.success() {
                // Curl failed but we couldn't parse HTTP code - likely a connection issue
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!(
                        "Failed to connect to coordinator at {}. Is the IAP tunnel running?\n\
                         Start with: gcloud compute ssh <coordinator> --zone=<zone> --tunnel-through-iap -- -L {}:localhost:3000 -N\n\
                         curl error: {}",
                        base_url, port, if stderr.is_empty() { "connection failed" } else { &stderr }
                    ),
                )));
            }
            Ok(())
        };

        // Update coordinator
        println!("{} Updating coordinator via API...", "HTTP:".cyan());
        curl_post("/api/update-coordinator")?;

        println!(
            "{} Coordinator restarting with new binary...",
            "OK".green().bold()
        );

        // Wait for coordinator to come back up
        println!("{}", "Waiting for coordinator to restart...".dimmed());
        let mut retries = 0;
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let status = std::process::Command::new("curl")
                .args(["-s", "-f", &format!("{}/status", base_url)])
                .status();
            match status {
                Ok(s) if s.success() => break,
                _ => {
                    retries += 1;
                    if retries > 30 {
                        return Err(HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Coordinator did not come back up after 30 seconds",
                        )));
                    }
                }
            }
        }

        println!("{} Coordinator is back up.", "OK".green().bold());

        // Trigger fleet update immediately - workers will get the signal when they poll
        println!("{} Triggering worker updates via API...", "HTTP:".cyan());
        curl_post("/api/update-fleet")?;

        // Wait for workers to re-register and update
        // Workers poll every ~1s, so we wait for them to show up with recent heartbeats
        println!(
            "{}",
            "Waiting for workers to register and restart...".dimmed()
        );

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);
        let mut last_count = 0;
        let mut stable_iterations = 0;
        let mut max_workers_seen = 0;
        let mut iterations = 0;

        loop {
            std::thread::sleep(std::time::Duration::from_secs(2));
            iterations += 1;

            // Check how many workers have recent heartbeats (< 10 seconds old = updated)
            let output = std::process::Command::new("curl")
                .args(["-s", &format!("{}/api/dashboard/workers", base_url)])
                .output()
                .map_err(HailError::Io)?;

            if let Ok(workers) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                if let Some(arr) = workers.as_array() {
                    let total_workers = arr.len();
                    let updated_count = arr
                        .iter()
                        .filter(|w| {
                            w.get("last_seen_secs")
                                .and_then(|v| v.as_f64())
                                .map(|s| s < 10.0)
                                .unwrap_or(false)
                        })
                        .count();

                    max_workers_seen = max_workers_seen.max(total_workers);

                    if total_workers > 0 {
                        if updated_count != last_count {
                            println!(
                                "  {} {}/{} workers online",
                                "Progress:".dimmed(),
                                updated_count,
                                total_workers
                            );
                        }
                        last_count = updated_count;

                        // Consider done when all workers are online
                        if updated_count == total_workers {
                            stable_iterations += 1;
                            if stable_iterations >= 2 {
                                break;
                            }
                        } else {
                            stable_iterations = 0;
                        }
                    } else if iterations % 3 == 0 {
                        // Print waiting message every 6 seconds if no workers seen
                        print!(".");
                        use std::io::Write;
                        std::io::stdout().flush().ok();
                    }
                }
            }

            if start.elapsed() > timeout {
                println!();
                if max_workers_seen == 0 {
                    println!(
                        "{}",
                        "No workers registered yet. They may still be updating in background."
                            .yellow()
                    );
                } else {
                    println!(
                        "{}",
                        "Timed out waiting for workers. Some may still be updating.".yellow()
                    );
                }
                break;
            }
        }

        println!();
        if max_workers_seen > 0 {
            println!(
                "{} Binary updated on coordinator and {} workers.",
                "Done!".green().bold(),
                max_workers_seen
            );
        } else {
            println!("{} Coordinator updated.", "Done!".green().bold());
        }

        Ok(())
    }

    /// Submit a job to the worker pool.
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
    pub fn submit(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
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
        let features: Vec<&str> = if command.len() >= 2 && command[0] == "export" && command[1] == "clickhouse" {
            vec!["clickhouse"]
        } else if command.len() >= 2 && command[0] == "ingest" && command[1] == "manhattan" {
            vec!["clickhouse"]  // Ingest manhattan requires clickhouse feature
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
            println!("{}", "Found bundled worker binary, skipping build...".dimmed());
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
                    println!("{}", "Coordinator already running, skipping build...".dimmed());
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
            self.scale(name, target, zone, binary_path.clone(), true, pool_config)?;
        }

        // Run the actual job
        let result = self.submit_internal(
            name,
            zone,
            binary_path.clone(),
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
                println!(
                    "\n{} Autoscaling down to 0 workers...",
                    "Cleanup:".cyan()
                );
                // Ignore errors during scale down to ensure we return the job result
                if let Err(e) = self.scale(name, 0, zone, binary_path, true, pool_config) {
                    eprintln!("{} Failed to scale down: {}", "Warning:".yellow(), e);
                }
            }
        }

        result
    }

    /// Internal submit implementation (called by submit, handles the actual job).
    fn submit_internal(
        &self,
        name: &str,
        zone: &str,
        binary_path: Option<String>,
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

        // 3. Deploy binary - auto-skip if coordinator is already running (binary was deployed earlier)
        let should_deploy = if use_distributed {
            let coord = coordinator.as_ref().unwrap();
            let coord_running = self.check_coordinator_status(coord, zone);
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
                self.stage_binary_to_gcs(binary, db_path).ok()
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
                let stop_cmd = "pkill -f 'genohype service start-coordinator' || true";
                let _ = self
                    .provider
                    .get_ssh_command(&coord.name, zone, stop_cmd)
                    .status();
                std::thread::sleep(std::time::Duration::from_secs(1));

                // Deploy binary to coordinator via GCS (fast) or SCP (fallback)
                if let Some(ref gcs_url) = staging_url {
                    println!(
                        "{}",
                        "Deploying binary to coordinator via GCS...".dimmed()
                    );
                    let update_coord_cmd = format!(
                        "gsutil cp {} /tmp/genohype && chmod +x /tmp/genohype && sudo mv /tmp/genohype /usr/local/bin/genohype",
                        gcs_url
                    );
                    self.provider
                        .get_ssh_command(&coord.name, zone, &update_coord_cmd)
                        .status()
                        .map_err(HailError::Io)?;
                } else {
                    println!(
                        "{}",
                        "Deploying binary to coordinator via SCP...".dimmed()
                    );
                    self.deploy_binary(binary, &[coord.clone()], zone)?;
                }

                // Start coordinator service
                println!(
                    "{}",
                    "Starting coordinator service to serve binary/API...".dimmed()
                );
                let mut coord_cmd = String::from(
                    "nohup /usr/local/bin/genohype service start-coordinator \
                     --port 3000 \
                     --db-path /var/lib/genohype/ops.db",
                );
                if let Some(backup) = pool_db_path {
                    coord_cmd.push_str(&format!(" --backup-path {}", backup));
                }
                coord_cmd
                    .push_str(" > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid");
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

                // Update workers via GCS (zero-SSH) or coordinator pull (fallback)
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
                    println!(
                        "{}",
                        "Workers pulling binary from coordinator...".dimmed()
                    );
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
                .template("{prefix:.green.bold} [{bar:30.green/white}] {pos}/{len} partitions | {msg}")
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
                let mut cmd = self.provider.get_ssh_command(&inst_name, &inst_zone, &remote_cmd);
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
                                total_partitions_expected.load(Ordering::Relaxed) as u64,
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
    fn submit_distributed(
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
        let (input_path, mut job_spec, filters, intervals) = Self::parse_command_to_job_spec(command)?;

        // For IngestManhattan jobs, we don't read Hail table metadata
        // The coordinator discovers phenotypes at runtime
        let (total_partitions, engine) = if matches!(job_spec, crate::distributed::message::JobSpec::IngestManhattan { .. }) {
            println!("Ingestion job: phenotypes will be discovered by coordinator");
            (0, None)  // Coordinator will set this after discovering phenotypes
        } else if let crate::distributed::message::JobSpec::Stress(ref spec) = job_spec {
            println!("Stress job: queuing {} synthetic partitions", spec.partitions);
            (spec.partitions, None)
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

        // For ManhattanBatch jobs, compute layout and partition counts for all unique tables
        if let crate::distributed::message::JobSpec::ManhattanBatch { ref mut specs, .. } = job_spec {
            use crate::manhattan::layout::{ChromosomeLayout, YScale};
            use crate::manhattan::reference::get_contig_lengths;
            use std::collections::HashMap;

            if specs.is_empty() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "ManhattanBatch has no specs",
                )));
            }

            // Get first available width/height for layout
            let first_spec = &specs[0];
            let width = first_spec.width;
            let height = first_spec.height;

            // Compute layout from the first available table
            println!("  {} Computing chromosome layout...", "Setup:".cyan());
            let contigs = get_contig_lengths(engine.as_ref().unwrap());
            let contig_map: HashMap<String, u32> = contigs.iter().cloned().collect();
            let layout = ChromosomeLayout::new(&contigs, width, 4);
            let y_scale = YScale::new(height, 300.0);

            // Collect all unique table paths
            let mut exome_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut genome_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

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

            // Apply layout and partition counts to all specs
            for spec in specs.iter_mut() {
                spec.layout = Some(layout.clone());
                spec.y_scale = Some(y_scale.clone());
                spec.contig_lengths = Some(contig_map.clone());

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
            let with_exome = specs.iter().filter(|s| s.exome_partitions.is_some()).count();
            let with_genome = specs.iter().filter(|s| s.genome_partitions.is_some()).count();
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
            let base_dir: Option<String> = specs.first()
                .and_then(|s| s.output_path.rsplit_once('/'))
                .and_then(|(parent, _)| parent.rsplit_once('/'))
                .map(|(base, _)| base.to_string());

            if let Some(ref base_dir) = base_dir {
                let checkpoint_path = format!("{}/.completed", base_dir);
                println!("  {} Checking for completed phenotypes...", "Resume:".cyan());

                match read_completed_checkpoint(&checkpoint_path) {
                    Ok(completed) => {
                        if !completed.is_empty() {
                            let before = specs.len();
                            specs.retain(|s| {
                                // Extract relative path (ancestry/id) from output_path
                                let rel_path = s.output_path
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
        }

        drop(engine);  // Drop the QueryEngine if it exists (Option<QueryEngine>)

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

            // Start workers FIRST so they're connected when we submit the job
            println!(
                "Starting {} worker(s)...",
                workers.len().to_string().bright_white()
            );
            self.start_worker_services(workers, coord_ip, zone)?;

            // Give workers a moment to connect to coordinator
            println!("{}", "Waiting for workers to connect...".dimmed());
            std::thread::sleep(std::time::Duration::from_secs(3));

            // For ClickHouse export jobs, create the target table before submitting
            // Workers will INSERT into this table, so it must exist first
            #[cfg(feature = "clickhouse")]
            if let crate::distributed::message::JobSpec::ExportClickhouse {
                ref clickhouse_url,
                ref table_name,
            } = job_spec
            {
                use crate::export::clickhouse::{generate_create_table, ClickHouseClient};
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

                println!(
                    "  {} Table '{}' ready",
                    "OK".green(),
                    table_name
                );
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
            let mut coord_cmd = String::from(
                "nohup /usr/local/bin/genohype service start-coordinator \
                 --port 3000 \
                 --db-path /var/lib/genohype/ops.db"
            );
            if let Some(backup) = config.and_then(|c| c.pool_db_path.as_deref()) {
                coord_cmd.push_str(&format!(" --backup-path {}", backup));
            }
            coord_cmd.push_str(" > /tmp/coordinator.log 2>&1 & echo $! > /tmp/coordinator.pid");

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
                let log_cmd = "tail -50 /tmp/coordinator.log 2>/dev/null || echo '(no log file)'";
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

            // Start workers
            println!(
                "Starting {} worker(s)...",
                workers.len().to_string().bright_white()
            );
            self.start_worker_services(workers, coord_ip, zone)?;

            // Give workers a moment to connect to coordinator
            println!("{}", "Waiting for workers to connect...".dimmed());
            std::thread::sleep(std::time::Duration::from_secs(3));

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
        println!(
            "{} Distributed job submitted!",
            "OK".green().bold()
        );
        println!("  {} {}", "Coordinator:".cyan(), coordinator.name);
        println!("  {} {}", "Workers:".cyan(), workers.len());
        println!("  {} {}", "Total partitions:".cyan(), total_partitions);
        println!();
        println!("{}", "Streaming coordinator logs (Ctrl+C to exit)...".dimmed());
        println!();

        // Stream coordinator logs, exiting when the coordinator process exits
        let mut log_cmd = self.provider.get_ssh_command(
            &coordinator.name,
            zone,
            "tail -n +1 -f --pid=$(cat /tmp/coordinator.pid) /tmp/coordinator.log",
        );

        // This blocks until coordinator exits or user interrupts
        let _ = log_cmd.status();

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
                "Job finished. Stopping pool instances (--auto-stop)..."
                    .yellow()
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
    fn start_worker_services(&self, workers: &[Instance], coord_ip: &str, zone: &str) -> Result<()> {
        use rayon::prelude::*;

        let worker_results: Vec<Result<()>> = workers
            .par_iter()
            .map(|worker| {
                let worker_cmd = format!(
                    "nohup /usr/local/bin/genohype service start-worker \
                     --url http://{}:3000 \
                     --worker-id {} \
                     > /tmp/worker.log 2>&1 &",
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
    fn fetch_and_display_summary_results(&self, coordinator: &Instance, zone: &str) -> Result<()> {
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
        let response: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse result JSON: {}", e),
            )))?;

        // Check if results are available
        if !response.get("available").and_then(|v| v.as_bool()).unwrap_or(false) {
            let error = response.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Results not available: {}", error),
            )));
        }

        // Get the array of partial results from workers
        let results = response.get("result").and_then(|v| v.as_array()).ok_or_else(|| {
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
        println!("{} {}", "Row Count:".green(), total_rows.to_string().bright_white().bold());
        println!();

        // Print field statistics
        println!("{}", "Field Statistics:".green().bold());
        println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
            "Field".cyan(), "Count".cyan(), "Nulls".cyan(), "Min".cyan(), "Max".cyan());
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

            println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
                field_display,
                s.count,
                s.null_count,
                min_display,
                max_display
            );
        }

        Ok(())
    }

    /// Deploy binary to instances via SCP upload.
    fn deploy_binary(&self, binary: &Path, instances: &[Instance], zone: &str) -> Result<()> {
        instances.par_iter().try_for_each(|inst| {
            // Upload to /tmp first (user writable)
            self.provider
                .upload_file(binary, "/tmp/genohype", &inst.name, zone)?;

            // Make executable and move to /usr/local/bin (needs sudo)
            let setup_cmd =
                "chmod +x /tmp/genohype && sudo mv /tmp/genohype /usr/local/bin/genohype";
            let status = self
                .provider
                .get_ssh_command(&inst.name, zone, setup_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to install binary on {}", inst.name),
                )));
            }

            println!("   {} {}", "Deployed to".dimmed(), inst.name.cyan());
            Ok(())
        })
    }

    /// Have workers pull the binary from the coordinator over GCP internal network.
    ///
    /// This is much faster than uploading to each worker via SCP from the client machine,
    /// since it leverages the high-bandwidth GCP internal network.
    fn propagate_binary_from_coordinator(
        &self,
        coordinator_ip: &str,
        workers: &[Instance],
        zone: &str,
    ) -> Result<()> {
        workers.par_iter().try_for_each(|worker| {
            // Download binary from coordinator, install it, and restart worker process
            // The worker process must be restarted to pick up the new binary!
            let curl_cmd = format!(
                "curl -sL --retry 3 --retry-delay 2 http://{}:3000/api/binary -o /tmp/genohype && \
                 chmod +x /tmp/genohype && \
                 sudo mv /tmp/genohype /usr/local/bin/genohype && \
                 pkill -f 'genohype service start-worker' || true && \
                 sleep 1 && \
                 nohup /usr/local/bin/genohype service start-worker --url http://{}:3000 --worker-id {} > /tmp/worker.log 2>&1 &",
                coordinator_ip, coordinator_ip, worker.name
            );

            let status = self
                .provider
                .get_ssh_command(&worker.name, zone, &curl_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Failed to pull binary from coordinator on {}",
                        worker.name
                    ),
                )));
            }

            println!(
                "   {} {} (from coordinator)",
                "Deployed to".dimmed(),
                worker.name.cyan()
            );
            Ok(())
        })
    }

    /// Pre-deploy binary from coordinator to workers without starting worker processes.
    /// Used during pool creation to prime workers with the binary.
    fn pre_deploy_binary_to_workers(
        &self,
        coordinator_ip: &str,
        workers: &[Instance],
        zone: &str,
    ) -> Result<()> {
        workers.par_iter().try_for_each(|worker| {
            // Download binary from coordinator and install it, but don't start worker process
            let curl_cmd = format!(
                "curl -sL --retry 3 --retry-delay 2 http://{}:3000/api/binary -o /tmp/genohype && \
                 chmod +x /tmp/genohype && \
                 sudo mv /tmp/genohype /usr/local/bin/genohype",
                coordinator_ip
            );

            let status = self
                .provider
                .get_ssh_command(&worker.name, zone, &curl_cmd)
                .status()
                .map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Failed to pre-deploy binary to {}",
                        worker.name
                    ),
                )));
            }

            println!(
                "   {} {}",
                "Binary deployed to".dimmed(),
                worker.name.cyan()
            );
            Ok(())
        })
    }

    /// Run a job on a single worker, streaming output.
    fn run_worker_job(
        worker_id: usize,
        mut cmd: std::process::Command,
        tx: &mpsc::Sender<WorkerMessage>,
    ) -> Result<()> {
        let mut child = cmd.spawn().map_err(HailError::Io)?;

        // Stream stdout
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                if let Ok(l) = line {
                    // Check if line is JSON
                    if l.trim().starts_with('{') {
                        // Try to parse as progress update first
                        if l.contains("\"type\":\"progress\"") {
                            if let Ok(update) = serde_json::from_str::<ProgressUpdate>(&l) {
                                let _ = tx.send(WorkerMessage::Progress { worker_id, update });
                                continue;
                            }
                        }
                        // Try to parse as benchmark report
                        if l.contains("\"total_rows\"") {
                            if let Ok(report) = serde_json::from_str::<BenchmarkReport>(&l) {
                                let _ = tx.send(WorkerMessage::Report { worker_id, report });
                                continue;
                            }
                        }
                    }
                    // Otherwise send as log line
                    let _ = tx.send(WorkerMessage::Log {
                        worker_id,
                        line: l,
                    });
                }
            }
        }

        let status = child.wait().map_err(HailError::Io)?;
        if !status.success() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Worker {} exited with status: {}", worker_id, status),
            )));
        }

        let _ = tx.send(WorkerMessage::Complete { worker_id });
        Ok(())
    }

    /// Build the Linux binary for deployment to workers.
    ///
    /// On macOS, uses `cargo linux` (cargo-zigbuild) to cross-compile.
    /// On Linux, uses regular `cargo build`.
    fn build_linux_binary(features: &[&str]) -> Result<()> {
        let is_macos = cfg!(target_os = "macos");

        if is_macos {
            println!("{}", "Building Linux binary (cross-compiling)...".dimmed());

            // Build feature flag string if features are specified
            let features_flag = if features.is_empty() {
                String::new()
            } else {
                format!(" --features {}", features.join(","))
            };

            // Use shell to set ulimit first (fixes "too many open files" during linking)
            // Use full zigbuild command (not cargo linux alias) so it works from any directory
            // Suppress compiler warnings (already seen during local build) with RUSTFLAGS
            let cmd = format!(
                "ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null; RUSTFLAGS='-Awarnings' cargo zigbuild --target x86_64-unknown-linux-gnu --release{}",
                features_flag
            );

            let status = std::process::Command::new("sh")
                .args(["-c", &cmd])
                .status()
                .map_err(|e| {
                    HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "Failed to run 'cargo zigbuild'. Is cargo-zigbuild installed?\n\
                             Install with: cargo install cargo-zigbuild\n\
                             Error: {}",
                            e
                        ),
                    ))
                })?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to build Linux binary. Check cargo output above.",
                )));
            }

            // Verify the binary was created
            let binary_path = PathBuf::from("target/x86_64-unknown-linux-gnu/release/genohype");
            if !binary_path.exists() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Linux binary not found at: {}", binary_path.display()),
                )));
            }

            println!(
                "{} Linux binary built: {}",
                "OK".green().bold(),
                binary_path.display().to_string().dimmed()
            );
        } else {
            println!("{}", "Building release binary...".dimmed());

            let mut cmd = std::process::Command::new("cargo");
            cmd.args(["build", "--release", "--bin", "genohype"]);

            if !features.is_empty() {
                cmd.arg("--features");
                cmd.arg(features.join(","));
            }

            let status = cmd.status().map_err(HailError::Io)?;

            if !status.success() {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to build release binary",
                )));
            }

            println!("{} Release binary built", "OK".green().bold());
        }

        Ok(())
    }

    /// Check if a bundled worker binary exists next to the executable.
    fn has_bundled_binary(&self) -> bool {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                // Check for genohype-worker (standard release name)
                if exe_dir.join("genohype-worker").exists() {
                    return true;
                }
            }
        }
        false
    }

    /// Locate the Linux binary for deployment.
    fn locate_binary(&self, path: Option<String>) -> Result<PathBuf> {
        if let Some(p) = path {
            let path = PathBuf::from(&p);
            if path.exists() {
                return Ok(path);
            }
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Binary not found at: {}", p),
            )));
        }

        // 1. Try bundled binary (Release mode) - Check next to executable
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let bundled_path = exe_dir.join("genohype-worker");
                if bundled_path.exists() {
                    return Ok(bundled_path);
                }
            }
        }

        // 2. Try default cross-compile path (Dev mode)
        let default_path = PathBuf::from("target/x86_64-unknown-linux-gnu/release/genohype");
        if default_path.exists() {
            return Ok(default_path);
        }

        // 3. Try release path (if running on Linux)
        let release_path = PathBuf::from("target/release/genohype");
        if release_path.exists() {
            return Ok(release_path);
        }

        Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Linux binary not found.\n\
             \n\
             If on macOS, cross-compile for Linux:\n\
               cargo install cross\n\
               cross build --release --target x86_64-unknown-linux-gnu\n\
             \n\
             Or specify path with --binary",
        )))
    }

    /// Parse a command array into a JobSpec and input path.
    ///
    /// Supported formats:
    /// - `export parquet <input> <output> [--where ...] [--interval ...]`
    /// - `export json <input> <output> [--where ...] [--interval ...]`
    ///
    /// Returns (input_path, job_spec, filters, intervals)
    fn parse_command_to_job_spec(
        command: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::JobSpec;

        if command.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Empty command",
            )));
        }

        let cmd = command.get(0).map(|s| s.as_str()).unwrap_or("<empty>");

        // Handle 'summary <input>' command
        if cmd == "summary" {
            if command.len() < 2 {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Summary command requires: summary <input>\n\
                     Example: summary gs://bucket/input.ht",
                )));
            }
            let input_path = command[1].clone();
            return Ok((input_path, JobSpec::Summary, Vec::new(), Vec::new()));
        }

        // Handle 'manhattan' command
        if cmd == "manhattan" {
            return Self::parse_manhattan_command(&command[1..]);
        }

        // Handle 'manhattan-batch' command
        if cmd == "manhattan-batch" {
            return Self::parse_manhattan_batch_command(&command[1..]);
        }

        // Handle 'loci' command
        if cmd == "loci" {
            return Self::parse_loci_command(&command[1..]);
        }

        // Handle 'ingest' command
        if cmd == "ingest" {
            return Self::parse_ingest_command(&command[1..]);
        }

        // Handle 'stress' command
        if cmd == "stress" {
            return Self::parse_stress_command(&command[1..]);
        }

        // Expect: export <type> <input> <output> [args...]
        if cmd != "export" {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Distributed mode supports: export, summary, manhattan, manhattan-batch, loci, ingest. Got: '{}'\n\
                     Examples:\n  \
                     pool submit mypool -- export parquet gs://bucket/input.ht gs://bucket/output/\n  \
                     pool submit mypool -- summary gs://bucket/input.ht\n  \
                     pool submit mypool -- manhattan --exome gs://bucket/exome.ht --output gs://bucket/out/\n  \
                     pool submit mypool -- manhattan-batch --assets-json ./assets.json --output-dir gs://bucket/manhattans/\n  \
                     pool submit mypool -- loci --dir gs://bucket/manhattan_output/ --exome gs://bucket/exome.ht\n  \
                     pool submit mypool -- ingest manhattan --input-dir gs://bucket/manhattans/ --clickhouse-url http://ch:8123",
                    cmd
                ),
            )));
        }

        if command.len() < 4 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Export command requires: export <type> <input> <output>\n\
                 Example: export parquet gs://bucket/input.ht gs://bucket/output/",
            )));
        }

        let export_type = &command[1];
        let input_path = command[2].clone();
        let output_path = command[3].clone();

        // Parse optional arguments (--where, --interval)
        let mut filters = Vec::new();
        let mut intervals = Vec::new();
        let mut i = 4;
        while i < command.len() {
            match command[i].as_str() {
                "--where" => {
                    if i + 1 < command.len() {
                        filters.push(command[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--interval" => {
                    if i + 1 < command.len() {
                        intervals.push(command[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let job_spec = match export_type.as_str() {
            "parquet" => JobSpec::ExportParquet {
                output_path,
            },
            "json" => JobSpec::ExportJson {
                output_path,
                group_by: None,
            },
            "clickhouse" => {
                // Format: export clickhouse <input> <url> <table>
                // command[2] = input, command[3] = url, command[4] = table
                if command.len() < 5 {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Export clickhouse requires: export clickhouse <input> <url> <table>\n\
                         Example: export clickhouse gs://bucket/input.ht http://clickhouse:8123 my_table",
                    )));
                }
                let clickhouse_url = command[3].clone();
                let table_name = command[4].clone();

                JobSpec::ExportClickhouse {
                    clickhouse_url,
                    table_name,
                }
            },
            other => {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Unsupported export type for distributed mode: '{}'\n\
                         Supported types: parquet, json, clickhouse",
                        other
                    ),
                )));
            }
        };

        Ok((input_path, job_spec, filters, intervals))
    }

    /// Parse a `manhattan` command into a ManhattanSpec job.
    ///
    /// Supports: manhattan --exome <path> --genome <path> --output <path> [--threshold ...] ...
    fn parse_manhattan_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, ManhattanSpec};

        // Parse named arguments
        let mut exome: Option<String> = None;
        let mut exome_annotations: Option<String> = None;
        let mut genome: Option<String> = None;
        let mut genome_annotations: Option<String> = None;
        let mut gene_burden: Option<String> = None;
        let mut genes: Option<String> = None;
        let mut output: Option<String> = None;
        let mut threshold: f64 = 5e-8;
        let mut gene_threshold: f64 = 2.5e-6;
        let mut locus_threshold: f64 = 0.01;
        let mut locus_window: i32 = 1_000_000;
        let mut locus_plots = false;
        let mut min_variants_per_locus: usize = 1;
        let mut skip_composite = false;
        let mut width: u32 = 3000;
        let mut height: u32 = 800;
        let mut y_field = "Pvalue".to_string();
        let mut scan_only = false;
        let mut aggregate_only = false;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--exome" => {
                    if i + 1 < args.len() {
                        exome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome-annotations" => {
                    if i + 1 < args.len() {
                        exome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome" => {
                    if i + 1 < args.len() {
                        genome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome-annotations" => {
                    if i + 1 < args.len() {
                        genome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-burden" => {
                    if i + 1 < args.len() {
                        gene_burden = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genes" => {
                    if i + 1 < args.len() {
                        genes = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--output" => {
                    if i + 1 < args.len() {
                        output = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" | "--variant-threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().unwrap_or(5e-8);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().unwrap_or(2.5e-6);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-threshold" => {
                    if i + 1 < args.len() {
                        locus_threshold = args[i + 1].parse().unwrap_or(0.01);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().unwrap_or(1_000_000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = true;
                    i += 1;
                }
                "--min-variants-per-locus" => {
                    if i + 1 < args.len() {
                        min_variants_per_locus = args[i + 1].parse().unwrap_or(1);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--no-composite" => {
                    skip_composite = true;
                    i += 1;
                }
                "--width" => {
                    if i + 1 < args.len() {
                        width = args[i + 1].parse().unwrap_or(3000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--height" => {
                    if i + 1 < args.len() {
                        height = args[i + 1].parse().unwrap_or(800);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--y-field" => {
                    if i + 1 < args.len() {
                        y_field = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--scan-only" => {
                    scan_only = true;
                    i += 1;
                }
                "--aggregate-only" => {
                    aggregate_only = true;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Validate: need at least one input table and an output
        let output_path = output.ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Manhattan command requires --output <path>",
            ))
        })?;

        // Determine primary input for partition counting
        let input_path = exome
            .as_ref()
            .or(genome.as_ref())
            .or(gene_burden.as_ref())
            .cloned()
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Manhattan command requires at least one input table: --exome, --genome, or --gene-burden",
                ))
            })?;

        let spec = ManhattanSpec {
            // Identity metadata - None for single mode, extracted from output path by coordinator
            phenotype: None,
            ancestry: None,
            exome,
            exome_annotations,
            genome,
            genome_annotations,
            gene_burden,
            genes,
            exome_exp_p: None,  // Not supported in single-job CLI mode
            genome_exp_p: None, // Not supported in single-job CLI mode
            threshold,
            gene_threshold,
            locus_threshold,
            locus_window,
            locus_plots,
            min_variants_per_locus,
            width,
            height,
            y_field,
            output_path,
            layout: None,  // Computed by coordinator before dispatch
            y_scale: None, // Computed by coordinator before dispatch
            contig_lengths: None, // Computed by submit_distributed
            skip_composite,
            exome_partitions: None, // Computed by submit_distributed
            genome_partitions: None, // Computed by submit_distributed
            styling: crate::manhattan::config::ManhattanConfig::default(),
        };

        let mode = if scan_only {
            crate::distributed::message::ExecutionMode::ScanOnly
        } else if aggregate_only {
            crate::distributed::message::ExecutionMode::AggregateOnly
        } else {
            crate::distributed::message::ExecutionMode::Full
        };

        Ok((input_path, JobSpec::Manhattan { spec, mode }, Vec::new(), Vec::new()))
    }

    /// Parse a `manhattan-batch` command into a ManhattanBatch job.
    ///
    /// Supports: manhattan-batch --config <path> or --assets-json <path> --output-dir <path> [--analysis-ids <id,...>] ...
    fn parse_manhattan_batch_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, ManhattanSpec};
        use crate::manhattan::batch::{load_and_group_assets, create_specs, BatchConfig};
        use crate::manhattan::config::ManhattanJobConfig;

        // Parse named arguments
        let mut config_path: Option<String> = None;
        let mut assets_json: Option<String> = None;
        let mut output_dir: Option<String> = None;
        let mut analysis_ids: Option<Vec<String>> = None;
        let mut ancestries: Option<Vec<String>> = None;
        let mut sample: Option<f64> = None;
        let mut limit: Option<usize> = None;
        let mut genes: Option<String> = None;
        let mut exome_annotations: Option<String> = None;
        let mut genome_annotations: Option<String> = None;
        let mut threshold: Option<f64> = None;
        let mut gene_threshold: Option<f64> = None;
        let mut locus_threshold: Option<f64> = None;
        let mut locus_window: Option<i32> = None;
        let mut locus_plots: Option<bool> = None;
        let mut width: Option<u32> = None;
        let mut height: Option<u32> = None;
        let mut y_field: Option<String> = None;
        let mut scan_only = false;
        let mut aggregate_only = false;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--config" => {
                    if i + 1 < args.len() {
                        config_path = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--assets-json" => {
                    if i + 1 < args.len() {
                        assets_json = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--output-dir" => {
                    if i + 1 < args.len() {
                        output_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--analysis-ids" => {
                    if i + 1 < args.len() {
                        let ids: Vec<String> = args[i + 1]
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        if !ids.is_empty() {
                            analysis_ids = Some(ids);
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--ancestries" => {
                    if i + 1 < args.len() {
                        let ancs: Vec<String> = args[i + 1]
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        if !ancs.is_empty() {
                            ancestries = Some(ancs);
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--sample" => {
                    if i + 1 < args.len() {
                        sample = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--limit" => {
                    if i + 1 < args.len() {
                        limit = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genes" => {
                    if i + 1 < args.len() {
                        genes = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome-annotations" => {
                    if i + 1 < args.len() {
                        exome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome-annotations" => {
                    if i + 1 < args.len() {
                        genome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" | "--variant-threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-threshold" => {
                    if i + 1 < args.len() {
                        locus_threshold = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = Some(true);
                    i += 1;
                }
                "--width" => {
                    if i + 1 < args.len() {
                        width = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--height" => {
                    if i + 1 < args.len() {
                        height = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--y-field" => {
                    if i + 1 < args.len() {
                        y_field = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--scan-only" => {
                    scan_only = true;
                    i += 1;
                }
                "--aggregate-only" => {
                    aggregate_only = true;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Load config file if provided
        let job_config = if let Some(ref path) = config_path {
            ManhattanJobConfig::load(std::path::Path::new(path))?
        } else {
            ManhattanJobConfig::default()
        };

        // Merge CLI arguments with config (CLI overrides config)
        let assets_json = assets_json
            .or(job_config.job.assets_json.clone())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "manhattan-batch requires --assets-json <path> or job.assets_json in config",
                ))
            })?;

        let output_dir = output_dir
            .or(job_config.job.output_dir.clone())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "manhattan-batch requires --output-dir <path> or job.output_dir in config",
                ))
            })?;

        // Merge other settings (CLI overrides config)
        let analysis_ids = analysis_ids.or_else(|| {
            if job_config.job.analysis_ids.is_empty() {
                None
            } else {
                Some(job_config.job.analysis_ids.clone())
            }
        });
        let ancestries = ancestries.or_else(|| {
            if job_config.job.ancestries.is_empty() {
                None
            } else {
                Some(job_config.job.ancestries.clone())
            }
        });
        let sample = sample.or(job_config.job.sample);
        let limit = limit.or(job_config.job.limit);
        let genes = genes.or(job_config.job.genes.clone());
        let exome_annotations = exome_annotations.or(job_config.job.exome_annotations.clone());
        let genome_annotations = genome_annotations.or(job_config.job.genome_annotations.clone());
        let threshold = threshold.unwrap_or(job_config.job.threshold);
        let gene_threshold = gene_threshold.unwrap_or(job_config.job.gene_threshold);
        let locus_threshold = locus_threshold.unwrap_or(job_config.job.locus_threshold);
        let locus_window = locus_window.unwrap_or(job_config.job.locus_window);
        let locus_plots = locus_plots.unwrap_or(job_config.job.locus_plots);
        let min_variants_per_locus = job_config.job.min_variants_per_locus;
        let width = width.unwrap_or(job_config.job.width);
        let height = height.unwrap_or(job_config.job.height);
        let y_field = y_field.unwrap_or(job_config.job.y_field.clone());
        let styling = job_config.styling();

        // Load and group assets
        let inputs = load_and_group_assets(&assets_json, analysis_ids.as_deref(), ancestries.as_deref(), sample, limit)?;

        if inputs.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No phenotypes found in assets JSON (check filters if specified)",
            )));
        }

        // Create batch config
        let config = BatchConfig {
            output_dir,
            threshold,
            gene_threshold,
            locus_threshold,
            locus_window,
            locus_plots,
            min_variants_per_locus,
            width,
            height,
            y_field,
            genes_path: genes,
            exome_annotations,
            genome_annotations,
            styling,
        };

        // Convert to specs
        let specs: Vec<ManhattanSpec> = create_specs(inputs, &config);

        // For batch jobs, we need a dummy input path for the coordinator
        // The actual tables are specified per-spec. We use the first available
        // table path as the "primary" for any initialization the coordinator needs.
        let primary_input = specs
            .iter()
            .find_map(|s| s.primary_input_path())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "batch".to_string());

        let scan_only = scan_only || job_config.job.scan_only;
        let aggregate_only = aggregate_only || job_config.job.aggregate_only;

        let mode = if scan_only {
            crate::distributed::message::ExecutionMode::ScanOnly
        } else if aggregate_only {
            crate::distributed::message::ExecutionMode::AggregateOnly
        } else {
            crate::distributed::message::ExecutionMode::Full
        };

        Ok((primary_input, JobSpec::ManhattanBatch { specs, mode }, Vec::new(), Vec::new()))
    }

    /// Parse a `loci` command into a LociSpec job.
    fn parse_loci_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, LociSpec};

        let mut output_dir: Option<String> = None;
        let mut exome: Option<String> = None;
        let mut genome: Option<String> = None;
        let mut gene_burden: Option<String> = None;
        let mut threshold: f64 = 5e-8;
        let mut gene_threshold: f64 = 2.5e-6;
        let mut locus_window: i32 = 1_000_000;
        let mut locus_plots: bool = false;
        let mut min_variants_per_locus: usize = 1;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--dir" => {
                    if i + 1 < args.len() {
                        output_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome" => {
                    if i + 1 < args.len() {
                        exome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome" => {
                    if i + 1 < args.len() {
                        genome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-burden" => {
                    if i + 1 < args.len() {
                        gene_burden = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().unwrap_or(5e-8);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().unwrap_or(2.5e-6);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().unwrap_or(1_000_000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = true;
                    i += 1;
                }
                "--min-variants-per-locus" => {
                    if i + 1 < args.len() {
                        min_variants_per_locus = args[i + 1].parse().unwrap_or(1);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let output_dir = output_dir.ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Loci command requires --dir <manhattan_output_directory>",
            ))
        })?;

        if exome.is_none() && genome.is_none() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Loci command requires at least one of --exome or --genome",
            )));
        }

        let spec = LociSpec {
            output_dir: output_dir.clone(),
            exome_results: exome,
            genome_results: genome,
            gene_burden,
            locus_window,
            threshold,
            gene_threshold,
            locus_plots,
            min_variants_per_locus,
        };

        // Use output_dir as the "input_path" for job tracking
        Ok((output_dir, JobSpec::Loci(spec), Vec::new(), Vec::new()))
    }

    /// Parse a `stress` command into a StressSpec job.
    fn parse_stress_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, StressSpec};

        let mut partitions = 100;
        let mut cpu_secs = 0.0;
        let mut memory_mb = 0;
        let mut read_path = None;
        let mut write_dir = None;
        let mut generate_read_data = false;
        let mut read_data_size_mb = 32;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--partitions" | "--tasks" => {
                    if i + 1 < args.len() {
                        partitions = args[i + 1].parse().unwrap_or(100);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--cpu-secs" => {
                    if i + 1 < args.len() {
                        cpu_secs = args[i + 1].parse().unwrap_or(0.0);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--memory-mb" => {
                    if i + 1 < args.len() {
                        memory_mb = args[i + 1].parse().unwrap_or(0);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--read-path" => {
                    if i + 1 < args.len() {
                        read_path = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--write-dir" => {
                    if i + 1 < args.len() {
                        write_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--generate-read-data" => {
                    generate_read_data = true;
                    i += 1;
                }
                "--read-data-size-mb" => {
                    if i + 1 < args.len() {
                        read_data_size_mb = args[i + 1].parse().unwrap_or(32);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => i += 1,
            }
        }

        // Validate: --generate-read-data requires --write-dir
        if generate_read_data && write_dir.is_none() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--generate-read-data requires --write-dir to be set",
            )));
        }

        let spec = StressSpec {
            partitions,
            cpu_secs,
            memory_mb,
            read_path,
            write_dir,
            generate_read_data,
            read_data_size_mb,
        };

        // Use a dummy input path and empty filters since stress tests don't read Hail tables
        Ok(("stress_synthetic".to_string(), JobSpec::Stress(spec), Vec::new(), Vec::new()))
    }

    /// Parse an `ingest` command into an IngestManhattan job.
    ///
    /// Supports: ingest manhattan --input-dir <path> --clickhouse-url <url> [--database <db>]
    fn parse_ingest_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        if args.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Ingest command requires a subcommand: ingest manhattan ...",
            )));
        }

        let subcommand = args[0].as_str();

        match subcommand {
            "manhattan" => Self::parse_ingest_manhattan_command(&args[1..]),
            other => Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Unknown ingest subcommand: '{}'\n\
                     Supported: manhattan",
                    other
                ),
            ))),
        }
    }

    /// Parse `ingest manhattan` command arguments.
    fn parse_ingest_manhattan_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{InitStrategy, JobSpec};
        use crate::manhattan::config::ManhattanJobConfig;

        let mut config_path: Option<String> = None;
        let mut input_dir: Option<String> = None;
        let mut clickhouse_url: Option<String> = None;
        let mut database: Option<String> = None;
        let mut init_strategy: Option<InitStrategy> = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--config" => {
                    if i + 1 < args.len() {
                        config_path = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--input-dir" => {
                    if i + 1 < args.len() {
                        input_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--clickhouse-url" => {
                    if i + 1 < args.len() {
                        clickhouse_url = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--database" => {
                    if i + 1 < args.len() {
                        database = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--init-strategy" => {
                    if i + 1 < args.len() {
                        init_strategy = Some(match args[i + 1].to_lowercase().as_str() {
                            "create" => InitStrategy::Create,
                            "replace" => InitStrategy::Replace,
                            "append" => InitStrategy::Append,
                            other => {
                                return Err(HailError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    format!(
                                        "Invalid init-strategy '{}'. Must be: create, replace, or append",
                                        other
                                    ),
                                )));
                            }
                        });
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Load config file if provided
        let job_config = if let Some(ref path) = config_path {
            ManhattanJobConfig::load(std::path::Path::new(path))?
        } else {
            ManhattanJobConfig::default()
        };

        // Merge CLI args with config (CLI overrides)
        let input_dir = input_dir
            .or(job_config.ingest_input_dir())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Ingest manhattan requires --input-dir <gcs_path> or ingest.input_dir/job.output_dir in config\n\
                     Example: ingest manhattan --input-dir gs://bucket/manhattans/ --clickhouse-url http://ch:8123",
                ))
            })?;

        let clickhouse_url = clickhouse_url
            .or(job_config.ingest.clickhouse_url.clone())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Ingest manhattan requires --clickhouse-url <url> or ingest.clickhouse_url in config\n\
                     Example: ingest manhattan --input-dir gs://bucket/manhattans/ --clickhouse-url http://ch:8123",
            ))
        })?;

        // Merge database and init_strategy with config defaults
        let database = database.unwrap_or(job_config.ingest.database.clone());
        let init_strategy = init_strategy.unwrap_or_else(|| {
            match job_config.ingest.init_strategy.to_lowercase().as_str() {
                "replace" => InitStrategy::Replace,
                "append" => InitStrategy::Append,
                _ => InitStrategy::Create,
            }
        });

        let spec = JobSpec::IngestManhattan {
            input_dir: input_dir.clone(),
            clickhouse_url,
            database,
            init_strategy,
        };

        // Use input_dir as the "input_path" for job tracking
        Ok((input_dir, spec, Vec::new(), Vec::new()))
    }

    /// Show real-time worker activity.
    pub fn workers(&self, name: &str, zone: &str) -> Result<()> {
        use crate::distributed::message::DashboardWorker;

        let instances = self.provider.list_instances(name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No coordinator found for pool '{}'", name),
                ))
            })?;

        let json_str =
            self.fetch_coordinator_api(coordinator, zone, "/api/dashboard/workers", 3000)?;

        let workers: Vec<DashboardWorker> = serde_json::from_str(&json_str).map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse worker response: {}", e),
            ))
        })?;

        println!("{}", "Worker Activity".bold().underline());
        println!();

        for w in &workers {
            let status_color = match w.status.as_str() {
                "active" | "Active" => w.status.green().to_string(),
                "idle" | "Idle" => w.status.yellow().to_string(),
                _ => w.status.red().to_string(),
            };

            println!(
                "  {} [{}] last seen {:.1}s ago",
                w.worker_id.cyan(),
                status_color,
                w.last_seen_secs
            );

            if let Some(ref task) = w.current_task {
                let duration_s = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
                    - task.started_at_ms)
                    / 1000;
                println!(
                    "    Task: {} phase={} tasks={:?} ({}s)",
                    task.task_id.dimmed(),
                    task.phase.bright_white(),
                    task.tasks,
                    duration_s
                );
            } else {
                println!("    {}", "(no active task)".dimmed());
            }

            if let Some(ref latest) = w.telemetry {
                let cpu = latest.cpu_percent.unwrap_or(0.0);
                let mem_gb = latest
                    .memory_used_bytes
                    .map(|b| b as f64 / 1_073_741_824.0)
                    .unwrap_or(0.0);
                println!(
                    "    CPU: {:.1}%  Mem: {:.1} GB  Rows/s: {:.0}",
                    cpu, mem_gb, latest.items_per_sec
                );
            }
            println!();
        }

        Ok(())
    }

    /// Tail the event log.
    pub fn events(&self, name: &str, zone: &str, follow: bool) -> Result<()> {
        use crate::distributed::message::EventsResponse;

        let instances = self.provider.list_instances(name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No coordinator found for pool '{}'", name),
                ))
            })?;

        let fetch_events = |since_ms: u64| -> Result<EventsResponse> {
            let endpoint = format!("/api/events?since_ms={}", since_ms);
            let json_str = self.fetch_coordinator_api(coordinator, zone, &endpoint, 3000)?;

            serde_json::from_str(&json_str).map_err(|e| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse events response: {}", e),
                ))
            })
        };

        let print_event = |event: &crate::distributed::message::JobEvent| {
            let type_color = match event.event_type.as_str() {
                "completed" => event.event_type.green().to_string(),
                "failed" => event.event_type.red().to_string(),
                "assigned" => event.event_type.cyan().to_string(),
                "requeued" => event.event_type.yellow().to_string(),
                _ => event.event_type.to_string(),
            };

            // Format timestamp as seconds since epoch for simplicity
            let secs = event.timestamp_ms / 1000;
            let millis = event.timestamp_ms % 1000;
            let timestamp = format!("{}.{:03}", secs, millis);

            println!(
                "[{}] {} {} {}",
                timestamp.dimmed(),
                type_color,
                event
                    .worker_id
                    .as_deref()
                    .unwrap_or("-")
                    .cyan(),
                event.details
            );
        };

        println!("{}", "Event Log".bold().underline());
        println!();

        // Initial fetch (get all events)
        let response = fetch_events(0)?;
        let mut last_timestamp = 0u64;
        for event in &response.events {
            print_event(event);
            if event.timestamp_ms > last_timestamp {
                last_timestamp = event.timestamp_ms;
            }
        }

        if follow {
            println!();
            println!("{}", "(following events, Ctrl+C to stop)".dimmed());
            loop {
                std::thread::sleep(std::time::Duration::from_secs(2));
                let response = fetch_events(last_timestamp)?;
                for event in &response.events {
                    print_event(event);
                    if event.timestamp_ms > last_timestamp {
                        last_timestamp = event.timestamp_ms;
                    }
                }
            }
        }

        Ok(())
    }

    /// Show recent task failures.
    pub fn failures(&self, name: &str, zone: &str) -> Result<()> {
        use crate::distributed::message::FailuresResponse;

        let instances = self.provider.list_instances(name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No coordinator found for pool '{}'", name),
                ))
            })?;

        let json_str = self.fetch_coordinator_api(coordinator, zone, "/api/failures", 3000)?;

        let response: FailuresResponse = serde_json::from_str(&json_str).map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse failures response: {}", e),
            ))
        })?;

        println!("{}", "Recent Failures".bold().underline());
        println!();

        if response.failures.is_empty() {
            println!("{}", "No failures recorded.".green());
        } else {
            for f in &response.failures {
                // Format timestamp as seconds since epoch for simplicity
                let secs = f.timestamp_ms / 1000;
                let millis = f.timestamp_ms % 1000;
                let timestamp = format!("{}.{:03}", secs, millis);

                println!(
                    "[{}] {} tasks {:?}",
                    timestamp.dimmed(),
                    f.worker_id.cyan(),
                    f.tasks
                );
                println!("  {}: {}", "Error".red(), f.error);
                if f.retry_count > 0 {
                    println!("  Retry count: {}", f.retry_count);
                }
                println!();
            }
        }

        Ok(())
    }

    /// Show tail of a specific worker's logs.
    pub fn logs(&self, name: &str, zone: &str, worker_id: &str) -> Result<()> {
        let instances = self.provider.list_instances(name)?;
        let coordinator = instances
            .iter()
            .find(|i| i.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No coordinator found for pool '{}'", name),
                ))
            })?;

        let endpoint = format!("/api/workers/{}/logs", worker_id);
        let json_str = self.fetch_coordinator_api(coordinator, zone, &endpoint, 3000)?;

        let logs: Vec<String> = serde_json::from_str(&json_str).map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse logs response: {}", e),
            ))
        })?;

        println!(
            "{} (last 50 lines)",
            format!("Logs for worker {}", worker_id).bold().underline()
        );
        println!();

        for line in &logs {
            println!("{}", line);
        }

        Ok(())
    }
}

/// Messages sent from worker threads to the coordinator.
enum WorkerMessage {
    /// A log line from the worker
    Log { worker_id: usize, line: String },
    /// A progress update from the worker
    Progress {
        worker_id: usize,
        update: ProgressUpdate,
    },
    /// A benchmark report from the worker
    Report {
        worker_id: usize,
        report: BenchmarkReport,
    },
    /// Worker completed successfully
    Complete { worker_id: usize },
    /// Worker encountered an error
    Error { worker_id: usize, message: String },
}

/// Read the checkpoint file listing completed phenotypes.
///
/// The checkpoint file is a simple newline-delimited list of relative paths
/// like "meta/height" or "afr/1234".
fn read_completed_checkpoint(checkpoint_path: &str) -> Result<std::collections::HashSet<String>> {
    use object_store::ObjectStore;
    use object_store::path::Path as ObjPath;

    let url = url::Url::parse(checkpoint_path).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid checkpoint URL: {}", e),
        ))
    })?;

    let (store, path): (std::sync::Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Missing bucket in GCS URL",
                ))
            })?;
            let path = url.path().trim_start_matches('/');
            (genohype_core::io::get_gcs_client(bucket)?, ObjPath::from(path))
        }
        scheme => {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unsupported URL scheme for checkpoint: {}", scheme),
            )));
        }
    };

    // Read the file contents
    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    })?;

    let bytes = rt.block_on(async {
        store.get(&path).await?.bytes().await
    }).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to read checkpoint: {}", e),
        ))
    })?;

    // Parse as newline-delimited list
    let content = String::from_utf8_lossy(&bytes);
    let completed: std::collections::HashSet<String> = content
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    Ok(completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_locate_binary_not_found() {
        struct MockProvider;
        impl CloudProvider for MockProvider {
            fn create_pool(&self, _: &PoolConfig) -> Result<()> {
                Ok(())
            }
            fn list_instances(&self, _: &str) -> Result<Vec<Instance>> {
                Ok(vec![])
            }
            fn destroy_pool(&self, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn create_instances(&self, _: &[super::super::InstanceSetup]) -> Result<()> {
                Ok(())
            }
            fn delete_instances(&self, _: &[String], _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn upload_file(&self, _: &Path, _: &str, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
            fn get_ssh_command(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> std::process::Command {
                std::process::Command::new("echo")
            }
        }

        let manager = PoolManager::new(MockProvider);
        let result = manager.locate_binary(Some("/nonexistent/path".to_string()));
        assert!(result.is_err());
    }
}
