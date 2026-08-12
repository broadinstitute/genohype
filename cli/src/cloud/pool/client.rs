//! Coordinator API client operations.

use super::PoolManager;
use crate::cloud::{CloudProvider, Instance};
use crate::HailError;
use crate::Result;
use owo_colors::OwoColorize;

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Check if coordinator service is already running and reachable.
    pub(crate) fn check_coordinator_status(&self, coordinator: &Instance, zone: &str) -> bool {
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
                return stdout.contains("\"pending_tasks\"")
                    || stdout.contains("\"completed_tasks\"");
            }
        }
        false
    }

    /// Fetch data from coordinator API, trying local tunnel first (fast), then SSH (slow).
    ///
    /// If you have an IAP tunnel running (`gcloud compute ssh ... -L 3000:localhost:3000`),
    /// this will use it directly, avoiding the overhead of SSH per-request.
    pub(crate) fn fetch_coordinator_api(
        &self,
        coordinator: &Instance,
        zone: &str,
        endpoint: &str,
        port: u16,
    ) -> Result<String> {
        #[cfg(test)]
        let _ = port;

        // Fast path: try local tunnel first (sub-second). Unit tests skip this
        // ambient endpoint so recording providers remain hermetic.
        #[cfg(not(test))]
        {
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
        }

        // Slow path: SSH through IAP (can take 5-30+ seconds)
        let remote_curl = format!("curl -s http://localhost:3000{}", endpoint);
        let mut cmd = self
            .provider
            .get_ssh_command(&coordinator.name, zone, &remote_curl);
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
    pub(crate) fn submit_job_via_api(
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
    ) -> Result<crate::distributed::message::JobSubmissionAck> {
        use crate::distributed::message::{JobConfigRequest, JobConfigResponse, JobSubmissionAck};

        // Use provided batch_size or fall back to sensible defaults per job type
        // Larger batches let workers parallelize with rayon
        let batch_size = if matches!(
            job_spec,
            crate::distributed::message::JobSpec::SafeExport(_)
        ) {
            Some(1)
        } else {
            batch_size.or_else(|| match job_spec {
                crate::distributed::message::JobSpec::Manhattan { .. } => Some(40),
                crate::distributed::message::JobSpec::ExportParquet { .. } => Some(100),
                _ => Some(50),
            })
        };

        // Phase 3: Job-type memory weight hints (MB per partition)
        // Use CLI override if provided, otherwise fall back to job-type heuristics
        let memory_weight_mb = memory_weight_mb.or_else(|| match job_spec {
            crate::distributed::message::JobSpec::Manhattan { .. }
            | crate::distributed::message::JobSpec::ManhattanBatch { .. }
            | crate::distributed::message::JobSpec::ManhattanScan(_) => Some(1024), // 1GB per partition
            crate::distributed::message::JobSpec::SafeExport(_)
            | crate::distributed::message::JobSpec::ExportParquet { .. }
            | crate::distributed::message::JobSpec::ExportJson { .. } => Some(256), // 256MB
            crate::distributed::message::JobSpec::Summary => Some(64), // 64MB, very light
            crate::distributed::message::JobSpec::Custom { .. } => Some(512), // Safe default for external bins
            _ => None,
        });

        let safe_count = match job_spec {
            crate::distributed::message::JobSpec::SafeExport(spec) => {
                if force || !filters.is_empty() || !intervals.is_empty() {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "safe export forbids force, filters, and intervals",
                    )));
                }
                spec.validate()?;
                spec.table.partitions.len()
            }
            _ => total_partitions,
        };
        let request = JobConfigRequest {
            input_path: input_path.to_string(),
            job_spec: job_spec.clone(),
            total_tasks: safe_count,
            batch_size,
            force,
            filters: filters.to_vec(),
            intervals: intervals.to_vec(),
            memory_weight_mb,
        };

        let json_payload = serde_json::to_string(&request).map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize job config: {}", e),
            ))
        })?;

        // Stream the request on stdin instead of embedding it in the remote shell
        // command or uploading a temporary file. Registration-only submit never SCPs.
        let curl_cmd = "curl -s -X POST -H 'Content-Type: application/json' --data-binary @- http://localhost:3000/api/job";
        let mut cmd = self
            .provider
            .get_ssh_command(&coordinator.name, zone, curl_cmd);
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(HailError::Io)?;
        {
            use std::io::Write;
            let mut stdin = child.stdin.take().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Failed to open coordinator submission stdin",
                ))
            })?;
            stdin
                .write_all(json_payload.as_bytes())
                .map_err(HailError::Io)?;
        }
        let output = child.wait_with_output().map_err(HailError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("Failed to submit job via API: {stderr}"),
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let response = serde_json::from_str::<JobConfigResponse>(&stdout).map_err(|error| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse coordinator response: {error} (raw: {stdout})"),
            ))
        })?;
        if !response.acknowledged {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                response
                    .error
                    .unwrap_or_else(|| "Coordinator rejected job".to_string()),
            )));
        }

        match response.job_id {
            Some(job_id) => Ok(JobSubmissionAck::Job { job_id }),
            None if matches!(
                job_spec,
                crate::distributed::message::JobSpec::ManhattanBatch { specs, .. }
                    if specs.is_empty()
            ) =>
            {
                Ok(JobSubmissionAck::Configuration)
            }
            None => Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Coordinator acknowledged an executable job without an exact job_id",
            ))),
        }
    }

    /// Export metrics database to GCS via coordinator API.
    /// Best-effort: failures are logged but don't block pool destruction.
    pub(crate) fn export_metrics_to_gcs(
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

        println!("{} Exporting metrics to GCS...", "Saving:".cyan());

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

        let mut cmd = self
            .provider
            .get_ssh_command(&coordinator.name, zone, &curl_cmd);
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
                        response
                            .error
                            .unwrap_or_else(|| "Unknown error".to_string())
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
    pub(crate) fn cancel(&self, name: &str, zone: &str) -> Result<()> {
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

            let mut cmd = self.provider.get_ssh_command(&coord.name, zone, &cmd_str);
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
    pub(crate) fn status(&self, name: &str, zone: &str) -> Result<()> {
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

    /// Print deterministic durable custom-task receipts for one exact job.
    pub(crate) fn custom_receipts(&self, name: &str, zone: &str, job_id: &str) -> Result<()> {
        let instances = self.provider.list_instances(name)?;
        let coordinator = instances
            .iter()
            .find(|instance| instance.name.ends_with("-coordinator"))
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No coordinator found for pool '{name}'"),
                ))
            })?;
        let encoded_job_id: String =
            url::form_urlencoded::byte_serialize(job_id.as_bytes()).collect();
        let endpoint = format!("/api/jobs/{encoded_job_id}/custom-receipts");
        let json = self.fetch_coordinator_api(coordinator, zone, &endpoint, 3000)?;
        let response = serde_json::from_str::<crate::distributed::message::CustomReceiptSet>(&json)
            .map_err(|error| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse custom receipt response: {error}"),
                ))
            })?;
        println!(
            "{}",
            serde_json::to_string(&response).map_err(|error| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to serialize custom receipt response: {error}"),
                ))
            })?
        );
        Ok(())
    }

    /// Show real-time worker activity.
    pub(crate) fn workers(&self, name: &str, zone: &str) -> Result<()> {
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
    pub(crate) fn events(&self, name: &str, zone: &str, follow: bool) -> Result<()> {
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
                event.worker_id.as_deref().unwrap_or("-").cyan(),
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
    pub(crate) fn failures(&self, name: &str, zone: &str) -> Result<()> {
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
    pub(crate) fn logs(&self, name: &str, zone: &str, worker_id: &str) -> Result<()> {
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
