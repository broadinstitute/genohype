//! Binary compilation and deployment operations.

use super::PoolManager;
use crate::cloud::{CloudProvider, Instance};
use crate::HailError;
use crate::Result;
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Stage the genohype binary to GCS for fast worker pulls.
    pub(crate) fn stage_binary_to_gcs(&self, binary: &Path, pool_db_path: &str) -> Result<String> {
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
    pub(crate) fn update_binary(
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
        let stop_cmd = "sudo systemctl stop genohype-coordinator 2>/dev/null || true; \
                        sudo systemctl stop genohype-worker 2>/dev/null || true; \
                        fuser -k 3000/tcp 2>/dev/null || true";
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
        let backup_arg = pool_db_path
            .as_ref()
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
    pub(crate) fn update_binary_via_api(
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

    /// Deploy binary to instances via SCP upload.
    pub(crate) fn deploy_binary(&self, binary: &Path, instances: &[Instance], zone: &str) -> Result<()> {
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
    pub(crate) fn propagate_binary_from_coordinator(
        &self,
        coordinator_ip: &str,
        workers: &[Instance],
        zone: &str,
    ) -> Result<()> {
        workers.par_iter().try_for_each(|worker| {
            // Download binary from coordinator, install it, and restart worker service
            // The worker service must be restarted to pick up the new binary!
            let curl_cmd = format!(
                "sudo systemctl stop genohype-worker 2>/dev/null || true && \
                 curl -sL --retry 3 --retry-delay 2 http://{}:3000/api/binary -o /tmp/genohype && \
                 chmod +x /tmp/genohype && \
                 sudo mv /tmp/genohype /usr/local/bin/genohype && \
                 sudo systemctl start genohype-worker",
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

    /// Build the Linux binary for deployment to workers.
    ///
    /// On macOS, uses `cargo linux` (cargo-zigbuild) to cross-compile.
    /// On Linux, uses regular `cargo build`.
    pub(crate) fn build_linux_binary(features: &[&str]) -> Result<()> {
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
    pub(crate) fn has_bundled_binary(&self) -> bool {
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
    pub(crate) fn locate_binary(&self, path: Option<String>) -> Result<PathBuf> {
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
}
