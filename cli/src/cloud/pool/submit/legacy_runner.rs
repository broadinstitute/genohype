//! Legacy single-machine worker job runner.

use super::super::PoolManager;
use crate::benchmark::BenchmarkReport;
use crate::cloud::{CloudProvider, ProgressUpdate};
use crate::HailError;
use crate::Result;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;

/// Messages sent from worker threads to the coordinator.
pub enum WorkerMessage {
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

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Run a job on a single worker, streaming output.
    pub(crate) fn run_worker_job(
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
}
