//! Worker telemetry module.
//!
//! Contains shared state structures and the background heartbeat loop that
//! sends system metrics to the coordinator.

use crate::distributed::message::{CoreTaskInfo, HeartbeatRequest, TelemetrySnapshot};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Sentinel value for when no partition is being processed.
pub const NO_ACTIVE_PARTITION: usize = usize::MAX;

/// Shared state between the main worker loop and the telemetry background task.
pub struct TelemetryState {
    /// Total rows processed so far
    pub total_rows: AtomicUsize,
    /// Currently active partition (usize::MAX = none)
    pub active_partition: AtomicUsize,
    /// Total partitions completed
    pub partitions_completed: AtomicUsize,
    /// Signal to stop the telemetry loop
    pub stop: AtomicBool,
    /// Map of Rayon thread ID to currently executing task info
    pub core_tasks: std::sync::Mutex<std::collections::HashMap<usize, CoreTaskInfo>>,
}

impl TelemetryState {
    /// Create a new TelemetryState with default values.
    pub fn new() -> Self {
        Self {
            total_rows: AtomicUsize::new(0),
            active_partition: AtomicUsize::new(NO_ACTIVE_PARTITION),
            partitions_completed: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            core_tasks: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for TelemetryState {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard to safely register and unregister a task for a Rayon thread.
///
/// When created, this guard registers the current Rayon thread's ID with the task
/// it is executing. When dropped (either normally or due to panic/error), it automatically
/// removes the registration, ensuring the core_tasks map stays accurate.
pub struct CoreTaskGuard<'a> {
    ts: &'a Arc<TelemetryState>,
    thread_id: Option<usize>,
}

impl<'a> CoreTaskGuard<'a> {
    /// Create a new guard that registers the current Rayon thread as executing the given task.
    pub fn new(ts: &'a Arc<TelemetryState>, task_info: CoreTaskInfo) -> Self {
        let thread_id = rayon::current_thread_index();
        if let Some(tid) = thread_id {
            if let Ok(mut map) = ts.core_tasks.lock() {
                map.insert(tid, task_info);
            }
        }
        Self { ts, thread_id }
    }

    /// Create a guard for a partition-based task.
    pub fn partition(ts: &'a Arc<TelemetryState>, partition_id: usize) -> Self {
        Self::new(ts, CoreTaskInfo::partition(partition_id))
    }

    /// Create a guard for a phenotype-based task.
    pub fn phenotype(ts: &'a Arc<TelemetryState>, phenotype_id: impl Into<String>, label: Option<String>) -> Self {
        Self::new(ts, CoreTaskInfo::phenotype(phenotype_id, label))
    }

    /// Create a guard for a custom task type.
    pub fn custom(ts: &'a Arc<TelemetryState>, task_type: impl Into<String>, task_id: impl Into<String>) -> Self {
        Self::new(ts, CoreTaskInfo::custom(task_type, task_id))
    }
}

impl<'a> Drop for CoreTaskGuard<'a> {
    fn drop(&mut self) {
        if let Some(tid) = self.thread_id {
            if let Ok(mut map) = self.ts.core_tasks.lock() {
                map.remove(&tid);
            }
        }
    }
}

/// Spawn a background task that periodically sends heartbeats with telemetry to the coordinator.
pub fn spawn_telemetry_loop(
    client: reqwest::Client,
    coordinator_url: String,
    worker_id: String,
    state: Arc<TelemetryState>,
) -> tokio::task::JoinHandle<()> {
    let heartbeat_url = format!("{}/heartbeat", coordinator_url);
    let start_time = Instant::now();

    // Initialize sysinfo for system metrics (CPU, memory)
    let sys = std::sync::Mutex::new({
        use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                .with_memory(MemoryRefreshKind::new().with_ram()),
        );
        // Initial refresh to establish baseline
        sys.refresh_all();
        sys
    });

    // Initialize disk and network monitoring
    let disks = std::sync::Mutex::new(sysinfo::Disks::new_with_refreshed_list());
    let networks = std::sync::Mutex::new(sysinfo::Networks::new_with_refreshed_list());

    let mut prev_rows: usize = 0;
    let mut prev_time = start_time;

    // Track previous network counters for rate calculation
    let mut prev_net_rx: u64 = 0;
    let mut prev_net_tx: u64 = 0;

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            if state.stop.load(Ordering::Relaxed) {
                break;
            }

            let total_rows = state.total_rows.load(Ordering::Relaxed);
            let active_part = state.active_partition.load(Ordering::Relaxed);
            let parts_done = state.partitions_completed.load(Ordering::Relaxed);

            let now = Instant::now();
            let dt = now.duration_since(prev_time).as_secs_f64();
            let rows_per_sec = if dt > 0.0 {
                (total_rows.saturating_sub(prev_rows)) as f64 / dt
            } else {
                0.0
            };
            prev_rows = total_rows;
            prev_time = now;

            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Collect system metrics (CPU and memory)
            let (cpu, cpu_per_core, mem_used, mem_total) = {
                use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};
                let mut s = sys.lock().unwrap();
                s.refresh_specifics(
                    RefreshKind::new()
                        .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                        .with_memory(MemoryRefreshKind::new().with_ram()),
                );
                let per_core: Vec<f32> = s.cpus().iter().map(|c| c.cpu_usage()).collect();
                let cpu_avg = per_core.iter().sum::<f32>() / per_core.len().max(1) as f32;
                (
                    Some(cpu_avg),
                    Some(per_core),
                    Some(s.used_memory()),
                    Some(s.total_memory()),
                )
            };

            // Collect disk metrics
            let (disk_used, disk_total) = {
                let mut d = disks.lock().unwrap();
                d.refresh_list();
                let mut used = 0u64;
                let mut total = 0u64;
                for disk in d.iter() {
                    total += disk.total_space();
                    used += disk.total_space().saturating_sub(disk.available_space());
                }
                (Some(used), Some(total))
            };

            // Collect network metrics
            let (net_rx_sec, net_tx_sec, _net_rx_total, _net_tx_total) = {
                let mut n = networks.lock().unwrap();
                n.refresh();
                let (current_rx, current_tx) = n
                    .iter()
                    .fold((0u64, 0u64), |(rx, tx), (_, iface)| {
                        (rx + iface.total_received(), tx + iface.total_transmitted())
                    });

                // Calculate rates (bytes/sec)
                let rx_sec = if dt > 0.0 {
                    current_rx.saturating_sub(prev_net_rx) as f64 / dt
                } else {
                    0.0
                };
                let tx_sec = if dt > 0.0 {
                    current_tx.saturating_sub(prev_net_tx) as f64 / dt
                } else {
                    0.0
                };
                prev_net_rx = current_rx;
                prev_net_tx = current_tx;

                (Some(rx_sec), Some(tx_sec), Some(current_rx), Some(current_tx))
            };

            // Collect the core tasks map (Rayon thread ID -> partition ID)
            let core_tasks = {
                let map = state.core_tasks.lock().unwrap();
                if map.is_empty() {
                    None
                } else {
                    Some(map.clone())
                }
            };

            let snapshot = TelemetrySnapshot {
                timestamp_ms,
                cpu_percent: cpu,
                memory_used_bytes: mem_used,
                memory_total_bytes: mem_total,
                items_per_sec: rows_per_sec,
                total_items: total_rows,
                active_partition: if active_part == NO_ACTIVE_PARTITION {
                    None
                } else {
                    Some(active_part)
                },
                partitions_completed: parts_done,
                // Extended metrics
                cpu_per_core,
                disk_read_bytes_sec: None, // sysinfo doesn't provide disk I/O rates directly
                disk_write_bytes_sec: None,
                disk_used_bytes: disk_used,
                disk_total_bytes: disk_total,
                network_rx_bytes_sec: net_rx_sec,
                network_tx_bytes_sec: net_tx_sec,
                core_tasks,
                current_batch_size: None, // Set by coordinator, not known to worker
                max_batch_capacity: None, // Set by coordinator, not known to worker
            };

            let req = HeartbeatRequest {
                worker_id: worker_id.clone(),
                telemetry: snapshot,
                build_version: Some(env!("GIT_HASH").to_string()),
            };

            // Best-effort: don't let heartbeat failures block the worker
            let _ = client.post(&heartbeat_url).json(&req).send().await;
        }
    })
}
