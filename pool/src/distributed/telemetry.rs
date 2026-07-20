use crate::distributed::message::TelemetrySnapshot;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, Networks, RefreshKind, System};

pub struct SystemMetrics {
    sys: Mutex<System>,
    disks: Mutex<Disks>,
    networks: Mutex<Networks>,
    prev_net_rx: AtomicU64,
    prev_net_tx: AtomicU64,
    items_processed: AtomicUsize,
    tasks_completed: AtomicUsize,
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemMetrics {
    pub fn new() -> Self {
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                .with_memory(MemoryRefreshKind::new().with_ram()),
        );
        sys.refresh_all();

        Self {
            sys: Mutex::new(sys),
            disks: Mutex::new(Disks::new_with_refreshed_list()),
            networks: Mutex::new(Networks::new_with_refreshed_list()),
            prev_net_rx: AtomicU64::new(0),
            prev_net_tx: AtomicU64::new(0),
            items_processed: AtomicUsize::new(0),
            tasks_completed: AtomicUsize::new(0),
        }
    }

    pub fn record_task_completion(&self, items: usize) {
        self.items_processed.fetch_add(items, Ordering::Relaxed);
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self, dt_secs: f64) -> TelemetrySnapshot {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let total_items = self.items_processed.load(Ordering::Relaxed);
        let completed_tasks = self.tasks_completed.load(Ordering::Relaxed);
        let items_per_sec = if dt_secs > 0.0 {
            (total_items as f64) / dt_secs
        } else {
            0.0
        };

        // CPU & Memory
        let (cpu, cpu_per_core, mem_used, mem_total) = {
            let mut s = self.sys.lock().unwrap();
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

        // Disk
        let (disk_used, disk_total) = {
            let mut d = self.disks.lock().unwrap();
            d.refresh_list();
            let mut used = 0u64;
            let mut total = 0u64;
            for disk in d.iter() {
                total += disk.total_space();
                used += disk.total_space().saturating_sub(disk.available_space());
            }
            (Some(used), Some(total))
        };

        // Network
        let (net_rx_sec, net_tx_sec) = {
            let mut n = self.networks.lock().unwrap();
            n.refresh();
            let (current_rx, current_tx) = n.iter().fold((0u64, 0u64), |(rx, tx), (_, iface)| {
                (rx + iface.total_received(), tx + iface.total_transmitted())
            });

            let prev_rx = self.prev_net_rx.swap(current_rx, Ordering::Relaxed);
            let prev_tx = self.prev_net_tx.swap(current_tx, Ordering::Relaxed);

            let rx_sec = if dt_secs > 0.0 {
                current_rx.saturating_sub(prev_rx) as f64 / dt_secs
            } else {
                0.0
            };
            let tx_sec = if dt_secs > 0.0 {
                current_tx.saturating_sub(prev_tx) as f64 / dt_secs
            } else {
                0.0
            };

            (Some(rx_sec), Some(tx_sec))
        };

        TelemetrySnapshot {
            timestamp_ms,
            cpu_percent: cpu,
            memory_used_bytes: mem_used,
            memory_total_bytes: mem_total,
            items_per_sec,
            total_items,
            active_partition: None,
            partitions_completed: completed_tasks,
            cpu_per_core,
            disk_read_bytes_sec: None,
            disk_write_bytes_sec: None,
            disk_used_bytes: disk_used,
            disk_total_bytes: disk_total,
            network_rx_bytes_sec: net_rx_sec,
            network_tx_bytes_sec: net_tx_sec,
            core_tasks: None,
            current_batch_size: None,
            max_batch_capacity: None,
            current_phenotype_id: None,
            current_phase: None,
            current_source: None,
            current_ancestry: None,
            prefetch_depth: None,
        }
    }
}
