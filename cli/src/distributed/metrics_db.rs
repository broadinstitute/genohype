//! SQLite-backed metrics storage for persistent dashboard data.

use crate::distributed::message::TelemetrySnapshot;
use rusqlite::{params, Connection, Result as SqliteResult};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Database handle for metrics storage.
pub struct MetricsDb {
    conn: Arc<Mutex<Connection>>,
}

impl MetricsDb {
    /// Open or create a metrics database at the given path.
    /// Use ":memory:" for in-memory database or a file path for persistence.
    pub fn open<P: AsRef<Path>>(path: P) -> SqliteResult<Self> {
        let conn = Connection::open(path)?;

        // Create tables if they don't exist
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                cpu_percent REAL,
                memory_used_bytes INTEGER,
                memory_total_bytes INTEGER,
                rows_per_sec REAL NOT NULL,
                total_rows INTEGER NOT NULL,
                active_partition INTEGER,
                partitions_completed INTEGER NOT NULL,
                -- Extended metrics for btop-style dashboard (added in v2)
                disk_used_bytes INTEGER,
                disk_total_bytes INTEGER,
                network_rx_bytes_sec REAL,
                network_tx_bytes_sec REAL,
                network_rx_total_bytes INTEGER,
                network_tx_total_bytes INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_telemetry_worker_time
                ON telemetry(worker_id, timestamp_ms);

            CREATE INDEX IF NOT EXISTS idx_telemetry_time
                ON telemetry(timestamp_ms);
            "#,
        )?;

        // Add columns if they don't exist (for existing databases)
        // SQLite doesn't support IF NOT EXISTS for ALTER TABLE, so we check first
        let has_disk_used: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('telemetry') WHERE name = 'disk_used_bytes'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0) > 0;

        if !has_disk_used {
            // Add new columns for existing databases
            let _ = conn.execute_batch(
                r#"
                ALTER TABLE telemetry ADD COLUMN disk_used_bytes INTEGER;
                ALTER TABLE telemetry ADD COLUMN disk_total_bytes INTEGER;
                ALTER TABLE telemetry ADD COLUMN network_rx_bytes_sec REAL;
                ALTER TABLE telemetry ADD COLUMN network_tx_bytes_sec REAL;
                ALTER TABLE telemetry ADD COLUMN network_rx_total_bytes INTEGER;
                ALTER TABLE telemetry ADD COLUMN network_tx_total_bytes INTEGER;
                "#,
            );
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory database (useful for testing or when persistence isn't needed).
    pub fn in_memory() -> SqliteResult<Self> {
        Self::open(":memory:")
    }

    /// Insert a telemetry snapshot for a worker.
    pub fn insert_snapshot(&self, worker_id: &str, snapshot: &TelemetrySnapshot) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO telemetry (
                worker_id, timestamp_ms, cpu_percent, memory_used_bytes,
                memory_total_bytes, rows_per_sec, total_rows,
                active_partition, partitions_completed,
                disk_used_bytes, disk_total_bytes,
                network_rx_bytes_sec, network_tx_bytes_sec,
                network_rx_total_bytes, network_tx_total_bytes
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            "#,
            params![
                worker_id,
                snapshot.timestamp_ms as i64,
                snapshot.cpu_percent,
                snapshot.memory_used_bytes.map(|v| v as i64),
                snapshot.memory_total_bytes.map(|v| v as i64),
                snapshot.items_per_sec,
                snapshot.total_items as i64,
                snapshot.active_partition.map(|v| v as i64),
                snapshot.partitions_completed as i64,
                snapshot.disk_used_bytes.map(|v| v as i64),
                snapshot.disk_total_bytes.map(|v| v as i64),
                snapshot.network_rx_bytes_sec,
                snapshot.network_tx_bytes_sec,
                None::<i64>, // network_rx_total_bytes (no longer in TelemetrySnapshot)
                None::<i64>, // network_tx_total_bytes (no longer in TelemetrySnapshot)
            ],
        )?;
        Ok(())
    }

    /// Get all snapshots for a worker, ordered by timestamp.
    pub fn get_worker_snapshots(&self, worker_id: &str) -> SqliteResult<Vec<TelemetrySnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_ms, cpu_percent, memory_used_bytes, memory_total_bytes,
                   rows_per_sec, total_rows, active_partition, partitions_completed,
                   disk_used_bytes, disk_total_bytes,
                   network_rx_bytes_sec, network_tx_bytes_sec,
                   network_rx_total_bytes, network_tx_total_bytes
            FROM telemetry
            WHERE worker_id = ?1
            ORDER BY timestamp_ms ASC
            "#,
        )?;

        let snapshots = stmt
            .query_map([worker_id], |row| {
                Ok(TelemetrySnapshot {
                    timestamp_ms: row.get::<_, i64>(0)? as u64,
                    cpu_percent: row.get(1)?,
                    memory_used_bytes: row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                    memory_total_bytes: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    items_per_sec: row.get(4)?,
                    total_items: row.get::<_, i64>(5)? as usize,
                    active_partition: row.get::<_, Option<i64>>(6)?.map(|v| v as usize),
                    partitions_completed: row.get::<_, i64>(7)? as usize,
                    // Extended metrics (not persisted: cpu_per_core, disk_read/write_bytes_sec, core_tasks)
                    cpu_per_core: None,
                    disk_read_bytes_sec: None,
                    disk_write_bytes_sec: None,
                    disk_used_bytes: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
                    disk_total_bytes: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
                    network_rx_bytes_sec: row.get(10)?,
                    network_tx_bytes_sec: row.get(11)?,
                    core_tasks: None, // Not persisted - real-time only
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(snapshots)
    }

    /// Get recent snapshots for a worker (last N entries).
    pub fn get_worker_snapshots_recent(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> SqliteResult<Vec<TelemetrySnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_ms, cpu_percent, memory_used_bytes, memory_total_bytes,
                   rows_per_sec, total_rows, active_partition, partitions_completed,
                   disk_used_bytes, disk_total_bytes,
                   network_rx_bytes_sec, network_tx_bytes_sec,
                   network_rx_total_bytes, network_tx_total_bytes
            FROM telemetry
            WHERE worker_id = ?1
            ORDER BY timestamp_ms DESC
            LIMIT ?2
            "#,
        )?;

        let mut snapshots: Vec<TelemetrySnapshot> = stmt
            .query_map(params![worker_id, limit as i64], |row| {
                Ok(TelemetrySnapshot {
                    timestamp_ms: row.get::<_, i64>(0)? as u64,
                    cpu_percent: row.get(1)?,
                    memory_used_bytes: row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                    memory_total_bytes: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    items_per_sec: row.get(4)?,
                    total_items: row.get::<_, i64>(5)? as usize,
                    active_partition: row.get::<_, Option<i64>>(6)?.map(|v| v as usize),
                    partitions_completed: row.get::<_, i64>(7)? as usize,
                    // Extended metrics (not persisted: cpu_per_core, disk_read/write_bytes_sec, core_tasks)
                    cpu_per_core: None,
                    disk_read_bytes_sec: None,
                    disk_write_bytes_sec: None,
                    disk_used_bytes: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
                    disk_total_bytes: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
                    network_rx_bytes_sec: row.get(10)?,
                    network_tx_bytes_sec: row.get(11)?,
                    core_tasks: None, // Not persisted - real-time only
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        // Reverse to get chronological order
        snapshots.reverse();
        Ok(snapshots)
    }

    /// Get list of all worker IDs that have telemetry data.
    pub fn get_worker_ids(&self) -> SqliteResult<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT DISTINCT worker_id FROM telemetry ORDER BY worker_id")?;
        let ids = stmt
            .query_map([], |row| row.get(0))?
            .collect::<SqliteResult<Vec<_>>>()?;
        Ok(ids)
    }

    /// Clear all telemetry data (useful when starting a new job).
    pub fn clear(&self) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM telemetry", [])?;
        Ok(())
    }

    /// Get total count of snapshots.
    pub fn count(&self) -> SqliteResult<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM telemetry", [], |row| row.get(0))?;
        Ok(count as usize)
    }
}

impl Clone for MetricsDb {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_db_basic() {
        let db = MetricsDb::in_memory().unwrap();

        let snapshot = TelemetrySnapshot {
            timestamp_ms: 1000,
            cpu_percent: Some(50.0),
            memory_used_bytes: Some(1024),
            memory_total_bytes: Some(2048),
            items_per_sec: 1000.0,
            total_items: 5000,
            active_partition: Some(5),
            partitions_completed: 10,
            // Extended metrics
            cpu_per_core: Some(vec![45.0, 55.0, 48.0, 52.0]),
            disk_read_bytes_sec: None,
            disk_write_bytes_sec: None,
            disk_used_bytes: Some(50_000_000_000),
            disk_total_bytes: Some(100_000_000_000),
            network_rx_bytes_sec: Some(1_000_000.0),
            network_tx_bytes_sec: Some(500_000.0),
        };

        db.insert_snapshot("worker-1", &snapshot).unwrap();
        db.insert_snapshot("worker-1", &snapshot).unwrap();
        db.insert_snapshot("worker-2", &snapshot).unwrap();

        assert_eq!(db.count().unwrap(), 3);

        let worker1_snaps = db.get_worker_snapshots("worker-1").unwrap();
        assert_eq!(worker1_snaps.len(), 2);
        // Verify extended metrics are retrieved
        assert_eq!(worker1_snaps[0].disk_used_bytes, Some(50_000_000_000));
        assert_eq!(worker1_snaps[0].network_rx_bytes_sec, Some(1_000_000.0));

        let ids = db.get_worker_ids().unwrap();
        assert_eq!(ids, vec!["worker-1", "worker-2"]);

        db.clear().unwrap();
        assert_eq!(db.count().unwrap(), 0);
    }
}
