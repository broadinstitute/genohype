//! SQLite-backed metrics storage for persistent dashboard data.

use crate::distributed::message::{
    FailureRecord, JobEvent, JobRecord, PhenotypeStatus, TelemetrySnapshot,
};
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

        // Enable high-concurrency WAL mode for non-blocking reads/writes
        // This allows workers to write telemetry while dashboard reads metrics
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;"
        )?;

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
                network_tx_total_bytes INTEGER,
                current_batch_size INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_telemetry_worker_time
                ON telemetry(worker_id, timestamp_ms);

            CREATE INDEX IF NOT EXISTS idx_telemetry_time
                ON telemetry(timestamp_ms);

            -- Job history tables
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                start_time_ms INTEGER NOT NULL,
                end_time_ms INTEGER,
                job_spec_json TEXT,
                input_path TEXT,
                total_tasks INTEGER,
                job_type TEXT,
                final_summary_json TEXT
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                worker_id TEXT,
                phenotype_id TEXT,
                details TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id);

            CREATE TABLE IF NOT EXISTS failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                phenotype_id TEXT,
                tasks TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                error TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                wasted_duration_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_failures_job_id ON failures(job_id);

            CREATE TABLE IF NOT EXISTS batch_phenotypes (
                job_id TEXT NOT NULL,
                phenotype_id TEXT NOT NULL,
                status_json TEXT NOT NULL,
                PRIMARY KEY (job_id, phenotype_id)
            );
            "#,
        )?;

        // Add job_id column to telemetry if it doesn't exist
        let has_job_id: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('telemetry') WHERE name = 'job_id'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0) > 0;

        if !has_job_id {
            let _ = conn.execute_batch(
                "ALTER TABLE telemetry ADD COLUMN job_id TEXT;
                 CREATE INDEX IF NOT EXISTS idx_telemetry_job_id ON telemetry(job_id);",
            );
        }

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

        // Add current_batch_size column if it doesn't exist (for existing databases)
        let has_batch_size: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('telemetry') WHERE name = 'current_batch_size'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0) > 0;

        if !has_batch_size {
            let _ = conn.execute_batch("ALTER TABLE telemetry ADD COLUMN current_batch_size INTEGER;");
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory database (useful for testing or when persistence isn't needed).
    #[allow(dead_code)]
    pub fn in_memory() -> SqliteResult<Self> {
        Self::open(":memory:")
    }

    /// Insert a telemetry snapshot for a worker.
    pub fn insert_snapshot(&self, worker_id: &str, snapshot: &TelemetrySnapshot) -> SqliteResult<()> {
        self.insert_snapshot_with_job_id(worker_id, snapshot, None)
    }

    /// Insert a telemetry snapshot for a worker, associated with a job.
    pub fn insert_snapshot_with_job_id(
        &self,
        worker_id: &str,
        snapshot: &TelemetrySnapshot,
        job_id: Option<&str>,
    ) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO telemetry (
                worker_id, timestamp_ms, cpu_percent, memory_used_bytes,
                memory_total_bytes, rows_per_sec, total_rows,
                active_partition, partitions_completed,
                disk_used_bytes, disk_total_bytes,
                network_rx_bytes_sec, network_tx_bytes_sec,
                network_rx_total_bytes, network_tx_total_bytes, current_batch_size, job_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
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
                snapshot.current_batch_size.map(|v| v as i64),
                job_id,
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
                   network_rx_total_bytes, network_tx_total_bytes, current_batch_size
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
                    current_batch_size: row.get::<_, Option<i64>>(14)?.map(|v| v as usize),
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(snapshots)
    }

    /// Get recent snapshots for a worker (last N entries).
    #[allow(dead_code)]
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
                   network_rx_total_bytes, network_tx_total_bytes, current_batch_size
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
                    current_batch_size: row.get::<_, Option<i64>>(14)?.map(|v| v as usize),
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
    #[allow(dead_code)]
    pub fn count(&self) -> SqliteResult<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM telemetry", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Force a WAL checkpoint to ensure all data is written to the main DB file
    /// before performing a backup to GCS.
    ///
    /// This flushes the Write-Ahead Log (WAL) into the main database file and
    /// truncates the WAL, ensuring the single .db file contains the complete
    /// current state. Call this before uploading ops.db to GCS.
    pub fn checkpoint_for_backup(&self) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    // =========================================================================
    // Job History CRUD Operations
    // =========================================================================

    /// Insert a new job record.
    pub fn insert_job(&self, job: &JobRecord) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        let job_spec_json = job
            .job_spec_json
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default());
        conn.execute(
            r#"
            INSERT INTO jobs (job_id, status, start_time_ms, end_time_ms, job_spec_json, input_path, total_tasks, job_type)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                job.job_id,
                job.status,
                job.start_time_ms as i64,
                job.end_time_ms.map(|v| v as i64),
                job_spec_json,
                job.input_path,
                job.total_tasks as i64,
                job.job_type,
            ],
        )?;
        Ok(())
    }

    /// Update job status and optionally set the end time and final summary.
    pub fn update_job_status(
        &self,
        job_id: &str,
        status: &str,
        end_time_ms: Option<u64>,
        final_summary_json: Option<&str>,
    ) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            UPDATE jobs
            SET status = ?2, end_time_ms = ?3, final_summary_json = ?4
            WHERE job_id = ?1
            "#,
            params![
                job_id,
                status,
                end_time_ms.map(|v| v as i64),
                final_summary_json,
            ],
        )?;
        Ok(())
    }

    /// Get all jobs ordered by start time (most recent first).
    pub fn get_jobs(&self) -> SqliteResult<Vec<JobRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT job_id, status, start_time_ms, end_time_ms, job_spec_json, input_path, total_tasks, job_type
            FROM jobs
            ORDER BY start_time_ms DESC
            "#,
        )?;

        let jobs = stmt
            .query_map([], |row| {
                let job_spec_str: Option<String> = row.get(4)?;
                let job_spec_json = job_spec_str
                    .and_then(|s| serde_json::from_str(&s).ok());
                Ok(JobRecord {
                    job_id: row.get(0)?,
                    status: row.get(1)?,
                    start_time_ms: row.get::<_, i64>(2)? as u64,
                    end_time_ms: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    job_spec_json,
                    input_path: row.get(5)?,
                    total_tasks: row.get::<_, i64>(6)? as usize,
                    job_type: row.get(7)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(jobs)
    }

    /// Get a specific job by ID.
    pub fn get_job(&self, job_id: &str) -> SqliteResult<Option<JobRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT job_id, status, start_time_ms, end_time_ms, job_spec_json, input_path, total_tasks, job_type
            FROM jobs
            WHERE job_id = ?1
            "#,
        )?;

        let result = stmt.query_row([job_id], |row| {
            let job_spec_str: Option<String> = row.get(4)?;
            let job_spec_json = job_spec_str.and_then(|s| serde_json::from_str(&s).ok());
            Ok(JobRecord {
                job_id: row.get(0)?,
                status: row.get(1)?,
                start_time_ms: row.get::<_, i64>(2)? as u64,
                end_time_ms: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                job_spec_json,
                input_path: row.get(5)?,
                total_tasks: row.get::<_, i64>(6)? as usize,
                job_type: row.get(7)?,
            })
        });

        match result {
            Ok(job) => Ok(Some(job)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get the final summary JSON for a job.
    pub fn get_job_summary(&self, job_id: &str) -> SqliteResult<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let result: Result<Option<String>, _> = conn.query_row(
            "SELECT final_summary_json FROM jobs WHERE job_id = ?1",
            [job_id],
            |row| row.get(0),
        );
        match result {
            Ok(summary) => Ok(summary),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Log an event to the database.
    pub fn log_event(&self, job_id: &str, event: &JobEvent) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO events (job_id, timestamp_ms, event_type, worker_id, phenotype_id, details)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                job_id,
                event.timestamp_ms as i64,
                event.event_type,
                event.worker_id,
                event.phenotype_id,
                event.details,
            ],
        )?;
        Ok(())
    }

    /// Get events for a job.
    pub fn get_job_events(&self, job_id: &str) -> SqliteResult<Vec<JobEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_ms, event_type, worker_id, phenotype_id, details
            FROM events
            WHERE job_id = ?1
            ORDER BY timestamp_ms ASC
            "#,
        )?;

        let events = stmt
            .query_map([job_id], |row| {
                Ok(JobEvent {
                    timestamp_ms: row.get::<_, i64>(0)? as u64,
                    event_type: row.get(1)?,
                    worker_id: row.get(2)?,
                    phenotype_id: row.get(3)?,
                    details: row.get(4)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(events)
    }

    /// Log a failure to the database.
    pub fn log_failure(&self, job_id: &str, failure: &FailureRecord) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        let tasks_json = serde_json::to_string(&failure.tasks).unwrap_or_default();
        conn.execute(
            r#"
            INSERT INTO failures (job_id, timestamp_ms, phenotype_id, tasks, worker_id, error, retry_count, wasted_duration_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                job_id,
                failure.timestamp_ms as i64,
                failure.phenotype_id,
                tasks_json,
                failure.worker_id,
                failure.error,
                failure.retry_count as i64,
                failure.wasted_duration_ms as i64,
            ],
        )?;
        Ok(())
    }

    /// Get failures for a job.
    pub fn get_job_failures(&self, job_id: &str) -> SqliteResult<Vec<FailureRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_ms, phenotype_id, tasks, worker_id, error, retry_count, wasted_duration_ms
            FROM failures
            WHERE job_id = ?1
            ORDER BY timestamp_ms ASC
            "#,
        )?;

        let failures = stmt
            .query_map([job_id], |row| {
                let tasks_json: String = row.get(2)?;
                let tasks: Vec<String> =
                    serde_json::from_str(&tasks_json).unwrap_or_default();
                Ok(FailureRecord {
                    timestamp_ms: row.get::<_, i64>(0)? as u64,
                    phenotype_id: row.get(1)?,
                    tasks,
                    worker_id: row.get(3)?,
                    error: row.get(4)?,
                    retry_count: row.get::<_, i64>(5)? as usize,
                    wasted_duration_ms: row.get::<_, i64>(6)? as u64,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(failures)
    }

    /// Upsert a batch phenotype status.
    pub fn upsert_batch_phenotype(
        &self,
        job_id: &str,
        phenotype_id: &str,
        status: &PhenotypeStatus,
    ) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        let status_json = serde_json::to_string(status).unwrap_or_default();
        conn.execute(
            r#"
            INSERT OR REPLACE INTO batch_phenotypes (job_id, phenotype_id, status_json)
            VALUES (?1, ?2, ?3)
            "#,
            params![job_id, phenotype_id, status_json],
        )?;
        Ok(())
    }

    /// Get batch phenotype statuses for a job.
    pub fn get_job_batch_phenotypes(&self, job_id: &str) -> SqliteResult<Vec<PhenotypeStatus>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT status_json FROM batch_phenotypes WHERE job_id = ?1
            "#,
        )?;

        let statuses = stmt
            .query_map([job_id], |row| {
                let json: String = row.get(0)?;
                let status: PhenotypeStatus =
                    serde_json::from_str(&json).unwrap_or_else(|_| PhenotypeStatus {
                        id: String::new(),
                        stage: "unknown".to_string(),
                        partitions_done: 0,
                        partitions_total: 0,
                        result: None,
                        error: None,
                        duration_secs: None,
                        cpu_core_secs: None,
                    });
                Ok(status)
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(statuses)
    }

    /// Delete a job and all its associated data.
    ///
    /// Performs a cascading delete across all tables: jobs, events, failures,
    /// batch_phenotypes, and telemetry. Uses a transaction for atomicity.
    pub fn delete_job(&self, job_id: &str) -> SqliteResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        tx.execute("DELETE FROM jobs WHERE job_id = ?1", params![job_id])?;
        tx.execute("DELETE FROM events WHERE job_id = ?1", params![job_id])?;
        tx.execute("DELETE FROM failures WHERE job_id = ?1", params![job_id])?;
        tx.execute(
            "DELETE FROM batch_phenotypes WHERE job_id = ?1",
            params![job_id],
        )?;
        tx.execute("DELETE FROM telemetry WHERE job_id = ?1", params![job_id])?;

        tx.commit()?;
        Ok(())
    }

    /// Get metrics for a specific job.
    pub fn get_job_metrics(&self, job_id: &str) -> SqliteResult<Vec<(String, Vec<TelemetrySnapshot>)>> {
        let conn = self.conn.lock().unwrap();

        // First get distinct worker IDs for this job
        let mut worker_stmt = conn.prepare(
            "SELECT DISTINCT worker_id FROM telemetry WHERE job_id = ?1 ORDER BY worker_id",
        )?;
        let worker_ids: Vec<String> = worker_stmt
            .query_map([job_id], |row| row.get(0))?
            .collect::<SqliteResult<Vec<_>>>()?;

        drop(worker_stmt);

        let mut results = Vec::new();
        for worker_id in worker_ids {
            let mut stmt = conn.prepare(
                r#"
                SELECT timestamp_ms, cpu_percent, memory_used_bytes, memory_total_bytes,
                       rows_per_sec, total_rows, active_partition, partitions_completed,
                       disk_used_bytes, disk_total_bytes,
                       network_rx_bytes_sec, network_tx_bytes_sec, current_batch_size
                FROM telemetry
                WHERE job_id = ?1 AND worker_id = ?2
                ORDER BY timestamp_ms ASC
                "#,
            )?;

            let snapshots: Vec<TelemetrySnapshot> = stmt
                .query_map(params![job_id, &worker_id], |row| {
                    Ok(TelemetrySnapshot {
                        timestamp_ms: row.get::<_, i64>(0)? as u64,
                        cpu_percent: row.get(1)?,
                        memory_used_bytes: row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                        memory_total_bytes: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                        items_per_sec: row.get(4)?,
                        total_items: row.get::<_, i64>(5)? as usize,
                        active_partition: row.get::<_, Option<i64>>(6)?.map(|v| v as usize),
                        partitions_completed: row.get::<_, i64>(7)? as usize,
                        cpu_per_core: None,
                        disk_read_bytes_sec: None,
                        disk_write_bytes_sec: None,
                        disk_used_bytes: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
                        disk_total_bytes: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
                        network_rx_bytes_sec: row.get(10)?,
                        network_tx_bytes_sec: row.get(11)?,
                        core_tasks: None,
                        current_batch_size: row.get::<_, Option<i64>>(12)?.map(|v| v as usize),
                    })
                })?
                .collect::<SqliteResult<Vec<_>>>()?;

            results.push((worker_id, snapshots));
        }

        Ok(results)
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
            core_tasks: None,
            current_batch_size: Some(24),
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
