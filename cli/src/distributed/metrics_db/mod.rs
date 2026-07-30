//! SQLite-backed metrics storage for persistent dashboard data.

use crate::distributed::message::{
    CompleteRequest, CustomReceiptSet, CustomTaskReceipt, FailureRecord, JobEvent, JobRecord,
    PhenotypeStatus, TelemetrySnapshot,
};
use rusqlite::{params, Connection, OptionalExtension, Result as SqliteResult};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Database handle for metrics storage.
pub struct MetricsDb {
    conn: Arc<Mutex<Connection>>,
}

/// One coordinator-issued custom assignment to fence durably before dispatch.
pub(crate) struct DurableCustomAssignment<'a> {
    pub task_id: &'a str,
    pub partition_id: usize,
    pub assignment_attempt: u64,
    pub lease_token: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustomCompletionOutcome {
    Stored,
    Duplicate,
}

fn sha256_hex(domain: &[u8], bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update([0]);
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn lease_identity(lease_token: &str) -> String {
    sha256_hex(
        b"genohype/custom-assignment-lease/v1",
        lease_token.as_bytes(),
    )
}

fn has_exact_completion_shape(request: &CompleteRequest) -> bool {
    if request.tasks.is_empty() || request.tasks.len() != request.assignments.len() {
        return false;
    }
    let task_ids: HashSet<&str> = request.tasks.iter().map(String::as_str).collect();
    let assignment_ids: HashSet<&str> = request
        .assignments
        .iter()
        .map(|assignment| assignment.task_id.as_str())
        .collect();
    task_ids.len() == request.tasks.len()
        && assignment_ids.len() == request.assignments.len()
        && task_ids == assignment_ids
}

fn canonical_report(request: &CompleteRequest) -> Result<(String, String, &'static str), String> {
    let value = serde_json::json!({
        "error": request.error,
        "items_processed": request.items_processed,
        "result_json": request.result_json,
    });
    let json = serde_json::to_string(&value)
        .map_err(|error| format!("failed to serialize custom completion report: {error}"))?;
    let digest = sha256_hex(b"genohype/custom-task-report/v1", json.as_bytes());
    let status = if request.error.is_some() {
        "failed"
    } else {
        "accepted"
    };
    Ok((json, digest, status))
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
             PRAGMA synchronous=NORMAL;",
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
                current_batch_size INTEGER,
                max_batch_capacity INTEGER
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

            CREATE TABLE IF NOT EXISTS stress_job_params (
                job_id TEXT PRIMARY KEY,
                cpu_secs REAL,
                memory_mb INTEGER,
                leak_memory_mb INTEGER,
                memory_jitter_pct INTEGER,
                skip_memory_check BOOLEAN,
                read_path TEXT,
                write_dir TEXT
            );

            -- Live custom assignments are durable only so receipt creation can
            -- validate and consume the exact current fence in one transaction.
            -- Raw lease capabilities are never persisted.
            CREATE TABLE IF NOT EXISTS current_custom_assignments (
                job_id TEXT NOT NULL,
                coordinator_session_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                assignment_attempt INTEGER NOT NULL,
                lease_identity_sha256 TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                assigned_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, task_id)
            );

            CREATE TABLE IF NOT EXISTS custom_task_receipts (
                job_id TEXT NOT NULL,
                coordinator_session_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                assignment_attempt INTEGER NOT NULL,
                lease_identity_sha256 TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                worker_build_version TEXT,
                terminal_status TEXT NOT NULL CHECK (terminal_status IN ('accepted', 'failed')),
                report_json TEXT NOT NULL,
                report_sha256 TEXT NOT NULL,
                accepted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, task_id, assignment_attempt)
            );
            CREATE INDEX IF NOT EXISTS idx_custom_task_receipts_job
                ON custom_task_receipts(job_id, task_id, assignment_attempt);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_task_receipts_one_accepted
                ON custom_task_receipts(job_id, task_id)
                WHERE terminal_status = 'accepted';

            CREATE TRIGGER IF NOT EXISTS custom_task_receipts_immutable_update
            BEFORE UPDATE ON custom_task_receipts
            BEGIN
                SELECT RAISE(ABORT, 'custom task receipts are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS custom_task_receipts_immutable_delete
            BEFORE DELETE ON custom_task_receipts
            BEGIN
                SELECT RAISE(ABORT, 'custom task receipts are immutable');
            END;
            "#,
        )?;

        // Add job_id column to telemetry if it doesn't exist
        let has_job_id: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('telemetry') WHERE name = 'job_id'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;

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
            let _ =
                conn.execute_batch("ALTER TABLE telemetry ADD COLUMN current_batch_size INTEGER;");
        }

        // Add max_batch_capacity column if it doesn't exist (for existing databases)
        let has_max_cap: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('telemetry') WHERE name = 'max_batch_capacity'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0) > 0;

        if !has_max_cap {
            let _ =
                conn.execute_batch("ALTER TABLE telemetry ADD COLUMN max_batch_capacity INTEGER;");
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
    pub fn insert_snapshot(
        &self,
        worker_id: &str,
        snapshot: &TelemetrySnapshot,
    ) -> SqliteResult<()> {
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
                network_rx_total_bytes, network_tx_total_bytes, current_batch_size, job_id,
                max_batch_capacity
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
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
                snapshot.max_batch_capacity.map(|v| v as i64),
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
                   network_rx_total_bytes, network_tx_total_bytes, current_batch_size,
                   max_batch_capacity
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
                    max_batch_capacity: row.get::<_, Option<i64>>(15)?.map(|v| v as usize),
                    // Phenotype visibility (not persisted - real-time only)
                    current_phenotype_id: None,
                    current_phase: None,
                    current_source: None,
                    current_ancestry: None,
                    prefetch_depth: None,
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
                   network_rx_total_bytes, network_tx_total_bytes, current_batch_size,
                   max_batch_capacity
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
                    max_batch_capacity: row.get::<_, Option<i64>>(15)?.map(|v| v as usize),
                    // Phenotype visibility (not persisted - real-time only)
                    current_phenotype_id: None,
                    current_phase: None,
                    current_source: None,
                    current_ancestry: None,
                    prefetch_depth: None,
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
        let mut stmt =
            conn.prepare("SELECT DISTINCT worker_id FROM telemetry ORDER BY worker_id")?;
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

    /// Persist a batch of custom assignments before exposing their lease tokens
    /// to a worker. Existing stale assignments may only be replaced by a newer
    /// attempt for the same exact job and task.
    pub(crate) fn persist_custom_assignments(
        &self,
        job_id: &str,
        coordinator_session_id: &str,
        worker_id: &str,
        assignments: &[DurableCustomAssignment<'_>],
        assigned_at_ms: u64,
    ) -> Result<(), String> {
        if assignments.is_empty() {
            return Err("custom assignment batch is empty".to_string());
        }
        let assigned_at_ms = i64::try_from(assigned_at_ms)
            .map_err(|_| "assignment timestamp exceeds SQLite INTEGER".to_string())?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if status.as_deref() != Some("running") {
            return Err(format!(
                "custom job {job_id} is not durably running (status={status:?})"
            ));
        }

        for assignment in assignments {
            let partition_id = i64::try_from(assignment.partition_id)
                .map_err(|_| "partition ID exceeds SQLite INTEGER".to_string())?;
            let attempt = i64::try_from(assignment.assignment_attempt)
                .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
            let accepted: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM custom_task_receipts
                     WHERE job_id = ?1 AND task_id = ?2 AND terminal_status = 'accepted'",
                    params![job_id, assignment.task_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            if accepted != 0 {
                return Err(format!(
                    "task {} already has an accepted terminal receipt",
                    assignment.task_id
                ));
            }
            let prior_attempt: Option<i64> = tx
                .query_row(
                    "SELECT MAX(assignment_attempt) FROM (
                         SELECT assignment_attempt FROM custom_task_receipts WHERE job_id = ?1 AND task_id = ?2
                         UNION ALL
                         SELECT assignment_attempt FROM current_custom_assignments WHERE job_id = ?1 AND task_id = ?2
                     )",
                    params![job_id, assignment.task_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            if prior_attempt.is_some_and(|prior| attempt <= prior) {
                return Err(format!(
                    "assignment attempt {} for task {} is not newer than durable attempt {:?}",
                    assignment.assignment_attempt, assignment.task_id, prior_attempt
                ));
            }
            tx.execute(
                "INSERT INTO current_custom_assignments (
                     job_id, coordinator_session_id, task_id, partition_id,
                     assignment_attempt, lease_identity_sha256, worker_id, assigned_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(job_id, task_id) DO UPDATE SET
                     coordinator_session_id = excluded.coordinator_session_id,
                     partition_id = excluded.partition_id,
                     assignment_attempt = excluded.assignment_attempt,
                     lease_identity_sha256 = excluded.lease_identity_sha256,
                     worker_id = excluded.worker_id,
                     assigned_at_ms = excluded.assigned_at_ms",
                params![
                    job_id,
                    coordinator_session_id,
                    assignment.task_id,
                    partition_id,
                    attempt,
                    lease_identity(assignment.lease_token),
                    worker_id,
                    assigned_at_ms,
                ],
            )
            .map_err(|error| error.to_string())?;
        }
        tx.commit().map_err(|error| error.to_string())
    }

    /// Return whether a delivery exactly matches immutable receipts already
    /// committed for every assignment in the request.
    pub(crate) fn is_identical_custom_completion(
        &self,
        job_id: &str,
        request: &CompleteRequest,
    ) -> Result<bool, String> {
        if !has_exact_completion_shape(request) {
            return Ok(false);
        }
        let (report_json, report_sha256, terminal_status) = canonical_report(request)?;
        let conn = self.conn.lock().unwrap();
        let job_status: Option<String> = conn
            .query_row(
                "SELECT status FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if !matches!(job_status.as_deref(), Some("running" | "completed")) {
            return Ok(false);
        }
        for task_id in &request.tasks {
            let Some(lease) = request
                .assignments
                .iter()
                .find(|assignment| assignment.task_id == *task_id)
            else {
                return Ok(false);
            };
            let attempt = i64::try_from(lease.assignment_attempt)
                .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
            let matches: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM custom_task_receipts
                     WHERE job_id = ?1 AND task_id = ?2 AND assignment_attempt = ?3
                       AND coordinator_session_id = ?4 AND lease_identity_sha256 = ?5
                       AND worker_id = ?6 AND terminal_status = ?7
                       AND report_json = ?8 AND report_sha256 = ?9",
                    params![
                        job_id,
                        task_id,
                        attempt,
                        request.session_id.as_deref(),
                        lease_identity(&lease.lease_token),
                        request.worker_id,
                        terminal_status,
                        report_json,
                        report_sha256,
                    ],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            if matches != 1 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Validate and consume every durable current assignment and insert the
    /// corresponding immutable terminal receipts in the same SQLite transaction.
    pub(crate) fn accept_custom_completion(
        &self,
        job_id: &str,
        worker_build_version: Option<&str>,
        request: &CompleteRequest,
        accepted_at_ms: u64,
    ) -> Result<CustomCompletionOutcome, String> {
        if self.is_identical_custom_completion(job_id, request)? {
            return Ok(CustomCompletionOutcome::Duplicate);
        }
        if !has_exact_completion_shape(request) {
            return Err(
                "custom completion must contain a non-empty one-to-one task/lease set".into(),
            );
        }
        let coordinator_session_id = request
            .session_id
            .as_deref()
            .ok_or_else(|| "custom completion is missing coordinator session".to_string())?;
        let accepted_at_ms = i64::try_from(accepted_at_ms)
            .map_err(|_| "receipt timestamp exceeds SQLite INTEGER".to_string())?;
        let (report_json, report_sha256, terminal_status) = canonical_report(request)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if status.as_deref() != Some("running") {
            return Err(format!(
                "custom job {job_id} is not durably running (status={status:?})"
            ));
        }

        for task_id in &request.tasks {
            let lease = request
                .assignments
                .iter()
                .find(|assignment| assignment.task_id == *task_id)
                .ok_or_else(|| format!("missing assignment lease for task {task_id}"))?;
            let attempt = i64::try_from(lease.assignment_attempt)
                .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
            let lease_hash = lease_identity(&lease.lease_token);
            let current: Option<(i64, String, String, i64)> = tx
                .query_row(
                    "SELECT partition_id, coordinator_session_id, worker_id, assignment_attempt
                     FROM current_custom_assignments
                     WHERE job_id = ?1 AND task_id = ?2 AND assignment_attempt = ?3
                       AND coordinator_session_id = ?4 AND lease_identity_sha256 = ?5
                       AND worker_id = ?6",
                    params![
                        job_id,
                        task_id,
                        attempt,
                        coordinator_session_id,
                        lease_hash,
                        request.worker_id,
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(|error| error.to_string())?;
            let Some((partition_id, _, _, _)) = current else {
                return Err(format!(
                    "task {task_id} has no matching durable current assignment"
                ));
            };

            tx.execute(
                "INSERT INTO custom_task_receipts (
                     job_id, coordinator_session_id, task_id, partition_id,
                     assignment_attempt, lease_identity_sha256, worker_id,
                     worker_build_version, terminal_status, report_json,
                     report_sha256, accepted_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    job_id,
                    coordinator_session_id,
                    task_id,
                    partition_id,
                    attempt,
                    lease_hash,
                    request.worker_id,
                    worker_build_version,
                    terminal_status,
                    report_json,
                    report_sha256,
                    accepted_at_ms,
                ],
            )
            .map_err(|error| {
                format!("conflicting custom completion for task {task_id}: {error}")
            })?;
            let removed = tx
                .execute(
                    "DELETE FROM current_custom_assignments
                     WHERE job_id = ?1 AND task_id = ?2 AND assignment_attempt = ?3
                       AND coordinator_session_id = ?4 AND lease_identity_sha256 = ?5
                       AND worker_id = ?6",
                    params![
                        job_id,
                        task_id,
                        attempt,
                        coordinator_session_id,
                        lease_hash,
                        request.worker_id,
                    ],
                )
                .map_err(|error| error.to_string())?;
            if removed != 1 {
                return Err(format!(
                    "durable assignment for task {task_id} changed during acceptance"
                ));
            }
        }
        tx.commit().map_err(|error| error.to_string())?;
        Ok(CustomCompletionOutcome::Stored)
    }

    /// Remove live durable assignment fences for a terminal or superseded job.
    pub(crate) fn clear_current_custom_assignments(&self, job_id: &str) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM current_custom_assignments WHERE job_id = ?1",
            [job_id],
        )?;
        Ok(())
    }

    /// Exact-job receipt query. Results are sorted deterministically and the
    /// canonical digest covers accepted receipts only, so failed retry attempts
    /// can never become authoritative output.
    pub(crate) fn get_custom_receipts(&self, job_id: &str) -> Result<CustomReceiptSet, String> {
        let conn = self.conn.lock().unwrap();
        let job: Option<(String, i64)> = conn
            .query_row(
                "SELECT status, total_tasks FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        let mut stmt = conn
            .prepare(
                "SELECT coordinator_session_id, task_id, partition_id, assignment_attempt,
                        lease_identity_sha256, worker_id, worker_build_version, terminal_status,
                        report_json, report_sha256, accepted_at_ms
                 FROM custom_task_receipts WHERE job_id = ?1
                 ORDER BY task_id COLLATE BINARY ASC, assignment_attempt ASC",
            )
            .map_err(|error| error.to_string())?;
        let receipts = stmt
            .query_map([job_id], |row| {
                let report_json: String = row.get(8)?;
                let report = serde_json::from_str(&report_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        report_json.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok(CustomTaskReceipt {
                    schema_version: 1,
                    job_id: job_id.to_string(),
                    coordinator_session_id: row.get(0)?,
                    task_id: row.get(1)?,
                    partition_id: row.get::<_, i64>(2)? as usize,
                    assignment_attempt: row.get::<_, i64>(3)? as u64,
                    lease_identity_sha256: row.get(4)?,
                    worker_id: row.get(5)?,
                    worker_build_version: row.get(6)?,
                    terminal_status: row.get(7)?,
                    report,
                    report_sha256: row.get(9)?,
                    accepted_at_ms: row.get::<_, i64>(10)? as u64,
                })
            })
            .map_err(|error| error.to_string())?
            .collect::<SqliteResult<Vec<_>>>()
            .map_err(|error| error.to_string())?;
        let accepted: Vec<&CustomTaskReceipt> = receipts
            .iter()
            .filter(|receipt| receipt.terminal_status == "accepted")
            .collect();
        let accepted_count = accepted.len();
        let failed_attempt_count = receipts.len() - accepted_count;
        let canonical_sha256 = if accepted.is_empty() {
            None
        } else {
            let canonical = serde_json::to_vec(&accepted).map_err(|error| error.to_string())?;
            Some(sha256_hex(b"genohype/custom-receipt-set/v1", &canonical))
        };
        let (job_found, job_status, expected_task_count) = match job {
            Some((status, total)) => (true, Some(status), total.max(0) as usize),
            None => (false, None, 0),
        };
        let non_terminal_job = !matches!(
            job_status.as_deref(),
            Some("cancelled" | "superseded" | "failed")
        );
        let complete = job_found
            && non_terminal_job
            && expected_task_count > 0
            && accepted_count == expected_task_count;
        let error = if !job_found {
            Some(format!("job {job_id} not found"))
        } else if !complete {
            Some(format!(
                "receipt set incomplete: {accepted_count}/{expected_task_count} accepted custom tasks (job status {})",
                job_status.as_deref().unwrap_or("unknown")
            ))
        } else {
            None
        };
        Ok(CustomReceiptSet {
            schema_version: 1,
            job_id: job_id.to_string(),
            job_found,
            job_status,
            expected_task_count,
            complete,
            accepted_count,
            failed_attempt_count,
            terminal_receipt_count: receipts.len(),
            canonical_sha256,
            receipts,
            error,
        })
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

        // Insert stress job parameters if this is a stress job
        if let Some(spec_val) = &job.job_spec_json {
            if spec_val.get("type").and_then(|v| v.as_str()) == Some("Stress") {
                let _ = conn.execute(
                    r#"
                    INSERT INTO stress_job_params (job_id, cpu_secs, memory_mb, leak_memory_mb, memory_jitter_pct, skip_memory_check, read_path, write_dir)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                    "#,
                    params![
                        job.job_id,
                        spec_val.get("cpu_secs").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        spec_val.get("memory_mb").and_then(|v| v.as_i64()).unwrap_or(0),
                        spec_val.get("leak_memory_mb").and_then(|v| v.as_i64()),
                        spec_val.get("memory_jitter_pct").and_then(|v| v.as_i64()),
                        spec_val.get("skip_memory_check").and_then(|v| v.as_bool()).unwrap_or(false),
                        spec_val.get("read_path").and_then(|v| v.as_str()),
                        spec_val.get("write_dir").and_then(|v| v.as_str()),
                    ],
                );
            }
        }

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
            SELECT job_id, status, start_time_ms, end_time_ms, NULL as job_spec_json, input_path, total_tasks, job_type
            FROM jobs
            ORDER BY start_time_ms DESC
            "#,
        )?;

        let jobs = stmt
            .query_map([], |row| {
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
                let tasks: Vec<String> = serde_json::from_str(&tasks_json).unwrap_or_default();
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
    /// Performs a cascading delete across mutable history tables. Immutable
    /// custom-task receipts are intentionally retained as durable audit records.
    /// Uses a transaction for atomicity.
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
        tx.execute(
            "DELETE FROM stress_job_params WHERE job_id = ?1",
            params![job_id],
        )?;
        tx.execute("DELETE FROM telemetry WHERE job_id = ?1", params![job_id])?;
        tx.execute(
            "DELETE FROM current_custom_assignments WHERE job_id = ?1",
            params![job_id],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Get metrics for a specific job.
    pub fn get_job_metrics(
        &self,
        job_id: &str,
        since_ms: u64,
    ) -> SqliteResult<Vec<(String, Vec<TelemetrySnapshot>)>> {
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
                       network_rx_bytes_sec, network_tx_bytes_sec, current_batch_size,
                       max_batch_capacity
                FROM telemetry
                WHERE job_id = ?1 AND worker_id = ?2 AND timestamp_ms > ?3
                ORDER BY timestamp_ms ASC
                "#,
            )?;

            let snapshots: Vec<TelemetrySnapshot> = stmt
                .query_map(params![job_id, &worker_id, since_ms as i64], |row| {
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
                        max_batch_capacity: row.get::<_, Option<i64>>(13)?.map(|v| v as usize),
                        // Phenotype visibility (not persisted - real-time only)
                        current_phenotype_id: None,
                        current_phase: None,
                        current_source: None,
                        current_ancestry: None,
                        prefetch_depth: None,
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
    use crate::distributed::message::{AssignmentLease, JobSpec};

    fn insert_custom_job(db: &MetricsDb, job_id: &str, total_tasks: usize) {
        db.insert_job(&JobRecord {
            job_id: job_id.to_string(),
            status: "running".to_string(),
            start_time_ms: 1,
            end_time_ms: None,
            job_spec_json: serde_json::to_value(JobSpec::Custom {
                payload: serde_json::json!({"test": true}),
                tasks: total_tasks,
                manifest: None,
            })
            .ok(),
            input_path: "input".to_string(),
            total_tasks,
            job_type: Some("custom".to_string()),
        })
        .unwrap();
    }

    fn completion(
        worker_id: &str,
        task_id: &str,
        attempt: u64,
        lease_token: &str,
    ) -> CompleteRequest {
        CompleteRequest {
            worker_id: worker_id.to_string(),
            tasks: vec![task_id.to_string()],
            items_processed: 7,
            result_json: Some(serde_json::json!({"task": task_id, "rows": 7})),
            error: None,
            session_id: Some("session-1".to_string()),
            assignments: vec![AssignmentLease {
                task_id: task_id.to_string(),
                assignment_attempt: attempt,
                lease_token: lease_token.to_string(),
            }],
        }
    }

    #[test]
    fn custom_receipt_migration_is_idempotent_and_backup_roundtrip_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ops.db");
        {
            let legacy = Connection::open(&db_path).unwrap();
            legacy
                .execute_batch(
                    "CREATE TABLE jobs (
                         job_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                         start_time_ms INTEGER NOT NULL, end_time_ms INTEGER,
                         job_spec_json TEXT, input_path TEXT, total_tasks INTEGER,
                         job_type TEXT, final_summary_json TEXT
                     );",
                )
                .unwrap();
        }

        let db = MetricsDb::open(&db_path).unwrap();
        insert_custom_job(&db, "job-backup", 1);
        let raw_lease = "raw-secret-lease-capability";
        db.persist_custom_assignments(
            "job-backup",
            "session-1",
            "worker-a",
            &[DurableCustomAssignment {
                task_id: "custom_0",
                partition_id: 0,
                assignment_attempt: 1,
                lease_token: raw_lease,
            }],
            10,
        )
        .unwrap();
        let request = completion("worker-a", "custom_0", 1, raw_lease);
        assert_eq!(
            db.accept_custom_completion("job-backup", Some("build-a"), &request, 20)
                .unwrap(),
            CustomCompletionOutcome::Stored
        );
        let first = db.get_custom_receipts("job-backup").unwrap();
        assert!(first.complete);
        assert_eq!(first.accepted_count, 1);
        assert_eq!(
            db.accept_custom_completion("job-backup", Some("build-a"), &request, 30)
                .unwrap(),
            CustomCompletionOutcome::Duplicate
        );
        assert_eq!(db.get_custom_receipts("job-backup").unwrap(), first);
        assert!(db
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE custom_task_receipts SET terminal_status = 'failed' WHERE job_id = ?1",
                ["job-backup"],
            )
            .is_err());

        db.checkpoint_for_backup().unwrap();
        let backup_path = dir.path().join("restored.db");
        std::fs::copy(&db_path, &backup_path).unwrap();
        drop(db);

        // Opening twice proves the migration remains idempotent on restored ops.db.
        let restored = MetricsDb::open(&backup_path).unwrap();
        drop(restored);
        let restored = MetricsDb::open(&backup_path).unwrap();
        assert_eq!(restored.get_custom_receipts("job-backup").unwrap(), first);
        let bytes = std::fs::read(&backup_path).unwrap();
        assert!(!bytes
            .windows(raw_lease.len())
            .any(|window| window == raw_lease.as_bytes()));
    }

    #[test]
    fn custom_receipt_acceptance_is_atomic_and_exact_job_scoped() {
        let db = MetricsDb::in_memory().unwrap();
        insert_custom_job(&db, "job-a", 1);
        insert_custom_job(&db, "job-b", 1);
        db.persist_custom_assignments(
            "job-a",
            "session-1",
            "worker-a",
            &[DurableCustomAssignment {
                task_id: "custom_0",
                partition_id: 0,
                assignment_attempt: 1,
                lease_token: "lease-a",
            }],
            10,
        )
        .unwrap();

        for wrong in [
            completion("worker-b", "custom_0", 1, "lease-a"),
            completion("worker-a", "custom_0", 2, "lease-a"),
            completion("worker-a", "custom_0", 1, "wrong-lease"),
            completion("worker-a", "wrong-task", 1, "lease-a"),
        ] {
            assert!(db
                .accept_custom_completion("job-a", None, &wrong, 20)
                .is_err());
            assert_eq!(
                db.get_custom_receipts("job-a")
                    .unwrap()
                    .terminal_receipt_count,
                0
            );
        }

        let correct = completion("worker-a", "custom_0", 1, "lease-a");
        assert_eq!(
            db.accept_custom_completion("job-a", None, &correct, 20)
                .unwrap(),
            CustomCompletionOutcome::Stored
        );
        let mut conflict = correct.clone();
        conflict.items_processed = 8;
        assert!(db
            .accept_custom_completion("job-a", None, &conflict, 21)
            .is_err());
        let a = db.get_custom_receipts("job-a").unwrap();
        let b = db.get_custom_receipts("job-b").unwrap();
        assert_eq!(a.accepted_count, 1);
        assert_eq!(a.terminal_receipt_count, 1);
        assert_eq!(b.accepted_count, 0);
        assert!(b.receipts.is_empty());
        assert!(!b.complete);
        assert_ne!(a.canonical_sha256, b.canonical_sha256);
    }

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
            max_batch_capacity: Some(32),
            // Phenotype visibility
            current_phenotype_id: Some("test-phenotype".to_string()),
            current_phase: Some("scan".to_string()),
            current_source: Some("exome".to_string()),
            current_ancestry: Some("EUR".to_string()),
            prefetch_depth: Some(4),
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
