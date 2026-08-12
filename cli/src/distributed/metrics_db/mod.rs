//! SQLite-backed metrics storage for persistent dashboard data.

use crate::distributed::message::{
    CompleteRequest, CustomReceiptSet, CustomTaskReceipt, FailureRecord, JobEvent, JobRecord,
    JobSpec, PhenotypeStatus, TelemetrySnapshot,
};
use rusqlite::{
    backup::{Backup, StepResult},
    params, Connection, OpenFlags, OptionalExtension, Result as SqliteResult,
};
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

/// One safe-export assignment persisted before its lease capability is exposed.
pub(crate) struct DurableSafeExportAssignment<'a> {
    pub export_id: &'a str,
    pub task_id: &'a str,
    pub partition_id: usize,
    pub assignment_attempt: u64,
    pub lease_token: &'a str,
    pub worker_id: &'a str,
    pub worker_git_commit: &'a str,
    pub worker_build_identity: &'a str,
    pub worker_binary_sha256: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustomCompletionOutcome {
    Stored,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SafePublicationClaimOutcome {
    Claimed,
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
                worker_build_version TEXT,
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

            -- Safe export has a parallel typed authority boundary. It must
            -- never reuse the arbitrary custom-task report tables.
            CREATE TABLE IF NOT EXISTS safe_export_runs (
                job_id TEXT PRIMARY KEY,
                export_id TEXT NOT NULL,
                expected_part_count INTEGER NOT NULL CHECK (expected_part_count >= 0),
                imported_part_count INTEGER NOT NULL DEFAULT 0 CHECK (imported_part_count >= 0),
                completed_part_count INTEGER NOT NULL DEFAULT 0 CHECK (completed_part_count >= 0),
                phase TEXT NOT NULL CHECK (phase IN ('running', 'finalizing', 'completed', 'failed', 'cancelled', 'superseded')),
                release_uri TEXT,
                release_version TEXT,
                release_etag TEXT,
                release_size INTEGER,
                release_sha256 TEXT,
                success_uri TEXT,
                success_version TEXT,
                success_etag TEXT,
                success_size INTEGER,
                success_sha256 TEXT,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_safe_export_runs_export
                ON safe_export_runs(export_id, job_id);

            CREATE TABLE IF NOT EXISTS current_safe_export_assignments (
                job_id TEXT NOT NULL,
                export_id TEXT NOT NULL,
                coordinator_session_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
                assignment_attempt INTEGER NOT NULL CHECK (assignment_attempt > 0),
                lease_identity_sha256 TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                worker_git_commit TEXT NOT NULL,
                worker_build_identity TEXT NOT NULL,
                worker_binary_sha256 TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('assigned', 'publishing')),
                report_json TEXT,
                report_sha256 TEXT,
                assigned_at_ms INTEGER NOT NULL,
                publishing_at_ms INTEGER,
                PRIMARY KEY (job_id, task_id),
                CHECK ((state = 'assigned' AND report_json IS NULL AND report_sha256 IS NULL AND publishing_at_ms IS NULL)
                    OR (state = 'publishing' AND report_json IS NOT NULL AND report_sha256 IS NOT NULL AND publishing_at_ms IS NOT NULL))
            );

            CREATE TABLE IF NOT EXISTS safe_export_attempt_receipts (
                job_id TEXT NOT NULL,
                export_id TEXT NOT NULL,
                coordinator_session_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
                assignment_attempt INTEGER NOT NULL CHECK (assignment_attempt > 0),
                lease_identity_sha256 TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                worker_git_commit TEXT NOT NULL,
                worker_build_identity TEXT NOT NULL,
                worker_binary_sha256 TEXT NOT NULL,
                terminal_status TEXT NOT NULL CHECK (terminal_status IN ('accepted', 'failed')),
                report_json TEXT,
                report_sha256 TEXT,
                error TEXT,
                accepted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, task_id, assignment_attempt),
                CHECK ((terminal_status = 'accepted' AND report_json IS NOT NULL AND report_sha256 IS NOT NULL AND error IS NULL)
                    OR (terminal_status = 'failed' AND error IS NOT NULL))
            );
            CREATE INDEX IF NOT EXISTS idx_safe_export_attempts_job
                ON safe_export_attempt_receipts(job_id, task_id, assignment_attempt);
            CREATE TRIGGER IF NOT EXISTS safe_export_attempt_receipts_immutable_update
            BEFORE UPDATE ON safe_export_attempt_receipts
            BEGIN
                SELECT RAISE(ABORT, 'safe export attempt receipts are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS safe_export_attempt_receipts_immutable_delete
            BEFORE DELETE ON safe_export_attempt_receipts
            BEGIN
                SELECT RAISE(ABORT, 'safe export attempt receipts are immutable');
            END;

            CREATE TABLE IF NOT EXISTS safe_export_part_receipts (
                export_id TEXT NOT NULL,
                partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
                task_id TEXT NOT NULL,
                first_job_id TEXT NOT NULL,
                first_assignment_attempt INTEGER NOT NULL CHECK (first_assignment_attempt > 0),
                receipt_json TEXT NOT NULL,
                receipt_sha256 TEXT NOT NULL,
                receipt_uri TEXT NOT NULL,
                receipt_version TEXT NOT NULL,
                receipt_etag TEXT NOT NULL,
                receipt_size INTEGER NOT NULL CHECK (receipt_size >= 0),
                receipt_object_sha256 TEXT NOT NULL,
                output_uri TEXT NOT NULL,
                output_version TEXT NOT NULL,
                output_etag TEXT NOT NULL,
                output_size INTEGER NOT NULL CHECK (output_size >= 0),
                output_sha256 TEXT NOT NULL,
                row_count INTEGER NOT NULL CHECK (row_count >= 0),
                row_group_count INTEGER NOT NULL CHECK (row_group_count >= 0),
                schema_fingerprint_sha256 TEXT NOT NULL,
                decoded_rows_sha256 TEXT NOT NULL,
                accepted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (export_id, partition_id),
                UNIQUE (export_id, task_id)
            );
            CREATE TRIGGER IF NOT EXISTS safe_export_part_receipts_immutable_update
            BEFORE UPDATE ON safe_export_part_receipts
            BEGIN
                SELECT RAISE(ABORT, 'safe export part receipts are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS safe_export_part_receipts_immutable_delete
            BEFORE DELETE ON safe_export_part_receipts
            BEGIN
                SELECT RAISE(ABORT, 'safe export part receipts are immutable');
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

        // Assignment-bound build identity was added after durable custom
        // assignment fencing. Existing outstanding assignments migrate as
        // unknown rather than borrowing mutable registry metadata.
        let has_assignment_build: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('current_custom_assignments') WHERE name = 'worker_build_version'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
            > 0;
        if !has_assignment_build {
            conn.execute_batch(
                "ALTER TABLE current_custom_assignments ADD COLUMN worker_build_version TEXT;",
            )?;
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

    /// Require a complete WAL checkpoint before backup. SQLite reports busy
    /// checkpoints as successful pragma rows, so all result columns must be
    /// inspected explicitly.
    pub fn checkpoint_for_backup(&self) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| format!("WAL checkpoint failed: {error}"))?;
        if busy != 0 || checkpointed_frames != log_frames {
            return Err(format!(
                "WAL checkpoint incomplete (busy={busy}, log={log_frames}, checkpointed={checkpointed_frames})"
            ));
        }
        Ok(())
    }

    /// Write a transactionally consistent standalone SQLite snapshot and
    /// verify it before callers may upload it. The live main file is never
    /// copied directly.
    pub(crate) fn write_verified_backup_snapshot<P: AsRef<Path>>(
        &self,
        snapshot_path: P,
    ) -> Result<(), String> {
        self.checkpoint_for_backup()?;
        let snapshot_path = snapshot_path.as_ref();
        if snapshot_path.exists() {
            return Err(format!(
                "backup snapshot destination already exists: {}",
                snapshot_path.display()
            ));
        }

        {
            let source = self.conn.lock().unwrap();
            let mut destination = Connection::open(snapshot_path)
                .map_err(|error| format!("failed to create backup snapshot: {error}"))?;
            let backup = Backup::new(&source, &mut destination)
                .map_err(|error| format!("failed to initialize SQLite backup: {error}"))?;
            match backup
                .step(-1)
                .map_err(|error| format!("SQLite backup failed: {error}"))?
            {
                StepResult::Done => {}
                StepResult::Busy => return Err("SQLite backup source was busy".to_string()),
                StepResult::Locked => return Err("SQLite backup source was locked".to_string()),
                StepResult::More => {
                    return Err("SQLite backup did not produce a complete snapshot".to_string())
                }
                _ => return Err("SQLite backup returned an unknown incomplete state".to_string()),
            }
        }

        let verified = (|| -> Result<(), String> {
            let snapshot =
                Connection::open_with_flags(snapshot_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .map_err(|error| format!("failed to reopen backup snapshot: {error}"))?;
            let integrity: String = snapshot
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .map_err(|error| format!("failed to verify backup integrity: {error}"))?;
            if integrity != "ok" {
                return Err(format!(
                    "backup snapshot failed integrity_check: {integrity}"
                ));
            }
            let required_tables: i64 = snapshot
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name IN (
                         'jobs', 'current_custom_assignments', 'custom_task_receipts',
                         'safe_export_runs', 'current_safe_export_assignments',
                         'safe_export_attempt_receipts', 'safe_export_part_receipts'
                     )",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| format!("failed to verify backup schema: {error}"))?;
            if required_tables != 7 {
                return Err(format!(
                    "backup snapshot is missing receipt-authority tables ({required_tables}/7 present)"
                ));
            }
            Ok(())
        })();
        if let Err(error) = verified {
            let _ = std::fs::remove_file(snapshot_path);
            return Err(error);
        }
        Ok(())
    }

    /// Register the durable run authority after the generic job row exists.
    pub(crate) fn register_safe_export_run(
        &self,
        job_id: &str,
        export_id: &str,
        expected_part_count: usize,
        created_at_ms: u64,
    ) -> Result<(), String> {
        let expected = i64::try_from(expected_part_count)
            .map_err(|_| "safe-export part count exceeds SQLite INTEGER".to_string())?;
        let created = i64::try_from(created_at_ms)
            .map_err(|_| "safe-export run timestamp exceeds SQLite INTEGER".to_string())?;
        let conn = self.conn.lock().unwrap();
        let status: Option<String> = conn
            .query_row(
                "SELECT status FROM jobs WHERE job_id = ?1",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if status.as_deref() != Some("running") {
            return Err(format!("safe-export job {job_id} is not durably running"));
        }
        conn.execute(
            "INSERT INTO safe_export_runs (
                 job_id, export_id, expected_part_count, imported_part_count,
                 completed_part_count, phase, created_at_ms
             ) VALUES (?1, ?2, ?3, 0, 0, 'running', ?4)",
            params![job_id, export_id, expected, created],
        )
        .map_err(|error| format!("failed to register safe-export run: {error}"))?;
        Ok(())
    }

    /// Persist exactly one safe assignment before exposing its raw lease token.
    pub(crate) fn persist_safe_export_assignment(
        &self,
        job_id: &str,
        coordinator_session_id: &str,
        assignment: &DurableSafeExportAssignment<'_>,
        assigned_at_ms: u64,
    ) -> Result<(), String> {
        if assignment.assignment_attempt == 0 {
            return Err("safe-export assignment attempt must be nonzero".to_string());
        }
        let partition_id = i64::try_from(assignment.partition_id)
            .map_err(|_| "partition ID exceeds SQLite INTEGER".to_string())?;
        let attempt = i64::try_from(assignment.assignment_attempt)
            .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
        let assigned_at = i64::try_from(assigned_at_ms)
            .map_err(|_| "assignment timestamp exceeds SQLite INTEGER".to_string())?;
        let lease_hash = genohype_core::io::lease_identity_sha256(assignment.lease_token)
            .map_err(|error| format!("invalid safe-export lease: {error}"))?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let authority: Option<(String, String)> = tx
            .query_row(
                "SELECT jobs.status, safe_export_runs.phase
                 FROM jobs JOIN safe_export_runs USING (job_id)
                 WHERE jobs.job_id = ?1 AND safe_export_runs.export_id = ?2",
                params![job_id, assignment.export_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if authority
            .as_ref()
            .map(|(job, run)| (job.as_str(), run.as_str()))
            != Some(("running", "running"))
        {
            return Err(format!(
                "safe-export run {job_id}/{} is not durably running",
                assignment.export_id
            ));
        }
        let accepted: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM safe_export_part_receipts
                 WHERE export_id = ?1 AND partition_id = ?2",
                params![assignment.export_id, partition_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if accepted != 0 {
            return Err(format!(
                "safe-export partition {} already has authority",
                assignment.partition_id
            ));
        }
        let prior_attempt: Option<i64> = tx
            .query_row(
                "SELECT MAX(assignment_attempt) FROM (
                     SELECT assignment_attempt FROM safe_export_attempt_receipts
                     WHERE job_id = ?1 AND task_id = ?2
                     UNION ALL
                     SELECT assignment_attempt FROM current_safe_export_assignments
                     WHERE job_id = ?1 AND task_id = ?2
                 )",
                params![job_id, assignment.task_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if prior_attempt.is_some_and(|prior| attempt <= prior) {
            return Err(format!(
                "safe-export attempt {} for {} is not newer than durable attempt {:?}",
                assignment.assignment_attempt, assignment.task_id, prior_attempt
            ));
        }
        tx.execute(
            "INSERT INTO current_safe_export_assignments (
                 job_id, export_id, coordinator_session_id, task_id, partition_id,
                 assignment_attempt, lease_identity_sha256, worker_id,
                 worker_git_commit, worker_build_identity, worker_binary_sha256,
                 state, assigned_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'assigned', ?12)",
            params![
                job_id,
                assignment.export_id,
                coordinator_session_id,
                assignment.task_id,
                partition_id,
                attempt,
                lease_hash,
                assignment.worker_id,
                assignment.worker_git_commit,
                assignment.worker_build_identity,
                assignment.worker_binary_sha256,
                assigned_at,
            ],
        )
        .map_err(|error| format!("failed to persist safe-export assignment: {error}"))?;
        tx.commit().map_err(|error| error.to_string())
    }

    /// Append one immutable failed attempt and consume its exact assignment.
    pub(crate) fn fail_safe_export_assignment(
        &self,
        job_id: &str,
        coordinator_session_id: &str,
        assignment: &DurableSafeExportAssignment<'_>,
        error: &str,
        accepted_at_ms: u64,
    ) -> Result<(), String> {
        if error.is_empty() {
            return Err("safe-export failure must include an error".to_string());
        }
        if error.contains(assignment.lease_token) {
            return Err("safe-export failure contains the raw lease capability".to_string());
        }
        let partition_id = i64::try_from(assignment.partition_id)
            .map_err(|_| "partition ID exceeds SQLite INTEGER".to_string())?;
        let attempt = i64::try_from(assignment.assignment_attempt)
            .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
        let accepted_at = i64::try_from(accepted_at_ms)
            .map_err(|_| "failure timestamp exceeds SQLite INTEGER".to_string())?;
        let lease_hash = genohype_core::io::lease_identity_sha256(assignment.lease_token)
            .map_err(|failure| format!("invalid safe-export lease: {failure}"))?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|failure| failure.to_string())?;
        let authority: Option<(String, String)> = tx
            .query_row(
                "SELECT jobs.status, safe_export_runs.phase
                 FROM jobs JOIN safe_export_runs USING (job_id)
                 WHERE jobs.job_id = ?1 AND safe_export_runs.export_id = ?2",
                params![job_id, assignment.export_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|failure| failure.to_string())?;
        if authority
            .as_ref()
            .map(|(job, run)| (job.as_str(), run.as_str()))
            != Some(("running", "running"))
        {
            return Err("safe-export run is not durably running".to_string());
        }
        let inserted = tx
            .execute(
                "INSERT INTO safe_export_attempt_receipts (
                     job_id, export_id, coordinator_session_id, task_id, partition_id,
                     assignment_attempt, lease_identity_sha256, worker_id,
                     worker_git_commit, worker_build_identity, worker_binary_sha256,
                     terminal_status, report_json, report_sha256, error, accepted_at_ms
                 )
                 SELECT job_id, export_id, coordinator_session_id, task_id, partition_id,
                        assignment_attempt, lease_identity_sha256, worker_id,
                        worker_git_commit, worker_build_identity, worker_binary_sha256,
                        'failed', NULL, NULL, ?12, ?13
                 FROM current_safe_export_assignments
                 WHERE job_id = ?1 AND export_id = ?2 AND coordinator_session_id = ?3
                   AND task_id = ?4 AND partition_id = ?5 AND assignment_attempt = ?6
                   AND lease_identity_sha256 = ?7 AND worker_id = ?8
                   AND worker_git_commit = ?9 AND worker_build_identity = ?10
                   AND worker_binary_sha256 = ?11 AND state = 'assigned'",
                params![
                    job_id,
                    assignment.export_id,
                    coordinator_session_id,
                    assignment.task_id,
                    partition_id,
                    attempt,
                    lease_hash,
                    assignment.worker_id,
                    assignment.worker_git_commit,
                    assignment.worker_build_identity,
                    assignment.worker_binary_sha256,
                    error,
                    accepted_at,
                ],
            )
            .map_err(|failure| format!("failed to append safe-export failure: {failure}"))?;
        if inserted != 1 {
            return Err("no exact assigned safe-export fence to fail".to_string());
        }
        let removed = tx
            .execute(
                "DELETE FROM current_safe_export_assignments
                 WHERE job_id = ?1 AND task_id = ?2 AND assignment_attempt = ?3
                   AND lease_identity_sha256 = ?4 AND state = 'assigned'",
                params![job_id, assignment.task_id, attempt, lease_hash],
            )
            .map_err(|failure| failure.to_string())?;
        if removed != 1 {
            return Err("safe-export assignment changed during failure commit".to_string());
        }
        tx.commit().map_err(|failure| failure.to_string())
    }

    /// Atomically claim an exact current assignment for cloud publication.
    /// Exact report redelivery is idempotent; changed bytes or identity fail.
    pub(crate) fn begin_safe_export_publication(
        &self,
        job_id: &str,
        coordinator_session_id: &str,
        assignment: &DurableSafeExportAssignment<'_>,
        report_json: &str,
        publishing_at_ms: u64,
    ) -> Result<SafePublicationClaimOutcome, String> {
        let attempt = i64::try_from(assignment.assignment_attempt)
            .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
        let publishing_at = i64::try_from(publishing_at_ms)
            .map_err(|_| "publishing timestamp exceeds SQLite INTEGER".to_string())?;
        let lease_hash = genohype_core::io::lease_identity_sha256(assignment.lease_token)
            .map_err(|error| format!("invalid safe-export lease: {error}"))?;
        let report_sha256 = sha256_hex(
            b"genohype/safe-export-worker-report/v1",
            report_json.as_bytes(),
        );
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let authority: Option<(String, String)> = tx
            .query_row(
                "SELECT jobs.status, safe_export_runs.phase
                 FROM jobs JOIN safe_export_runs USING (job_id)
                 WHERE jobs.job_id = ?1 AND safe_export_runs.export_id = ?2",
                params![job_id, assignment.export_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if authority
            .as_ref()
            .map(|(job, run)| (job.as_str(), run.as_str()))
            != Some(("running", "running"))
        {
            return Err("safe-export run is not durably running".to_string());
        }
        let current: Option<(String, Option<String>, Option<String>)> = tx
            .query_row(
                "SELECT state, report_json, report_sha256
                 FROM current_safe_export_assignments
                 WHERE job_id = ?1 AND export_id = ?2 AND coordinator_session_id = ?3
                   AND task_id = ?4 AND partition_id = ?5 AND assignment_attempt = ?6
                   AND lease_identity_sha256 = ?7 AND worker_id = ?8
                   AND worker_git_commit = ?9 AND worker_build_identity = ?10
                   AND worker_binary_sha256 = ?11",
                params![
                    job_id,
                    assignment.export_id,
                    coordinator_session_id,
                    assignment.task_id,
                    i64::try_from(assignment.partition_id)
                        .map_err(|_| "partition ID exceeds SQLite INTEGER".to_string())?,
                    attempt,
                    lease_hash,
                    assignment.worker_id,
                    assignment.worker_git_commit,
                    assignment.worker_build_identity,
                    assignment.worker_binary_sha256,
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        match current {
            Some((state, prior_json, prior_sha)) if state == "publishing" => {
                if prior_json.as_deref() == Some(report_json)
                    && prior_sha.as_deref() == Some(report_sha256.as_str())
                {
                    return Ok(SafePublicationClaimOutcome::Duplicate);
                }
                return Err("conflicting report for publishing safe-export assignment".to_string());
            }
            Some((state, _, _)) if state == "assigned" => {}
            _ => return Err("no exact assigned safe-export fence to publish".to_string()),
        }
        let updated = tx
            .execute(
                "UPDATE current_safe_export_assignments
                 SET state = 'publishing', report_json = ?12,
                     report_sha256 = ?13, publishing_at_ms = ?14
                 WHERE job_id = ?1 AND export_id = ?2 AND coordinator_session_id = ?3
                   AND task_id = ?4 AND partition_id = ?5 AND assignment_attempt = ?6
                   AND lease_identity_sha256 = ?7 AND worker_id = ?8
                   AND worker_git_commit = ?9 AND worker_build_identity = ?10
                   AND worker_binary_sha256 = ?11 AND state = 'assigned'",
                params![
                    job_id,
                    assignment.export_id,
                    coordinator_session_id,
                    assignment.task_id,
                    i64::try_from(assignment.partition_id)
                        .map_err(|_| "partition ID exceeds SQLite INTEGER".to_string())?,
                    attempt,
                    lease_hash,
                    assignment.worker_id,
                    assignment.worker_git_commit,
                    assignment.worker_build_identity,
                    assignment.worker_binary_sha256,
                    report_json,
                    report_sha256,
                    publishing_at,
                ],
            )
            .map_err(|error| error.to_string())?;
        if updated != 1 {
            return Err("safe-export assignment changed while claiming publication".to_string());
        }
        tx.commit().map_err(|error| error.to_string())?;
        Ok(SafePublicationClaimOutcome::Claimed)
    }

    /// Clear one assigned safe-export fence during timeout/preemption. A
    /// publishing claim is deliberately non-requeueable and is left intact.
    pub(crate) fn clear_assigned_safe_export_assignment(
        &self,
        job_id: &str,
        task_id: &str,
        assignment_attempt: u64,
        lease_token: &str,
    ) -> Result<bool, String> {
        let attempt = i64::try_from(assignment_attempt)
            .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
        let lease_hash = genohype_core::io::lease_identity_sha256(lease_token)
            .map_err(|error| format!("invalid safe-export lease: {error}"))?;
        let conn = self.conn.lock().unwrap();
        let removed = conn
            .execute(
                "DELETE FROM current_safe_export_assignments
                 WHERE job_id = ?1 AND task_id = ?2 AND assignment_attempt = ?3
                   AND lease_identity_sha256 = ?4 AND state = 'assigned'",
                params![job_id, task_id, attempt, lease_hash],
            )
            .map_err(|error| error.to_string())?;
        Ok(removed == 1)
    }

    pub(crate) fn clear_current_safe_export_assignments(&self, job_id: &str) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM current_safe_export_assignments WHERE job_id = ?1",
            [job_id],
        )?;
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
        worker_build_version: Option<&str>,
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
                     assignment_attempt, lease_identity_sha256, worker_id,
                     worker_build_version, assigned_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(job_id, task_id) DO UPDATE SET
                     coordinator_session_id = excluded.coordinator_session_id,
                     partition_id = excluded.partition_id,
                     assignment_attempt = excluded.assignment_attempt,
                     lease_identity_sha256 = excluded.lease_identity_sha256,
                     worker_id = excluded.worker_id,
                     worker_build_version = excluded.worker_build_version,
                     assigned_at_ms = excluded.assigned_at_ms",
                params![
                    job_id,
                    coordinator_session_id,
                    assignment.task_id,
                    partition_id,
                    attempt,
                    lease_identity(assignment.lease_token),
                    worker_id,
                    worker_build_version,
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
            let current: Option<(i64, Option<String>)> = tx
                .query_row(
                    "SELECT partition_id, worker_build_version
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
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| error.to_string())?;
            let Some((partition_id, worker_build_version)) = current else {
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
                    worker_build_version.as_deref(),
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

    /// Fail closed any custom or safe-export job running in a restored backup.
    /// Restored receipts remain immutable audit records, but cannot revive an
    /// old coordinator session or assignment fence.
    pub(crate) fn reconcile_restored_fenced_jobs(&self, end_time_ms: u64) -> Result<usize, String> {
        let end_time_ms = i64::try_from(end_time_ms)
            .map_err(|_| "restore reconciliation timestamp exceeds SQLite INTEGER".to_string())?;
        let summary = serde_json::json!({
            "error": "Fenced job interrupted by coordinator startup after database backup restore; restored receipts are not completion authority"
        })
        .to_string();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let running_jobs = {
            let mut stmt = tx
                .prepare(
                    "SELECT job_id, job_type, job_spec_json FROM jobs WHERE status = 'running'",
                )
                .map_err(|error| error.to_string())?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(|error| error.to_string())?;
            rows.collect::<SqliteResult<Vec<_>>>()
                .map_err(|error| error.to_string())?
        };
        let fenced_job_ids: Vec<(String, bool)> = running_jobs
            .into_iter()
            .filter_map(|(job_id, job_type, job_spec_json)| {
                let decoded = job_spec_json
                    .as_deref()
                    .and_then(|json| serde_json::from_str::<JobSpec>(json).ok());
                let is_custom = matches!(job_type.as_deref(), Some("custom" | "custom payload"))
                    || decoded
                        .as_ref()
                        .is_some_and(|spec| matches!(spec, JobSpec::Custom { .. }));
                let is_safe = job_type.as_deref() == Some("safe export parquet")
                    || decoded
                        .as_ref()
                        .is_some_and(|spec| matches!(spec, JobSpec::SafeExport(_)));
                (is_custom || is_safe).then_some((job_id, is_safe))
            })
            .collect();

        for (job_id, is_safe) in &fenced_job_ids {
            let updated = tx
                .execute(
                    "UPDATE jobs SET status = 'failed', end_time_ms = ?2,
                                     final_summary_json = ?3
                     WHERE job_id = ?1 AND status = 'running'",
                    params![job_id, end_time_ms, summary],
                )
                .map_err(|error| {
                    format!("failed to terminalize restored fenced job {job_id}: {error}")
                })?;
            if updated != 1 {
                return Err(format!(
                    "restored fenced job {job_id} changed during reconciliation"
                ));
            }
            tx.execute(
                "DELETE FROM current_custom_assignments WHERE job_id = ?1",
                [job_id],
            )
            .map_err(|error| format!("failed to fence restored custom job {job_id}: {error}"))?;
            tx.execute(
                "DELETE FROM current_safe_export_assignments WHERE job_id = ?1",
                [job_id],
            )
            .map_err(|error| {
                format!("failed to fence restored safe-export job {job_id}: {error}")
            })?;
            if *is_safe {
                tx.execute(
                    "UPDATE safe_export_runs SET phase = 'failed'
                     WHERE job_id = ?1 AND phase IN ('running', 'finalizing')",
                    [job_id],
                )
                .map_err(|error| {
                    format!("failed to terminalize safe-export run {job_id}: {error}")
                })?;
            }
        }
        tx.commit().map_err(|error| error.to_string())?;
        Ok(fenced_job_ids.len())
    }

    /// Atomically cancel a running custom job and remove every durable current
    /// assignment fence. Callers must not acknowledge or clear memory before
    /// this transaction commits.
    pub(crate) fn cancel_custom_job(&self, job_id: &str, end_time_ms: u64) -> Result<(), String> {
        let end_time_ms = i64::try_from(end_time_ms)
            .map_err(|_| "cancellation timestamp exceeds SQLite INTEGER".to_string())?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let updated = tx
            .execute(
                "UPDATE jobs SET status = 'cancelled', end_time_ms = ?2,
                                 final_summary_json = NULL
                 WHERE job_id = ?1 AND status = 'running'",
                params![job_id, end_time_ms],
            )
            .map_err(|error| error.to_string())?;
        if updated != 1 {
            return Err(format!(
                "custom job {job_id} is missing or is not durably running"
            ));
        }
        tx.execute(
            "DELETE FROM current_custom_assignments WHERE job_id = ?1",
            [job_id],
        )
        .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())
    }

    /// Remove one exact live durable assignment fence after coordinator preemption.
    pub(crate) fn clear_current_custom_assignment(
        &self,
        job_id: &str,
        task_id: &str,
        assignment_attempt: u64,
        lease_token: &str,
    ) -> Result<(), String> {
        let assignment_attempt = i64::try_from(assignment_attempt)
            .map_err(|_| "assignment attempt exceeds SQLite INTEGER".to_string())?;
        let lease_identity_sha256 = lease_identity(lease_token);
        let conn = self.conn.lock().unwrap();
        let deleted = conn
            .execute(
                "DELETE FROM current_custom_assignments
                 WHERE job_id = ?1 AND task_id = ?2
                   AND assignment_attempt = ?3 AND lease_identity_sha256 = ?4",
                params![job_id, task_id, assignment_attempt, lease_identity_sha256],
            )
            .map_err(|error| error.to_string())?;
        if deleted != 1 {
            return Err(format!(
                "expected one current assignment for {job_id}/{task_id}, deleted {deleted}"
            ));
        }
        Ok(())
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

    #[cfg(test)]
    pub(crate) fn execute_test_sql(&self, sql: &str) -> SqliteResult<()> {
        self.conn.lock().unwrap().execute_batch(sql)
    }

    #[cfg(test)]
    pub(crate) fn current_custom_assignment_count(&self, job_id: &str) -> SqliteResult<usize> {
        self.conn.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM current_custom_assignments WHERE job_id = ?1",
            [job_id],
            |row| row.get::<_, i64>(0).map(|count| count as usize),
        )
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
            job_type: Some(
                JobSpec::Custom {
                    payload: serde_json::Value::Null,
                    tasks: total_tasks,
                    manifest: None,
                }
                .description()
                .to_string(),
            ),
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

    fn insert_safe_run(db: &MetricsDb, job_id: &str, export_id: &str, total_tasks: usize) {
        db.insert_job(&JobRecord {
            job_id: job_id.to_string(),
            status: "running".to_string(),
            start_time_ms: 1,
            end_time_ms: None,
            job_spec_json: None,
            input_path: "gs://bucket/input.ht".to_string(),
            total_tasks,
            job_type: Some("safe export parquet".to_string()),
        })
        .unwrap();
        db.register_safe_export_run(job_id, export_id, total_tasks, 1)
            .unwrap();
    }

    #[test]
    fn safe_export_assignment_is_parallel_hashed_and_persisted_before_dispatch() {
        let db = MetricsDb::in_memory().unwrap();
        let export_id = "a".repeat(64);
        let binary = "b".repeat(64);
        insert_safe_run(&db, "safe-job", &export_id, 2);
        let raw_lease = "raw-capability-must-not-be-persisted";
        db.persist_safe_export_assignment(
            "safe-job",
            "session-1",
            &DurableSafeExportAssignment {
                export_id: &export_id,
                task_id: "part-00000000",
                partition_id: 0,
                assignment_attempt: 1,
                lease_token: raw_lease,
                worker_id: "worker-1",
                worker_git_commit: "commit-1",
                worker_build_identity: "build-1",
                worker_binary_sha256: &binary,
            },
            2,
        )
        .unwrap();

        let conn = db.conn.lock().unwrap();
        let row: (String, String, String, String, String) = conn
            .query_row(
                "SELECT lease_identity_sha256, worker_git_commit,
                        worker_build_identity, worker_binary_sha256, state
                 FROM current_safe_export_assignments
                 WHERE job_id = 'safe-job' AND task_id = 'part-00000000'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            row.0,
            genohype_core::io::lease_identity_sha256(raw_lease).unwrap()
        );
        assert_ne!(row.0, raw_lease);
        assert_eq!(
            (&row.1, &row.2, &row.3, &row.4),
            (
                &"commit-1".into(),
                &"build-1".into(),
                &binary,
                &"assigned".into()
            )
        );
        let dump: String = conn
            .query_row(
                "SELECT group_concat(value, '') FROM (
                     SELECT job_id AS value FROM current_safe_export_assignments
                     UNION ALL SELECT lease_identity_sha256 FROM current_safe_export_assignments
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!dump.contains(raw_lease));
        drop(conn);

        assert!(db
            .persist_safe_export_assignment(
                "safe-job",
                "session-1",
                &DurableSafeExportAssignment {
                    export_id: &export_id,
                    task_id: "part-00000000",
                    partition_id: 0,
                    assignment_attempt: 1,
                    lease_token: "another",
                    worker_id: "worker-2",
                    worker_git_commit: "commit-1",
                    worker_build_identity: "build-1",
                    worker_binary_sha256: &binary,
                },
                3,
            )
            .is_err());
        assert!(db
            .persist_safe_export_assignment(
                "safe-job",
                "session-1",
                &DurableSafeExportAssignment {
                    export_id: &export_id,
                    task_id: "part-overflow",
                    partition_id: usize::MAX,
                    assignment_attempt: 1,
                    lease_token: "lease",
                    worker_id: "worker-1",
                    worker_git_commit: "commit-1",
                    worker_build_identity: "build-1",
                    worker_binary_sha256: &binary,
                },
                3,
            )
            .is_err());
    }

    #[test]
    fn safe_export_restore_fails_old_session_and_clears_live_fence() {
        let db = MetricsDb::in_memory().unwrap();
        let export_id = "a".repeat(64);
        let binary = "b".repeat(64);
        insert_safe_run(&db, "safe-job", &export_id, 1);
        db.persist_safe_export_assignment(
            "safe-job",
            "old-session",
            &DurableSafeExportAssignment {
                export_id: &export_id,
                task_id: "part-00000000",
                partition_id: 0,
                assignment_attempt: 1,
                lease_token: "old-lease",
                worker_id: "worker-1",
                worker_git_commit: "commit-1",
                worker_build_identity: "build-1",
                worker_binary_sha256: &binary,
            },
            2,
        )
        .unwrap();
        assert_eq!(db.reconcile_restored_fenced_jobs(3).unwrap(), 1);
        let conn = db.conn.lock().unwrap();
        let authority: (String, String, i64) = conn
            .query_row(
                "SELECT jobs.status, safe_export_runs.phase,
                        (SELECT COUNT(*) FROM current_safe_export_assignments)
                 FROM jobs JOIN safe_export_runs USING (job_id)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(authority, ("failed".into(), "failed".into(), 0));
    }

    #[test]
    fn safe_export_failure_atomically_appends_attempt_and_consumes_assignment() {
        let db = MetricsDb::in_memory().unwrap();
        let export_id = "a".repeat(64);
        let binary = "b".repeat(64);
        let assignment = DurableSafeExportAssignment {
            export_id: &export_id,
            task_id: "part-00000000",
            partition_id: 0,
            assignment_attempt: 1,
            lease_token: "raw-lease",
            worker_id: "worker-1",
            worker_git_commit: "commit-1",
            worker_build_identity: "build-1",
            worker_binary_sha256: &binary,
        };
        insert_safe_run(&db, "safe-job", &export_id, 1);
        db.persist_safe_export_assignment("safe-job", "session-1", &assignment, 2)
            .unwrap();
        db.fail_safe_export_assignment("safe-job", "session-1", &assignment, "worker failure", 3)
            .unwrap();
        let conn = db.conn.lock().unwrap();
        let current: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM current_safe_export_assignments",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let failed: (String, String) = conn
            .query_row(
                "SELECT terminal_status, error FROM safe_export_attempt_receipts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(current, 0);
        assert_eq!(failed, ("failed".into(), "worker failure".into()));
    }

    #[test]
    fn safe_export_publication_claim_is_exact_idempotent_and_non_requeueable() {
        let db = MetricsDb::in_memory().unwrap();
        let export_id = "a".repeat(64);
        let binary = "b".repeat(64);
        let assignment = DurableSafeExportAssignment {
            export_id: &export_id,
            task_id: "part-00000000",
            partition_id: 0,
            assignment_attempt: 1,
            lease_token: "raw-lease",
            worker_id: "worker-1",
            worker_git_commit: "commit-1",
            worker_build_identity: "build-1",
            worker_binary_sha256: &binary,
        };
        insert_safe_run(&db, "safe-job", &export_id, 1);
        db.persist_safe_export_assignment("safe-job", "session-1", &assignment, 2)
            .unwrap();
        assert_eq!(
            db.begin_safe_export_publication(
                "safe-job",
                "session-1",
                &assignment,
                "{\"report\":1}",
                3,
            )
            .unwrap(),
            SafePublicationClaimOutcome::Claimed
        );
        assert!(!db
            .clear_assigned_safe_export_assignment("safe-job", "part-00000000", 1, "raw-lease")
            .unwrap());
        assert_eq!(
            db.begin_safe_export_publication(
                "safe-job",
                "session-1",
                &assignment,
                "{\"report\":1}",
                4,
            )
            .unwrap(),
            SafePublicationClaimOutcome::Duplicate
        );
        assert!(db
            .begin_safe_export_publication(
                "safe-job",
                "session-1",
                &assignment,
                "{\"report\":2}",
                4,
            )
            .is_err());
    }

    #[test]
    fn safe_export_evidence_tables_are_immutable() {
        let db = MetricsDb::in_memory().unwrap();
        let conn = db.conn.lock().unwrap();
        conn.execute_batch(
            "INSERT INTO safe_export_attempt_receipts VALUES (
                 'job','export','session','task',0,1,'lease','worker','commit','build','binary',
                 'failed',NULL,NULL,'failure',1);
             INSERT INTO safe_export_part_receipts VALUES (
                 'export',0,'task','job',1,'{}','digest','gs://b/r','1','etag',2,'rdigest',
                 'gs://b/o','2','etag2',3,'odigest',4,1,'schema','rows',5);",
        )
        .unwrap();
        assert!(conn
            .execute(
                "UPDATE safe_export_attempt_receipts SET error='changed'",
                []
            )
            .is_err());
        assert!(conn
            .execute("DELETE FROM safe_export_attempt_receipts", [])
            .is_err());
        assert!(conn
            .execute("UPDATE safe_export_part_receipts SET row_count=9", [])
            .is_err());
        assert!(conn
            .execute("DELETE FROM safe_export_part_receipts", [])
            .is_err());
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
                     );
                     CREATE TABLE current_custom_assignments (
                         job_id TEXT NOT NULL, coordinator_session_id TEXT NOT NULL,
                         task_id TEXT NOT NULL, partition_id INTEGER NOT NULL,
                         assignment_attempt INTEGER NOT NULL,
                         lease_identity_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
                         assigned_at_ms INTEGER NOT NULL,
                         PRIMARY KEY (job_id, task_id)
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
            Some("build-a"),
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
            db.accept_custom_completion("job-backup", &request, 20)
                .unwrap(),
            CustomCompletionOutcome::Stored
        );
        let first = db.get_custom_receipts("job-backup").unwrap();
        assert!(first.complete);
        assert_eq!(first.accepted_count, 1);
        assert_eq!(
            db.accept_custom_completion("job-backup", &request, 30)
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

        db.cancel_custom_job("job-backup", 40).unwrap();
        let cancelled = db.get_custom_receipts("job-backup").unwrap();
        assert_eq!(cancelled.job_status.as_deref(), Some("cancelled"));
        assert!(!cancelled.complete);
        assert_eq!(cancelled.receipts, first.receipts);

        let backup_path = dir.path().join("restored.db");
        db.write_verified_backup_snapshot(&backup_path).unwrap();
        drop(db);

        let restored = MetricsDb::open(&backup_path).unwrap();
        drop(restored);
        let restored = MetricsDb::open(&backup_path).unwrap();
        assert_eq!(
            restored.get_custom_receipts("job-backup").unwrap(),
            cancelled
        );
        let integrity: String = restored
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let bytes = std::fs::read(&backup_path).unwrap();
        assert!(!bytes
            .windows(raw_lease.len())
            .any(|window| window == raw_lease.as_bytes()));
    }

    #[test]
    fn held_reader_blocks_checkpoint_and_cannot_publish_a_stale_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ops.db");
        let snapshot_path = dir.path().join("snapshot.db");
        let db = MetricsDb::open(&db_path).unwrap();
        insert_custom_job(&db, "job-busy", 1);
        db.persist_custom_assignments(
            "job-busy",
            "session-1",
            "worker-a",
            Some("build-a"),
            &[DurableCustomAssignment {
                task_id: "custom_0",
                partition_id: 0,
                assignment_attempt: 1,
                lease_token: "lease-a",
            }],
            10,
        )
        .unwrap();
        db.accept_custom_completion(
            "job-busy",
            &completion("worker-a", "custom_0", 1, "lease-a"),
            20,
        )
        .unwrap();
        db.checkpoint_for_backup().unwrap();

        let reader = Connection::open(&db_path).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        let status: String = reader
            .query_row(
                "SELECT status FROM jobs WHERE job_id = 'job-busy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "running");
        db.cancel_custom_job("job-busy", 30).unwrap();

        let error = db
            .write_verified_backup_snapshot(&snapshot_path)
            .unwrap_err();
        assert!(
            error.contains("busy") || error.contains("incomplete"),
            "{error}"
        );
        assert!(!snapshot_path.exists());
        reader.execute_batch("ROLLBACK").unwrap();
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
            Some("build-a"),
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
            assert!(db.accept_custom_completion("job-a", &wrong, 20).is_err());
            assert_eq!(
                db.get_custom_receipts("job-a")
                    .unwrap()
                    .terminal_receipt_count,
                0
            );
        }

        let correct = completion("worker-a", "custom_0", 1, "lease-a");
        assert_eq!(
            db.accept_custom_completion("job-a", &correct, 20).unwrap(),
            CustomCompletionOutcome::Stored
        );
        let mut conflict = correct.clone();
        conflict.items_processed = 8;
        assert!(db.accept_custom_completion("job-a", &conflict, 21).is_err());
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
