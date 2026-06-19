//! Postgres export functionality for Hail tables (Phase 2b).
//!
//! This is the genohype Postgres loader for the gnomad-bench `postgres` (and
//! `tiered-*`) arms. Where the [`crate::export::elasticsearch`] loader reproduces
//! a prod document index, this loader targets the **wide JSONB table** decided in
//! the benchmark design (`gnomad-bench/DESIGN.md`):
//!
//! ```sql
//! CREATE TABLE variants (
//!     contig     TEXT NOT NULL,
//!     pos        INTEGER NOT NULL,
//!     variant_id TEXT NOT NULL,
//!     "ref"      TEXT,
//!     "alt"      TEXT,
//!     data       JSONB,
//!     PRIMARY KEY (contig, variant_id)
//! ) PARTITION BY LIST (contig);
//! ```
//!
//! Design rationale (see DESIGN.md §"Database schemas & models"): a normalized
//! relational schema is unworkable for gnomAD's deeply-nested rows, so the whole
//! row lands in a single `data JSONB` column while the columns the browser query
//! patterns filter/sort on — `(contig, pos)` for region/gene ranges and
//! `variant_id` for point lookups — are hoisted out as typed columns with B-tree
//! indexes. List-partitioning by `contig` mirrors ClickHouse's `PARTITION BY
//! contig` and gives the API's `(contig, pos)` range queries partition pruning.
//! No GIN index on `data` — the browser never does arbitrary JSON filtering.
//!
//! # Streaming load via `COPY`
//!
//! Rows are streamed with the Postgres binary-protocol `COPY ... FROM STDIN`
//! (text format) through `sqlx`, the fastest bulk-ingest path. To keep re-loads
//! **idempotent** (an explicit Phase-2b acceptance criterion) while still using
//! `COPY` — which has no `ON CONFLICT` clause — each batch is `COPY`d into a
//! per-connection `TEMP` staging table and then upserted into the partitioned
//! target with `INSERT … SELECT … ON CONFLICT (contig, variant_id) DO UPDATE`.
//! Re-indexing the same `variant_id` overwrites rather than appends, so the final
//! row count is unchanged — the relational analogue of the ES loader's stable
//! `_id`. List partitions are created lazily as new contigs are seen (a
//! partitioned table rejects a row with no matching partition).
//!
//! # Schema width / projection
//!
//! Like the ES loader, the schema-width dimension (`full` vs `browser-minimal`,
//! [`crate::projection::SchemaWidth`]) is expressed in the **`data` payload**:
//! callers pass already-projected rows, so the JSONB carries only the projected
//! fields. The hoisted columns are extracted from the (projected) row, so the
//! projection must retain the key fields (`locus`, `variant_id`, `alleles`) — the
//! CLI ensures this when it builds the decode projection.

use crate::codec::EncodedValue;
use crate::export::json::to_json_value;
use std::collections::HashSet;
use thiserror::Error;

#[cfg(feature = "postgres")]
use sqlx::{postgres::PgConnection, Connection};

/// Errors that can occur during Postgres export.
#[derive(Error, Debug)]
pub enum PostgresError {
    #[cfg(feature = "postgres")]
    #[error("Postgres error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("Postgres load error: {0}")]
    Load(String),

    #[error("Invalid row: {0}")]
    InvalidRow(String),
}

pub type Result<T> = std::result::Result<T, PostgresError>;

// ---------------------------------------------------------------------------
// Wide-table schema + DDL
// ---------------------------------------------------------------------------

/// The hoisted/indexed columns of the wide variants table, in `COPY` order. The
/// remaining `data JSONB` column is appended after these.
pub const COLUMNS: [&str; 5] = ["contig", "pos", "variant_id", "ref", "alt"];

/// Quote a SQL identifier (double-quote, doubling any embedded quote). Used so
/// `ref` (a non-reserved keyword) and lazily-generated partition names are always
/// safe to interpolate.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a SQL string literal (single-quote, doubling any embedded quote).
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// `CREATE TABLE IF NOT EXISTS` for the partitioned wide JSONB table.
///
/// The primary key is `(contig, variant_id)` — a partitioned table requires its
/// partition key (`contig`) to be part of any unique constraint, and that pair is
/// unique because `variant_id` encodes `contig-pos-ref-alt`. The PK index also
/// backs the `ON CONFLICT` upsert used for idempotent re-loads.
pub fn generate_create_table(table: &str) -> String {
    let t = quote_ident(table);
    format!(
        "CREATE TABLE IF NOT EXISTS {t} (\n    \
         contig TEXT NOT NULL,\n    \
         pos INTEGER NOT NULL,\n    \
         variant_id TEXT NOT NULL,\n    \
         \"ref\" TEXT,\n    \
         \"alt\" TEXT,\n    \
         data JSONB,\n    \
         PRIMARY KEY (contig, variant_id)\n\
         ) PARTITION BY LIST (contig)"
    )
}

/// `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES IN (contig)`.
///
/// Partition table names are derived as `{table}_{sanitized_contig}` where the
/// contig is sanitized to a safe identifier suffix (e.g. `chr22` → `variants_chr22`).
pub fn generate_create_partition(table: &str, contig: &str) -> String {
    let parent = quote_ident(table);
    let part = quote_ident(&partition_name(table, contig));
    format!(
        "CREATE TABLE IF NOT EXISTS {part} PARTITION OF {parent} FOR VALUES IN ({})",
        quote_literal(contig)
    )
}

/// Derive a partition table name for a contig, sanitizing characters that are not
/// safe in an unquoted-suffix identifier (anything non-alphanumeric → `_`).
fn partition_name(table: &str, contig: &str) -> String {
    let sanitized: String = contig
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("{table}_{sanitized}")
}

/// Secondary indexes created *after* the bulk load (faster than maintaining them
/// during ingest). On a partitioned parent these cascade to every partition.
///
/// - `(contig, pos)` composite B-tree — region/gene range queries.
/// - `variant_id` B-tree — variant-by-id point lookups.
///
/// The `(contig, variant_id)` PK index already exists from `CREATE TABLE`.
pub fn generate_indexes(table: &str) -> Vec<String> {
    let t = quote_ident(table);
    vec![
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {t} (contig, pos)",
            quote_ident(&format!("{table}_contig_pos_idx"))
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {t} (variant_id)",
            quote_ident(&format!("{table}_variant_id_idx"))
        ),
    ]
}

// ---------------------------------------------------------------------------
// Key-column extraction
// ---------------------------------------------------------------------------

/// The hoisted column values pulled out of a decoded row.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantColumns {
    pub contig: String,
    pub pos: i32,
    pub variant_id: String,
    pub ref_allele: Option<String>,
    pub alt_allele: Option<String>,
}

/// Look up a field by name in a struct value.
fn struct_field<'a>(row: &'a EncodedValue, name: &str) -> Option<&'a EncodedValue> {
    match row {
        EncodedValue::Struct(fields) => fields.iter().find(|(k, _)| k == name).map(|(_, v)| v),
        _ => None,
    }
}

fn as_string(value: &EncodedValue) -> Option<String> {
    match value {
        EncodedValue::Binary(_) => value.as_string(),
        EncodedValue::Struct(_) | EncodedValue::Array(_) | EncodedValue::Null => None,
        EncodedValue::Int32(v) => Some(v.to_string()),
        EncodedValue::Int64(v) => Some(v.to_string()),
        EncodedValue::Float32(v) => Some(v.to_string()),
        EncodedValue::Float64(v) => Some(v.to_string()),
        EncodedValue::Boolean(v) => Some(v.to_string()),
    }
}

fn as_pos(value: &EncodedValue) -> Option<i32> {
    match value {
        EncodedValue::Int32(v) => Some(*v),
        EncodedValue::Int64(v) => i32::try_from(*v).ok(),
        _ => None,
    }
}

/// Extract the hoisted columns `(contig, pos, variant_id, ref, alt)` from a
/// decoded gnomAD variant row.
///
/// Canonical gnomAD sites-HT sources:
/// - `contig`/`pos` from the `locus` struct (`{contig, position}`).
/// - `variant_id` from the top-level `variant_id` field.
/// - `ref`/`alt` from the `alleles` array (`alleles[0]` = ref, `alleles[1]` = alt).
///
/// Falls back to parsing the `variant_id` string (`contig-pos-ref-alt`) for any
/// piece the structured fields don't provide, so the loader still works against a
/// `browser-minimal` projection or a table that lacks an explicit `locus`/`alleles`.
/// `contig` and `pos` are required (they back the partition key / index); a row
/// missing both the structured value and a parseable `variant_id` is an error so
/// counts stay honest rather than silently dropping rows.
pub fn extract_columns(row: &EncodedValue) -> Result<VariantColumns> {
    let variant_id = struct_field(row, "variant_id")
        .and_then(as_string)
        .ok_or_else(|| {
            PostgresError::InvalidRow("row is missing a scalar `variant_id` field".to_string())
        })?;

    // `variant_id` is `contig-pos-ref-alt`; used as a fallback for any piece the
    // structured columns don't supply.
    let parts: Vec<&str> = variant_id.splitn(4, '-').collect();

    let contig = struct_field(row, "locus")
        .and_then(|l| struct_field(l, "contig"))
        .and_then(as_string)
        .or_else(|| parts.first().map(|s| s.to_string()))
        .ok_or_else(|| {
            PostgresError::InvalidRow(format!(
                "cannot determine contig for variant_id '{variant_id}'"
            ))
        })?;

    let pos = struct_field(row, "locus")
        .and_then(|l| struct_field(l, "position"))
        .and_then(as_pos)
        .or_else(|| parts.get(1).and_then(|s| s.parse::<i32>().ok()))
        .ok_or_else(|| {
            PostgresError::InvalidRow(format!(
                "cannot determine position for variant_id '{variant_id}'"
            ))
        })?;

    // Prefer the structured `alleles` array; fall back to the variant_id split.
    let (ref_allele, alt_allele) = match struct_field(row, "alleles") {
        Some(EncodedValue::Array(alleles)) if alleles.len() >= 2 => {
            (as_string(&alleles[0]), as_string(&alleles[1]))
        }
        _ => (
            parts.get(2).map(|s| s.to_string()),
            parts.get(3).map(|s| s.to_string()),
        ),
    };

    Ok(VariantColumns {
        contig,
        pos,
        variant_id,
        ref_allele,
        alt_allele,
    })
}

// ---------------------------------------------------------------------------
// COPY text-format encoding
// ---------------------------------------------------------------------------

/// Escape one field for the Postgres `COPY … FORMAT text` representation.
///
/// The text format is tab-delimited, newline-terminated, with backslash escapes.
/// We must escape backslash and the structural characters (tab, newline, CR) or a
/// `data` JSONB value containing a backslash would corrupt the stream. Compact
/// `serde_json` output contains no literal tab/newline, but escaping them anyway
/// makes the encoder correct for any input.
fn copy_escape(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    for ch in field.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

/// Serialize a decoded row to the JSON value stored in the `data` JSONB column.
/// Thin public wrapper over the crate-internal JSON encoder so callers/tests can
/// compute the expected payload.
pub fn row_to_json(row: &EncodedValue) -> serde_json::Value {
    to_json_value(row)
}

/// Append one `COPY` text row (`contig\tpos\tvariant_id\tref\talt\tdata\n`) to
/// `buf`. A `None` ref/alt is written as the COPY NULL token `\N`.
fn append_copy_row(buf: &mut String, cols: &VariantColumns, data_json: &str) {
    buf.push_str(&copy_escape(&cols.contig));
    buf.push('\t');
    buf.push_str(&cols.pos.to_string());
    buf.push('\t');
    buf.push_str(&copy_escape(&cols.variant_id));
    buf.push('\t');
    match &cols.ref_allele {
        Some(r) => buf.push_str(&copy_escape(r)),
        None => buf.push_str("\\N"),
    }
    buf.push('\t');
    match &cols.alt_allele {
        Some(a) => buf.push_str(&copy_escape(a)),
        None => buf.push_str("\\N"),
    }
    buf.push('\t');
    buf.push_str(&copy_escape(data_json));
    buf.push('\n');
}

// ---------------------------------------------------------------------------
// Client + streaming COPY inserter (postgres feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod client {
    use super::*;
    use tokio::runtime::Runtime;

    /// Name of the per-connection `TEMP` staging table used for `COPY`+upsert.
    const STAGING_TABLE: &str = "genohype_pg_copy_staging";

    /// A blocking Postgres client over an async `sqlx` connection.
    ///
    /// The genohype decode path is synchronous (`fn main` is not async; the cloud
    /// IO layer drives its own runtime — see `core/src/io/adapter.rs`), so this
    /// client owns a dedicated single-threaded Tokio runtime and `block_on`s each
    /// `sqlx` call, mirroring how the ClickHouse/ES loaders use a blocking HTTP
    /// client. A single held connection lets the `TEMP` staging table persist
    /// across batches.
    pub struct PostgresClient {
        runtime: Runtime,
        conn: PgConnection,
    }

    impl PostgresClient {
        /// Connect to Postgres (`postgres://user:pass@host:port/db`).
        pub fn connect(url: &str) -> Result<Self> {
            let runtime = Runtime::new()
                .map_err(|e| PostgresError::Load(format!("failed to create runtime: {e}")))?;
            let conn = runtime.block_on(async { PgConnection::connect(url).await })?;
            Ok(Self { runtime, conn })
        }

        /// Execute a statement, returning the number of affected rows.
        pub fn execute(&mut self, sql: &str) -> Result<u64> {
            let runtime = &self.runtime;
            let conn = &mut self.conn;
            runtime.block_on(async move {
                Ok(sqlx::query(sql).execute(&mut *conn).await?.rows_affected())
            })
        }

        /// Create the partitioned wide table if it does not yet exist.
        pub fn create_table(&mut self, table: &str) -> Result<()> {
            self.execute(&generate_create_table(table))?;
            Ok(())
        }

        /// Drop the table (and all its partitions) — used by `--recreate`.
        pub fn drop_table(&mut self, table: &str) -> Result<()> {
            self.execute(&format!("DROP TABLE IF EXISTS {} CASCADE", quote_ident(table)))?;
            Ok(())
        }

        /// Create the secondary `(contig,pos)` and `variant_id` indexes.
        pub fn create_indexes(&mut self, table: &str) -> Result<()> {
            for stmt in generate_indexes(table) {
                self.execute(&stmt)?;
            }
            Ok(())
        }

        /// Total row count of the (partitioned) table.
        pub fn count_rows(&mut self, table: &str) -> Result<i64> {
            let runtime = &self.runtime;
            let conn = &mut self.conn;
            let sql = format!("SELECT count(*) FROM {}", quote_ident(table));
            runtime.block_on(async move {
                Ok(sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(&mut *conn)
                    .await?)
            })
        }

        /// Fetch a single row's `pos` and `data` (as JSON text) by key. Used by
        /// the round-trip test to assert the hoisted column and JSONB payload
        /// round-trip; `data::text` avoids needing the sqlx `json` feature.
        pub fn fetch_pos_and_data(
            &mut self,
            table: &str,
            contig: &str,
            variant_id: &str,
        ) -> Result<(i32, String)> {
            let sql = format!(
                "SELECT pos, data::text FROM {} WHERE contig = $1 AND variant_id = $2",
                quote_ident(table)
            );
            let runtime = &self.runtime;
            let conn = &mut self.conn;
            runtime.block_on(async move {
                let row: (i32, Option<String>) = sqlx::query_as(&sql)
                    .bind(contig)
                    .bind(variant_id)
                    .fetch_one(&mut *conn)
                    .await?;
                Ok((row.0, row.1.unwrap_or_default()))
            })
        }

        /// Create the `TEMP` staging table (matching the wide-table columns, no
        /// constraints, `UNLOGGED` semantics via `TEMP`). Dropped automatically
        /// when the connection closes.
        fn ensure_staging(&mut self) -> Result<()> {
            let sql = format!(
                "CREATE TEMP TABLE IF NOT EXISTS {staging} (\
                 contig TEXT, pos INTEGER, variant_id TEXT, \"ref\" TEXT, \"alt\" TEXT, data JSONB) \
                 ON COMMIT PRESERVE ROWS",
                staging = STAGING_TABLE
            );
            self.execute(&sql)?;
            Ok(())
        }

        /// `COPY` a batch body into staging, upsert into the partitioned target,
        /// then truncate staging. Partitions for `contigs` are created first.
        fn flush_batch(&mut self, table: &str, body: &str, contigs: &[String]) -> Result<()> {
            // Lazily create a partition per new contig (the partitioned target
            // rejects a row with no matching partition).
            for contig in contigs {
                self.execute(&generate_create_partition(table, contig))?;
            }

            let copy_sql = format!(
                "COPY {staging} (contig, pos, variant_id, \"ref\", \"alt\", data) FROM STDIN WITH (FORMAT text)",
                staging = STAGING_TABLE
            );
            let upsert_sql = format!(
                "INSERT INTO {target} (contig, pos, variant_id, \"ref\", \"alt\", data) \
                 SELECT contig, pos, variant_id, \"ref\", \"alt\", data FROM {staging} \
                 ON CONFLICT (contig, variant_id) DO UPDATE SET \
                 pos = excluded.pos, \"ref\" = excluded.\"ref\", \"alt\" = excluded.\"alt\", data = excluded.data",
                target = quote_ident(table),
                staging = STAGING_TABLE
            );
            let truncate_sql = format!("TRUNCATE {staging}", staging = STAGING_TABLE);

            let runtime = &self.runtime;
            let conn = &mut self.conn;
            let body = body.to_string();
            runtime.block_on(async move {
                let mut copy = conn.copy_in_raw(&copy_sql).await?;
                copy.send(body.as_bytes()).await?;
                copy.finish().await?;
                sqlx::query(&upsert_sql).execute(&mut *conn).await?;
                sqlx::query(&truncate_sql).execute(&mut *conn).await?;
                Ok::<(), PostgresError>(())
            })?;
            Ok(())
        }
    }

    /// Buffers rows as `COPY` text and flushes in batches, mirroring the ES
    /// `BulkInserter`. Each flush stages the batch and upserts it (idempotent).
    pub struct CopyInserter<'a> {
        client: &'a mut PostgresClient,
        table: String,
        batch_size: usize,
        buffer: String,
        buffered_rows: usize,
        /// Distinct contigs in the current buffer (partitions to ensure on flush).
        batch_contigs: HashSet<String>,
        /// Contigs whose partition has already been created (across all batches).
        created_partitions: HashSet<String>,
        /// Total rows successfully upserted.
        pub total_rows: usize,
        /// Number of `COPY`+upsert batches flushed.
        pub flush_count: usize,
        /// Accumulated time spent in flush (COPY + upsert) (ms).
        pub insert_time_ms: u64,
    }

    impl<'a> CopyInserter<'a> {
        /// Create an inserter, ensuring the staging table exists.
        pub fn new(
            client: &'a mut PostgresClient,
            table: &str,
            batch_size: usize,
        ) -> Result<Self> {
            client.ensure_staging()?;
            Ok(Self {
                client,
                table: table.to_string(),
                batch_size: batch_size.max(1),
                buffer: String::new(),
                buffered_rows: 0,
                batch_contigs: HashSet::new(),
                created_partitions: HashSet::new(),
                total_rows: 0,
                flush_count: 0,
                insert_time_ms: 0,
            })
        }

        /// Buffer one decoded row. Auto-flushes at the batch size.
        pub fn add(&mut self, row: &EncodedValue) -> Result<()> {
            let cols = extract_columns(row)?;
            let data_json = to_json_value(row).to_string();
            self.batch_contigs.insert(cols.contig.clone());
            append_copy_row(&mut self.buffer, &cols, &data_json);
            self.buffered_rows += 1;
            if self.buffered_rows >= self.batch_size {
                self.flush()?;
            }
            Ok(())
        }

        /// Flush buffered rows (COPY into staging → upsert into target).
        pub fn flush(&mut self) -> Result<()> {
            if self.buffered_rows == 0 {
                return Ok(());
            }
            // Only pass not-yet-created contigs to the flush (DDL is idempotent,
            // but this avoids redundant round-trips for the common single-contig
            // export).
            let new_contigs: Vec<String> = self
                .batch_contigs
                .iter()
                .filter(|c| !self.created_partitions.contains(*c))
                .cloned()
                .collect();

            let body = std::mem::take(&mut self.buffer);
            let rows = self.buffered_rows;

            let start = std::time::Instant::now();
            self.client.flush_batch(&self.table, &body, &new_contigs)?;
            self.insert_time_ms += start.elapsed().as_millis() as u64;

            for c in new_contigs {
                self.created_partitions.insert(c);
            }
            self.batch_contigs.clear();
            self.total_rows += rows;
            self.flush_count += 1;
            self.buffered_rows = 0;
            Ok(())
        }

        /// Flush any remaining buffered rows.
        pub fn finish(&mut self) -> Result<()> {
            self.flush()
        }
    }
}

#[cfg(feature = "postgres")]
pub use client::{CopyInserter, PostgresClient};

#[cfg(test)]
mod tests {
    use super::*;

    fn locus(contig: &str, pos: i32) -> EncodedValue {
        EncodedValue::Struct(vec![
            ("contig".to_string(), EncodedValue::Binary(contig.as_bytes().to_vec())),
            ("position".to_string(), EncodedValue::Int32(pos)),
        ])
    }

    fn variant_row(contig: &str, pos: i32, vid: &str, r: &str, a: &str) -> EncodedValue {
        EncodedValue::Struct(vec![
            ("locus".to_string(), locus(contig, pos)),
            ("variant_id".to_string(), EncodedValue::Binary(vid.as_bytes().to_vec())),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary(r.as_bytes().to_vec()),
                    EncodedValue::Binary(a.as_bytes().to_vec()),
                ]),
            ),
        ])
    }

    #[test]
    fn test_create_table_is_partitioned_jsonb() {
        let ddl = generate_create_table("variants");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"variants\""));
        assert!(ddl.contains("contig TEXT NOT NULL"));
        assert!(ddl.contains("pos INTEGER NOT NULL"));
        assert!(ddl.contains("data JSONB"));
        assert!(ddl.contains("PRIMARY KEY (contig, variant_id)"));
        assert!(ddl.contains("PARTITION BY LIST (contig)"));
    }

    #[test]
    fn test_create_partition_sanitizes_name() {
        let ddl = generate_create_partition("variants", "chr22");
        assert!(ddl.contains("\"variants_chr22\" PARTITION OF \"variants\""));
        assert!(ddl.contains("FOR VALUES IN ('chr22')"));
    }

    #[test]
    fn test_indexes_cover_region_and_point_lookups() {
        let idx = generate_indexes("variants");
        assert!(idx.iter().any(|s| s.contains("(contig, pos)")));
        assert!(idx.iter().any(|s| s.contains("(variant_id)")));
    }

    #[test]
    fn test_extract_columns_from_structured_fields() {
        let row = variant_row("chr22", 16050075, "22-16050075-A-G", "A", "G");
        let cols = extract_columns(&row).unwrap();
        assert_eq!(cols.contig, "chr22");
        assert_eq!(cols.pos, 16050075);
        assert_eq!(cols.variant_id, "22-16050075-A-G");
        assert_eq!(cols.ref_allele.as_deref(), Some("A"));
        assert_eq!(cols.alt_allele.as_deref(), Some("G"));
    }

    #[test]
    fn test_extract_columns_falls_back_to_variant_id() {
        // browser-minimal style: no locus / no alleles, only variant_id present.
        let row = EncodedValue::Struct(vec![(
            "variant_id".to_string(),
            EncodedValue::Binary(b"1-12345-AC-T".to_vec()),
        )]);
        let cols = extract_columns(&row).unwrap();
        assert_eq!(cols.contig, "1");
        assert_eq!(cols.pos, 12345);
        assert_eq!(cols.ref_allele.as_deref(), Some("AC"));
        assert_eq!(cols.alt_allele.as_deref(), Some("T"));
    }

    #[test]
    fn test_extract_columns_missing_variant_id_errors() {
        let row = EncodedValue::Struct(vec![("locus".to_string(), locus("chr1", 1))]);
        assert!(extract_columns(&row).is_err());
    }

    #[test]
    fn test_copy_escape_handles_backslash_and_delimiters() {
        // A JSON value containing an escaped quote carries a literal backslash.
        assert_eq!(copy_escape(r#"{"a":"x\"y"}"#), r#"{"a":"x\\"y"}"#);
        assert_eq!(copy_escape("a\tb\nc"), "a\\tb\\nc");
        assert_eq!(copy_escape("plain"), "plain");
    }

    #[test]
    fn test_append_copy_row_layout_and_nulls() {
        let cols = VariantColumns {
            contig: "chr22".to_string(),
            pos: 100,
            variant_id: "22-100-A-G".to_string(),
            ref_allele: Some("A".to_string()),
            alt_allele: None,
        };
        let mut buf = String::new();
        append_copy_row(&mut buf, &cols, "{\"x\":1}");
        assert_eq!(buf, "chr22\t100\t22-100-A-G\tA\t\\N\t{\"x\":1}\n");
    }
}
