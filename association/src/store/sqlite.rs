use super::{
    AssociationBuildV1, AssociationRegionScanRequestV1, AssociationRegionScanSummaryV1,
    AssociationStore, AssociationStoreError, MAX_INSERT_BATCH_SIZE,
};
use crate::layout::canonical_contig;
use crate::model::AssociationPointV1;
use crate::page::{
    AssociationPageV1, AssociationRegionRequestV1, AssociationSortDirectionV1,
    AssociationTablePageRequestV1, AssociationTableSortV1, ASSOCIATION_PAGE_SCHEMA_VERSION_V1,
    MAX_PAGE_SIZE,
};
use crate::reduction::{compare_association_points, MAX_TOP_K};
use crate::session::{DatasetIdV1, STORE_SCHEMA_VERSION_V1};
use crate::JSON_SAFE_INTEGER_MAX;
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OpenFlags, OptionalExtension, Row};
use std::path::Path;

const MIGRATION_V1: &str = include_str!("migrations/v1.sql");
const POINT_COLUMNS: &str = "source_ordinal, contig, position, ref, alt, pvalue, neg_log10_p, beta, se, af, ac, gene, consequence, series";
const JOINED_POINT_COLUMNS: &str = "p.source_ordinal, p.contig, p.position, p.ref, p.alt, p.pvalue, p.neg_log10_p, p.beta, p.se, p.af, p.ac, p.gene, p.consequence, p.series";

pub struct SqliteAssociationStore {
    connection: Connection,
    read_only: bool,
    // Only an empty, unpublished store may hold this one-shot outer transaction.
    bulk_build: bool,
}

impl SqliteAssociationStore {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, AssociationStoreError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(backend)?;
        configure(&connection)?;
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .map_err(backend)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(backend)?;
        let version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(backend)?;
        if version == 0 {
            connection.execute_batch(MIGRATION_V1).map_err(backend)?;
        }
        validate_schema(&connection)?;
        Ok(Self {
            connection,
            read_only: false,
            bulk_build: false,
        })
    }

    /// Create an empty, exclusively owned staging store for one atomic bulk build.
    ///
    /// Batches remain bounded, validated and individually atomic (savepoints), but
    /// are not durably committed until `complete_build`. FULL synchronization and
    /// all V1 indexes/constraints are retained. SQLite spills dirty pages to disk;
    /// its page-cache target is 8 MiB rather than retaining the dataset in RAM.
    /// Drop, abort, or process failure rolls back the unpublished transaction.
    /// Never use this constructor on a published/shared generation.
    pub fn create_for_bulk_build(path: impl AsRef<Path>) -> Result<Self, AssociationStoreError> {
        let mut store = Self::create(path)?;
        store
            .connection
            .pragma_update(None, "cache_size", -8192)
            .map_err(backend)?;
        store
            .connection
            .pragma_update(None, "temp_store", "FILE")
            .map_err(backend)?;
        store
            .connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(backend)?;
        let datasets: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM association_datasets", [], |row| {
                row.get(0)
            })
            .map_err(backend)?;
        if datasets != 0 {
            // Dropping the connection rolls back without touching existing data.
            return Err(AssociationStoreError::InvalidBuildState);
        }
        store.bulk_build = true;
        Ok(store)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, AssociationStoreError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(backend)?;
        connection
            .pragma_update(None, "query_only", true)
            .map_err(backend)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(backend)?;
        validate_schema(&connection)?;
        Ok(Self {
            connection,
            read_only: true,
            bulk_build: false,
        })
    }

    fn writable(&self) -> Result<(), AssociationStoreError> {
        if self.read_only {
            Err(AssociationStoreError::ReadOnly)
        } else {
            Ok(())
        }
    }
    fn state(&self, id: &DatasetIdV1) -> Result<Option<String>, AssociationStoreError> {
        self.connection
            .query_row(
                "SELECT status FROM association_datasets WHERE dataset_id=?1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)
    }
    fn require_building(&self, id: &DatasetIdV1) -> Result<(), AssociationStoreError> {
        match self.state(id)? {
            Some(state) if state == "building" => Ok(()),
            Some(_) => Err(AssociationStoreError::InvalidBuildState),
            None => Err(AssociationStoreError::DatasetNotFound),
        }
    }
    fn require_complete(&self, id: &DatasetIdV1) -> Result<(), AssociationStoreError> {
        if self.bulk_build && !self.connection.is_autocommit() {
            return Err(AssociationStoreError::InvalidBuildState);
        }
        match self.state(id)? {
            Some(state) if state == "complete" => Ok(()),
            Some(_) => Err(AssociationStoreError::InvalidBuildState),
            None => Err(AssociationStoreError::DatasetNotFound),
        }
    }
}

impl AssociationStore for SqliteAssociationStore {
    fn begin_build(&mut self, build: &AssociationBuildV1) -> Result<(), AssociationStoreError> {
        self.writable()?;
        if self.bulk_build {
            let empty: bool = self
                .connection
                .query_row(
                    "SELECT NOT EXISTS(SELECT 1 FROM association_datasets)",
                    [],
                    |row| row.get(0),
                )
                .map_err(backend)?;
            if self.connection.is_autocommit() || !empty {
                return Err(AssociationStoreError::InvalidBuildState);
            }
        }
        if build.created_at.is_empty() || build.created_at.len() > 64 {
            return Err(AssociationStoreError::InvalidRequest(
                "build metadata cannot be empty".into(),
            ));
        }
        self.connection.execute("INSERT INTO association_datasets(dataset_id,source_fingerprint,created_at,status) VALUES(?1,NULL,?2,'building')", params![build.dataset_id.as_str(),build.created_at]).map_err(backend)?;
        Ok(())
    }

    fn insert_points(
        &mut self,
        dataset_id: &DatasetIdV1,
        points: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError> {
        self.writable()?;
        self.require_building(dataset_id)?;
        if points.is_empty() || points.len() > MAX_INSERT_BATCH_SIZE {
            return Err(AssociationStoreError::InvalidBatchSize);
        }
        for point in points {
            let (normalized, _) = point
                .clone()
                .normalize()
                .map_err(|e| AssociationStoreError::InvalidPoint(e.to_string()))?;
            if normalized != *point {
                return Err(AssociationStoreError::InvalidPoint(
                    "point is not canonical".into(),
                ));
            }
        }
        // Outermost savepoints commit exactly like the former batch transaction;
        // inside a bulk build they provide batch rollback without a per-batch fsync.
        let transaction = self.connection.savepoint().map_err(backend)?;
        {
            let mut statement=transaction.prepare("INSERT INTO association_points(dataset_id,source_ordinal,contig,contig_rank,position,ref,alt,pvalue,neg_log10_p,beta,se,af,ac,gene,consequence,series) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)").map_err(backend)?;
            for point in points {
                statement
                    .execute(params![
                        dataset_id.as_str(),
                        point.source_ordinal as i64,
                        point.contig,
                        contig_rank(&point.contig),
                        point.position,
                        point.ref_allele,
                        point.alt_allele,
                        point.pvalue,
                        point.neg_log10_p,
                        point.beta,
                        point.standard_error,
                        point.allele_frequency,
                        point.allele_count,
                        point.gene_symbol,
                        point.consequence,
                        point.series
                    ])
                    .map_err(backend)?;
            }
        }
        transaction.commit().map_err(backend)
    }

    fn replace_top_hits(
        &mut self,
        dataset_id: &DatasetIdV1,
        points: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError> {
        self.writable()?;
        self.require_building(dataset_id)?;
        if points.len() > MAX_TOP_K
            || !points
                .windows(2)
                .all(|pair| compare_association_points(&pair[0], &pair[1]).is_le())
        {
            return Err(AssociationStoreError::InvalidRequest(
                "top hits must be bounded and canonically sorted".into(),
            ));
        }
        for point in points {
            let (normalized, _) = point
                .clone()
                .normalize()
                .map_err(|e| AssociationStoreError::InvalidPoint(e.to_string()))?;
            if normalized != *point {
                return Err(AssociationStoreError::InvalidPoint(
                    "top hit is not canonical".into(),
                ));
            }
            let sql = format!(
                "SELECT {POINT_COLUMNS} FROM association_points WHERE dataset_id=?1 AND source_ordinal=?2"
            );
            let stored = self
                .connection
                .query_row(
                    &sql,
                    params![dataset_id.as_str(), point.source_ordinal as i64],
                    read_point,
                )
                .optional()
                .map_err(backend)?;
            if stored.as_ref() != Some(point) {
                return Err(AssociationStoreError::InvalidRequest(
                    "top hits must exactly match inserted points".into(),
                ));
            }
        }
        let transaction = self.connection.savepoint().map_err(backend)?;
        transaction
            .execute(
                "DELETE FROM top_hits WHERE dataset_id=?1",
                [dataset_id.as_str()],
            )
            .map_err(backend)?;
        {
            let mut statement = transaction
                .prepare("INSERT INTO top_hits(dataset_id,rank,source_ordinal) VALUES(?1,?2,?3)")
                .map_err(backend)?;
            for (index, point) in points.iter().enumerate() {
                statement
                    .execute(params![
                        dataset_id.as_str(),
                        (index + 1) as i64,
                        point.source_ordinal as i64
                    ])
                    .map_err(backend)?;
            }
        }
        transaction.commit().map_err(backend)
    }

    fn complete_build(
        &mut self,
        dataset_id: &DatasetIdV1,
        source_fingerprint: &str,
    ) -> Result<(), AssociationStoreError> {
        self.writable()?;
        self.require_building(dataset_id)?;
        if !valid_sha256(source_fingerprint) {
            return Err(AssociationStoreError::InvalidRequest(
                "source fingerprint must be canonical lowercase sha256 text".into(),
            ));
        }
        let changed = self
            .connection
            .execute(
                "UPDATE association_datasets SET source_fingerprint=?2,status='complete' WHERE dataset_id=?1 AND status='building'",
                params![dataset_id.as_str(), source_fingerprint],
            )
            .map_err(backend)?;
        if changed == 1 {
            if self.bulk_build {
                if let Err(error) = self.connection.execute_batch("COMMIT") {
                    // A failed commit must never expose a complete dataset through
                    // this handle. Roll back the entire unpublished generation.
                    let _ = self.connection.execute_batch("ROLLBACK");
                    return Err(backend(error));
                }
            }
            Ok(())
        } else {
            Err(AssociationStoreError::InvalidBuildState)
        }
    }
    fn abort_build(&mut self, dataset_id: &DatasetIdV1) -> Result<(), AssociationStoreError> {
        self.writable()?;
        self.require_building(dataset_id)?;
        if self.bulk_build {
            return self.connection.execute_batch("ROLLBACK").map_err(backend);
        }
        self.connection
            .execute(
                "DELETE FROM association_datasets WHERE dataset_id=?1",
                [dataset_id.as_str()],
            )
            .map_err(backend)?;
        Ok(())
    }
    fn is_complete(&self, dataset_id: &DatasetIdV1) -> Result<bool, AssociationStoreError> {
        Ok((!self.bulk_build || self.connection.is_autocommit())
            && self.state(dataset_id)?.as_deref() == Some("complete"))
    }

    fn top_hits(
        &self,
        dataset_id: &DatasetIdV1,
        limit: u32,
    ) -> Result<Vec<AssociationPointV1>, AssociationStoreError> {
        self.require_complete(dataset_id)?;
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(AssociationStoreError::InvalidRequest(
                "top-hit limit is outside 1..=1000".into(),
            ));
        }
        let sql=format!("SELECT {} FROM top_hits h JOIN association_points p ON p.dataset_id=h.dataset_id AND p.source_ordinal=h.source_ordinal WHERE h.dataset_id=?1 ORDER BY h.rank LIMIT ?2",JOINED_POINT_COLUMNS);
        query_points(
            &self.connection,
            &sql,
            vec![
                Value::Text(dataset_id.as_str().into()),
                Value::Integer(i64::from(limit)),
            ],
        )
    }

    fn table_page(
        &self,
        request: &AssociationTablePageRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        request
            .validate()
            .map_err(|e| AssociationStoreError::InvalidRequest(e.to_string()))?;
        self.require_complete(&request.dataset_id)?;
        let escaped = request
            .search
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(escape_like);
        let filter = if escaped.is_some() {
            " AND (contig LIKE ?2 ESCAPE '\\' COLLATE NOCASE OR CAST(position AS TEXT) LIKE ?2 ESCAPE '\\' OR ref LIKE ?2 ESCAPE '\\' COLLATE NOCASE OR alt LIKE ?2 ESCAPE '\\' COLLATE NOCASE OR gene LIKE ?2 ESCAPE '\\' COLLATE NOCASE OR consequence LIKE ?2 ESCAPE '\\' COLLATE NOCASE OR series LIKE ?2 ESCAPE '\\' COLLATE NOCASE)"
        } else {
            ""
        };
        let count_sql =
            format!("SELECT COUNT(*) FROM association_points WHERE dataset_id=?1{filter}");
        let total: i64 = if let Some(search) = &escaped {
            self.connection
                .query_row(
                    &count_sql,
                    params![request.dataset_id.as_str(), format!("%{search}%")],
                    |row| row.get(0),
                )
                .map_err(backend)?
        } else {
            self.connection
                .query_row(&count_sql, [request.dataset_id.as_str()], |row| row.get(0))
                .map_err(backend)?
        };
        let order = table_order(request.sort, request.direction);
        let sql=format!("SELECT {POINT_COLUMNS} FROM association_points WHERE dataset_id=?1{filter} ORDER BY {order} LIMIT ?{} OFFSET ?{}",if escaped.is_some(){3}else{2},if escaped.is_some(){4}else{3});
        let mut values = vec![Value::Text(request.dataset_id.as_str().into())];
        if let Some(search) = escaped {
            values.push(Value::Text(format!("%{search}%")));
        }
        values.push(Value::Integer(i64::from(request.limit)));
        values.push(Value::Integer(request.offset as i64));
        let points = query_points(&self.connection, &sql, values)?;
        let page = AssociationPageV1 {
            schema_version: ASSOCIATION_PAGE_SCHEMA_VERSION_V1.into(),
            dataset_id: request.dataset_id.clone(),
            points,
            offset: request.offset,
            limit: request.limit,
            total: total as u64,
            sort: Some(request.sort),
            direction: Some(request.direction),
            search: request.search.clone(),
        };
        page.validate()
            .map_err(|e| AssociationStoreError::Backend(e.to_string()))?;
        Ok(page)
    }

    fn region_page(
        &self,
        request: &AssociationRegionRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError> {
        let contig = request
            .validate()
            .map_err(|e| AssociationStoreError::InvalidRequest(e.to_string()))?;
        self.require_complete(&request.dataset_id)?;
        let total:i64=self.connection.query_row("SELECT COUNT(*) FROM association_points WHERE dataset_id=?1 AND contig=?2 AND position BETWEEN ?3 AND ?4",params![request.dataset_id.as_str(),contig,request.start,request.end],|row|row.get(0)).map_err(backend)?;
        let sql=format!("SELECT {POINT_COLUMNS} FROM association_points WHERE dataset_id=?1 AND contig=?2 AND position BETWEEN ?3 AND ?4 ORDER BY position, ref IS NOT NULL, ref, alt IS NOT NULL, alt, source_ordinal LIMIT ?5 OFFSET ?6");
        let points = query_points(
            &self.connection,
            &sql,
            vec![
                Value::Text(request.dataset_id.as_str().into()),
                Value::Text(contig),
                Value::Integer(request.start),
                Value::Integer(request.end),
                Value::Integer(i64::from(request.limit)),
                Value::Integer(request.offset as i64),
            ],
        )?;
        let page = AssociationPageV1 {
            schema_version: ASSOCIATION_PAGE_SCHEMA_VERSION_V1.into(),
            dataset_id: request.dataset_id.clone(),
            points,
            offset: request.offset,
            limit: request.limit,
            total: total as u64,
            sort: None,
            direction: None,
            search: None,
        };
        page.validate()
            .map_err(|e| AssociationStoreError::Backend(e.to_string()))?;
        Ok(page)
    }

    fn scan_region(
        &self,
        request: &AssociationRegionScanRequestV1,
        visitor: &mut dyn FnMut(&AssociationPointV1) -> Result<(), AssociationStoreError>,
    ) -> Result<AssociationRegionScanSummaryV1, AssociationStoreError> {
        let contig = request.validate()?;
        self.require_complete(&request.dataset_id)?;
        let sql = format!(
            "SELECT {POINT_COLUMNS} FROM association_points \
             WHERE dataset_id=?1 AND contig=?2 AND position BETWEEN ?3 AND ?4 \
             ORDER BY position, ref IS NOT NULL, ref, alt IS NOT NULL, alt, source_ordinal"
        );
        let mut statement = self.connection.prepare(&sql).map_err(backend)?;
        let mut rows = statement
            .query(params![
                request.dataset_id.as_str(),
                contig,
                request.start,
                request.end
            ])
            .map_err(backend)?;
        let mut point_count = 0_u64;
        while let Some(row) = rows.next().map_err(backend)? {
            let point = read_point(row).map_err(backend)?;
            let (normalized, _) = point
                .clone()
                .normalize()
                .map_err(|error| AssociationStoreError::InvalidPoint(error.to_string()))?;
            if normalized != point {
                return Err(AssociationStoreError::InvalidPoint(
                    "stored point is not canonical".into(),
                ));
            }
            point_count = point_count
                .checked_add(1)
                .filter(|count| *count <= JSON_SAFE_INTEGER_MAX)
                .ok_or_else(|| {
                    AssociationStoreError::Backend(
                        "region point count exceeds the supported integer range".into(),
                    )
                })?;
            visitor(&point)?;
        }
        Ok(AssociationRegionScanSummaryV1 {
            dataset_id: request.dataset_id.clone(),
            contig,
            start: request.start,
            end: request.end,
            point_count,
        })
    }
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn configure(connection: &Connection) -> Result<(), AssociationStoreError> {
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(backend)?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(backend)
}
fn validate_schema(connection: &Connection) -> Result<(), AssociationStoreError> {
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(backend)?;
    if version != STORE_SCHEMA_VERSION_V1 {
        return Err(AssociationStoreError::Schema(format!(
            "expected user_version {STORE_SCHEMA_VERSION_V1}, got {version}"
        )));
    }
    for (table, expected_columns) in [
        (
            "association_datasets",
            &["dataset_id", "source_fingerprint", "created_at", "status"][..],
        ),
        (
            "association_points",
            &[
                "dataset_id",
                "source_ordinal",
                "contig",
                "contig_rank",
                "position",
                "ref",
                "alt",
                "pvalue",
                "neg_log10_p",
                "beta",
                "se",
                "af",
                "ac",
                "gene",
                "consequence",
                "series",
            ][..],
        ),
        ("top_hits", &["dataset_id", "rank", "source_ordinal"][..]),
        (
            "loci",
            &[
                "dataset_id",
                "locus_id",
                "contig",
                "start",
                "stop",
                "lead_source_ordinal",
                "lead_pvalue",
            ][..],
        ),
    ] {
        let sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        if sql.as_ref().map_or(true, |sql| !sql.ends_with("STRICT")) {
            return Err(AssociationStoreError::Schema(format!(
                "missing or non-STRICT required table {table}"
            )));
        }
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(backend)?;
        let actual = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?;
        if actual != expected_columns {
            return Err(AssociationStoreError::Schema(format!(
                "table {table} has unexpected columns"
            )));
        }
    }
    for (index, expected) in [
        (
            "association_region_idx",
            vec!["dataset_id", "contig", "position"],
        ),
        ("association_pvalue_idx", vec!["dataset_id", "pvalue"]),
    ] {
        let found: Option<String> = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?1",
                [index],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        if found.is_none() {
            return Err(AssociationStoreError::Schema(format!(
                "missing required index {index}"
            )));
        }
        let mut statement = connection
            .prepare(&format!("PRAGMA index_info({index})"))
            .map_err(backend)?;
        let actual = statement
            .query_map([], |row| row.get::<_, String>(2))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?;
        if actual != expected {
            return Err(AssociationStoreError::Schema(format!(
                "index {index} has unexpected columns"
            )));
        }
    }

    let expected = Connection::open_in_memory().map_err(backend)?;
    configure(&expected)?;
    expected.execute_batch(MIGRATION_V1).map_err(backend)?;
    if schema_contract(connection)? != schema_contract(&expected)? {
        return Err(AssociationStoreError::Schema(
            "database does not match the exact V1 table/index contract".into(),
        ));
    }
    Ok(())
}

fn schema_contract(
    connection: &Connection,
) -> Result<Vec<(String, String, String)>, AssociationStoreError> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, sql FROM sqlite_master
             WHERE name IN (
               'association_datasets', 'association_points', 'top_hits', 'loci',
               'association_region_idx', 'association_pvalue_idx'
             )
             ORDER BY type, name",
        )
        .map_err(backend)?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(backend)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(backend)?;
    Ok(rows)
}
fn contig_rank(contig: &str) -> i64 {
    let canonical = canonical_contig(contig).unwrap_or_default();
    canonical
        .parse::<i64>()
        .ok()
        .filter(|rank| (1..=22).contains(rank))
        .unwrap_or_else(|| match canonical.as_str() {
            "X" => 23,
            "Y" => 24,
            "MT" => 25,
            _ => 65535,
        })
}
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
fn table_order(
    sort: AssociationTableSortV1,
    direction: AssociationSortDirectionV1,
) -> &'static str {
    match (sort, direction) {
        (AssociationTableSortV1::Pvalue, AssociationSortDirectionV1::Ascending) => "pvalue ASC, neg_log10_p DESC, contig_rank ASC, contig ASC, position ASC, ref IS NOT NULL ASC, ref ASC, alt IS NOT NULL ASC, alt ASC, source_ordinal ASC",
        (AssociationTableSortV1::Pvalue, AssociationSortDirectionV1::Descending) => "pvalue DESC, neg_log10_p ASC, contig_rank DESC, contig DESC, position DESC, ref IS NOT NULL DESC, ref DESC, alt IS NOT NULL DESC, alt DESC, source_ordinal DESC",
        (AssociationTableSortV1::Locus, AssociationSortDirectionV1::Ascending) => "contig_rank ASC, contig ASC, position ASC, ref IS NOT NULL ASC, ref ASC, alt IS NOT NULL ASC, alt ASC, source_ordinal ASC",
        (AssociationTableSortV1::Locus, AssociationSortDirectionV1::Descending) => "contig_rank DESC, contig DESC, position DESC, ref IS NOT NULL DESC, ref DESC, alt IS NOT NULL DESC, alt DESC, source_ordinal DESC",
    }
}
fn query_points(
    connection: &Connection,
    sql: &str,
    values: Vec<Value>,
) -> Result<Vec<AssociationPointV1>, AssociationStoreError> {
    let mut statement = connection.prepare(sql).map_err(backend)?;
    let mut rows = statement.query(params_from_iter(values)).map_err(backend)?;
    let mut points = Vec::new();
    while let Some(row) = rows.next().map_err(backend)? {
        points.push(read_point(row).map_err(backend)?);
    }
    Ok(points)
}
fn read_point(row: &Row<'_>) -> rusqlite::Result<AssociationPointV1> {
    Ok(AssociationPointV1 {
        source_ordinal: row.get::<_, i64>(0)? as u64,
        contig: row.get(1)?,
        position: row.get(2)?,
        ref_allele: row.get(3)?,
        alt_allele: row.get(4)?,
        pvalue: row.get(5)?,
        neg_log10_p: row.get(6)?,
        beta: row.get(7)?,
        standard_error: row.get(8)?,
        allele_frequency: row.get(9)?,
        allele_count: row.get(10)?,
        gene_symbol: row.get(11)?,
        consequence: row.get(12)?,
        series: row.get(13)?,
    })
}
fn backend(error: rusqlite::Error) -> AssociationStoreError {
    AssociationStoreError::Backend(error.to_string())
}
