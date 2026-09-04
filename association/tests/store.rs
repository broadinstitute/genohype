use genohype_association::{
    AssociationBuildV1, AssociationPointV1, AssociationRegionRequestV1,
    AssociationRegionScanRequestV1, AssociationSortDirectionV1, AssociationStore,
    AssociationStoreError, AssociationTablePageRequestV1, AssociationTableSortV1, DatasetIdV1,
    SqliteAssociationStore, JSON_SAFE_INTEGER_MAX,
};
use rusqlite::Connection;
use tempfile::TempDir;

fn id() -> DatasetIdV1 {
    DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap()
}

fn fingerprint() -> String {
    format!("sha256:{}", "a".repeat(64))
}

fn point(
    ordinal: u64,
    contig: &str,
    position: i64,
    pvalue: f64,
    significance: f64,
) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: contig.into(),
        position,
        ref_allele: Some("A".into()),
        alt_allele: Some("C".into()),
        pvalue,
        neg_log10_p: significance,
        beta: Some(0.1),
        standard_error: Some(0.01),
        allele_frequency: Some(0.2),
        allele_count: Some(2.0),
        gene_symbol: None,
        consequence: None,
        series: None,
        source_ordinal: ordinal,
    }
}

fn request(
    sort: AssociationTableSortV1,
    direction: AssociationSortDirectionV1,
) -> AssociationTablePageRequestV1 {
    AssociationTablePageRequestV1 {
        dataset_id: id(),
        offset: 0,
        limit: 100,
        sort,
        direction,
        search: None,
    }
}

#[test]
fn migration_has_required_version_tables_indexes_and_constraints() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let _store = SqliteAssociationStore::create(&path).unwrap();
    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
            .unwrap(),
        1
    );
    for name in ["association_points", "top_hits", "loci"] {
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [name],
                    |row| row.get::<_, u32>(0)
                )
                .unwrap(),
            1
        );
    }
    for (name, columns) in [
        (
            "association_region_idx",
            vec!["dataset_id", "contig", "position"],
        ),
        ("association_pvalue_idx", vec!["dataset_id", "pvalue"]),
    ] {
        let mut statement = connection
            .prepare(&format!("PRAGMA index_info({name})"))
            .unwrap();
        let actual = statement
            .query_map([], |row| row.get::<_, String>(2))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(actual, columns);
    }

    connection.execute("PRAGMA foreign_keys=ON", []).unwrap();
    connection
        .execute(
            "INSERT INTO association_datasets VALUES('123e4567-e89b-42d3-a456-426614174000',NULL,'now','building')",
            [],
        )
        .unwrap();
    for sql in [
        "INSERT INTO association_points(dataset_id,source_ordinal,contig,contig_rank,position,pvalue,neg_log10_p) VALUES('123e4567-e89b-42d3-a456-426614174000',1,'1',1,0,0.5,1)",
        "INSERT INTO association_points(dataset_id,source_ordinal,contig,contig_rank,position,pvalue,neg_log10_p) VALUES('123e4567-e89b-42d3-a456-426614174000',2,'1',1,1,1.1,1)",
        "INSERT INTO association_points(dataset_id,source_ordinal,contig,contig_rank,position,pvalue,neg_log10_p,af) VALUES('123e4567-e89b-42d3-a456-426614174000',3,'1',1,1,0.5,1,1.1)",
        "INSERT INTO association_points(dataset_id,source_ordinal,contig,contig_rank,position,pvalue,neg_log10_p,ac) VALUES('123e4567-e89b-42d3-a456-426614174000',4,'1',1,1,0.5,1,-1)",
    ] {
        assert!(connection.execute(sql, []).is_err(), "constraint accepted {sql}");
    }
    drop(connection);
    drop(_store);
    drop(SqliteAssociationStore::create(&path).unwrap());
}

#[test]
fn read_only_reopen_rejects_lookalike_v1_schemas() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    drop(SqliteAssociationStore::create(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP INDEX association_pvalue_idx;
             CREATE INDEX association_pvalue_idx
               ON association_points(dataset_id, pvalue)
               WHERE pvalue < 0.5;",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteAssociationStore::open_read_only(&path),
        Err(AssociationStoreError::Schema(_))
    ));
}

#[test]
fn read_only_reopen_rejects_unknown_schema_versions() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    drop(SqliteAssociationStore::create(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("PRAGMA user_version=2").unwrap();
    drop(connection);
    assert!(matches!(
        SqliteAssociationStore::open_read_only(&path),
        Err(AssociationStoreError::Schema(_))
    ));
}

#[test]
fn lifecycle_batches_top_hits_pages_search_regions_and_read_only_reopen() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let mut store = SqliteAssociationStore::create(&path).unwrap();
    let dataset_id = id();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: dataset_id.clone(),
            created_at: "2026-09-04T00:00:00Z".into(),
        })
        .unwrap();
    assert!(matches!(
        store.table_page(&request(
            AssociationTableSortV1::Pvalue,
            AssociationSortDirectionV1::Ascending
        )),
        Err(AssociationStoreError::InvalidBuildState)
    ));
    let observer = Connection::open(&path).unwrap();
    let pending: Option<String> = observer
        .query_row(
            "SELECT source_fingerprint FROM association_datasets WHERE dataset_id=?1",
            [dataset_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, None, "fingerprint must remain pending before EOF");
    drop(observer);

    let mut points = vec![
        point(0, "1", 10, 1e-3, 3.0),
        point(1, "2", 20, 0.0, 350.0),
        point(2, "2", 10, 0.0, 400.0),
        point(3, "X", 30, 1e-8, 8.0),
        point(4, "GL1", 1, 1.0, 0.0),
    ];
    points[0].gene_symbol = Some("GENE%LITERAL".into());
    points[3].gene_symbol = Some("GENE_UNDERSCORE".into());
    points[4].gene_symbol = Some(r"GENE\SLASH".into());
    store.insert_points(&dataset_id, &points).unwrap();

    let duplicate_batch = vec![point(99, "1", 99, 0.5, -0.5_f64.log10()), points[0].clone()];
    assert!(store.insert_points(&dataset_id, &duplicate_batch).is_err());

    let mut mismatched = points[2].clone();
    mismatched.gene_symbol = Some("not-persisted".into());
    assert!(matches!(
        store.replace_top_hits(&dataset_id, &[mismatched]),
        Err(AssociationStoreError::InvalidRequest(_))
    ));

    let top = vec![points[2].clone(), points[1].clone(), points[3].clone()];
    store.replace_top_hits(&dataset_id, &top).unwrap();
    assert!(matches!(
        store.complete_build(&dataset_id, "not-a-fingerprint"),
        Err(AssociationStoreError::InvalidRequest(_))
    ));
    assert!(!store.is_complete(&dataset_id).unwrap());
    store.complete_build(&dataset_id, &fingerprint()).unwrap();
    assert!(store.is_complete(&dataset_id).unwrap());
    let observer = Connection::open(&path).unwrap();
    assert_eq!(
        observer
            .query_row(
                "SELECT source_fingerprint FROM association_datasets WHERE dataset_id=?1 AND status='complete'",
                [dataset_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        fingerprint()
    );
    drop(observer);
    assert_eq!(store.top_hits(&dataset_id, 2).unwrap(), top[..2]);

    let p_page = store
        .table_page(&request(
            AssociationTableSortV1::Pvalue,
            AssociationSortDirectionV1::Ascending,
        ))
        .unwrap();
    assert_eq!(p_page.total, 5);
    assert_eq!(
        p_page
            .points
            .iter()
            .map(|point| point.source_ordinal)
            .collect::<Vec<_>>(),
        vec![2, 1, 3, 0, 4]
    );
    assert!(!p_page.points.iter().any(|point| point.source_ordinal == 99));

    let descending = store
        .table_page(&request(
            AssociationTableSortV1::Pvalue,
            AssociationSortDirectionV1::Descending,
        ))
        .unwrap();
    assert_eq!(
        descending
            .points
            .iter()
            .map(|point| point.source_ordinal)
            .collect::<Vec<_>>(),
        vec![4, 0, 3, 1, 2],
        "descending p-value order must reverse every deterministic tie key"
    );

    let mut paged_request = request(
        AssociationTableSortV1::Pvalue,
        AssociationSortDirectionV1::Ascending,
    );
    paged_request.limit = 2;
    let mut paged_ordinals = Vec::new();
    for offset in [0, 2, 4] {
        paged_request.offset = offset;
        paged_ordinals.extend(
            store
                .table_page(&paged_request)
                .unwrap()
                .points
                .into_iter()
                .map(|point| point.source_ordinal),
        );
    }
    assert_eq!(paged_ordinals, vec![2, 1, 3, 0, 4]);

    let locus_page = store
        .table_page(&request(
            AssociationTableSortV1::Locus,
            AssociationSortDirectionV1::Ascending,
        ))
        .unwrap();
    assert_eq!(
        locus_page
            .points
            .iter()
            .map(|point| point.source_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 2, 1, 3, 4]
    );

    let mut escaped_search = request(
        AssociationTableSortV1::Pvalue,
        AssociationSortDirectionV1::Ascending,
    );
    escaped_search.search = Some("%".into());
    let search_page = store.table_page(&escaped_search).unwrap();
    assert_eq!(search_page.total, 1);
    assert_eq!(search_page.points[0].source_ordinal, 0);
    for (search, expected_ordinal) in [("_", 3), (r"\", 4), ("30", 3)] {
        escaped_search.search = Some(search.into());
        let page = store.table_page(&escaped_search).unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.points[0].source_ordinal, expected_ordinal);
    }

    let region = store
        .region_page(&AssociationRegionRequestV1 {
            dataset_id: dataset_id.clone(),
            contig: "chr2".into(),
            start: 10,
            end: 20,
            offset: 0,
            limit: 100,
        })
        .unwrap();
    assert_eq!(region.total, 2);
    assert_eq!(
        region
            .points
            .iter()
            .map(|point| point.position)
            .collect::<Vec<_>>(),
        vec![10, 20]
    );
    drop(store);

    let mut read_only = SqliteAssociationStore::open_read_only(&path).unwrap();
    assert_eq!(read_only.top_hits(&dataset_id, 3).unwrap(), top);
    assert!(matches!(
        read_only.abort_build(&dataset_id),
        Err(AssociationStoreError::ReadOnly)
    ));
}

#[test]
fn region_scan_is_inclusive_canonicalizes_aliases_and_orders_ties() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let mut store = SqliteAssociationStore::create(&path).unwrap();
    let dataset_id = id();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: dataset_id.clone(),
            created_at: "2026-09-04T00:00:00Z".into(),
        })
        .unwrap();

    let mut points = vec![
        point(0, "1", 8, 0.5, -0.5_f64.log10()),
        point(1, "1", 10, 0.5, -0.5_f64.log10()),
        point(2, "1", 10, 0.5, -0.5_f64.log10()),
        point(3, "1", 10, 0.5, -0.5_f64.log10()),
        point(4, "1", 10, 0.5, -0.5_f64.log10()),
        point(5, "1", 10, 0.5, -0.5_f64.log10()),
        point(6, "1", 12, 0.5, -0.5_f64.log10()),
        point(7, "1", 13, 0.5, -0.5_f64.log10()),
    ];
    points[1].ref_allele = Some("A".into());
    points[1].alt_allele = Some("C".into());
    points[2].ref_allele = Some("A".into());
    points[2].alt_allele = Some("C".into());
    points[3].ref_allele = None;
    points[3].alt_allele = None;
    points[4].ref_allele = None;
    points[4].alt_allele = Some("T".into());
    points[5].ref_allele = Some("A".into());
    points[5].alt_allele = Some("G".into());
    store.insert_points(&dataset_id, &points).unwrap();
    store.complete_build(&dataset_id, &fingerprint()).unwrap();

    let request = AssociationRegionScanRequestV1 {
        dataset_id: dataset_id.clone(),
        contig: " cHr01 ".into(),
        start: 8,
        end: 12,
    };
    let mut visited = Vec::new();
    let summary = store
        .scan_region(&request, &mut |point| {
            visited.push((
                point.position,
                point.ref_allele.clone(),
                point.alt_allele.clone(),
                point.source_ordinal,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        visited,
        vec![
            (8, Some("A".into()), Some("C".into()), 0),
            (10, None, None, 3),
            (10, None, Some("T".into()), 4),
            (10, Some("A".into()), Some("C".into()), 1),
            (10, Some("A".into()), Some("C".into()), 2),
            (10, Some("A".into()), Some("G".into()), 5),
            (12, Some("A".into()), Some("C".into()), 6),
        ]
    );
    assert_eq!(summary.dataset_id, dataset_id);
    assert_eq!(summary.contig, "1");
    assert_eq!((summary.start, summary.end), (8, 12));
    assert_eq!(summary.point_count, 7);

    drop(store);
    let read_only = SqliteAssociationStore::open_read_only(&path).unwrap();
    let mut read_only_count = 0_u64;
    assert_eq!(
        read_only
            .scan_region(&request, &mut |_| {
                read_only_count += 1;
                Ok(())
            })
            .unwrap()
            .point_count,
        7
    );
    assert_eq!(read_only_count, 7);
}

#[test]
fn region_scan_rejects_invalid_incomplete_and_wrong_dataset_requests() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let mut store = SqliteAssociationStore::create(&path).unwrap();
    let dataset_id = id();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: dataset_id.clone(),
            created_at: "now".into(),
        })
        .unwrap();
    let request = AssociationRegionScanRequestV1 {
        dataset_id: dataset_id.clone(),
        contig: "1".into(),
        start: 1,
        end: 1,
    };
    assert!(matches!(
        store.scan_region(&request, &mut |_| Ok(())),
        Err(AssociationStoreError::InvalidBuildState)
    ));

    store
        .insert_points(&dataset_id, &[point(0, "1", 1, 0.5, -0.5_f64.log10())])
        .unwrap();
    store.complete_build(&dataset_id, &fingerprint()).unwrap();
    let wrong_dataset = AssociationRegionScanRequestV1 {
        dataset_id: DatasetIdV1::new("223e4567-e89b-42d3-a456-426614174000").unwrap(),
        ..request.clone()
    };
    assert!(matches!(
        store.scan_region(&wrong_dataset, &mut |_| Ok(())),
        Err(AssociationStoreError::DatasetNotFound)
    ));

    for invalid in [
        AssociationRegionScanRequestV1 {
            start: 0,
            ..request.clone()
        },
        AssociationRegionScanRequestV1 {
            start: 2,
            end: 1,
            ..request.clone()
        },
        AssociationRegionScanRequestV1 {
            end: JSON_SAFE_INTEGER_MAX as i64 + 1,
            ..request.clone()
        },
        AssociationRegionScanRequestV1 {
            contig: String::new(),
            ..request.clone()
        },
        AssociationRegionScanRequestV1 {
            contig: "x".repeat(4_097),
            ..request.clone()
        },
    ] {
        assert!(matches!(
            store.scan_region(&invalid, &mut |_| Ok(())),
            Err(AssociationStoreError::InvalidRequest(_))
        ));
    }
}

#[test]
fn region_scan_stops_immediately_and_preserves_sink_failure() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let mut store = SqliteAssociationStore::create(&path).unwrap();
    let dataset_id = id();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: dataset_id.clone(),
            created_at: "now".into(),
        })
        .unwrap();
    let points = (0..5)
        .map(|ordinal| point(ordinal, "1", ordinal as i64 + 1, 0.5, -0.5_f64.log10()))
        .collect::<Vec<_>>();
    store.insert_points(&dataset_id, &points).unwrap();
    store.complete_build(&dataset_id, &fingerprint()).unwrap();

    let mut visited = Vec::new();
    let error = store
        .scan_region(
            &AssociationRegionScanRequestV1 {
                dataset_id,
                contig: "chr1".into(),
                start: 1,
                end: 5,
            },
            &mut |point| {
                visited.push(point.source_ordinal);
                if visited.len() == 2 {
                    Err(AssociationStoreError::Sink(
                        "injected reducer failure".into(),
                    ))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
    assert_eq!(
        error,
        AssociationStoreError::Sink("injected reducer failure".into())
    );
    assert_eq!(visited, vec![0, 1]);
}

#[test]
fn region_scan_rejects_an_invalid_persisted_source_row_before_visiting_it() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    drop(SqliteAssociationStore::create(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints=ON")
        .unwrap();
    connection
        .execute(
            "INSERT INTO association_datasets(dataset_id,source_fingerprint,created_at,status) \
             VALUES(?1,?2,'now','complete')",
            rusqlite::params![id().as_str(), fingerprint()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO association_points( \
               dataset_id,source_ordinal,contig,contig_rank,position,pvalue,neg_log10_p \
             ) VALUES(?1,-1,'1',1,1,0.5,0.3010299956639812)",
            [id().as_str()],
        )
        .unwrap();
    drop(connection);

    let store = SqliteAssociationStore::open_read_only(&path).unwrap();
    let mut visited = 0;
    assert!(matches!(
        store.scan_region(
            &AssociationRegionScanRequestV1 {
                dataset_id: id(),
                contig: "1".into(),
                start: 1,
                end: 1,
            },
            &mut |_| {
                visited += 1;
                Ok(())
            }
        ),
        Err(AssociationStoreError::InvalidPoint(_))
    ));
    assert_eq!(visited, 0);
}

#[test]
fn region_scan_query_plan_uses_the_region_index() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    drop(SqliteAssociationStore::create(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    let mut statement = connection
        .prepare(
            "EXPLAIN QUERY PLAN \
             SELECT source_ordinal, contig, position, ref, alt, pvalue, neg_log10_p, beta, se, af, ac, gene, consequence, series \
             FROM association_points \
             WHERE dataset_id=?1 AND contig=?2 AND position BETWEEN ?3 AND ?4 \
             ORDER BY position, ref IS NOT NULL, ref, alt IS NOT NULL, alt, source_ordinal",
        )
        .unwrap();
    let details = statement
        .query_map(
            rusqlite::params![id().as_str(), "1", 1_i64, 100_i64],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("USING INDEX association_region_idx")),
        "query plan did not use association_region_idx: {details:?}"
    );
}

#[test]
fn million_row_region_scan_keeps_the_caller_sink_constant_space() {
    const ROW_COUNT: u64 = 1_000_000;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    drop(SqliteAssociationStore::create(&path).unwrap());
    let mut connection = Connection::open(&path).unwrap();
    connection.execute_batch("PRAGMA foreign_keys=ON").unwrap();
    let transaction = connection.transaction().unwrap();
    transaction
        .execute(
            "INSERT INTO association_datasets(dataset_id,source_fingerprint,created_at,status) \
             VALUES(?1,?2,'now','complete')",
            rusqlite::params![id().as_str(), fingerprint()],
        )
        .unwrap();
    transaction
        .execute(
            "WITH RECURSIVE source_rows(source_ordinal) AS ( \
               VALUES(0) \
               UNION ALL \
               SELECT source_ordinal + 1 FROM source_rows WHERE source_ordinal + 1 < ?2 \
             ) \
             INSERT INTO association_points( \
               dataset_id,source_ordinal,contig,contig_rank,position,pvalue,neg_log10_p \
             ) \
             SELECT ?1,source_ordinal,'1',1,source_ordinal + 1,0.5,0.3010299956639812 \
             FROM source_rows",
            rusqlite::params![id().as_str(), ROW_COUNT as i64],
        )
        .unwrap();
    transaction.commit().unwrap();
    drop(connection);

    let store = SqliteAssociationStore::open_read_only(&path).unwrap();
    let mut visited = 0_u64;
    let mut first = None;
    let mut last = None;
    let summary = store
        .scan_region(
            &AssociationRegionScanRequestV1 {
                dataset_id: id(),
                contig: "chr1".into(),
                start: 1,
                end: ROW_COUNT as i64,
            },
            &mut |point| {
                visited += 1;
                first.get_or_insert(point.position);
                last = Some(point.position);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(visited, ROW_COUNT);
    assert_eq!(summary.point_count, ROW_COUNT);
    assert_eq!(first, Some(1));
    assert_eq!(last, Some(ROW_COUNT as i64));
}

#[test]
fn batches_and_query_inputs_are_hard_bounded() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let mut store = SqliteAssociationStore::create(&path).unwrap();
    let dataset_id = id();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: dataset_id.clone(),
            created_at: "now".into(),
        })
        .unwrap();
    assert!(matches!(
        store.insert_points(&dataset_id, &[]),
        Err(AssociationStoreError::InvalidBatchSize)
    ));
    let oversized = (0..1001)
        .map(|ordinal| point(ordinal, "1", ordinal as i64 + 1, 0.5, -0.5_f64.log10()))
        .collect::<Vec<_>>();
    assert!(matches!(
        store.insert_points(&dataset_id, &oversized),
        Err(AssociationStoreError::InvalidBatchSize)
    ));
}
