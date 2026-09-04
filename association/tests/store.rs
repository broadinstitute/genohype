use genohype_association::{
    AssociationBuildV1, AssociationPointV1, AssociationRegionRequestV1, AssociationSortDirectionV1,
    AssociationStore, AssociationStoreError, AssociationTablePageRequestV1, AssociationTableSortV1,
    DatasetIdV1, SqliteAssociationStore,
};
use rusqlite::Connection;
use tempfile::TempDir;

fn id() -> DatasetIdV1 {
    DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap()
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
            "INSERT INTO association_datasets VALUES('123e4567-e89b-42d3-a456-426614174000','f','now','building')",
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
            source_fingerprint: "sha256:test".into(),
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
    store.complete_build(&dataset_id).unwrap();
    assert!(store.is_complete(&dataset_id).unwrap());
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
    for (search, expected_ordinal) in [("_", 3), (r"\", 4)] {
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
fn batches_and_query_inputs_are_hard_bounded() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("associations.sqlite3");
    let mut store = SqliteAssociationStore::create(&path).unwrap();
    let dataset_id = id();
    store
        .begin_build(&AssociationBuildV1 {
            dataset_id: dataset_id.clone(),
            source_fingerprint: "x".into(),
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
