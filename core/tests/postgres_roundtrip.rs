//! End-to-end Postgres round-trip acceptance test (Phase 2b).
//!
//! This is the executable form of the Phase-2b acceptance check:
//! *round-trip on a subset reconciles counts vs source; re-load is idempotent;
//! the projected `data` JSONB is respected.* It requires a running Postgres and
//! is therefore `#[ignore]`d by default.
//!
//! By default it drives **synthetic gnomAD-shaped rows** spanning two contigs, so
//! it exercises the full COPY path: list-partition routing across contigs, the
//! COPY-into-staging → `ON CONFLICT` upsert, idempotent re-load, and JSONB
//! payload round-trip — without needing a variant-shaped Hail fixture (the
//! `test_hail_data` fixtures are generic type tests with no `locus`/`variant_id`).
//! Point it at a real gnomAD v4 sites subset to reconcile against a true Hail
//! source:
//!
//! ```sh
//! GENOHYPE_PG_TEST_URL=postgres://postgres@localhost:5432/postgres \
//!   cargo test -p genohype-core --features postgres --test postgres_roundtrip -- --ignored --nocapture
//!
//! # against a real gnomAD v4 sites subset (chr21+chr22):
//! GENOHYPE_PG_TEST_URL=postgres://postgres@localhost:5432/postgres \
//! GENOHYPE_PG_TEST_TABLE=gs://.../gnomad.exomes.v4.1.1.sites.chr22.ht \
//!   cargo test -p genohype-core --features postgres --test postgres_roundtrip -- --ignored --nocapture
//! ```
#![cfg(feature = "postgres")]

use genohype_core::codec::EncodedValue;
use genohype_core::export::postgres::{extract_columns, CopyInserter, PostgresClient};
use genohype_core::query::QueryEngine;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// A synthetic gnomAD-shaped variant row: `{locus{contig,position}, variant_id,
/// alleles[ref,alt], data_marker}`. `data_marker` stands in for the wide payload
/// so we can assert the JSONB `data` column round-trips.
fn variant_row(contig: &str, pos: i32, r: &str, a: &str, marker: i32) -> EncodedValue {
    let variant_id = format!("{}-{}-{}-{}", contig.trim_start_matches("chr"), pos, r, a);
    EncodedValue::Struct(vec![
        (
            "locus".to_string(),
            EncodedValue::Struct(vec![
                (
                    "contig".to_string(),
                    EncodedValue::Binary(contig.as_bytes().to_vec()),
                ),
                ("position".to_string(), EncodedValue::Int32(pos)),
            ]),
        ),
        (
            "variant_id".to_string(),
            EncodedValue::Binary(variant_id.into_bytes()),
        ),
        (
            "alleles".to_string(),
            EncodedValue::Array(vec![
                EncodedValue::Binary(r.as_bytes().to_vec()),
                EncodedValue::Binary(a.as_bytes().to_vec()),
            ]),
        ),
        ("payload".to_string(), EncodedValue::Int32(marker)),
    ])
}

/// Synthetic rows spanning two contigs (to exercise multi-partition routing).
fn synthetic_rows() -> Vec<EncodedValue> {
    let mut rows = Vec::new();
    for i in 0..50 {
        rows.push(variant_row("chr21", 1_000_000 + i, "A", "G", i));
    }
    for i in 0..30 {
        rows.push(variant_row("chr22", 2_000_000 + i, "C", "T", i));
    }
    rows
}

/// Collect the source rows: either decoded from a real Hail table or synthetic.
fn source_rows(table_env: Option<&str>) -> Vec<EncodedValue> {
    match table_env {
        Some(table) => {
            let engine = QueryEngine::open_path(table).expect("open table");
            engine
                .query_iter(&[])
                .expect("scan")
                .map(|r| r.expect("decode row"))
                .collect()
        }
        None => synthetic_rows(),
    }
}

/// Load `rows` into `pg_table`, returning rows upserted.
fn load(
    client: &mut PostgresClient,
    rows: &[EncodedValue],
    pg_table: &str,
    recreate: bool,
) -> usize {
    if recreate {
        client.drop_table(pg_table).expect("drop");
    }
    client.create_table(pg_table).expect("create table");
    let mut inserter = CopyInserter::new(client, pg_table, 16).expect("inserter");
    for row in rows {
        inserter.add(row).expect("copy add");
    }
    inserter.finish().expect("copy finish");
    inserter.total_rows
}

#[test]
#[ignore = "requires a running Postgres (set GENOHYPE_PG_TEST_URL)"]
fn roundtrip_reconciles_counts_and_is_idempotent() {
    let url = env_or(
        "GENOHYPE_PG_TEST_URL",
        "postgres://postgres@localhost:5432/postgres",
    );
    let table_env = std::env::var("GENOHYPE_PG_TEST_TABLE").ok();
    let pg_table = env_or("GENOHYPE_PG_TEST_PG_TABLE", "genohype_pg_roundtrip_test");

    let mut client = PostgresClient::connect(&url).expect("connect");
    let rows = source_rows(table_env.as_deref());
    let source_count = rows.len() as i64;
    assert!(source_count > 0, "no source rows");

    // 1. Fresh load: PG row count must reconcile with the source row count.
    let sent = load(&mut client, &rows, &pg_table, true);
    client.create_indexes(&pg_table).expect("indexes");
    let pg_count = client.count_rows(&pg_table).expect("count");
    println!("source={source_count} sent={sent} pg_count={pg_count}");
    assert_eq!(
        pg_count, source_count,
        "PG row count must equal source row count"
    );

    // 2. Idempotent re-load (no recreate): upsert on (contig, variant_id)
    //    overwrites, so the count is unchanged.
    let _ = load(&mut client, &rows, &pg_table, false);
    let pg_count_2 = client.count_rows(&pg_table).expect("count after reload");
    assert_eq!(
        pg_count_2, source_count,
        "re-load must be idempotent (count unchanged)"
    );

    // 3. The hoisted `pos` column + `data` JSONB payload round-trip for a known
    //    row (the data payload faithfully carries the projected row → projection
    //    is respected end to end).
    let probe = &rows[0];
    let cols = extract_columns(probe).expect("extract");
    let (pos, data_text) = client
        .fetch_pos_and_data(&pg_table, &cols.contig, &cols.variant_id)
        .expect("probe fetch");
    assert_eq!(pos, cols.pos, "pos column round-trips");
    let stored: serde_json::Value = serde_json::from_str(&data_text).expect("parse data jsonb");
    let expected = genohype_core::export::postgres::row_to_json(probe);
    assert_eq!(stored, expected, "data JSONB payload round-trips");

    println!(
        "round-trip OK: counts reconcile, re-load idempotent, columns + data JSONB round-trip"
    );
}
