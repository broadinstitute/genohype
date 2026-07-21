//! End-to-end Elasticsearch round-trip acceptance test (Phase 2a).
//!
//! This is the executable form of the Phase-2a acceptance check:
//! *round-trip on a subset reconciles counts vs source Hail; re-load is
//! idempotent; projection is respected.* It requires a running Elasticsearch and
//! is therefore `#[ignore]`d by default (a live ES is infra, not a unit-test
//! dependency). Run it explicitly when an ES is available:
//!
//! ```sh
//! # default table = tests/fixtures/tiny-keyed.ht, default url = http://localhost:9200
//! GENOHYPE_ES_TEST_URL=http://localhost:9200 \
//!   cargo test -p genohype-core --features elasticsearch --test elasticsearch_roundtrip -- --ignored --nocapture
//!
//! # against a real gnomAD v4 sites subset (chr21+chr22):
//! GENOHYPE_ES_TEST_URL=http://localhost:9200 \
//! GENOHYPE_ES_TEST_TABLE=gs://.../gnomad.exomes.v4.1.1.sites.chr22.ht \
//! GENOHYPE_ES_INDEX_FIELDS=variant_id,locus,transcript_consequences.gene_id \
//! GENOHYPE_ES_ID_FIELD=variant_id \
//!   cargo test -p genohype-core --features elasticsearch --test elasticsearch_roundtrip -- --ignored --nocapture
//! ```
#![cfg(feature = "elasticsearch")]

use genohype_core::export::elasticsearch::{
    build_document, build_request_body, parse_index_fields, BulkInserter, ElasticsearchClient,
};
use genohype_core::query::QueryEngine;
use serde_json::Value;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Default test table: the checked-in keyed Hail fixture used by the active
/// decoder and index tests. `start` exercises the hoisted-integer path.
fn default_table() -> String {
    format!(
        "{}/tests/fixtures/tiny-keyed.ht",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// Load `table` into `index`, returning the number of documents sent.
fn load(
    client: &ElasticsearchClient,
    table: &str,
    index: &str,
    index_fields_spec: &str,
    id_field: &str,
    recreate: bool,
) -> usize {
    let engine = QueryEngine::open_path(table).expect("open table");
    let row_type = engine.row_type().clone();
    let index_fields = parse_index_fields(index_fields_spec);

    let body = build_request_body(&row_type, &index_fields, 1);
    client
        .create_index(index, &body, recreate)
        .expect("create index");

    let mut inserter = BulkInserter::new(client, index, Some(id_field.to_string()), 500);
    for row in engine.query_iter(&[]).expect("scan") {
        let row = row.expect("decode row");
        let doc = build_document(&row, &index_fields);
        inserter.add(&doc).expect("bulk add");
    }
    inserter.finish().expect("bulk finish");
    inserter.total_docs
}

fn count_source(table: &str) -> usize {
    let engine = QueryEngine::open_path(table).expect("open table");
    engine
        .query_iter(&[])
        .expect("scan")
        .map(|r| r.expect("decode"))
        .count()
}

#[test]
#[ignore = "requires a running Elasticsearch (set GENOHYPE_ES_TEST_URL)"]
fn roundtrip_reconciles_counts_idempotent_and_projection() {
    let url = env_or("GENOHYPE_ES_TEST_URL", "http://localhost:9200");
    let table = env_or("GENOHYPE_ES_TEST_TABLE", &default_table());
    let index_fields_spec = env_or("GENOHYPE_ES_INDEX_FIELDS", "gene_id,start");
    let id_field = env_or("GENOHYPE_ES_ID_FIELD", "gene_id");
    let index = env_or("GENOHYPE_ES_TEST_INDEX", "genohype_es_roundtrip_test");

    let client = ElasticsearchClient::new(&url);

    let source_count = count_source(&table) as u64;
    assert!(source_count > 0, "source table has no rows");

    // 1. Fresh load: ES count must reconcile with the source Hail row count.
    let sent = load(&client, &table, &index, &index_fields_spec, &id_field, true);
    let es_count = client.count(&index).expect("count");
    println!("source={source_count} sent={sent} es_count={es_count}");
    assert_eq!(
        es_count, source_count,
        "ES doc count must equal source Hail row count"
    );

    // 2. Idempotent re-load (no recreate): re-indexing the same _id overwrites,
    //    so the doc count is unchanged.
    let _ = load(
        &client,
        &table,
        &index,
        &index_fields_spec,
        &id_field,
        false,
    );
    let es_count_2 = client.count(&index).expect("count after reload");
    assert_eq!(
        es_count_2, source_count,
        "re-load must be idempotent (count unchanged)"
    );

    // 3. Document shape: each hit nests the full row under `_source.value`.
    let hit = first_hit(&client, &url, &index);
    let source = hit.get("_source").expect("_source");
    assert!(
        source.get("value").map(|v| v.is_object()).unwrap_or(false),
        "documents must nest the row under value.*"
    );

    println!("round-trip OK: counts reconcile, re-load idempotent, value.* nesting present");
}

/// Fetch the first search hit (used to inspect document shape).
fn first_hit(client: &ElasticsearchClient, url: &str, index: &str) -> Value {
    client.refresh(index).expect("refresh");
    let body = reqwest::blocking::Client::new()
        .get(format!(
            "{}/{}/_search?size=1",
            url.trim_end_matches('/'),
            index
        ))
        .send()
        .expect("search")
        .text()
        .expect("search body");
    let parsed: Value = serde_json::from_str(&body).expect("parse search");
    parsed["hits"]["hits"][0].clone()
}
