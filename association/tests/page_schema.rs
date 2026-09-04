use genohype_association::{
    generate_association_page_schema, AssociationPageV1, AssociationPointV1,
    AssociationSortDirectionV1, AssociationTableSortV1, DatasetIdV1, PageError,
    ASSOCIATION_PAGE_SCHEMA_VERSION_V1, MAX_PAGE_POINT_TEXT_BYTES, MAX_POINT_FIELD_BYTES,
};
use jsonschema::Validator;
use serde_json::{json, Value};

fn point(ordinal: u64) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: "1".into(),
        position: ordinal as i64 + 1,
        ref_allele: Some("A".into()),
        alt_allele: Some("C".into()),
        pvalue: 0.5,
        neg_log10_p: -0.5_f64.log10(),
        beta: None,
        standard_error: None,
        allele_frequency: None,
        allele_count: None,
        gene_symbol: None,
        consequence: None,
        series: None,
        source_ordinal: ordinal,
    }
}

fn page() -> AssociationPageV1 {
    AssociationPageV1 {
        schema_version: ASSOCIATION_PAGE_SCHEMA_VERSION_V1.into(),
        dataset_id: DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap(),
        points: vec![point(0)],
        offset: 0,
        limit: 100,
        total: 1,
        sort: Some(AssociationTableSortV1::Pvalue),
        direction: Some(AssociationSortDirectionV1::Ascending),
        search: Some(String::new()),
    }
}

#[test]
fn checked_in_page_schema_matches_rust_authority() {
    let checked_in: Value =
        serde_json::from_str(include_str!("../schema/association-page-v1.schema.json")).unwrap();
    let generated = serde_json::to_value(generate_association_page_schema()).unwrap();
    assert_eq!(
        checked_in, generated,
        "schema drift: regenerate association-page-v1.schema.json"
    );
    assert_eq!(generated["properties"]["points"]["maxItems"], 1000);
    assert_eq!(generated["properties"]["search"]["maxLength"], 256);
}

#[test]
fn page_schema_rejects_unsafe_ids_counts_rows_and_strings() {
    let schema = serde_json::to_value(generate_association_page_schema()).unwrap();
    let validator = Validator::new(&schema).unwrap();
    let value = serde_json::to_value(page()).unwrap();
    assert!(validator.is_valid(&value));
    for (pointer, invalid) in [
        ("/dataset_id", json!("../secret")),
        ("/limit", json!(1001)),
        ("/offset", json!(9_007_199_254_740_992_u64)),
        (
            "/points/0/gene_symbol",
            json!("x".repeat(MAX_POINT_FIELD_BYTES + 1)),
        ),
        ("/search", json!("x".repeat(257))),
    ] {
        let mut candidate = value.clone();
        *candidate.pointer_mut(pointer).unwrap() = invalid;
        assert!(
            !validator.is_valid(&candidate),
            "accepted invalid {pointer}"
        );
    }
}

#[test]
fn page_runtime_validation_enforces_aggregate_text_budget() {
    let mut page = page();
    page.limit = 100;
    page.total = 65;
    page.points = (0..65)
        .map(|ordinal| {
            let mut point = point(ordinal);
            point.contig = "A".repeat(MAX_POINT_FIELD_BYTES);
            point.ref_allele = Some("C".repeat(MAX_POINT_FIELD_BYTES));
            point.alt_allele = Some("G".repeat(MAX_POINT_FIELD_BYTES));
            point.gene_symbol = Some("T".repeat(MAX_POINT_FIELD_BYTES));
            point
        })
        .collect();
    assert!(65 * 4 * MAX_POINT_FIELD_BYTES > MAX_PAGE_POINT_TEXT_BYTES);
    assert_eq!(page.validate(), Err(PageError::PayloadTooLarge));
}
