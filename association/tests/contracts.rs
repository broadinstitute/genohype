use genohype_association::{
    generate_manifest_schema, AssociationDatasetSummaryV1, AssociationPointV1,
    AssociationVisualizationManifestV1,
};
use schemars::schema_for;
use serde_json::Value;
use std::collections::BTreeSet;

const MANIFEST_GOLDEN: &str = include_str!("golden/association-visualization-v1.json");
const MANIFEST_SCHEMA: &str = include_str!("../schema/association-visualization-v1.schema.json");

fn property_names(schema: &Value) -> BTreeSet<String> {
    schema["properties"]
        .as_object()
        .expect("object schema properties")
        .keys()
        .cloned()
        .collect()
}

#[test]
fn checked_in_schema_is_exactly_generated_from_rust_authority() {
    let checked_in: Value = serde_json::from_str(MANIFEST_SCHEMA).unwrap();
    let generated = serde_json::to_value(generate_manifest_schema()).unwrap();
    assert_eq!(checked_in, generated);
}

#[test]
fn canonical_contract_fields_do_not_drift() {
    let point = serde_json::to_value(schema_for!(AssociationPointV1)).unwrap();
    assert_eq!(
        property_names(&point),
        [
            "allele_count",
            "allele_frequency",
            "alt_allele",
            "beta",
            "consequence",
            "contig",
            "gene_symbol",
            "neg_log10_p",
            "position",
            "pvalue",
            "ref_allele",
            "series",
            "source_ordinal",
            "standard_error",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );

    let summary = serde_json::to_value(schema_for!(AssociationDatasetSummaryV1)).unwrap();
    assert_eq!(
        property_names(&summary),
        [
            "rendered_point_count",
            "row_count",
            "significant_hit_count",
            "skipped_row_count",
            "valid_row_count",
            "warnings",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );

    let manifest = serde_json::to_value(schema_for!(AssociationVisualizationManifestV1)).unwrap();
    assert_eq!(
        property_names(&manifest),
        [
            "assembly",
            "coordinate_convention",
            "created_at",
            "genohype_version",
            "kind",
            "layout",
            "layout_hash",
            "plot_id",
            "rendered_point_count",
            "resources",
            "row_count",
            "schema_version",
            "significant_hit_count",
            "skipped_row_count",
            "source_fingerprint",
            "thresholds",
            "title",
            "top_hits",
            "valid_row_count",
            "warnings",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );
}

#[test]
fn golden_manifest_round_trips_and_validates() {
    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_str(MANIFEST_GOLDEN).unwrap();
    manifest.validate().unwrap();
    let encoded = format!("{}\n", serde_json::to_string_pretty(&manifest).unwrap());
    assert_eq!(encoded, MANIFEST_GOLDEN);
}
