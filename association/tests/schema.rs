use genohype_association::generate_manifest_schema;
use jsonschema::Validator;
use serde_json::{json, Value};

#[test]
fn checked_in_schema_matches_schemars_output() {
    let checked_in: Value = serde_json::from_str(include_str!(
        "../schema/association-visualization-v1.schema.json"
    ))
    .unwrap();
    let generated = serde_json::to_value(generate_manifest_schema()).unwrap();

    assert_eq!(
        checked_in, generated,
        "schema drift: run `cargo run -p genohype-association --example generate_schema > association/schema/association-visualization-v1.schema.json`"
    );
    assert_eq!(generated["properties"]["top_hits"]["maxItems"], 1_000);
    assert_eq!(
        generated["properties"]["schema_version"]["pattern"],
        "^1\\.[0-9]+$"
    );
}

#[test]
fn schema_rejects_invalid_domain_values_before_cross_language_consumption() {
    let schema = serde_json::to_value(generate_manifest_schema()).unwrap();
    let validator = Validator::new(&schema).unwrap();
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/association-visualization-v1.json")).unwrap();
    assert!(validator.is_valid(&fixture));

    for (pointer, invalid) in [
        ("/layout/width", json!(0)),
        ("/layout/contigs/0/length", json!(0)),
        ("/layout/y_scale/max_neg_log_p", json!(300.0)),
        ("/top_hits/0/position", json!(0)),
        ("/top_hits/0/pvalue", json!(-0.1)),
        ("/top_hits/0/allele_frequency", json!(1.1)),
        ("/resources/raster/url", json!("api/%2e%2e/secret")),
        ("/resources/raster/media_type", json!("")),
    ] {
        let mut candidate = fixture.clone();
        *candidate.pointer_mut(pointer).unwrap() = invalid;
        assert!(
            !validator.is_valid(&candidate),
            "schema unexpectedly accepted invalid value at {pointer}"
        );
    }
}
