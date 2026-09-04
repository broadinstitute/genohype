use genohype_association::{generate_manifest_schema, JSON_SAFE_INTEGER_MAX};
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
    for pointer in [
        "/properties/row_count/maximum",
        "/properties/valid_row_count/maximum",
        "/properties/skipped_row_count/maximum",
        "/properties/rendered_point_count/maximum",
        "/properties/significant_hit_count/maximum",
        "/$defs/AssociationPointV1/properties/position/maximum",
        "/$defs/AssociationPointV1/properties/source_ordinal/maximum",
        "/$defs/ContigLayoutV1/properties/length/maximum",
        "/$defs/ResourceDescriptorV1/properties/byte_size/maximum",
    ] {
        assert_eq!(
            generated.pointer(pointer),
            Some(&json!(JSON_SAFE_INTEGER_MAX)),
            "missing browser-safe maximum at {pointer}"
        );
    }
    for pointer in [
        "/$defs/GenomeLayoutV1/properties/width/maximum",
        "/$defs/GenomeLayoutV1/properties/height/maximum",
        "/$defs/GenomeLayoutV1/properties/contig_gap/maximum",
        "/$defs/YScale/properties/height/maximum",
    ] {
        assert_eq!(
            generated.pointer(pointer),
            Some(&json!(u32::MAX)),
            "missing u32 wire maximum at {pointer}"
        );
    }
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
        ("/title", json!("x".repeat(513))),
        ("/warnings/0", json!("x".repeat(1_025))),
        ("/top_hits/0/gene_symbol", json!("x".repeat(4_097))),
    ] {
        let mut candidate = fixture.clone();
        *candidate.pointer_mut(pointer).unwrap() = invalid;
        assert!(
            !validator.is_valid(&candidate),
            "schema unexpectedly accepted invalid value at {pointer}"
        );
    }

    for pointer in [
        "/row_count",
        "/valid_row_count",
        "/skipped_row_count",
        "/rendered_point_count",
        "/significant_hit_count",
        "/top_hits/0/position",
        "/top_hits/0/source_ordinal",
        "/layout/contigs/0/length",
        "/resources/raster/byte_size",
    ] {
        let mut boundary = fixture.clone();
        *boundary.pointer_mut(pointer).unwrap() = json!(JSON_SAFE_INTEGER_MAX);
        assert!(
            validator.is_valid(&boundary),
            "schema unexpectedly rejected browser-safe boundary at {pointer}"
        );
        *boundary.pointer_mut(pointer).unwrap() = json!(JSON_SAFE_INTEGER_MAX + 1);
        assert!(
            !validator.is_valid(&boundary),
            "schema unexpectedly accepted an unsafe integer at {pointer}"
        );
    }

    for pointer in [
        "/layout/width",
        "/layout/height",
        "/layout/contig_gap",
        "/layout/y_scale/height",
    ] {
        let mut boundary = fixture.clone();
        *boundary.pointer_mut(pointer).unwrap() = json!(u64::from(u32::MAX) + 1);
        assert!(
            !validator.is_valid(&boundary),
            "schema unexpectedly accepted a value outside u32 at {pointer}"
        );
    }
}
