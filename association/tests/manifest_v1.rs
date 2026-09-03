use genohype_association::{
    AssociationVisualizationManifestV1, ManifestError, COORDINATE_CONVENTION_V1,
    JSON_SAFE_INTEGER_MAX, SCHEMA_VERSION_V1,
};
use serde_json::{json, Value};

fn fixture_value() -> Value {
    serde_json::from_str(include_str!("fixtures/association-visualization-v1.json")).unwrap()
}

#[test]
fn golden_manifest_round_trips_and_validates() {
    let expected = fixture_value();
    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_value(expected.clone()).unwrap();

    assert_eq!(manifest.schema_version, SCHEMA_VERSION_V1);
    assert_eq!(manifest.coordinate_convention, COORDINATE_CONVENTION_V1);
    assert_eq!(manifest.top_hits[0].neg_log10_p, 400.0);
    manifest.validate().unwrap();
    assert_eq!(serde_json::to_value(manifest).unwrap(), expected);
}

#[test]
fn compatible_minor_unknown_fields_are_tolerated_but_major_versions_are_rejected() {
    let mut compatible = fixture_value();
    compatible["schema_version"] = json!("1.37");
    compatible["future_minor_field"] = json!({"ignored": true});
    let parsed: AssociationVisualizationManifestV1 = serde_json::from_value(compatible).unwrap();
    assert_eq!(parsed.schema_version, "1.37");

    let mut incompatible = fixture_value();
    incompatible["schema_version"] = json!("2.0");
    let error = serde_json::from_value::<AssociationVisualizationManifestV1>(incompatible)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unsupported association manifest schema major version 2"));
}

#[test]
fn layouts_must_be_the_exact_canonical_derivation_even_with_a_matching_hash() {
    let mut manifest: AssociationVisualizationManifestV1 =
        serde_json::from_value(fixture_value()).unwrap();
    manifest.layout.y_scale.max_neg_log_p = 300.0;
    manifest.layout_hash = manifest.layout.layout_hash(&manifest.assembly);
    assert!(matches!(
        manifest.validate(),
        Err(ManifestError::InvalidLayout(_))
    ));

    let mut manifest: AssociationVisualizationManifestV1 =
        serde_json::from_value(fixture_value()).unwrap();
    manifest.layout.contigs[0].x_end += 1.0;
    manifest.layout_hash = manifest.layout.layout_hash(&manifest.assembly);
    assert!(matches!(
        manifest.validate(),
        Err(ManifestError::InvalidLayout(_))
    ));
}

#[test]
fn manifest_validation_rejects_browser_unsafe_wire_integers() {
    let mut count: AssociationVisualizationManifestV1 =
        serde_json::from_value(fixture_value()).unwrap();
    count.row_count = JSON_SAFE_INTEGER_MAX + 1;
    assert_eq!(
        count.validate(),
        Err(ManifestError::BrowserUnsafeInteger("row_count"))
    );

    let mut bytes: AssociationVisualizationManifestV1 =
        serde_json::from_value(fixture_value()).unwrap();
    bytes.resources.raster.byte_size = Some(JSON_SAFE_INTEGER_MAX + 1);
    assert_eq!(
        bytes.validate(),
        Err(ManifestError::BrowserUnsafeInteger("resource byte_size"))
    );
}

#[test]
fn resources_must_be_relative_and_credential_free() {
    for unsafe_url in [
        "/tmp/plot.png",
        "file:plot.png",
        "https://example.test/plot.png",
        "plot.png?token=secret",
        "user@example.test/plot.png",
        "plots/../secret",
        "api/%2e%2e/secret",
        "api/%2Fsecret",
        "api//secret",
        "api/./secret",
    ] {
        let mut value = fixture_value();
        value["resources"]["raster"]["url"] = json!(unsafe_url);
        let manifest: AssociationVisualizationManifestV1 = serde_json::from_value(value).unwrap();
        assert!(matches!(
            manifest.validate(),
            Err(ManifestError::UnsafeResourceUrl(url)) if url == unsafe_url
        ));
    }
}
