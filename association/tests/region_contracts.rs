use genohype_association::{
    generate_region_overlay_schema, generate_region_render_manifest_schema,
    AssociationRegionOverlayV1, AssociationRegionRenderManifestV1, RegionContractError,
};
use jsonschema::Validator;
use serde_json::{json, Value};

const MANIFEST_SCHEMA: &str = include_str!("../schema/association-region-render-v1.schema.json");
const OVERLAY_SCHEMA: &str = include_str!("../schema/association-region-overlay-v1.schema.json");
const MANIFEST_GOLDEN: &str = include_str!("fixtures/association-region-render-v1.json");
const OVERLAY_GOLDEN: &str = include_str!("fixtures/association-region-overlay-v1.json");

#[test]
fn checked_in_region_schemas_are_exactly_generated_from_rust() {
    let checked_manifest: Value = serde_json::from_str(MANIFEST_SCHEMA).unwrap();
    let checked_overlay: Value = serde_json::from_str(OVERLAY_SCHEMA).unwrap();
    assert_eq!(
        checked_manifest,
        serde_json::to_value(generate_region_render_manifest_schema()).unwrap(),
        "region manifest schema drift: run the generate_region_render_schema example"
    );
    assert_eq!(
        checked_overlay,
        serde_json::to_value(generate_region_overlay_schema()).unwrap(),
        "region overlay schema drift: run the generate_region_overlay_schema example"
    );
    assert_eq!(checked_overlay["properties"]["points"]["maxItems"], 1_000);
    assert_eq!(
        checked_overlay["$defs"]["GeneAnnotationsV1"]["properties"]["features"]["maxItems"],
        1_000
    );
    assert_eq!(
        checked_manifest["$defs"]["RegionLayoutV1"]["properties"]["dpr"]["maximum"],
        4
    );
    assert_eq!(
        checked_manifest["$defs"]["RegionLayoutV1"]["properties"]["requested_width"]["maximum"],
        4_050
    );
    assert_eq!(
        checked_manifest["properties"]["significance_threshold"]["exclusiveMinimum"],
        0.0
    );
    assert_eq!(
        checked_manifest["properties"]["significance_threshold"]["maximum"],
        1.0
    );
}

#[test]
fn schemas_accept_goldens_and_reject_wire_bound_violations() {
    let manifest_schema: Value = serde_json::from_str(MANIFEST_SCHEMA).unwrap();
    let overlay_schema: Value = serde_json::from_str(OVERLAY_SCHEMA).unwrap();
    let manifest_validator = Validator::new(&manifest_schema).unwrap();
    let overlay_validator = Validator::new(&overlay_schema).unwrap();
    let manifest: Value = serde_json::from_str(MANIFEST_GOLDEN).unwrap();
    let overlay: Value = serde_json::from_str(OVERLAY_GOLDEN).unwrap();
    assert!(manifest_validator.is_valid(&manifest));
    assert!(overlay_validator.is_valid(&overlay));

    for (pointer, invalid) in [
        ("/layout/requested_width", json!(49)),
        ("/layout/dpr", json!(5)),
        ("/layout/physical_width", json!(16_385)),
        ("/layout/y_scale/max_neg_log_p", json!(300.0)),
        ("/layout_hash", json!("not-a-hash")),
        ("/resources/raster/url", json!("plot.png?secret=yes")),
    ] {
        let mut candidate = manifest.clone();
        *candidate.pointer_mut(pointer).unwrap() = invalid;
        assert!(
            !manifest_validator.is_valid(&candidate),
            "accepted {pointer}"
        );
    }

    for threshold in [json!(-1.0), json!(0.0), json!(1.1)] {
        let mut invalid_manifest = manifest.clone();
        invalid_manifest["significance_threshold"] = threshold.clone();
        assert!(!manifest_validator.is_valid(&invalid_manifest));
        let mut invalid_overlay = overlay.clone();
        invalid_overlay["significance_threshold"] = threshold;
        assert!(!overlay_validator.is_valid(&invalid_overlay));
    }

    let mut too_many_points = overlay;
    too_many_points["points"] = Value::Array(vec![Value::Null; 1_001]);
    assert!(!overlay_validator.is_valid(&too_many_points));
}

#[test]
fn golden_region_contracts_round_trip_and_validate() {
    let manifest: AssociationRegionRenderManifestV1 =
        serde_json::from_str(MANIFEST_GOLDEN).unwrap();
    let overlay: AssociationRegionOverlayV1 = serde_json::from_str(OVERLAY_GOLDEN).unwrap();
    manifest.validate().unwrap();
    overlay.validate_against(&manifest).unwrap();
    assert_eq!(
        serde_json::to_string_pretty(&manifest).unwrap(),
        MANIFEST_GOLDEN.trim_end()
    );
    assert_eq!(
        serde_json::to_string_pretty(&overlay).unwrap(),
        OVERLAY_GOLDEN.trim_end()
    );
    assert_eq!(manifest.layout_hash, overlay.layout_hash);
    assert_eq!(manifest.raster_hash, overlay.raster_hash);
    assert_eq!(manifest.render_hash, overlay.render_hash);
    assert_eq!(manifest.overlay_hash, overlay.overlay_hash);
    assert_eq!(manifest.aggregation, overlay.aggregation);
}

#[test]
fn pair_validation_rejects_resource_substitution_and_arbitrary_digests() {
    let manifest: AssociationRegionRenderManifestV1 =
        serde_json::from_str(MANIFEST_GOLDEN).unwrap();
    let overlay: AssociationRegionOverlayV1 = serde_json::from_str(OVERLAY_GOLDEN).unwrap();

    let mut altered_manifest = manifest.clone();
    altered_manifest.overlay_hash = format!("sha256:{}", "0".repeat(64));
    altered_manifest.validate().unwrap();
    assert!(matches!(
        overlay.validate_against(&altered_manifest),
        Err(RegionContractError::ManifestOverlayMismatch)
    ));

    let mut altered_overlay = overlay;
    altered_overlay.raster_hash = format!("sha256:{}", "1".repeat(64));
    altered_overlay.render_hash = genohype_association::region_render_hash(
        &altered_overlay.layout_hash,
        &altered_overlay.raster_hash,
    );
    assert!(altered_overlay.validate().is_err());
}

#[test]
fn golden_manifest_rejects_geometry_hash_and_disclosure_drift() {
    let manifest: AssociationRegionRenderManifestV1 =
        serde_json::from_str(MANIFEST_GOLDEN).unwrap();

    let mut dimensions = manifest.clone();
    dimensions.layout.physical_width += 1;
    assert!(matches!(
        dimensions.validate(),
        Err(RegionContractError::NonCanonicalLayout)
    ));

    let mut layout_hash = manifest.clone();
    layout_hash.layout_hash = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        layout_hash.validate(),
        Err(RegionContractError::LayoutHashMismatch)
    ));

    let mut raster_hash = manifest.clone();
    raster_hash.significance_threshold = 1e-6;
    assert!(matches!(
        raster_hash.validate(),
        Err(RegionContractError::RasterHashMismatch)
    ));

    let mut render_hash = manifest.clone();
    render_hash.render_hash = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        render_hash.validate(),
        Err(RegionContractError::RenderHashMismatch)
    ));

    let mut disclosure = manifest;
    disclosure.aggregation.omitted_ordinary_count += 1;
    assert!(matches!(
        disclosure.validate(),
        Err(RegionContractError::InconsistentCounts)
    ));
}

#[test]
fn compatible_minor_fields_parse_but_unsupported_major_does_not() {
    let mut value: Value = serde_json::from_str(MANIFEST_GOLDEN).unwrap();
    value["schema_version"] = Value::String("1.7".into());
    value["future_minor_field"] = serde_json::json!({"accepted": true});
    assert!(serde_json::from_value::<AssociationRegionRenderManifestV1>(value.clone()).is_ok());
    value["schema_version"] = Value::String("2.0".into());
    assert!(serde_json::from_value::<AssociationRegionRenderManifestV1>(value).is_err());
}
