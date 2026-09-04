use genohype_association::{
    reduce_region_overlay, AssociationPointV1, AssociationRegionCountsV1,
    AssociationRegionRenderManifestV1, AssociationRegionResourcesV1, DatasetIdV1,
    GeneAnnotationStateV1, GeneAnnotationsV1, GeneFeatureV1, GeneStrandV1, RegionLayoutV1,
    RegionRenderIdentityV1, ResourceDescriptorV1, ASSOCIATION_REGION_SCHEMA_VERSION_V1,
    COORDINATE_CONVENTION_V1,
};
use std::collections::HashSet;

fn point(
    position: i64,
    pvalue: f64,
    significance: f64,
    consequence: Option<&str>,
    ordinal: u64,
) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: "4".into(),
        position,
        ref_allele: Some("A".into()),
        alt_allele: Some("G".into()),
        pvalue,
        neg_log10_p: significance,
        beta: Some(0.1),
        standard_error: Some(0.02),
        allele_frequency: Some(0.01),
        allele_count: Some(20.0),
        gene_symbol: Some("GENE1".into()),
        consequence: consequence.map(str::to_string),
        series: Some("genome".into()),
        source_ordinal: ordinal,
    }
}

fn main() {
    let layout =
        RegionLayoutV1::new("chr4", 190_214_555, 68_000_111, 68_999_001, 777, 320, 2).unwrap();
    let dataset_id = DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap();
    let identity = RegionRenderIdentityV1::new(
        dataset_id.clone(),
        "sha256:fixture",
        "GRCh38",
        "white",
        layout.clone(),
        5e-8,
    )
    .unwrap();
    let points = vec![
        point(68_000_001, 0.0, 400.0, Some("stop_gained"), 7),
        point(68_500_000, 1e-12, 12.0, Some("intron_variant"), 8),
        point(68_999_999, 0.5, -0.5_f64.log10(), Some("intron_variant"), 9),
    ];
    let selected = HashSet::from([9]);
    let genes = GeneAnnotationsV1 {
        state: GeneAnnotationStateV1::Available,
        features: vec![GeneFeatureV1 {
            id: "ENSG000001".into(),
            symbol: Some("GENE1".into()),
            contig: "4".into(),
            start: 68_100_000,
            end: 68_900_000,
            strand: GeneStrandV1::Plus,
        }],
        message: None,
    };
    let overlay = reduce_region_overlay(&identity, points, &selected, genes).unwrap();

    let counts = AssociationRegionCountsV1 {
        source_point_count: overlay.aggregation.input_point_count,
        rasterized_point_count: overlay.aggregation.rasterized_point_count,
        significant_point_count: overlay.aggregation.protected_significant_count,
        coding_point_count: overlay.aggregation.protected_coding_count,
        selected_point_count: overlay.aggregation.protected_selected_count,
        overlay_point_count: overlay.aggregation.overlay_point_count,
    };
    let resource = |url: &str, media_type: &str| ResourceDescriptorV1 {
        url: url.into(),
        media_type: media_type.into(),
        byte_size: None,
        checksum: None,
        expires_at: None,
    };
    let manifest = AssociationRegionRenderManifestV1 {
        schema_version: ASSOCIATION_REGION_SCHEMA_VERSION_V1.into(),
        dataset_id,
        title: "Height locus".into(),
        assembly: "GRCh38".into(),
        coordinate_convention: COORDINATE_CONVENTION_V1.into(),
        source_fingerprint: "sha256:fixture".into(),
        layout,
        layout_hash: identity.layout_hash().into(),
        background_id: "white".into(),
        raster_hash: identity.raster_hash().into(),
        render_hash: identity.render_hash().into(),
        overlay_hash: overlay.overlay_hash.clone(),
        significance_threshold: 5e-8,
        counts,
        aggregation: overlay.aggregation.clone(),
        resources: AssociationRegionResourcesV1 {
            raster: resource("region/raster/fixture.png", "image/png"),
            overlay: resource("region/overlay/fixture.json", "application/json"),
        },
    };
    overlay.validate_against(&manifest).unwrap();

    match std::env::args().nth(1).as_deref() {
        Some("manifest") => println!("{}", serde_json::to_string_pretty(&manifest).unwrap()),
        Some("overlay") => println!("{}", serde_json::to_string_pretty(&overlay).unwrap()),
        _ => panic!("expected manifest or overlay"),
    }
}
