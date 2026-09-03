use genohype_association::{
    AssociationPointV1, AssociationThresholdV1, AssociationVisualizationManifestV1,
    AssociationVisualizationResourcesV1, GenomeLayoutV1, PlotKindV1, ResourceDescriptorV1,
    COORDINATE_CONVENTION_V1, SCHEMA_VERSION_V1,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let layout = GenomeLayoutV1::new([("chr2", 80), ("chr1", 100)], 96, 48, 4)?;
    let layout_hash = layout.layout_hash("GRCh38");
    let manifest = AssociationVisualizationManifestV1 {
        schema_version: SCHEMA_VERSION_V1.into(),
        plot_id: "height-grch38".into(),
        kind: PlotKindV1::Manhattan,
        title: "Height associations".into(),
        assembly: "GRCh38".into(),
        coordinate_convention: COORDINATE_CONVENTION_V1.into(),
        source_fingerprint: "sha256:0123456789abcdef".into(),
        genohype_version: "0.1.0-test".into(),
        created_at: "2026-09-03T00:00:00Z".into(),
        layout,
        layout_hash,
        thresholds: vec![AssociationThresholdV1 {
            name: "genome_wide".into(),
            pvalue: 5e-8,
        }],
        row_count: 3,
        valid_row_count: 2,
        skipped_row_count: 1,
        rendered_point_count: 2,
        significant_hit_count: 1,
        top_hits: vec![AssociationPointV1 {
            contig: "1".into(),
            position: 42,
            ref_allele: Some("A".into()),
            alt_allele: Some("G".into()),
            pvalue: 5e-9,
            neg_log10_p: 8.301029995663981,
            beta: Some(0.25),
            standard_error: Some(0.05),
            allele_frequency: Some(0.1),
            allele_count: Some(20.0),
            gene_symbol: Some("TEST1".into()),
            consequence: Some("missense".into()),
            series: Some("genome".into()),
            source_ordinal: 1,
        }],
        warnings: vec!["1 invalid p-value row skipped".into()],
        resources: AssociationVisualizationResourcesV1 {
            raster: ResourceDescriptorV1 {
                url: "height.png".into(),
                media_type: "image/png".into(),
                byte_size: Some(1234),
                checksum: Some("sha256:beef".into()),
                expires_at: None,
            },
            region_api: Some(ResourceDescriptorV1 {
                url: "api/region".into(),
                media_type: "application/json".into(),
                byte_size: None,
                checksum: None,
                expires_at: Some("2026-09-04T00:00:00Z".into()),
            }),
            table_api: None,
        },
    };
    manifest.validate()?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}
