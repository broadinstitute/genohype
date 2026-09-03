use genohype_association::{AssociationPointV1, AssociationRasterizer, Background, GenomeLayoutV1};
use sha2::{Digest, Sha256};

fn point(contig: &str, position: i64, significance: f64, ordinal: u64) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: contig.into(),
        position,
        ref_allele: None,
        alt_allele: None,
        pvalue: if significance > 300.0 {
            0.0
        } else {
            10_f64.powf(-significance)
        },
        neg_log10_p: significance,
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

fn render(significance: f64) -> AssociationRasterizer {
    let layout = GenomeLayoutV1::new([("1", 100), ("2", 80)], 96, 48, 4).unwrap();
    let points = [
        point("chr1", 1, 2.0, 0),
        point("1", 50, significance, 1),
        point("2", 80, 20.0, 2),
    ];
    let mut raster = AssociationRasterizer::from_layout(&layout, Background::White).unwrap();
    assert_eq!(raster.render_points(&layout, &points, 2.5, 178).unwrap(), 3);
    raster.render_threshold(&layout, 5e-8, "#B71C1C").unwrap();
    raster
}

#[test]
fn fixed_raster_has_stable_dimensions_pixel_digest_and_png_bytes() {
    let raster = render(8.0);
    assert_eq!((raster.width(), raster.height()), (96, 48));
    assert_eq!(raster.raster_bytes().len(), 96 * 48 * 4);
    assert_eq!(
        format!("{:x}", Sha256::digest(raster.raster_bytes())),
        "d088793499cc58fd6feb0cf83fc0d5f2c49753f1967b1783efdaf75b22727f2f"
    );
    let png = raster.png_bytes().unwrap();
    assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
}

#[test]
fn raster_clips_only_at_display_time() {
    assert_eq!(render(350.0).raster_bytes(), render(999.0).raster_bytes());
}
