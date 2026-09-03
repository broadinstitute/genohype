use genohype_association::AssociationVisualizationManifestV1;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

#[test]
fn legacy_manhattan_writes_canonical_manifest_and_keeps_sidecar() {
    let temp = tempfile::tempdir().unwrap();
    let output_prefix = temp.path().join("fixture-plot");
    let output = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .args([
            "manhattan",
            "--table",
            fixture_path().to_str().unwrap(),
            "--output",
            output_prefix.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .expect("failed to run legacy Manhattan command");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let png_path = output_prefix.with_extension("png");
    let sidecar_path = output_prefix.with_extension("json");
    let manifest_path = PathBuf::from(format!("{}.manifest.json", output_prefix.display()));
    assert!(png_path.is_file());
    assert!(sidecar_path.is_file(), "legacy JSON sidecar must remain");
    let sidecar: serde_json::Value =
        serde_json::from_slice(&fs::read(&sidecar_path).unwrap()).unwrap();
    assert!(sidecar["significant_hits"].is_array());

    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.validate().unwrap();
    assert_eq!(manifest.schema_version, "1.0");
    assert_eq!(manifest.plot_id, "fixture-plot");
    assert_eq!(manifest.assembly, "GRCh38");
    assert_eq!(manifest.coordinate_convention, "1-based-inclusive");
    assert_eq!(manifest.thresholds.len(), 1);
    assert_eq!(manifest.thresholds[0].pvalue, 5e-8);
    assert_eq!(manifest.row_count, 12);
    assert_eq!(manifest.valid_row_count, 10);
    assert_eq!(manifest.skipped_row_count, 2);
    assert_eq!(manifest.rendered_point_count, 10);
    assert_eq!(manifest.significant_hit_count, 5);
    assert_eq!(manifest.top_hits.len(), 10);
    assert_eq!(manifest.warnings, ["pvalue_underflow"]);

    let tied: Vec<_> = manifest
        .top_hits
        .iter()
        .filter(|point| point.pvalue == 1e-12)
        .map(|point| (point.contig.as_str(), point.position, point.source_ordinal))
        .collect();
    assert_eq!(tied, [("1", 101_003, 2), ("4", 68_000_010, 6)]);

    assert_eq!(manifest.layout.width, 800);
    assert_eq!(manifest.layout.height, 400);
    assert_eq!(
        manifest.layout_hash,
        manifest.layout.layout_hash(&manifest.assembly)
    );
    assert_eq!(manifest.resources.raster.url, "fixture-plot.png");
    assert!(!manifest.resources.raster.url.contains('/'));
    let png = fs::read(&png_path).unwrap();
    assert_eq!(manifest.resources.raster.byte_size, Some(png.len() as u64));
    assert_eq!(
        manifest.resources.raster.checksum.as_deref(),
        Some(format!("sha256:{:x}", Sha256::digest(&png)).as_str())
    );
    assert!(manifest.source_fingerprint.starts_with("sha256:"));
    assert_eq!(manifest.source_fingerprint.len(), 71);
    assert!(!manifest
        .source_fingerprint
        .contains(fixture_path().to_str().unwrap()));
    assert!(manifest.created_at.ends_with('Z'));
    assert_eq!(manifest.created_at.len(), 20);

    // A different output location and parallel scheduling must not change source identity
    // or canonical top-hit order.
    let second_prefix = temp.path().join("second-plot");
    let second_output = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .args([
            "manhattan",
            "--table",
            fixture_path().to_str().unwrap(),
            "--output",
            second_prefix.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .unwrap();
    assert!(second_output.status.success());
    let second_manifest: AssociationVisualizationManifestV1 = serde_json::from_slice(
        &fs::read(format!("{}.manifest.json", second_prefix.display())).unwrap(),
    )
    .unwrap();
    assert_eq!(
        second_manifest.source_fingerprint,
        manifest.source_fingerprint
    );
    assert_eq!(second_manifest.top_hits, manifest.top_hits);
}
