use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use genohype_association::AssociationVisualizationManifestV1;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn rewrite_gzip_json(path: &Path, update: impl FnOnce(&mut serde_json::Value)) {
    let mut decoded = String::new();
    GzDecoder::new(fs::File::open(path).unwrap())
        .read_to_string(&mut decoded)
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&decoded).unwrap();
    update(&mut value);
    let mut encoder = GzEncoder::new(fs::File::create(path).unwrap(), Compression::default());
    encoder
        .write_all(serde_json::to_string(&value).unwrap().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
}

fn two_partition_fixture(destination: &Path) {
    copy_tree(&fixture_path(), destination);
    fs::copy(
        destination.join("rows/parts/part-0"),
        destination.join("rows/parts/part-1"),
    )
    .unwrap();
    copy_tree(
        &destination.join("index/part-0.idx"),
        &destination.join("index/part-1.idx"),
    );
    rewrite_gzip_json(&destination.join("metadata.json.gz"), |value| {
        value["components"]["partition_counts"]["counts"] = serde_json::json!([12, 12]);
    });
    rewrite_gzip_json(&destination.join("rows/metadata.json.gz"), |value| {
        value["_partFiles"] = serde_json::json!(["part-0", "part-1"]);
        let bound = value["_jRangeBounds"][0].clone();
        value["_jRangeBounds"] = serde_json::json!([bound.clone(), bound]);
    });
}

fn read_manifest(path: impl AsRef<Path>) -> AssociationVisualizationManifestV1 {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
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

#[test]
fn integrated_multi_partition_identity_matches_legacy_and_is_deterministic() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = temp.path().join("two-partition.ht");
    two_partition_fixture(&fixture);

    let legacy_stem = temp.path().join("legacy");
    let legacy = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .env("RAYON_NUM_THREADS", "2")
        .args([
            "manhattan",
            "--table",
            fixture.to_str().unwrap(),
            "--output",
            legacy_stem.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .unwrap();
    assert!(
        legacy.status.success(),
        "{}",
        String::from_utf8_lossy(&legacy.stderr)
    );
    let legacy_manifest = read_manifest(format!("{}.manifest.json", legacy_stem.display()));
    let legacy_png = fs::read(legacy_stem.with_extension("png")).unwrap();
    let legacy_sidecar = fs::read(legacy_stem.with_extension("json")).unwrap();

    // Two source partitions produce two concurrently rendered in-memory shards. Their explicit
    // source-order merge must make both the raster bytes and sidecar hit order repeatable.
    let repeated_stem = temp.path().join("legacy-repeated");
    let repeated = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .env("RAYON_NUM_THREADS", "2")
        .args([
            "manhattan",
            "--table",
            fixture.to_str().unwrap(),
            "--output",
            repeated_stem.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .unwrap();
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    let repeated_manifest = read_manifest(format!("{}.manifest.json", repeated_stem.display()));
    assert_eq!(repeated_manifest.row_count, legacy_manifest.row_count);
    assert_eq!(repeated_manifest.top_hits, legacy_manifest.top_hits);
    assert_eq!(
        repeated_manifest.source_fingerprint,
        legacy_manifest.source_fingerprint
    );
    assert_eq!(
        fs::read(repeated_stem.with_extension("png")).unwrap(),
        legacy_png
    );
    assert_eq!(
        fs::read(repeated_stem.with_extension("json")).unwrap(),
        legacy_sidecar
    );

    let mut integrated_manifests = Vec::new();
    let mut integrated_rasters = Vec::new();
    for run in ["integrated-a", "integrated-b"] {
        let output = temp.path().join(run);
        let result = Command::new(env!("CARGO_BIN_EXE_genohype"))
            .args([
                "manhattan",
                "--exome",
                fixture.to_str().unwrap(),
                "--output",
                output.to_str().unwrap(),
                "--width",
                "800",
                "--height",
                "400",
                "--scan-only",
            ])
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        integrated_manifests.push(read_manifest(
            output.join("plots/exome_manhattan.manifest.json"),
        ));
        integrated_rasters.push(fs::read(output.join("plots/exome_manhattan.png")).unwrap());
    }

    for manifest in &integrated_manifests {
        assert_eq!(manifest.row_count, 24);
        assert_eq!(
            manifest.source_fingerprint,
            legacy_manifest.source_fingerprint
        );
        assert_eq!(manifest.top_hits, legacy_manifest.top_hits);
        assert!(manifest
            .top_hits
            .iter()
            .any(|point| point.source_ordinal >> 32 == 1));
    }
    assert_eq!(
        integrated_manifests[0].top_hits,
        integrated_manifests[1].top_hits
    );
    assert_eq!(
        integrated_manifests[0].source_fingerprint,
        integrated_manifests[1].source_fingerprint
    );
    assert_eq!(integrated_rasters[0], integrated_rasters[1]);
}

#[test]
fn legacy_limit_is_the_exact_partition_then_row_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = temp.path().join("two-partition.ht");
    two_partition_fixture(&fixture);

    let full_first_partition = temp.path().join("single");
    let single = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .args([
            "manhattan",
            "--table",
            fixture_path().to_str().unwrap(),
            "--output",
            full_first_partition.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .unwrap();
    assert!(single.status.success());

    let limited_prefix = temp.path().join("limited");
    let limited = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .env("RAYON_NUM_THREADS", "4")
        .args([
            "manhattan",
            "--table",
            fixture.to_str().unwrap(),
            "--limit",
            "12",
            "--output",
            limited_prefix.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .unwrap();
    assert!(
        limited.status.success(),
        "{}",
        String::from_utf8_lossy(&limited.stderr)
    );

    let expected = read_manifest(format!("{}.manifest.json", full_first_partition.display()));
    let actual = read_manifest(format!("{}.manifest.json", limited_prefix.display()));
    assert_eq!(actual.row_count, 12);
    assert_eq!(actual.valid_row_count, expected.valid_row_count);
    assert_eq!(actual.skipped_row_count, expected.skipped_row_count);
    assert_eq!(actual.rendered_point_count, expected.rendered_point_count);
    assert_eq!(actual.significant_hit_count, expected.significant_hit_count);
    assert_eq!(actual.top_hits, expected.top_hits);
    assert_eq!(actual.source_fingerprint, expected.source_fingerprint);
    assert_eq!(
        fs::read(limited_prefix.with_extension("png")).unwrap(),
        fs::read(full_first_partition.with_extension("png")).unwrap()
    );
    assert_eq!(
        fs::read(limited_prefix.with_extension("json")).unwrap(),
        fs::read(full_first_partition.with_extension("json")).unwrap()
    );
}
