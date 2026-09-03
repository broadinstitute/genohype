use genohype_association::AssociationVisualizationManifestV1;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

fn run_integrated(source_flag: &str, output_dir: &Path, mode: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_genohype"))
        .args([
            "manhattan",
            source_flag,
            fixture_path().to_str().unwrap(),
            "--output",
            output_dir.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
            mode,
        ])
        .output()
        .expect("failed to run integrated Manhattan command")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn read_and_assert_manifest(output_dir: &Path, source: &str) -> AssociationVisualizationManifestV1 {
    let raster_name = format!("{source}_manhattan.png");
    let raster_path = output_dir.join("plots").join(&raster_name);
    let manifest_path = output_dir
        .join("plots")
        .join(format!("{source}_manhattan.manifest.json"));
    assert!(raster_path.is_file());
    assert!(manifest_path.is_file());

    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
    manifest.validate().unwrap();
    assert_eq!(manifest.schema_version, "1.0");
    assert_eq!(manifest.assembly, "GRCh38");
    assert_eq!(manifest.coordinate_convention, "1-based-inclusive");
    assert_eq!(manifest.plot_id, format!("{source}_manhattan"));
    assert_eq!(manifest.resources.raster.url, raster_name);
    assert_eq!(manifest.layout.width, 800);
    assert_eq!(manifest.layout.height, 400);
    assert_eq!(manifest.layout.contigs.len(), 25);
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

    let raster = fs::read(raster_path).unwrap();
    assert_eq!(
        manifest.resources.raster.byte_size,
        Some(raster.len() as u64)
    );
    assert_eq!(
        manifest.resources.raster.checksum.as_deref(),
        Some(format!("sha256:{:x}", Sha256::digest(&raster)).as_str())
    );
    manifest
}

#[test]
fn integrated_exome_scan_publishes_v1_manifest_and_aggregate_only_preserves_hits() {
    let temp = tempfile::tempdir().unwrap();
    let output_dir = temp.path().join("exome-output");
    let scan = run_integrated("--exome", &output_dir, "--scan-only");
    assert_success(&scan);
    read_and_assert_manifest(&output_dir, "exome");

    let significant_path = output_dir.join("significant.parquet");
    let before = fs::read(&significant_path).unwrap();
    let aggregate = run_integrated("--exome", &output_dir, "--aggregate-only");
    assert_success(&aggregate);
    assert_eq!(fs::read(significant_path).unwrap(), before);
}

#[test]
fn integrated_genome_scan_publishes_same_v1_validation_and_count_semantics() {
    let temp = tempfile::tempdir().unwrap();
    let output_dir = temp.path().join("genome-output");
    let scan = run_integrated("--genome", &output_dir, "--scan-only");
    assert_success(&scan);
    read_and_assert_manifest(&output_dir, "genome");
}
