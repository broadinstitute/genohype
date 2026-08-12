use genohype_core::io::{
    inventory_hail_table_path, InventoryAccess, InventoryOptions, SourceManifest,
};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_genohype")
}

fn full_fixture() -> PathBuf {
    fs::canonicalize(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test_hail_data/01_single_int.ht"),
    )
    .unwrap()
}

fn tiny_fixture() -> PathBuf {
    fs::canonicalize(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../core/tests/fixtures/tiny-keyed.ht"),
    )
    .unwrap()
}

fn run(args: &[&str]) -> Output {
    Command::new(binary()).args(args).output().unwrap()
}

#[test]
fn sequential_writes_canonical_manifest_and_digest_without_overwrite() {
    let temp = tempfile::tempdir().unwrap();
    let manifest_path = temp.path().join("source.json");
    let table = full_fixture();
    let output = run(&[
        "manifest",
        "inventory",
        table.to_str().unwrap(),
        "--output",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let bytes = fs::read(&manifest_path).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    let manifest = SourceManifest::from_json(&bytes).unwrap();
    assert_eq!(manifest.objects.len(), 17);
    let expected =
        inventory_hail_table_path(table.to_str().unwrap(), &InventoryOptions::default()).unwrap();
    assert_eq!(
        manifest.canonical_bytes().unwrap(),
        expected.manifest.canonical_bytes().unwrap()
    );

    let digest_path = PathBuf::from(format!("{}.sha256", manifest_path.display()));
    let digest = fs::read_to_string(&digest_path).unwrap();
    assert_eq!(
        digest,
        format!(
            "genohype.source-manifest.sha256.v1 {}\n",
            expected.digest_sha256
        )
    );

    let second = run(&[
        "manifest",
        "inventory",
        table.to_str().unwrap(),
        "--output",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("refusing to overwrite"));
}

#[test]
fn indexed_stdout_is_machine_clean_and_digest_is_on_stderr() {
    let table = full_fixture();
    let output = run(&[
        "manifest",
        "inventory",
        table.to_str().unwrap(),
        "--output",
        "-",
        "--access",
        "indexed",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = SourceManifest::from_json(&output.stdout).unwrap();
    assert_eq!(manifest.objects.len(), 41);
    let expected = inventory_hail_table_path(
        table.to_str().unwrap(),
        &InventoryOptions {
            access: InventoryAccess::Indexed,
            ..InventoryOptions::default()
        },
    )
    .unwrap();
    assert_eq!(manifest.digest_sha256().unwrap(), expected.digest_sha256);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&expected.digest_sha256), "{stderr}");
}

#[test]
fn failures_create_no_output() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = temp.path().join("source.json");

    let missing_success = run(&[
        "manifest",
        "inventory",
        tiny_fixture().to_str().unwrap(),
        "--output",
        manifest.to_str().unwrap(),
    ]);
    assert!(!missing_success.status.success());
    assert!(String::from_utf8_lossy(&missing_success.stderr).contains("_SUCCESS"));
    assert!(!manifest.exists());

    let unsupported = run(&[
        "manifest",
        "inventory",
        "s3://bucket/table.ht",
        "--output",
        manifest.to_str().unwrap(),
    ]);
    assert!(!unsupported.status.success());
    assert!(String::from_utf8_lossy(&unsupported.stderr)
        .contains("unsupported by source-manifest inventory v1"));
    assert!(!manifest.exists());

    let too_small = run(&[
        "manifest",
        "inventory",
        full_fixture().to_str().unwrap(),
        "--output",
        manifest.to_str().unwrap(),
        "--max-objects",
        "1",
    ]);
    assert!(!too_small.status.success());
    assert!(String::from_utf8_lossy(&too_small.stderr).contains("max_objects"));
    assert!(!manifest.exists());
}

#[test]
fn identical_output_paths_and_cloud_outputs_are_rejected_before_inventory() {
    let temp = tempfile::tempdir().unwrap();
    let same = temp.path().join("same");
    let output = run(&[
        "manifest",
        "inventory",
        full_fixture().to_str().unwrap(),
        "--output",
        same.to_str().unwrap(),
        "--digest-output",
        same.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be distinct"));
    assert!(!same.exists());

    let cloud = run(&[
        "manifest",
        "inventory",
        full_fixture().to_str().unwrap(),
        "--output",
        "gs://bucket/output.json",
    ]);
    assert!(!cloud.status.success());
    assert!(String::from_utf8_lossy(&cloud.stderr).contains("outputs must be local"));
}
