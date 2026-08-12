use std::process::Command;

fn fixture_path() -> String {
    format!(
        "{}/../core/tests/fixtures/tiny-keyed.ht",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn run_info(extra_args: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_genohype"));
    command.arg("info");
    command.args(extra_args);
    command.arg("--no-cache");
    command.arg(fixture_path());
    command.output().expect("failed to run genohype info")
}

#[test]
fn json_info_reports_versions_references_count_and_raw_hail_types() {
    let output = run_info(&["--json"]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let info: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(info["file_version"], 67328);
    assert_eq!(info["hail_version"], "0.2.137-733ac4ccd943");
    assert_eq!(info["references_rel_path"], "references");
    assert_eq!(info["total_rows"], 2);
    assert!(info["row_codec"]["physical_type"].as_str().is_some());
    assert!(info["row_codec"]["logical_type"].as_str().is_some());
    assert!(info["globals_codec"]["physical_type"].as_str().is_some());
    assert!(info["globals_codec"]["logical_type"].as_str().is_some());
    assert_eq!(info["row_codec"]["data_decode_supported"], true);
}

#[test]
fn text_info_reports_versions_references_fast_count_and_type_kinds() {
    let output = run_info(&[]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let text = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "Format version:",
        "67328",
        "Hail version:",
        "0.2.137-733ac4ccd943",
        "References:",
        "references",
        "Total Rows:",
        "2",
        "Row Logical Type (_vType):",
        "Row Physical Type (_eType):",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }
}
