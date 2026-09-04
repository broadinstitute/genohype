#![cfg(unix)]

use genohype_association::AssociationVisualizationManifestV1;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            unsafe {
                libc::kill(self.0.id() as i32, libc::SIGINT);
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if self.0.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[tokio::test]
async fn legacy_view_process_serves_canonical_artifacts_and_exits_on_sigint() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("plot");
    let mut child = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .args([
            "manhattan",
            "--table",
            fixture_path().to_str().unwrap(),
            "--y-field",
            "Pvalue",
            "--output",
            output.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
            "--view",
            "--no-open",
            "--port",
            "0",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let (line_tx, line_rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line_tx.send(line.unwrap_or_default()).is_err() {
                break;
            }
        }
    });
    let (stderr_tx, stderr_rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            if stderr_tx.send(line.unwrap_or_default()).is_err() {
                break;
            }
        }
    });
    let mut child = ChildGuard(child);
    let mut printed = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let url = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = line_rx.recv_timeout(remaining).unwrap_or_else(|_| {
            let stderr: Vec<_> = stderr_rx.try_iter().collect();
            panic!("timed out waiting for viewer URL; stdout={printed:?}; stderr={stderr:?}")
        });
        printed.push(line.clone());
        if let Some(url) = line.strip_prefix("Viewer URL: ") {
            break url.to_string();
        }
    };
    let parsed = url::Url::parse(&url).unwrap();
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));
    assert_ne!(parsed.port().unwrap(), 0);
    assert!(parsed.path().split('/').any(|segment| segment.len() == 36));

    let root = reqwest::get(url.clone()).await.unwrap();
    assert_eq!(root.status(), reqwest::StatusCode::OK);
    assert!(root.text().await.unwrap().contains("<!doctype html>"));

    let manifest_response = reqwest::get(parsed.join("api/manifest").unwrap())
        .await
        .unwrap();
    assert_eq!(manifest_response.status(), reqwest::StatusCode::OK);
    let manifest: AssociationVisualizationManifestV1 = manifest_response.json().await.unwrap();
    manifest.validate().unwrap();
    assert_eq!(manifest.resources.raster.url, "raster");
    let manifest_url = parsed.join("api/manifest").unwrap();
    let raster_response = reqwest::get(manifest_url.join(&manifest.resources.raster.url).unwrap())
        .await
        .unwrap();
    assert_eq!(raster_response.status(), reqwest::StatusCode::OK);
    assert_eq!(raster_response.headers()["content-type"], "image/png");
    let raster = raster_response.bytes().await.unwrap();
    assert_eq!(
        manifest.resources.raster.byte_size,
        Some(raster.len() as u64)
    );
    assert_eq!(
        manifest.resources.raster.checksum.as_deref(),
        Some(format!("sha256:{:x}", Sha256::digest(&raster)).as_str())
    );
    assert_eq!(
        raster.as_ref(),
        std::fs::read(output.with_extension("png")).unwrap()
    );

    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGINT) }, 0);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "viewer did not stop after SIGINT"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert!(status.success(), "viewer exited with {status}");
}

#[test]
fn incompatible_view_modes_fail_before_generation() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture_path();
    let cases: Vec<(Vec<String>, bool)> = vec![
        (vec!["--view".into()], true),
        (
            vec![
                "--view".into(),
                "--exome".into(),
                fixture.display().to_string(),
            ],
            true,
        ),
        (
            vec![
                "--view".into(),
                "--table".into(),
                fixture.display().to_string(),
                "--total-workers".into(),
                "2".into(),
            ],
            true,
        ),
        (
            vec![
                "--view".into(),
                "--table".into(),
                fixture.display().to_string(),
                "--output".into(),
                "gs://private/output".into(),
            ],
            false,
        ),
    ];
    for (index, (args, add_output)) in cases.into_iter().enumerate() {
        let output = temp.path().join(format!("rejected-{index}"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_genohype"));
        command.arg("manhattan").args(args);
        if add_output {
            command.args(["--output", output.to_str().unwrap()]);
        }
        let result = command.output().unwrap();
        assert!(
            !result.status.success(),
            "case {index} unexpectedly succeeded"
        );
        assert!(!output.with_extension("png").exists());
    }
}

#[test]
fn ordinary_legacy_manhattan_remains_nonblocking() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("ordinary");
    let result = Command::new(env!("CARGO_BIN_EXE_genohype"))
        .args([
            "manhattan",
            "--table",
            fixture_path().to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--width",
            "800",
            "--height",
            "400",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(output.with_extension("png").is_file());
    assert!(PathBuf::from(format!("{}.manifest.json", output.display())).is_file());
}
