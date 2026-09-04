#![cfg(unix)]

use genohype_association::AssociationVisualizationManifestV1;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

fn tsv_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.tsv")
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

struct RunningAssociationViewer {
    child: ChildGuard,
    url: url::Url,
    lines: Vec<String>,
    _stderr_rx: mpsc::Receiver<String>,
    cache_root: PathBuf,
}

impl RunningAssociationViewer {
    fn stop(mut self) -> ExitStatus {
        assert_eq!(
            unsafe { libc::kill(self.child.0.id() as i32, libc::SIGINT) },
            0
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.0.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "association viewer did not stop");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn output_text(&self) -> String {
        self.lines.join("\n")
    }
}

fn write_association_source(path: &Path) {
    fs::write(
        path,
        "CHR\tPOS\tP\tGENE\n1\t10\t0.5\tGENE1\n1\t20\t1e-9\tGENE2\n2\t30\tbad\tPRIVATE_REJECTED_VALUE\n",
    )
    .unwrap();
}

fn isolated_genohype_command(home: &Path) -> Command {
    fs::create_dir_all(home.join("Library/Caches")).unwrap();
    fs::create_dir_all(home.join("xdg-cache")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_genohype"));
    command
        .env("HOME", home)
        .env("XDG_CACHE_HOME", home.join("xdg-cache"));
    command
}

fn association_command_with_mapping(
    home: &Path,
    source: &Path,
    mapping: &str,
    extra: &[&str],
) -> Command {
    let mut command = isolated_genohype_command(home);
    command
        .args([
            "association",
            "view",
            source.to_str().unwrap(),
            "--build",
            "GRCh38",
            "--map",
            mapping,
            "--top-hits",
            "2",
            "--width",
            "320",
            "--height",
            "160",
            "--no-open",
            "--port",
            "0",
        ])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn association_command(home: &Path, source: &Path, extra: &[&str]) -> Command {
    association_command_with_mapping(
        home,
        source,
        "contig=CHR,position=POS,pvalue=P,gene=GENE",
        extra,
    )
}

fn inspect_emitted_view_command(home: &Path, source: &Path) -> Command {
    let mut inspect = isolated_genohype_command(home);
    let output = inspect
        .args([
            "association",
            "inspect",
            source.to_str().unwrap(),
            "--build",
            "GRCh38",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let emitted = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("genohype association view "))
        .expect("inspect-emitted view command");
    let words = emitted.split_ascii_whitespace().collect::<Vec<_>>();
    assert_eq!(words.first().copied(), Some("genohype"));

    let mut command = isolated_genohype_command(home);
    command
        .args(&words[1..])
        // Only process-lifecycle controls are added; every source, mapping, and
        // build option is exactly the command emitted by inspect.
        .args(["--no-open", "--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn start_association_viewer(
    home: &Path,
    source: &Path,
    extra: &[&str],
) -> RunningAssociationViewer {
    start_association_command(association_command(home, source, extra))
}

fn start_association_command(mut command: Command) -> RunningAssociationViewer {
    let mut child = command.spawn().unwrap();
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

    let child = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut lines = Vec::new();
    let mut cache_root = None;
    let url = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = line_rx.recv_timeout(remaining).unwrap_or_else(|_| {
            let stderr: Vec<_> = stderr_rx.try_iter().collect();
            panic!("timed out waiting for association viewer; stdout={lines:?}; stderr={stderr:?}")
        });
        if let Some(path) = line.strip_prefix("Association cache root: ") {
            cache_root = Some(PathBuf::from(path));
        }
        let found = line
            .strip_prefix("Viewer URL: ")
            .map(|value| url::Url::parse(value).unwrap());
        lines.push(line);
        if let Some(url) = found {
            break url;
        }
    };
    RunningAssociationViewer {
        child,
        url,
        lines,
        _stderr_rx: stderr_rx,
        cache_root: cache_root.expect("cache root announcement"),
    }
}

fn dataset_id(viewer: &RunningAssociationViewer) -> String {
    viewer
        .lines
        .iter()
        .find_map(|line| line.split_once("dataset_id=").map(|(_, id)| id.to_string()))
        .expect("dataset ID announcement")
}

fn completed_generation(cache_root: &Path, dataset_id: &str) -> PathBuf {
    cache_root.join("sessions").join(dataset_id)
}

#[tokio::test]
async fn association_view_cold_warm_rebuild_apis_cleanup_and_ephemeral_are_deterministic() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let source = temp.path().join("private-source.tsv");
    write_association_source(&source);

    let cold = start_association_viewer(&home, &source, &[]);
    let cold_output = cold.output_text();
    assert!(cold_output.contains("private local plaintext"));
    assert!(cold_output.contains("use --ephemeral"));
    assert!(cold_output.contains("Association session: cold build"));
    assert!(cold_output.contains("Rows: seen=3 valid=2 rejected=1"));
    assert!(!cold_output.contains("PRIVATE_REJECTED_VALUE"));
    let first_id = dataset_id(&cold);
    let first_generation = completed_generation(&cold.cache_root, &first_id);
    let first_marker = fs::read(first_generation.join("complete.json")).unwrap();

    let manifest_response = reqwest::get(cold.url.join("api/manifest").unwrap())
        .await
        .unwrap();
    assert_eq!(manifest_response.status(), reqwest::StatusCode::OK);
    let manifest_bytes = manifest_response.bytes().await.unwrap();
    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.plot_id, first_id);
    assert_eq!(manifest.title, "Association dataset");
    assert_eq!(manifest.row_count, 3);
    assert_eq!(manifest.valid_row_count, 2);
    assert_eq!(manifest.skipped_row_count, 1);
    let manifest_url = cold.url.join("api/manifest").unwrap();
    let table_url = manifest_url
        .join(&manifest.resources.table_api.as_ref().unwrap().url)
        .unwrap();
    let table_response = reqwest::get(table_url.clone()).await.unwrap();
    assert_eq!(table_response.status(), reqwest::StatusCode::OK);
    let table_bytes = table_response.bytes().await.unwrap();
    let table: serde_json::Value = serde_json::from_slice(&table_bytes).unwrap();
    assert_eq!(table["dataset_id"], first_id);
    assert_eq!(table["total"], 2);
    let region_url = manifest_url
        .join(&manifest.resources.region_api.as_ref().unwrap().url)
        .unwrap();
    let mut region_url = region_url;
    region_url.set_query(Some("contig=1&start=1&end=100&offset=0&limit=100"));
    let region_response = reqwest::get(region_url).await.unwrap();
    assert_eq!(region_response.status(), reqwest::StatusCode::OK);
    let region_bytes = region_response.bytes().await.unwrap();
    let region: serde_json::Value = serde_json::from_slice(&region_bytes).unwrap();
    assert_eq!(region["total"], 2);
    let wrong_id = "00000000-0000-4000-8000-000000000000";
    let wrong = table_url.as_str().replace(&first_id, wrong_id);
    assert_eq!(
        reqwest::get(wrong).await.unwrap().status(),
        reqwest::StatusCode::NOT_FOUND
    );
    for body in [&manifest_bytes[..], &table_bytes[..], &region_bytes[..]] {
        let body = String::from_utf8_lossy(body);
        assert!(!body.contains(source.to_str().unwrap()));
        assert!(!body.contains(cold.cache_root.to_str().unwrap()));
        assert!(!body.contains("PRIVATE_REJECTED_VALUE"));
    }
    assert!(cold.stop().success());

    let warm = start_association_viewer(&home, &source, &[]);
    assert!(warm
        .output_text()
        .contains("Association session: warm reuse"));
    assert_eq!(dataset_id(&warm), first_id);
    assert_eq!(
        fs::read(first_generation.join("complete.json")).unwrap(),
        first_marker
    );
    assert!(warm.stop().success());

    let rebuilt = start_association_viewer(&home, &source, &["--rebuild"]);
    assert!(rebuilt
        .output_text()
        .contains("Association session: cold build"));
    let second_id = dataset_id(&rebuilt);
    assert_ne!(second_id, first_id);
    assert!(first_generation.join("complete.json").is_file());
    assert!(completed_generation(&rebuilt.cache_root, &second_id)
        .join("complete.json")
        .is_file());
    assert!(rebuilt.stop().success());

    let cleaned = start_association_viewer(&home, &source, &["--cleanup-expired"]);
    assert!(cleaned.output_text().contains("sessions_removed=1"));
    assert_eq!(dataset_id(&cleaned), second_id);
    assert!(!first_generation.exists());
    let cache_root = cleaned.cache_root.clone();
    assert!(cleaned.stop().success());

    let ephemeral = start_association_viewer(&home, &source, &["--rebuild", "--ephemeral"]);
    assert!(ephemeral.output_text().contains("Retention: --ephemeral"));
    let ephemeral_generation = completed_generation(&ephemeral.cache_root, &dataset_id(&ephemeral));
    assert!(ephemeral_generation.is_dir());
    assert!(ephemeral.stop().success());
    assert!(!ephemeral_generation.exists());
    assert!(completed_generation(&cache_root, &second_id).is_dir());
}

#[test]
fn inspect_emitted_command_builds_and_reuses_the_checked_in_mixed_tsv() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let source = tsv_fixture_path();

    let cold = start_association_command(inspect_emitted_view_command(&home, &source));
    assert!(cold
        .output_text()
        .contains("Association session: cold build"));
    assert!(cold
        .output_text()
        .contains("Rows: seen=12 valid=10 rejected=2"));
    let first_id = dataset_id(&cold);
    assert!(cold.stop().success());

    let warm = start_association_command(inspect_emitted_view_command(&home, &source));
    assert!(warm
        .output_text()
        .contains("Association session: warm reuse"));
    assert_eq!(dataset_id(&warm), first_id);
    assert!(warm.stop().success());
}

#[test]
fn hail_view_cold_scan_and_warm_reuse_share_one_inventory_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let source = temp.path().join("single-variant.ht");
    assert!(!fixture_path().join("_SUCCESS").exists());
    copy_tree(&fixture_path(), &source);
    // Production Hail inventories require a completion marker. Keep that test-only
    // requirement in the temporary copy rather than changing the tracked fixture.
    fs::write(source.join("_SUCCESS"), b"").unwrap();
    let mapping = "contig=locus.contig,position=locus.position,pvalue=Pvalue,neg_log10_p=Pvalue_log10,beta=BETA,se=SE";

    let cold = start_association_command(association_command_with_mapping(
        &home,
        &source,
        mapping,
        &[],
    ));
    assert!(cold
        .output_text()
        .contains("Association session: cold build"));
    assert!(cold
        .output_text()
        .contains("Rows: seen=12 valid=10 rejected=2"));
    let first_id = dataset_id(&cold);
    assert!(cold.stop().success());

    let warm = start_association_command(association_command_with_mapping(
        &home,
        &source,
        mapping,
        &[],
    ));
    assert!(warm
        .output_text()
        .contains("Association session: warm reuse"));
    assert_eq!(dataset_id(&warm), first_id);
    assert!(warm.stop().success());
}

#[tokio::test]
async fn vcf_info_float_publishes_and_serves() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let source = temp.path().join("minimal.vcf");
    fs::write(
        &source,
        "##fileformat=VCFv4.2\n##INFO=<ID=P,Number=A,Type=Float,Description=\"P-value\">\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n1\t20\t.\tA\tG\t.\tPASS\tP=1e-9\n",
    )
    .unwrap();

    let viewer = start_association_command(association_command_with_mapping(
        &home,
        &source,
        "contig=CHROM,position=POS,ref=REF,alt=ALT,pvalue=info.P",
        &[],
    ));
    assert!(viewer
        .output_text()
        .contains("Association session: cold build"));
    assert!(viewer
        .output_text()
        .contains("Rows: seen=1 valid=1 rejected=0"));
    let manifest: AssociationVisualizationManifestV1 =
        reqwest::get(viewer.url.join("api/manifest").unwrap())
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(manifest.top_hits.len(), 1);
    assert!((manifest.top_hits[0].pvalue - 1e-9).abs() < 1e-15);
    assert!(viewer.stop().success());
}

#[test]
fn association_view_sigint_during_build_is_non_success_and_cleans_staging() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let source = temp.path().join("large.tsv");
    let mut contents = String::from("CHR\tPOS\tP\tGENE\n");
    for ordinal in 0..200_000_u64 {
        contents.push_str(&format!("1\t{}\t0.5\tGENE\n", ordinal + 1));
    }
    fs::write(&source, contents).unwrap();

    let mut child = association_command(&home, &source, &["--rebuild"])
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut stdout = BufReader::new(stdout);
    let mut startup = String::new();
    for _ in 0..3 {
        let mut line = String::new();
        assert!(stdout.read_line(&mut line).unwrap() > 0);
        startup.push_str(&line);
    }
    assert!(startup.contains("private local plaintext"));
    let cache_root = startup
        .lines()
        .find_map(|line| line.strip_prefix("Association cache root: "))
        .map(PathBuf::from)
        .unwrap();
    let staging = cache_root.join("staging");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            child.try_wait().unwrap().is_none(),
            "build exited before SIGINT"
        );
        let has_staging = match fs::read_dir(&staging) {
            Ok(mut entries) => entries.next().is_some(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => panic!("cannot inspect staging: {error}"),
        };
        if has_staging {
            break;
        }
        assert!(Instant::now() < deadline, "build never created staging");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    let status = child.wait().unwrap();
    assert!(!status.success());
    let mut remainder = String::new();
    stdout.read_to_string(&mut remainder).unwrap();
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        stderr.contains("cancelled") || remainder.contains("cancelled"),
        "stdout={startup}{remainder}; stderr={stderr}"
    );
    assert_eq!(fs::read_dir(cache_root.join("staging")).unwrap().count(), 0);
    assert_eq!(
        fs::read_dir(cache_root.join("sessions")).unwrap().count(),
        0
    );
}
