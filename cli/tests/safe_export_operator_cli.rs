use genohype_core::codec::{ETypeParser, EncodedValue};
use genohype_core::io::{
    build_attempt_staging_location, content_addressed_path, SourceManifest, SourceObject,
};
use genohype_core::metadata::BufferSpec;
use genohype_core::release::{
    ComponentDescriptor, ExportPartReport, GlobalsPayload, KeyDescriptor, KeyOrdering,
    ParquetDescriptor, PhysicalTypeDescriptor, ReleaseSourceSnapshot, ReleaseStatus, ReleaseTotals,
    SafeExportPartReceipt, SafeExportReceiptIdentity, SafeExportSpec, SafeExportSuccess,
    SafeExportWorkerIdentity, SourcePartition, TableDescriptor, TableReleaseSidecar, ToolIdentity,
    WriterPropertiesDescriptor, SAFE_EXPORT_SHARDING_RULE_V1,
};
use genohype_core::schema::{PairedSchema, VTypeParser};
use object_store::path::Path as ObjectPath;
use sha2::{Digest, Sha256};
use std::fs;
use std::process::{Command, Output};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_genohype")
}

fn run(args: &[&str]) -> Output {
    Command::new(binary()).args(args).output().unwrap()
}

fn seal_args<'a>(manifest: &'a str, worker: &'a str, output: &'a str) -> Vec<&'a str> {
    vec![
        "safe-export",
        "seal",
        "--table",
        "gs://source/table.ht",
        "--manifest",
        manifest,
        "--manifest-uri",
        "gs://authority/manifests/source.json",
        "--snapshot-timestamp",
        "2026-08-13T12:00:00Z",
        "--output-root",
        "gs://output/release",
        "--worker-binary",
        worker,
        "--worker-binary-uri",
        "gs://authority/bin/genohype",
        "--worker-git-commit",
        "abc1234",
        "--worker-build-identity",
        "0.1.0:abc1234",
        "--worker-feature",
        "gcp",
        "--output",
        output,
    ]
}

#[test]
fn seal_refuses_overwrite_before_reading_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("spec.json");
    fs::write(&output, b"keep\n").unwrap();
    let result = run(&seal_args(
        "/missing/manifest.json",
        "/missing/worker",
        output.to_str().unwrap(),
    ));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("refusing to overwrite"));
    assert_eq!(fs::read(&output).unwrap(), b"keep\n");
}

#[test]
fn malformed_manifest_creates_no_spec() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = temp.path().join("manifest.json");
    let worker = temp.path().join("worker");
    let output = temp.path().join("spec.json");
    fs::write(&manifest, b"{not-json").unwrap();
    fs::write(&worker, b"not-an-elf").unwrap();
    let result = run(&seal_args(
        manifest.to_str().unwrap(),
        worker.to_str().unwrap(),
        output.to_str().unwrap(),
    ));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("source manifest JSON"));
    assert!(!output.exists());
}

fn write_mock_worker(path: &std::path::Path, identity_byte: u8) {
    let mut bytes = vec![identity_byte; 64];
    bytes[..4].copy_from_slice(b"\x7fELF");
    bytes[4] = 2;
    bytes[5] = 1;
    bytes[18..20].copy_from_slice(&62_u16.to_le_bytes());
    bytes.extend_from_slice(b"abc1234");
    fs::write(path, bytes).unwrap();
}

fn black_box_seal(
    directory: &std::path::Path,
    name: &str,
    source: SourceObject,
    output_root: &str,
    worker_identity_byte: u8,
) -> SafeExportSpec {
    let base = unsealed_spec();
    let manifest = SourceManifest::new(vec![source]).unwrap();
    let manifest_path = directory.join(format!("{name}-manifest.json"));
    let metadata_path = directory.join(format!("{name}-metadata.json"));
    let worker_path = directory.join(format!("{name}-worker"));
    let output_path = directory.join(format!("{name}-spec.json"));
    fs::write(&manifest_path, manifest.canonical_bytes().unwrap()).unwrap();
    fs::write(
        &metadata_path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "canonical_table_uri": base.canonical_table_uri,
            "source_manifest_sha256": manifest.digest_sha256().unwrap(),
            "table": base.table,
            "globals": base.globals,
        }))
        .unwrap(),
    )
    .unwrap();
    write_mock_worker(&worker_path, worker_identity_byte);

    let manifest_arg = manifest_path.to_str().unwrap().to_string();
    let worker_arg = worker_path.to_str().unwrap().to_string();
    let output_arg = output_path.to_str().unwrap().to_string();
    let metadata_arg = metadata_path.to_str().unwrap().to_string();
    let mut args = seal_args(&manifest_arg, &worker_arg, &output_arg);
    let root_index = args.iter().position(|arg| *arg == "--output-root").unwrap() + 1;
    args[root_index] = output_root;
    args.push("--metadata-evidence-bundle");
    args.push(&metadata_arg);
    let result = run(&args);
    assert!(
        result.status.success(),
        "seal failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    SafeExportSpec::from_json(&fs::read(output_path).unwrap()).unwrap()
}

#[test]
fn black_box_seal_is_deterministic_and_binds_source_output_and_worker() {
    let temp = tempfile::tempdir().unwrap();
    let original_source = object("gs://source/table.ht/rows/parts/part-0");
    let first = black_box_seal(
        temp.path(),
        "first",
        original_source.clone(),
        "gs://output/release",
        0,
    );
    let again = black_box_seal(
        temp.path(),
        "again",
        original_source.clone(),
        "gs://output/release",
        0,
    );
    assert_eq!(
        fs::read(temp.path().join("first-spec.json")).unwrap(),
        fs::read(temp.path().join("again-spec.json")).unwrap()
    );
    assert_eq!(first.export_id, again.export_id);

    let mut changed_source = original_source.clone();
    changed_source.version = "8".into();
    changed_source.etag = "etag-8".into();
    let source = black_box_seal(
        temp.path(),
        "source",
        changed_source,
        "gs://output/release",
        0,
    );
    let output = black_box_seal(
        temp.path(),
        "output",
        original_source.clone(),
        "gs://output/other",
        0,
    );
    let worker = black_box_seal(
        temp.path(),
        "worker",
        original_source,
        "gs://output/release",
        1,
    );
    assert_ne!(source.export_id, first.export_id);
    assert_ne!(output.export_id, first.export_id);
    assert_ne!(worker.export_id, first.export_id);
}

#[test]
fn standalone_validate_refuses_missing_exact_success_without_output() {
    let temp = tempfile::tempdir().unwrap();
    let spec_path = temp.path().join("spec.json");
    let bundle_path = temp.path().join("bundle.json");
    let report_path = temp.path().join("report.json");
    fs::write(&spec_path, serde_json::to_vec(&spec()).unwrap()).unwrap();
    fs::write(&bundle_path, br#"{"schema_version":1,"objects":[]}"#).unwrap();

    let result = run(&[
        "safe-export",
        "validate",
        "--spec",
        spec_path.to_str().unwrap(),
        "--evidence-bundle",
        bundle_path.to_str().unwrap(),
        "--output",
        report_path.to_str().unwrap(),
    ]);
    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("_SUCCESS"), "{stderr}");
    assert!(!report_path.exists());
}

#[test]
fn standalone_validate_refuses_wrong_release_generation() {
    let temp = tempfile::tempdir().unwrap();
    let spec = spec();
    let spec_path = temp.path().join("spec.json");
    let bundle_path = temp.path().join("bundle.json");
    let report_path = temp.path().join("report.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();

    let release_bytes = b"{}";
    let release = SourceObject {
        uri: spec.release_uri().unwrap(),
        version: "wanted-generation".into(),
        etag: "wanted-etag".into(),
        size: release_bytes.len() as u64,
        crc32c: None,
        sha256: Some(format!("{:x}", Sha256::digest(release_bytes))),
    };
    let success = SafeExportSuccess {
        schema_version: 1,
        export_id: spec.export_id.clone(),
        release_object: release,
        receipts: vec![SafeExportReceiptIdentity {
            partition_index: 0,
            receipt_object: SourceObject {
                uri: spec.part_receipt_uri(0).unwrap(),
                version: "receipt-generation".into(),
                etag: "receipt-etag".into(),
                size: 1,
                crc32c: None,
                sha256: Some("d".repeat(64)),
            },
        }],
        receipt_set_sha256: "e".repeat(64),
    };
    let success_bytes = success.canonical_bytes(&spec).unwrap();
    let success_path = UrlPath::from_uri(&spec.success_uri().unwrap());
    let release_path = UrlPath::from_uri(&spec.release_uri().unwrap());
    let bundle = serde_json::json!({
        "schema_version": 1,
        "objects": [
            {
                "path": success_path,
                "version": "success-generation",
                "etag": "success-etag",
                "bytes_hex": hex(&success_bytes),
            },
            {
                "path": release_path,
                "version": "wrong-generation",
                "etag": "wrong-etag",
                "bytes_hex": hex(release_bytes),
            }
        ]
    });
    fs::write(&bundle_path, serde_json::to_vec(&bundle).unwrap()).unwrap();

    let result = run(&[
        "safe-export",
        "validate",
        "--spec",
        spec_path.to_str().unwrap(),
        "--evidence-bundle",
        bundle_path.to_str().unwrap(),
        "--output",
        report_path.to_str().unwrap(),
    ]);
    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("release identity/size/SHA"), "{stderr}");
    assert!(!report_path.exists());
}

fn evidence_object(uri: &str, version: &str, etag: &str, bytes: &[u8]) -> SourceObject {
    SourceObject {
        uri: uri.to_string(),
        version: version.to_string(),
        etag: etag.to_string(),
        size: bytes.len() as u64,
        crc32c: None,
        sha256: Some(format!("{:x}", Sha256::digest(bytes))),
    }
}

fn successful_evidence_bundle(spec: &SafeExportSpec) -> serde_json::Value {
    let mut tasks = spec.sealed_part_tasks().unwrap();
    let task = tasks.remove(0);
    let output_bytes = b"bounded local parquet evidence";
    let output_sha = format!("{:x}", Sha256::digest(output_bytes));
    let output_path =
        content_addressed_path(&ObjectPath::from("release"), &output_sha, "parquet").unwrap();
    let output_uri = format!("gs://output/{output_path}");
    let output = evidence_object(
        &output_uri,
        "output-generation",
        "output-etag",
        output_bytes,
    );
    let staging = build_attempt_staging_location(
        &spec.output_root_uri,
        &spec.export_id,
        &task.task_id,
        1,
        "local-mock-lease",
        "parquet",
    )
    .unwrap();
    let report = ExportPartReport {
        schema_version: 1,
        export_id: spec.export_id.clone(),
        task_id: task.task_id.clone(),
        partition_index: 0,
        source_manifest_sha256: spec.source_manifest_sha256.clone(),
        source_part_file: task.source_part_file.clone(),
        source_object: task.source_object.clone(),
        schema_fingerprint_sha256: spec.parquet.schema_fingerprint_sha256.clone(),
        staging_object: SourceObject {
            uri: staging.uri,
            version: "staging-generation".into(),
            etag: "staging-etag".into(),
            size: output_bytes.len() as u64,
            crc32c: None,
            sha256: Some(output_sha),
        },
        byte_size: output_bytes.len() as u64,
        row_count: 1,
        row_group_count: 1,
        decoded_rows_sha256: "d".repeat(64),
        first_key: Some(genohype_core::codec::LosslessValue::from(
            &EncodedValue::Binary(b"a".to_vec()),
        )),
        last_key: Some(genohype_core::codec::LosslessValue::from(
            &EncodedValue::Binary(b"a".to_vec()),
        )),
        worker_id: "worker-local".into(),
        worker_git_commit: spec.expected_worker.git_commit.clone(),
        worker_binary_sha256: spec.expected_worker.binary_sha256.clone(),
        assignment_attempt: 1,
        lease_identity_sha256: staging.lease_identity_sha256,
        started_at: "2026-08-13T12:00:00Z".into(),
        completed_at: "2026-08-13T12:00:01Z".into(),
    };
    let receipt =
        SafeExportPartReceipt::from_publication(spec, &task, &report, output.clone()).unwrap();
    let receipt_bytes = receipt.canonical_bytes(spec, &task, &output).unwrap();
    let receipt_object = evidence_object(
        &spec.part_receipt_uri(0).unwrap(),
        "receipt-generation",
        "receipt-etag",
        &receipt_bytes,
    );

    let manifest = SourceManifest::new(spec.source_parts.clone()).unwrap();
    let mut sidecar = TableReleaseSidecar {
        schema_version: 1,
        release_id: spec.export_id.clone(),
        status: ReleaseStatus::Complete,
        source: ReleaseSourceSnapshot {
            canonical_table_uri: spec.canonical_table_uri.clone(),
            snapshot_timestamp: spec.source_snapshot_timestamp.clone(),
            manifest,
            manifest_sha256: spec.source_manifest_sha256.clone(),
        },
        table: spec.table.clone(),
        globals: spec.globals.clone(),
        tool: spec.tool.clone(),
        parquet: spec.parquet.clone(),
        parts: vec![receipt.part],
        totals: ReleaseTotals {
            expected_partition_count: 1,
            completed_partition_count: 1,
            failed_partition_count: 0,
            expected_part_count: 1,
            completed_part_count: 1,
            failed_part_count: 0,
            source_row_count: None,
            output_row_count: 1,
            output_bytes: output_bytes.len() as u64,
            aggregate_sha256: String::new(),
        },
        receipt_set_sha256: String::new(),
    };
    let receipt_set = sidecar.computed_receipt_set_sha256().unwrap();
    sidecar.receipt_set_sha256 = receipt_set.clone();
    sidecar.totals.aggregate_sha256 = receipt_set.clone();
    sidecar.validate_complete().unwrap();
    let release_bytes = serde_json::to_vec(&sidecar).unwrap();
    let release_object = evidence_object(
        &spec.release_uri().unwrap(),
        "release-generation",
        "release-etag",
        &release_bytes,
    );
    let success = SafeExportSuccess {
        schema_version: 1,
        export_id: spec.export_id.clone(),
        release_object,
        receipts: vec![SafeExportReceiptIdentity {
            partition_index: 0,
            receipt_object,
        }],
        receipt_set_sha256: receipt_set,
    };
    let success_bytes = success.canonical_bytes(spec).unwrap();

    serde_json::json!({
        "schema_version": 1,
        "objects": [
            {
                "path": UrlPath::from_uri(&spec.success_uri().unwrap()),
                "version": "success-generation",
                "etag": "success-etag",
                "bytes_hex": hex(&success_bytes),
            },
            {
                "path": UrlPath::from_uri(&spec.release_uri().unwrap()),
                "version": "release-generation",
                "etag": "release-etag",
                "bytes_hex": hex(&release_bytes),
            },
            {
                "path": UrlPath::from_uri(&spec.part_receipt_uri(0).unwrap()),
                "version": "receipt-generation",
                "etag": "receipt-etag",
                "bytes_hex": hex(&receipt_bytes),
            },
            {
                "path": UrlPath::from_uri(&output.uri),
                "version": "output-generation",
                "etag": "output-etag",
                "bytes_hex": hex(output_bytes),
            }
        ]
    })
}

#[test]
fn black_box_validate_accepts_exact_local_evidence_bundle() {
    let temp = tempfile::tempdir().unwrap();
    let spec = spec();
    let spec_path = temp.path().join("spec.json");
    let bundle_path = temp.path().join("bundle.json");
    let report_path = temp.path().join("report.json");
    fs::write(&spec_path, serde_json::to_vec(&spec).unwrap()).unwrap();
    fs::write(
        &bundle_path,
        serde_json::to_vec(&successful_evidence_bundle(&spec)).unwrap(),
    )
    .unwrap();

    let result = run(&[
        "safe-export",
        "validate",
        "--spec",
        spec_path.to_str().unwrap(),
        "--evidence-bundle",
        bundle_path.to_str().unwrap(),
        "--output",
        report_path.to_str().unwrap(),
    ]);
    assert!(
        result.status.success(),
        "validate failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(report_path).unwrap()).unwrap();
    assert_eq!(report["valid"], true);
    assert_eq!(report["export_id"], spec.export_id);
    assert_eq!(report["expected_partition_count"], 1);
    assert_eq!(report["output_part_count"], 1);
    assert_eq!(report["output_row_count"], 1);
}

struct UrlPath;

impl UrlPath {
    fn from_uri(uri: &str) -> String {
        let without_scheme = uri.strip_prefix("gs://").unwrap();
        let (_, path) = without_scheme.split_once('/').unwrap();
        path.to_string()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn spec_bytes_are_deterministic_and_bound_to_source_output_and_worker() {
    let original = spec();
    let again = spec();
    assert_eq!(
        serde_json::to_vec(&original).unwrap(),
        serde_json::to_vec(&again).unwrap()
    );

    let mut source = unsealed_spec();
    source.source_parts[0].version = "8".into();
    source.source_parts[0].etag = "etag-8".into();
    source.source_manifest_sha256 = SourceManifest::new(source.source_parts.clone())
        .unwrap()
        .digest_sha256()
        .unwrap();
    let source = source.with_computed_export_id().unwrap();
    assert_ne!(source.export_id, original.export_id);

    let mut output = unsealed_spec();
    output.output_root_uri = "gs://output/other".into();
    let output = output.with_computed_export_id().unwrap();
    assert_ne!(output.export_id, original.export_id);

    let mut worker = unsealed_spec();
    worker.expected_worker.binary_sha256 = "c".repeat(64);
    worker.tool.binary_sha256 = "c".repeat(64);
    let worker = worker.with_computed_export_id().unwrap();
    assert_ne!(worker.export_id, original.export_id);
}

fn object(uri: &str) -> SourceObject {
    SourceObject {
        uri: uri.into(),
        version: "7".into(),
        etag: "etag-7".into(),
        size: 10,
        crc32c: Some("Nks/tw==".into()),
        sha256: None,
    }
}

fn component(part_files: Vec<String>) -> ComponentDescriptor {
    let physical = ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap();
    ComponentDescriptor {
        name: "OrderedRVDComponentSpec".into(),
        raw_e_type: "+EBaseStruct{x:+EBinary}".into(),
        physical_type: PhysicalTypeDescriptor::from(&physical),
        raw_v_type: "Struct{x:String}".into(),
        logical_type: VTypeParser::parse("Struct{x:String}").unwrap(),
        buffer_spec: BufferSpec::StreamBlock,
        part_files,
        attrs: serde_json::Value::Object(Default::default()),
        range_bounds: vec![],
        index: None,
    }
}

fn unsealed_spec() -> SafeExportSpec {
    let source = object("gs://source/table.ht/rows/parts/part-0");
    let manifest = SourceManifest::new(vec![source.clone()]).unwrap();
    let schema = PairedSchema::from_raw_vtype(
        ETypeParser::parse("+EBaseStruct{x:+EBinary}").unwrap(),
        "Struct{x:String}",
    )
    .unwrap()
    .arrow_plan()
    .unwrap();
    SafeExportSpec {
        schema_version: 1,
        export_id: String::new(),
        canonical_table_uri: "gs://source/table.ht".into(),
        source_manifest_uri: "gs://authority/manifests/a.json".into(),
        source_manifest_sha256: manifest.digest_sha256().unwrap(),
        source_snapshot_timestamp: "2026-08-13T12:00:00Z".into(),
        output_root_uri: "gs://output/release".into(),
        source_parts: vec![source],
        parquet: ParquetDescriptor {
            arrow_schema: serde_json::to_value(schema.descriptor()).unwrap(),
            schema_fingerprint_sha256: schema.fingerprint_sha256().unwrap(),
            writer_properties: WriterPropertiesDescriptor {
                compression: "zstd".into(),
                compression_level: None,
                max_row_group_size: 100_000,
                converter_batch_size: 4096,
                dictionary_enabled: true,
                statistics_enabled: true,
                data_page_version: "v1".into(),
                writer_version: "v1".into(),
            },
            sharding_rule: SAFE_EXPORT_SHARDING_RULE_V1.into(),
            projection: vec![],
            filters: vec![],
            output_ordering: vec!["x".into()],
        },
        expected_worker: SafeExportWorkerIdentity {
            git_commit: "abc1234".into(),
            build_identity: "0.1.0:abc1234".into(),
            binary_sha256: "b".repeat(64),
        },
        table: TableDescriptor {
            file_version: 1,
            hail_version: "0.2".into(),
            references_rel_path: "references".into(),
            table_type: "Table".into(),
            top_metadata: serde_json::json!({"name": "Table"}),
            key: KeyDescriptor {
                fields: vec!["x".into()],
                ordering: KeyOrdering::LexicographicAscending,
                distinctly_keyed: Some(true),
            },
            rows: component(vec!["part-0".into()]),
            globals: component(vec!["part-0".into()]),
            partitions: vec![SourcePartition {
                index: 0,
                part_file: "part-0".into(),
                range_bound: None,
                row_count: None,
                index_objects: vec![],
                output_part_ids: vec!["part-00000000".into()],
            }],
        },
        globals: GlobalsPayload::from_value(&EncodedValue::Struct(Vec::new())),
        tool: ToolIdentity {
            package_version: "0.1.0".into(),
            git_commit: "abc1234".into(),
            dirty_build: false,
            build_provenance: "0.1.0:abc1234".into(),
            binary_name: "genohype".into(),
            binary_uri: Some("gs://authority/bin/genohype".into()),
            binary_sha256: "b".repeat(64),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            enabled_features: vec!["gcp".into()],
            arrow_crate_version: "53.3".into(),
            parquet_crate_version: "53.3".into(),
        },
        receipt_schema_version: 1,
        completion_schema_version: 1,
    }
}

fn spec() -> SafeExportSpec {
    unsealed_spec().with_computed_export_id().unwrap()
}
