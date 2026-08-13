//! Safe-export publication outside the coordinator mutex.
//!
//! This module turns an exact, durably claimed worker report into immutable
//! content-addressed Parquet plus a strict export-bound receipt. SQLite remains
//! the final authority transition and is intentionally performed by the caller.

use genohype_core::io::{
    hash_conditioned, publish_content_addressed, put_immutable, read_conditioned,
    resolve_url_for_write, PublishedObject, SourceManifest, SourceObject, SourceSnapshot,
    StreamingWriteResult,
};
use genohype_core::release::{
    ExportPartReport, ReleaseSourceSnapshot, ReleaseStatus, ReleaseTotals, SafeExportPartReceipt,
    SafeExportPartTask, SafeExportReceiptIdentity, SafeExportSpec, SafeExportSuccess,
    TableReleaseSidecar, RELEASE_SIDECAR_SCHEMA_VERSION,
};
use object_store::path::Path;
use object_store::{ObjectStore, PutResult};
use std::sync::Arc;
use url::Url;

/// Immutable cloud evidence ready for one transactional SQLite acceptance.
#[derive(Debug, Clone)]
pub(crate) struct PublishedSafeExportPart {
    pub(crate) receipt_json: String,
    pub(crate) receipt_object: SourceObject,
}

#[derive(Debug, Clone)]
pub(crate) struct ImportedSafeExportPart {
    pub(crate) envelope: SafeExportPartReceipt,
    pub(crate) receipt_json: String,
    pub(crate) receipt_object: SourceObject,
}

#[derive(Debug, Clone)]
pub(crate) struct SafeExportRegistrationPlan {
    pub(crate) imported: Vec<ImportedSafeExportPart>,
    pub(crate) missing_partitions: Vec<usize>,
    pub(crate) imported_rows: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FinalizedSafeExport {
    pub(crate) release_object: SourceObject,
    pub(crate) success_object: SourceObject,
}

const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_COMPLETION_BYTES: u64 = 16 * 1024 * 1024;

/// Resolve production cloud storage and publish one exact report.
pub(crate) fn publish_claimed_part(
    spec: &SafeExportSpec,
    task: &SafeExportPartTask,
    report: &ExportPartReport,
) -> Result<PublishedSafeExportPart, String> {
    let (store, output_prefix) = resolve_url_for_write(&spec.output_root_uri)
        .map_err(|error| format!("failed to resolve safe-export output: {error}"))?;
    let staging_path = same_store_path(&spec.output_root_uri, &report.staging_object.uri)?;
    let receipt_uri = spec
        .part_receipt_uri(task.partition_index)
        .map_err(|error| error.to_string())?;
    let receipt_path = same_store_path(&spec.output_root_uri, &receipt_uri)?;
    publish_claimed_part_with_store(
        store,
        output_prefix,
        staging_path,
        receipt_path,
        receipt_uri,
        spec,
        task,
        report,
    )
}

/// Injectable object-store seam used by focused uncertainty/conflict tests.
#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_claimed_part_with_store(
    store: Arc<dyn ObjectStore>,
    output_prefix: Path,
    staging_path: Path,
    receipt_path: Path,
    receipt_uri: String,
    spec: &SafeExportSpec,
    task: &SafeExportPartTask,
    report: &ExportPartReport,
) -> Result<PublishedSafeExportPart, String> {
    let staging_sha = report
        .staging_object
        .sha256
        .clone()
        .ok_or_else(|| "safe-export staging report has no SHA-256".to_string())?;
    let staged = StreamingWriteResult {
        byte_count: report.byte_size,
        sha256: staging_sha,
        put_result: PutResult {
            e_tag: Some(report.staging_object.etag.clone()),
            version: Some(report.staging_object.version.clone()),
        },
    };
    let published = publish_content_addressed(
        store.clone(),
        &output_prefix,
        &staging_path,
        "parquet",
        &staged,
    )
    .map_err(|error| format!("safe-export content publication failed: {error}"))?;
    let output_uri = object_uri(&spec.output_root_uri, &published.path)?;
    let output_object = published_source_object(output_uri, &published)?;

    let receipt =
        SafeExportPartReceipt::from_publication(spec, task, report, output_object.clone())
            .map_err(|error| format!("failed to construct safe-export receipt: {error}"))?;
    let receipt_bytes = receipt
        .canonical_bytes(spec, task, &output_object)
        .map_err(|error| format!("failed to encode safe-export receipt: {error}"))?;
    let receipt_json = String::from_utf8(receipt_bytes.clone())
        .map_err(|error| format!("safe-export receipt was not UTF-8: {error}"))?;
    let receipt_published = put_immutable(store, &receipt_path, receipt_bytes.into())
        .map_err(|error| format!("safe-export receipt publication failed: {error}"))?;
    let receipt_object = published_source_object(receipt_uri, &receipt_published)?;

    Ok(PublishedSafeExportPart {
        receipt_json,
        receipt_object,
    })
}

/// Fresh registration preflight. It derives every receipt path exactly and
/// intentionally treats only receipt NotFound as missing work.
pub(crate) fn prepare_registration(
    spec: &SafeExportSpec,
) -> Result<SafeExportRegistrationPlan, String> {
    spec.validate()
        .map_err(|error| format!("invalid safe-export spec: {error}"))?;
    let snapshot =
        SourceSnapshot::load_and_verify(&spec.source_manifest_uri, &spec.source_manifest_sha256)
            .map_err(|error| format!("failed to load sealed source manifest: {error}"))?;
    let (store, output_prefix) = resolve_url_for_write(&spec.output_root_uri)
        .map_err(|error| format!("failed to resolve safe-export output: {error}"))?;
    prepare_registration_with_store(spec, store, output_prefix, snapshot.manifest())
}

pub(crate) fn prepare_registration_with_store(
    spec: &SafeExportSpec,
    store: Arc<dyn ObjectStore>,
    output_prefix: Path,
    manifest: &SourceManifest,
) -> Result<SafeExportRegistrationPlan, String> {
    spec.validate().map_err(|error| error.to_string())?;
    if manifest
        .digest_sha256()
        .map_err(|error| error.to_string())?
        != spec.source_manifest_sha256
    {
        return Err("safe-export source manifest digest mismatch".to_string());
    }
    // This also exact-matches each sealed source identity against the manifest.
    let tasks = spec
        .part_tasks(manifest)
        .map_err(|error| format!("invalid safe-export task plan: {error}"))?;
    let mut imported = Vec::with_capacity(tasks.len());
    let mut missing_partitions = Vec::new();
    let mut imported_rows = 0_u64;
    for task in &tasks {
        let receipt_uri = spec
            .part_receipt_uri(task.partition_index)
            .map_err(|error| error.to_string())?;
        let receipt_path = same_store_path(&spec.output_root_uri, &receipt_uri)?;
        // Assert the derived exact path belongs beneath the resolved prefix;
        // object-store adapters never receive an unsealed arbitrary location.
        if !receipt_path.as_ref().starts_with(output_prefix.as_ref()) {
            return Err("safe-export receipt path escaped the sealed output prefix".to_string());
        }
        match validate_receipt_and_output(spec, task, store.clone(), &receipt_path, &receipt_uri) {
            Ok(part) => {
                imported_rows = imported_rows
                    .checked_add(part.envelope.part.row_count)
                    .ok_or_else(|| "safe-export imported row count overflow".to_string())?;
                imported.push(part);
            }
            Err(ReceiptValidationError::Missing) => missing_partitions.push(task.partition_index),
            Err(ReceiptValidationError::Invalid(error)) => return Err(error),
        }
    }
    Ok(SafeExportRegistrationPlan {
        imported,
        missing_partitions,
        imported_rows,
    })
}

#[derive(Debug)]
enum ReceiptValidationError {
    Missing,
    Invalid(String),
}

fn validate_receipt_and_output(
    spec: &SafeExportSpec,
    task: &SafeExportPartTask,
    store: Arc<dyn ObjectStore>,
    receipt_path: &Path,
    receipt_uri: &str,
) -> Result<ImportedSafeExportPart, ReceiptValidationError> {
    let receipt_read = match read_conditioned(store.clone(), receipt_path, MAX_RECEIPT_BYTES) {
        Ok(value) => value,
        Err(genohype_core::HailError::ObjectStore(object_store::Error::NotFound { .. })) => {
            return Err(ReceiptValidationError::Missing)
        }
        Err(error) => {
            return Err(ReceiptValidationError::Invalid(format!(
                "failed conditioned receipt read at {receipt_uri}: {error}"
            )))
        }
    };
    let untrusted: SafeExportPartReceipt =
        serde_json::from_slice(&receipt_read.bytes).map_err(|error| {
            ReceiptValidationError::Invalid(format!("invalid receipt JSON: {error}"))
        })?;
    let output_path = same_store_path(&spec.output_root_uri, &untrusted.part.output_object.uri)
        .map_err(ReceiptValidationError::Invalid)?;
    let output_read = hash_conditioned(store, &output_path, untrusted.part.output_object.size)
        .map_err(|error| {
            ReceiptValidationError::Invalid(format!(
                "failed conditioned output validation for {}: {error}",
                untrusted.part.output_object.uri
            ))
        })?;
    let output_object =
        published_source_object(untrusted.part.output_object.uri.clone(), &output_read)
            .map_err(ReceiptValidationError::Invalid)?;
    if output_object != untrusted.part.output_object {
        return Err(ReceiptValidationError::Invalid(
            "conditioned output identity/size/SHA does not match receipt".to_string(),
        ));
    }
    let envelope =
        SafeExportPartReceipt::from_json(&receipt_read.bytes, spec, task, &output_object).map_err(
            |error| ReceiptValidationError::Invalid(format!("invalid receipt: {error}")),
        )?;
    let receipt_object = conditioned_source_object(receipt_uri.to_string(), &receipt_read)
        .map_err(ReceiptValidationError::Invalid)?;
    let receipt_json = String::from_utf8(receipt_read.bytes.to_vec()).map_err(|error| {
        ReceiptValidationError::Invalid(format!("receipt is not UTF-8: {error}"))
    })?;
    let canonical = envelope
        .canonical_bytes(spec, task, &output_object)
        .map_err(|error| ReceiptValidationError::Invalid(error.to_string()))?;
    if canonical.as_slice() != receipt_json.as_bytes() {
        return Err(ReceiptValidationError::Invalid(
            "safe-export receipt is not canonical compact JSON".to_string(),
        ));
    }
    Ok(ImportedSafeExportPart {
        envelope,
        receipt_json,
        receipt_object,
    })
}

/// Independently rebuild and publish deterministic completion evidence. This
/// function reimports every exact receipt/output before trusting any part.
pub(crate) fn finalize_claimed(
    spec: &SafeExportSpec,
    db: &crate::distributed::metrics_db::MetricsDb,
) -> Result<FinalizedSafeExport, String> {
    let snapshot =
        SourceSnapshot::load_and_verify(&spec.source_manifest_uri, &spec.source_manifest_sha256)
            .map_err(|error| format!("failed to reload sealed source manifest: {error}"))?;
    let (store, output_prefix) = resolve_url_for_write(&spec.output_root_uri)
        .map_err(|error| format!("failed to resolve safe-export output: {error}"))?;
    let plan =
        prepare_registration_with_store(spec, store.clone(), output_prefix, snapshot.manifest())?;
    db.verify_safe_export_finalization_evidence(&spec.export_id, &plan)?;
    publish_completion_with_store(spec, store, snapshot.manifest(), plan)
}

pub(crate) fn finalize_with_store(
    spec: &SafeExportSpec,
    store: Arc<dyn ObjectStore>,
    output_prefix: Path,
    manifest: &SourceManifest,
) -> Result<FinalizedSafeExport, String> {
    let plan = prepare_registration_with_store(spec, store.clone(), output_prefix, manifest)?;
    publish_completion_with_store(spec, store, manifest, plan)
}

fn publish_completion_with_store(
    spec: &SafeExportSpec,
    store: Arc<dyn ObjectStore>,
    manifest: &SourceManifest,
    plan: SafeExportRegistrationPlan,
) -> Result<FinalizedSafeExport, String> {
    if !plan.missing_partitions.is_empty() || plan.imported.len() != spec.table.partitions.len() {
        return Err(
            "safe-export finalization requires exact complete receipt coverage".to_string(),
        );
    }
    let parts: Vec<_> = plan
        .imported
        .iter()
        .map(|imported| imported.envelope.part.clone())
        .collect();
    let output_bytes = parts.iter().try_fold(0_u64, |total, part| {
        total
            .checked_add(part.byte_size)
            .ok_or_else(|| "safe-export output byte count overflow".to_string())
    })?;
    let source_row_count = spec
        .table
        .partitions
        .iter()
        .map(|partition| partition.row_count)
        .collect::<Option<Vec<_>>>()
        .map(|counts| {
            counts.into_iter().try_fold(0_u64, |total, count| {
                total
                    .checked_add(count)
                    .ok_or_else(|| "safe-export source row count overflow".to_string())
            })
        })
        .transpose()?;
    if source_row_count.is_some_and(|count| count != plan.imported_rows) {
        return Err("safe-export source/output row totals disagree".to_string());
    }
    spec.globals
        .validate_digest()
        .map_err(|error| format!("invalid globals digest: {error}"))?;
    let mut release_manifest = manifest.clone();
    release_manifest
        .objects
        .sort_by(|left, right| left.uri.cmp(&right.uri));
    let mut sidecar = TableReleaseSidecar {
        schema_version: RELEASE_SIDECAR_SCHEMA_VERSION,
        release_id: spec.export_id.clone(),
        status: ReleaseStatus::Complete,
        source: ReleaseSourceSnapshot {
            canonical_table_uri: spec.canonical_table_uri.clone(),
            snapshot_timestamp: spec.source_snapshot_timestamp.clone(),
            manifest: release_manifest,
            manifest_sha256: spec.source_manifest_sha256.clone(),
        },
        table: spec.table.clone(),
        globals: spec.globals.clone(),
        tool: spec.tool.clone(),
        parquet: spec.parquet.clone(),
        parts,
        totals: ReleaseTotals {
            expected_partition_count: spec.table.partitions.len(),
            completed_partition_count: spec.table.partitions.len(),
            failed_partition_count: 0,
            expected_part_count: spec.table.partitions.len(),
            completed_part_count: spec.table.partitions.len(),
            failed_part_count: 0,
            source_row_count,
            output_row_count: plan.imported_rows,
            output_bytes,
            aggregate_sha256: String::new(),
        },
        receipt_set_sha256: String::new(),
    };
    let receipt_set = sidecar
        .computed_receipt_set_sha256()
        .map_err(|error| error.to_string())?;
    sidecar.receipt_set_sha256 = receipt_set.clone();
    sidecar.totals.aggregate_sha256 = receipt_set.clone();
    sidecar
        .validate_complete()
        .map_err(|error| format!("invalid complete release sidecar: {error}"))?;
    let release_bytes = serde_json::to_vec(&sidecar)
        .map_err(|error| format!("failed to encode release sidecar: {error}"))?;
    let release_uri = spec.release_uri().map_err(|error| error.to_string())?;
    let release_path = same_store_path(&spec.output_root_uri, &release_uri)?;
    let release_published = put_immutable(store.clone(), &release_path, release_bytes.into())
        .map_err(|error| format!("release.json publication failed: {error}"))?;
    let release_object = published_source_object(release_uri, &release_published)?;

    let success = SafeExportSuccess {
        schema_version: spec.completion_schema_version,
        export_id: spec.export_id.clone(),
        release_object: release_object.clone(),
        receipts: plan
            .imported
            .iter()
            .map(|part| SafeExportReceiptIdentity {
                partition_index: part.envelope.partition_index,
                receipt_object: part.receipt_object.clone(),
            })
            .collect(),
        receipt_set_sha256: receipt_set,
    };
    let success_bytes = success
        .canonical_bytes(spec)
        .map_err(|error| format!("invalid safe-export success marker: {error}"))?;
    if u64::try_from(success_bytes.len()).map_err(|_| "_SUCCESS size exceeds u64".to_string())?
        > MAX_COMPLETION_BYTES
    {
        return Err("_SUCCESS exceeds the conditioned readback limit".to_string());
    }
    let success_uri = spec.success_uri().map_err(|error| error.to_string())?;
    let success_path = same_store_path(&spec.output_root_uri, &success_uri)?;
    let success_published =
        put_immutable(store.clone(), &success_path, success_bytes.clone().into())
            .map_err(|error| format!("_SUCCESS publication failed: {error}"))?;
    let success_object = published_source_object(success_uri, &success_published)?;
    let readback = read_conditioned(store, &success_path, MAX_COMPLETION_BYTES)
        .map_err(|error| format!("_SUCCESS conditioned readback failed: {error}"))?;
    if readback.bytes.as_ref() != success_bytes.as_slice()
        || readback.sha256 != success_object.sha256.clone().unwrap_or_default()
        || readback.put_result.version != Some(success_object.version.clone())
        || readback.put_result.e_tag != Some(success_object.etag.clone())
    {
        return Err("_SUCCESS conditioned readback identity/bytes mismatch".to_string());
    }
    Ok(FinalizedSafeExport {
        release_object,
        success_object,
    })
}

fn conditioned_source_object(
    uri: String,
    read: &genohype_core::io::ConditionedRead,
) -> Result<SourceObject, String> {
    let version = read
        .put_result
        .version
        .clone()
        .ok_or_else(|| format!("conditioned object {} has no generation/version", read.path))?;
    let etag = read
        .put_result
        .e_tag
        .clone()
        .ok_or_else(|| format!("conditioned object {} has no ETag", read.path))?;
    Ok(SourceObject {
        uri,
        version,
        etag,
        size: read.byte_count,
        crc32c: None,
        sha256: Some(read.sha256.clone()),
    })
}

fn published_source_object(
    uri: String,
    published: &PublishedObject,
) -> Result<SourceObject, String> {
    let version = published.put_result.version.clone().ok_or_else(|| {
        format!(
            "published object {} has no generation/version",
            published.path
        )
    })?;
    let etag = published
        .put_result
        .e_tag
        .clone()
        .ok_or_else(|| format!("published object {} has no ETag", published.path))?;
    Ok(SourceObject {
        uri,
        version,
        etag,
        size: published.byte_count,
        crc32c: None,
        sha256: Some(published.sha256.clone()),
    })
}

fn same_store_path(root: &str, object: &str) -> Result<Path, String> {
    let root = Url::parse(root).map_err(|error| format!("invalid output root URI: {error}"))?;
    let object = Url::parse(object).map_err(|error| format!("invalid object URI: {error}"))?;
    if root.scheme() != object.scheme() || root.host_str() != object.host_str() {
        return Err("safe-export object is not in the sealed output store".to_string());
    }
    Path::from_url_path(object.path())
        .map_err(|error| format!("invalid safe-export object path: {error}"))
}

fn object_uri(root: &str, path: &Path) -> Result<String, String> {
    let mut uri = Url::parse(root).map_err(|error| format!("invalid output root URI: {error}"))?;
    uri.set_path(&format!("/{path}"));
    Ok(uri.to_string())
}
