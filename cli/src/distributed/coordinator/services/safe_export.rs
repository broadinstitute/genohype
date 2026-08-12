//! Safe-export publication outside the coordinator mutex.
//!
//! This module turns an exact, durably claimed worker report into immutable
//! content-addressed Parquet plus a strict export-bound receipt. SQLite remains
//! the final authority transition and is intentionally performed by the caller.

use genohype_core::io::{
    publish_content_addressed, put_immutable, resolve_url_for_write, PublishedObject, SourceObject,
    StreamingWriteResult,
};
use genohype_core::release::{
    ExportPartReport, SafeExportPartReceipt, SafeExportPartTask, SafeExportSpec,
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
