mod assets;
mod detail;
mod open;
mod security;
mod server;

use crate::association_session::CompletedAssociationSession;
use genohype_association::{
    AssociationSessionCompletionV1, AssociationVisualizationManifestV1, MAX_ENCODED_RASTER_BYTES,
};
use genohype_core::{HailError, Result};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::future::{poll_fn, Future};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAX_COMPLETION_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ViewerArtifacts {
    pub(crate) manifest_path: PathBuf,
    pub(crate) raster_path: PathBuf,
}

impl ViewerArtifacts {
    pub(crate) fn new(manifest_path: impl Into<PathBuf>, raster_path: impl Into<PathBuf>) -> Self {
        Self {
            manifest_path: manifest_path.into(),
            raster_path: raster_path.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ViewerOptions {
    pub(crate) port: u16,
    pub(crate) open: bool,
}

#[derive(Debug)]
struct PreparedArtifacts {
    manifest: Vec<u8>,
    raster: Vec<u8>,
}

pub(crate) fn serve_viewer(artifacts: ViewerArtifacts, options: ViewerOptions) -> Result<()> {
    serve_prepared(prepare_artifacts(&artifacts)?, options, None)
}

/// Serves one already-completed indexed association session. The session handle is
/// retained in Axum state until graceful shutdown, which also retains its lifecycle locks.
pub(crate) fn serve_dataset_viewer(
    session: CompletedAssociationSession,
    options: ViewerOptions,
) -> Result<()> {
    let prepared = prepare_dataset_artifacts(&session)?;
    serve_prepared(prepared, options, Some(session))
}

fn serve_prepared(
    prepared: PreparedArtifacts,
    options: ViewerOptions,
    session: Option<CompletedAssociationSession>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(HailError::Io)?;

    runtime.block_on(async move {
        let token = server::generate_url_token();
        let mut state = server::ServerState::new(token.clone(), prepared.manifest, prepared.raster);
        if let Some(session) = session {
            // `prepare_dataset_artifacts` has already validated this typed V1
            // projection against the immutable completion marker and raster.
            let root_manifest: AssociationVisualizationManifestV1 =
                serde_json::from_slice(&state.manifest).map_err(invalid_format)?;
            let dataset = server::DatasetService::from_session(session, root_manifest);
            dataset.validate_complete().await.map_err(invalid_format)?;
            state = state.with_dataset(dataset);
        }
        let app = server::build_router(state);
        let listener = server::bind_loopback(options.port)
            .await
            .map_err(HailError::Io)?;
        let url = server::viewer_url(listener.local_addr().map_err(HailError::Io)?, &token)
            .map_err(HailError::Io)?;

        // Poll once before publishing the URL so Tokio installs its process-wide
        // Ctrl-C listener. A client may signal immediately after observing the
        // URL; leaving the first poll until serve_with_shutdown creates a window
        // where that signal is lost and the viewer serves indefinitely.
        let mut shutdown = Box::pin(server::shutdown_signal());
        let interrupted =
            poll_fn(|context| std::task::Poll::Ready(shutdown.as_mut().poll(context).is_ready()))
                .await;
        if interrupted {
            return Ok(());
        }

        announce_and_open(
            &url,
            options.open,
            &mut io::stdout(),
            &mut io::stderr(),
            open::open,
        )
        .map_err(HailError::Io)?;
        server::serve_with_shutdown(listener, app, shutdown)
            .await
            .map_err(HailError::Io)
    })
}

fn prepare_artifacts(artifacts: &ViewerArtifacts) -> Result<PreparedArtifacts> {
    let manifest_bytes = read_bounded(&artifacts.manifest_path, MAX_MANIFEST_BYTES, "manifest")?;
    let raster = read_bounded(
        &artifacts.raster_path,
        MAX_ENCODED_RASTER_BYTES as usize,
        "raster",
    )?;
    prepare_manifest_and_raster(manifest_bytes, raster, None)
}

fn prepare_dataset_artifacts(session: &CompletedAssociationSession) -> Result<PreparedArtifacts> {
    let marker = session.completion();
    marker.validate().map_err(invalid_format)?;
    if &marker.dataset_id != session.dataset_id() {
        return Err(invalid_format("completed association dataset ID mismatch"));
    }

    let database_path = session.database_path();
    let directory = database_path
        .parent()
        .ok_or_else(|| invalid_format("completed association session has no directory"))?;
    let marker_bytes = read_bounded(
        &directory.join("complete.json"),
        MAX_COMPLETION_BYTES,
        "completion marker",
    )?;
    let disk_marker: AssociationSessionCompletionV1 =
        serde_json::from_slice(&marker_bytes).map_err(invalid_format)?;
    disk_marker.validate().map_err(invalid_format)?;
    if &disk_marker != marker {
        return Err(invalid_format(
            "completed association session marker changed before serving",
        ));
    }

    validate_artifact_file(&database_path, &marker.database, "database")?;
    let manifest_bytes = read_completed_artifact(
        &session.manifest_path(),
        &marker.manifest,
        MAX_MANIFEST_BYTES,
        "manifest",
    )?;
    let raster = read_completed_artifact(
        &session.raster_path(),
        &marker.raster,
        MAX_ENCODED_RASTER_BYTES as usize,
        "raster",
    )?;

    let table_url = format!("datasets/{}/table", session.dataset_id());
    let region_url = format!("datasets/{}/region", session.dataset_id());
    let prepared =
        prepare_manifest_and_raster(manifest_bytes, raster, Some((&table_url, &region_url)))?;
    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&prepared.manifest).map_err(invalid_format)?;
    if manifest.plot_id != session.dataset_id().as_str()
        || manifest.source_fingerprint != marker.source_fingerprint
        || manifest.created_at != marker.created_at
        || manifest.layout.width != marker.raster_width
        || manifest.layout.height != marker.raster_height
        || manifest.resources.raster.byte_size != Some(marker.raster.byte_size)
        || manifest.resources.raster.checksum.as_deref() != Some(&marker.raster.checksum)
        || manifest
            .resources
            .table_api
            .as_ref()
            .map_or(true, |resource| resource.media_type != "application/json")
        || manifest
            .resources
            .region_api
            .as_ref()
            .map_or(true, |resource| resource.media_type != "application/json")
    {
        return Err(invalid_format(
            "completed association manifest does not match its completion marker",
        ));
    }
    Ok(prepared)
}

fn prepare_manifest_and_raster(
    manifest_bytes: Vec<u8>,
    raster: Vec<u8>,
    dataset_urls: Option<(&str, &str)>,
) -> Result<PreparedArtifacts> {
    let mut manifest_value: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).map_err(invalid_format)?;
    let manifest: AssociationVisualizationManifestV1 =
        serde_json::from_value(manifest_value.clone()).map_err(invalid_format)?;
    manifest.validate().map_err(invalid_format)?;

    let descriptor = &manifest.resources.raster;
    if descriptor.media_type != "image/png" {
        return Err(invalid_format(format!(
            "association raster media type must be image/png, got {:?}",
            descriptor.media_type
        )));
    }
    if descriptor
        .byte_size
        .is_some_and(|size| size != raster.len() as u64)
    {
        return Err(invalid_format(
            "association raster byte size does not match manifest",
        ));
    }
    if let Some(expected) = &descriptor.checksum {
        validate_checksum(&raster, expected, "raster")?;
    }
    let pixmap = tiny_skia::Pixmap::decode_png(&raster)
        .map_err(|error| invalid_format(format!("invalid association PNG: {error}")))?;
    if pixmap.width() != manifest.layout.width || pixmap.height() != manifest.layout.height {
        return Err(invalid_format(format!(
            "association raster dimensions {}x{} do not match manifest {}x{}",
            pixmap.width(),
            pixmap.height(),
            manifest.layout.width,
            manifest.layout.height
        )));
    }

    // Modify only the known URL leaves in the raw JSON tree. Deserializing a V1
    // projection before and after this rebase enforces the typed contract without
    // discarding compatible minor-version fields unknown to this binary.
    set_manifest_url(&mut manifest_value, "/resources/raster/url", "raster")?;
    if let Some((table_url, region_url)) = dataset_urls {
        set_manifest_url(&mut manifest_value, "/resources/table_api/url", table_url)?;
        set_manifest_url(&mut manifest_value, "/resources/region_api/url", region_url)?;
    }
    let served_projection: AssociationVisualizationManifestV1 =
        serde_json::from_value(manifest_value.clone()).map_err(invalid_format)?;
    served_projection.validate().map_err(invalid_format)?;

    let mut served_manifest = serde_json::to_vec_pretty(&manifest_value).map_err(invalid_format)?;
    served_manifest.push(b'\n');
    Ok(PreparedArtifacts {
        manifest: served_manifest,
        raster,
    })
}

fn set_manifest_url(manifest: &mut serde_json::Value, pointer: &str, url: &str) -> Result<()> {
    let value = manifest
        .pointer_mut(pointer)
        .ok_or_else(|| invalid_format(format!("association manifest is missing {pointer}")))?;
    if !value.is_string() {
        return Err(invalid_format(format!(
            "association manifest {pointer} must be a string"
        )));
    }
    *value = serde_json::Value::String(url.to_string());
    Ok(())
}

fn read_bounded(path: &Path, maximum: usize, name: &str) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() > maximum as u64 {
        return Err(invalid_format(format!(
            "association {name} exceeds {maximum} bytes"
        )));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(invalid_format(format!(
            "association {name} exceeds {maximum} bytes"
        )));
    }
    Ok(bytes)
}

fn read_completed_artifact(
    path: &Path,
    expected: &genohype_association::CompletedArtifactV1,
    maximum: usize,
    name: &str,
) -> Result<Vec<u8>> {
    if expected.byte_size > maximum as u64 {
        return Err(invalid_format(format!(
            "completed association {name} exceeds {maximum} bytes"
        )));
    }
    let bytes = read_bounded(path, maximum, name)?;
    validate_artifact_bytes(&bytes, expected, name)?;
    Ok(bytes)
}

fn validate_artifact_file(
    path: &Path,
    expected: &genohype_association::CompletedArtifactV1,
    name: &str,
) -> Result<()> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected.byte_size {
        return Err(invalid_format(format!(
            "completed association {name} byte size does not match marker"
        )));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    validate_digest(digest.finalize().as_slice(), &expected.checksum, name)
}

fn validate_artifact_bytes(
    bytes: &[u8],
    expected: &genohype_association::CompletedArtifactV1,
    name: &str,
) -> Result<()> {
    if bytes.len() as u64 != expected.byte_size {
        return Err(invalid_format(format!(
            "completed association {name} byte size does not match marker"
        )));
    }
    validate_checksum(bytes, &expected.checksum, name)
}

fn validate_checksum(bytes: &[u8], expected: &str, name: &str) -> Result<()> {
    validate_digest(Sha256::digest(bytes).as_slice(), expected, name)
}

fn validate_digest(actual: &[u8], expected: &str, name: &str) -> Result<()> {
    let Some(expected_digest) = expected.strip_prefix("sha256:") else {
        return Err(invalid_format(format!(
            "unsupported association {name} checksum"
        )));
    };
    let actual = hex_lower(actual);
    if !constant_time_eq(expected_digest.as_bytes(), actual.as_bytes()) {
        return Err(invalid_format(format!(
            "association {name} checksum does not match"
        )));
    }
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn announce_and_open<W1, W2, F>(
    url: &str,
    should_open: bool,
    stdout: &mut W1,
    stderr: &mut W2,
    opener: F,
) -> io::Result<()>
where
    W1: Write,
    W2: Write,
    F: FnOnce(&str) -> io::Result<()>,
{
    writeln!(stdout, "Viewer URL: {url}")?;
    stdout.flush()?;
    if should_open {
        if let Err(error) = opener(url) {
            writeln!(
                stderr,
                "Could not open the browser ({error}). Open this URL manually: {url}"
            )?;
            stderr.flush()?;
        }
    }
    Ok(())
}

fn invalid_format(error: impl std::fmt::Display) -> HailError {
    HailError::InvalidFormat(error.to_string())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests;
