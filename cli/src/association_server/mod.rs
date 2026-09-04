mod assets;
mod open;
mod security;
mod server;

use genohype_association::AssociationVisualizationManifestV1;
use genohype_core::{HailError, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

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
    let prepared = prepare_artifacts(&artifacts)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(HailError::Io)?;

    runtime.block_on(async move {
        let token = server::generate_url_token();
        let state = server::ServerState::new(token.clone(), prepared.manifest, prepared.raster);
        let app = server::build_router(state);
        let listener = server::bind_loopback(options.port)
            .await
            .map_err(HailError::Io)?;
        let url = server::viewer_url(listener.local_addr().map_err(HailError::Io)?, &token)
            .map_err(HailError::Io)?;
        announce_and_open(
            &url,
            options.open,
            &mut io::stdout(),
            &mut io::stderr(),
            open::open,
        )
        .map_err(HailError::Io)?;
        server::serve_with_shutdown(listener, app, server::shutdown_signal())
            .await
            .map_err(HailError::Io)
    })
}

fn prepare_artifacts(artifacts: &ViewerArtifacts) -> Result<PreparedArtifacts> {
    let manifest_bytes = fs::read(&artifacts.manifest_path)?;
    if manifest_bytes.len() > MAX_MANIFEST_BYTES {
        return Err(invalid_format(format!(
            "association manifest exceeds {} bytes",
            MAX_MANIFEST_BYTES
        )));
    }
    let mut manifest: AssociationVisualizationManifestV1 =
        serde_json::from_slice(&manifest_bytes).map_err(invalid_format)?;
    manifest.validate().map_err(invalid_format)?;

    let raster = fs::read(&artifacts.raster_path)?;
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
        let Some(expected_digest) = expected.strip_prefix("sha256:") else {
            return Err(invalid_format(format!(
                "unsupported association raster checksum {expected:?}"
            )));
        };
        let actual = format!("{:x}", Sha256::digest(&raster));
        if !constant_time_eq(expected_digest.as_bytes(), actual.as_bytes()) {
            return Err(invalid_format(
                "association raster checksum does not match manifest",
            ));
        }
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

    // The on-disk canonical manifest is immutable. Only its in-memory transport copy
    // is rebased to the token-relative allowlisted raster endpoint.
    manifest.resources.raster.url = "raster".into();
    manifest.validate().map_err(invalid_format)?;
    let mut served_manifest = serde_json::to_vec_pretty(&manifest).map_err(invalid_format)?;
    served_manifest.push(b'\n');

    Ok(PreparedArtifacts {
        manifest: served_manifest,
        raster,
    })
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
