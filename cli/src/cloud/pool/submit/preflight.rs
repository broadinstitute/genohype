//! Pre-flight validation and checkpoint handling.

use crate::HailError;
use crate::Result;

/// Run a future, using the current tokio runtime if available, or creating a new one.
/// This avoids "Cannot start a runtime from within a runtime" panics when called
/// from the coordinator (which runs inside tokio).
fn block_on_async<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // We're inside a tokio runtime — use spawn_blocking + block_on to avoid nesting
            std::thread::scope(|s| {
                s.spawn(|| {
                    tokio::runtime::Runtime::new()
                        .map_err(|e| {
                            HailError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                        })
                        .map(|rt| rt.block_on(future))
                })
                .join()
                .expect("thread panicked")
            })
        }
        Err(_) => {
            // No runtime — create one
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
            Ok(rt.block_on(future))
        }
    }
}

/// Read the checkpoint file listing completed phenotypes.
///
/// The checkpoint file is a simple newline-delimited list of relative paths
/// like "meta/height" or "afr/1234".
pub fn read_completed_checkpoint(
    checkpoint_path: &str,
) -> Result<std::collections::HashSet<String>> {
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;

    let url = url::Url::parse(checkpoint_path).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid checkpoint URL: {}", e),
        ))
    })?;

    let (store, path): (std::sync::Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Missing bucket in GCS URL",
                ))
            })?;
            let path = url.path().trim_start_matches('/');
            (
                genohype_core::io::get_gcs_client(bucket)?,
                ObjPath::from(path),
            )
        }
        scheme => {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unsupported URL scheme for checkpoint: {}", scheme),
            )));
        }
    };

    let bytes = block_on_async(async { store.get(&path).await?.bytes().await })?.map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to read checkpoint: {}", e),
        ))
    })?;

    // Parse as newline-delimited list
    let content = String::from_utf8_lossy(&bytes);
    let completed: std::collections::HashSet<String> = content
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    Ok(completed)
}

/// List marker filenames in the `.completed_phenos/` directory.
///
/// Each marker file represents one completed phenotype, named `ancestry_id`
/// (e.g., "meta_1740556"). Returns the list of filenames (not full paths).
pub fn list_completed_markers(dir_url: &str) -> Result<Vec<String>> {
    use futures::TryStreamExt;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;

    let url = url::Url::parse(dir_url).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid markers URL: {}", e),
        ))
    })?;

    let (store, prefix): (std::sync::Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Missing bucket in GCS URL",
                ))
            })?;
            let path = url.path().trim_start_matches('/');
            (
                genohype_core::io::get_gcs_client(bucket)?,
                ObjPath::from(path),
            )
        }
        scheme => {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unsupported URL scheme for markers: {}", scheme),
            )));
        }
    };

    let markers = block_on_async(async {
        let mut markers = Vec::new();
        let mut list_stream = store.list(Some(&prefix));
        while let Some(meta) = list_stream.try_next().await? {
            if let Some(filename) = meta.location.filename() {
                if !filename.is_empty() {
                    markers.push(filename.to_string());
                }
            }
        }
        Ok::<Vec<String>, object_store::Error>(markers)
    })?
    .map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to list markers: {}", e),
        ))
    })?;

    Ok(markers)
}
