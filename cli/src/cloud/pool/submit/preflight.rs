//! Pre-flight validation and checkpoint handling.

use crate::HailError;
use crate::Result;

/// Read the checkpoint file listing completed phenotypes.
///
/// The checkpoint file is a simple newline-delimited list of relative paths
/// like "meta/height" or "afr/1234".
pub fn read_completed_checkpoint(checkpoint_path: &str) -> Result<std::collections::HashSet<String>> {
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

    // Read the file contents
    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;

    let bytes = rt
        .block_on(async { store.get(&path).await?.bytes().await })
        .map_err(|e| {
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
