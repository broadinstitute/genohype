//! Unified cloud URI parsing and ObjectStore resolution.
//!
//! This module centralizes the logic for parsing cloud URLs (gs://, s3://, http://, https://)
//! and creating the appropriate ObjectStore instances. This eliminates code duplication
//! across `adapter.rs` and `writer.rs`.

use crate::{HailError, Result};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// Result of resolving a cloud URL: an ObjectStore and the path within it.
pub type ResolvedStore = (Arc<dyn ObjectStore>, ObjPath);

/// Resolve a cloud URL string into an ObjectStore and path.
///
/// Supports:
/// - `gs://bucket/path` - Google Cloud Storage (requires `gcp` feature)
/// - `s3://bucket/path` - Amazon S3 (requires `aws` feature)
/// - `http://` or `https://` - HTTP(S) URLs (requires `http` feature)
///
/// # Example
///
/// ```no_run
/// use genohype_core::io::resolve_url;
///
/// let (store, path) = resolve_url("gs://my-bucket/data/file.parquet")?;
/// # Ok::<(), hail_decoder::HailError>(())
/// ```
pub fn resolve_url(url_str: &str) -> Result<ResolvedStore> {
    let url = Url::parse(url_str)
        .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::InvalidFormat("Missing bucket in GCS URL".to_string())
            })?;
            let path = url.path().trim_start_matches('/');
            Ok((crate::io::get_gcs_client(bucket)?, ObjPath::from(path)))
        }
        #[cfg(feature = "aws")]
        "s3" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::InvalidFormat("Missing bucket in S3 URL".to_string())
            })?;
            let path = url.path().trim_start_matches('/');
            let mut s3_builder = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket);
            if std::env::var("AWS_SKIP_SIGNATURE").ok().as_deref() == Some("true") {
                s3_builder = s3_builder.with_skip_signature(true);
            }
            let s3 = s3_builder.build().map_err(|e| {
                HailError::InvalidFormat(format!("Failed to create S3 client: {}", e))
            })?;
            Ok((Arc::new(s3), ObjPath::from(path)))
        }
        #[cfg(feature = "http")]
        "http" | "https" => {
            let http = object_store::http::HttpBuilder::new()
                .with_url(url_str)
                .build()
                .map_err(|e| {
                    HailError::InvalidFormat(format!("Failed to create HTTP client: {}", e))
                })?;
            Ok((Arc::new(http), ObjPath::from("")))
        }
        scheme => Err(HailError::InvalidFormat(format!(
            "Unsupported URL scheme: {}",
            scheme
        ))),
    }
}

/// Resolve a cloud URL for writing (only supports gs:// and s3://).
///
/// HTTP URLs are not supported for writing.
pub fn resolve_url_for_write(url_str: &str) -> Result<ResolvedStore> {
    let url = Url::parse(url_str)
        .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::InvalidFormat("Missing bucket in GCS URL".to_string())
            })?;
            let path = url.path().trim_start_matches('/');
            Ok((crate::io::get_gcs_client(bucket)?, ObjPath::from(path)))
        }
        #[cfg(feature = "aws")]
        "s3" => {
            let bucket = url.host_str().ok_or_else(|| {
                HailError::InvalidFormat("Missing bucket in S3 URL".to_string())
            })?;
            let path = url.path().trim_start_matches('/');
            let mut s3_builder = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket);
            if std::env::var("AWS_SKIP_SIGNATURE").ok().as_deref() == Some("true") {
                s3_builder = s3_builder.with_skip_signature(true);
            }
            let s3 = s3_builder.build().map_err(|e| {
                HailError::InvalidFormat(format!("Failed to create S3 client: {}", e))
            })?;
            Ok((Arc::new(s3), ObjPath::from(path)))
        }
        scheme => Err(HailError::InvalidFormat(format!(
            "Unsupported URL scheme for writing: {}",
            scheme
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsupported_scheme() {
        let result = resolve_url("ftp://example.com/path");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported URL scheme"));
    }

    #[test]
    fn test_invalid_url() {
        let result = resolve_url("not a url");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid URL"));
    }
}
