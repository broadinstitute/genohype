//! Versioned, canonical identities for immutable source snapshots.
//!
//! A [`SourceSnapshot`] is passed explicitly to readers. It is intentionally not
//! process-global: pinned and legacy reads may run concurrently without sharing
//! source identity or cache entries.

use crate::{HailError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;

pub const SOURCE_MANIFEST_SCHEMA_VERSION: u32 = 1;
const DIGEST_DOMAIN: &[u8] = b"genohype.source-manifest.sha256.v1\0";

/// Exact immutable identity of one source object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObject {
    /// Canonical, complete URI. Query strings and fragments are forbidden.
    pub uri: String,
    /// Provider object generation/version.
    pub version: String,
    /// Provider ETag, including quotes when returned that way by the provider.
    pub etag: String,
    /// Full object size in bytes.
    pub size: u64,
    /// Base64 or hexadecimal CRC32C supplied by the object inventory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
    /// Lowercase hexadecimal SHA-256 of the object bytes, when independently known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Canonical source inventory. Objects are serialized in URI order for hashing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceManifest {
    pub schema_version: u32,
    pub objects: Vec<SourceObject>,
}

impl SourceManifest {
    pub fn new(objects: Vec<SourceObject>) -> Result<Self> {
        let manifest = Self {
            schema_version: SOURCE_MANIFEST_SCHEMA_VERSION,
            objects,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Parse and validate a manifest. Array order in input is not significant.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(data).map_err(|error| {
            HailError::InvalidFormat(format!("invalid source manifest JSON: {error}"))
        })?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Load a manifest from a legacy local/cloud path. Callers must verify its
    /// expected digest before using it as snapshot authority.
    pub fn from_path(path: &str) -> Result<Self> {
        let mut reader = super::get_reader(path)?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        Self::from_json(&data)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != SOURCE_MANIFEST_SCHEMA_VERSION {
            return Err(HailError::InvalidFormat(format!(
                "unsupported source manifest schema version {}; expected {}",
                self.schema_version, SOURCE_MANIFEST_SCHEMA_VERSION
            )));
        }
        if self.objects.is_empty() {
            return Err(HailError::InvalidFormat(
                "source manifest contains no objects".to_string(),
            ));
        }

        let mut uris = HashSet::with_capacity(self.objects.len());
        for object in &self.objects {
            validate_object(object)?;
            if !uris.insert(object.uri.as_str()) {
                return Err(HailError::InvalidFormat(format!(
                    "duplicate source manifest URI: {}",
                    object.uri
                )));
            }
        }
        Ok(())
    }

    /// Canonical compact JSON, with objects sorted by exact URI.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut canonical = self.clone();
        canonical.objects.sort_by(|a, b| a.uri.cmp(&b.uri));
        serde_json::to_vec(&canonical).map_err(|error| {
            HailError::InvalidFormat(format!("failed to serialize source manifest: {error}"))
        })
    }

    /// Domain-separated SHA-256 over canonical bytes.
    pub fn digest_sha256(&self) -> Result<String> {
        let canonical = self.canonical_bytes()?;
        let mut hasher = Sha256::new();
        hasher.update(DIGEST_DOMAIN);
        hasher.update((canonical.len() as u64).to_be_bytes());
        hasher.update(canonical);
        Ok(format_sha256(hasher.finalize().as_slice()))
    }
}

fn validate_object(object: &SourceObject) -> Result<()> {
    if object.uri.is_empty() || object.version.is_empty() || object.etag.is_empty() {
        return Err(HailError::InvalidFormat(format!(
            "source object URI, version, and ETag must be non-empty: {:?}",
            object.uri
        )));
    }
    if object.uri.contains('?') || object.uri.contains('#') {
        return Err(HailError::InvalidFormat(format!(
            "source object URI must not contain a query or fragment: {}",
            object.uri
        )));
    }
    if object.crc32c.as_deref().is_none_or(str::is_empty)
        && object.sha256.as_deref().is_none_or(str::is_empty)
    {
        return Err(HailError::InvalidFormat(format!(
            "source object must contain CRC32C and/or SHA-256: {}",
            object.uri
        )));
    }
    if let Some(sha256) = &object.sha256 {
        if sha256.len() != 64
            || !sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(HailError::InvalidFormat(format!(
                "source object SHA-256 must be 64 lowercase hexadecimal characters: {}",
                object.uri
            )));
        }
    }
    Ok(())
}

fn format_sha256(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

/// Validated immutable source inventory and its canonical digest.
#[derive(Debug, Clone)]
pub struct SourceSnapshot {
    inner: Arc<SnapshotInner>,
}

#[derive(Debug)]
struct SnapshotInner {
    manifest: SourceManifest,
    digest_sha256: String,
    by_uri: HashMap<String, SourceObject>,
}

impl SourceSnapshot {
    /// Construct a snapshot and require an independently supplied digest.
    pub fn new(manifest: SourceManifest, expected_digest_sha256: &str) -> Result<Self> {
        let actual = manifest.digest_sha256()?;
        if actual != expected_digest_sha256 {
            return Err(HailError::InvalidFormat(format!(
                "source manifest digest mismatch: expected {expected_digest_sha256}, got {actual}"
            )));
        }
        let by_uri = manifest
            .objects
            .iter()
            .cloned()
            .map(|object| (object.uri.clone(), object))
            .collect();
        Ok(Self {
            inner: Arc::new(SnapshotInner {
                manifest,
                digest_sha256: actual,
                by_uri,
            }),
        })
    }

    /// Load, validate, and digest-check a manifest before it can authorize reads.
    pub fn load_and_verify(path: &str, expected_digest_sha256: &str) -> Result<Self> {
        Self::new(SourceManifest::from_path(path)?, expected_digest_sha256)
    }

    pub fn digest_sha256(&self) -> &str {
        &self.inner.digest_sha256
    }

    pub fn manifest(&self) -> &SourceManifest {
        &self.inner.manifest
    }

    /// Resolve one exact URI or fail closed when the snapshot does not contain it.
    pub fn object(&self, uri: &str) -> Result<&SourceObject> {
        self.inner.by_uri.get(uri).ok_or_else(|| {
            HailError::InvalidFormat(format!(
                "source snapshot {} has no object identity for {uri}",
                self.digest_sha256()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(uri: &str, version: &str) -> SourceObject {
        SourceObject {
            uri: uri.to_string(),
            version: version.to_string(),
            etag: format!("etag-{version}"),
            size: 3,
            crc32c: Some("Nks/tw==".to_string()),
            sha256: None,
        }
    }

    #[test]
    fn canonical_digest_is_order_independent_and_domain_separated() {
        let a = SourceManifest::new(vec![
            object("gs://bucket/z", "2"),
            object("gs://bucket/a", "1"),
        ])
        .unwrap();
        let b = SourceManifest::new(vec![
            object("gs://bucket/a", "1"),
            object("gs://bucket/z", "2"),
        ])
        .unwrap();
        assert_eq!(a.canonical_bytes().unwrap(), b.canonical_bytes().unwrap());
        assert_eq!(a.digest_sha256().unwrap(), b.digest_sha256().unwrap());

        let plain = Sha256::digest(a.canonical_bytes().unwrap());
        assert_ne!(a.digest_sha256().unwrap(), format_sha256(&plain));
    }

    #[test]
    fn duplicates_and_missing_identity_are_rejected() {
        let duplicate = SourceManifest {
            schema_version: 1,
            objects: vec![object("gs://bucket/a", "1"), object("gs://bucket/a", "2")],
        };
        assert!(duplicate
            .validate()
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        let mut missing = object("gs://bucket/a", "1");
        missing.etag.clear();
        let manifest = SourceManifest {
            schema_version: 1,
            objects: vec![missing],
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn expected_digest_is_required_before_lookup() {
        let manifest = SourceManifest::new(vec![object("gs://bucket/a", "1")]).unwrap();
        let digest = manifest.digest_sha256().unwrap();
        assert!(SourceSnapshot::new(manifest.clone(), "00").is_err());
        let snapshot = SourceSnapshot::new(manifest, &digest).unwrap();
        assert_eq!(snapshot.object("gs://bucket/a").unwrap().version, "1");
        assert!(snapshot.object("gs://bucket/extra").is_err());
    }
}
