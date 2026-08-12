//! Immutable publication policy for successfully completed streaming uploads.
//!
//! Writers upload to a unique, attempt-scoped staging key. An authoritative
//! caller may then atomically copy that object to its digest-derived final key.
//! Staging objects are intentionally retained for higher-layer recovery and GC.

use crate::io::adapter::IO_RUNTIME;
use crate::io::writer::StreamingWriteResult;
use crate::{HailError, Result};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, ObjectMeta, ObjectStore, PutMode, PutResult};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

const LEASE_HASH_DOMAIN: &[u8] = b"genohype.safe-export.lease.sha256.v1\0";

/// A fresh, attempt-scoped object location that never exposes the lease token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptStagingLocation {
    pub uri: String,
    pub path: Path,
    pub lease_identity_sha256: String,
    pub upload_id: Uuid,
}

/// How an immutable publication request reached its final object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationDisposition {
    Created,
    ExistingIdentical,
}

/// Identity of a validated immutable final object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedObject {
    pub path: Path,
    pub byte_count: u64,
    pub sha256: String,
    pub put_result: PutResult,
    pub disposition: PublicationDisposition,
}

/// Build a fresh staging location beneath a cloud output prefix.
pub fn build_attempt_staging_location(
    output_prefix_uri: &str,
    export_id: &str,
    task_id: &str,
    assignment_attempt: u64,
    lease_token: &str,
    extension: &str,
) -> Result<AttemptStagingLocation> {
    build_attempt_staging_location_with_id(
        output_prefix_uri,
        export_id,
        task_id,
        assignment_attempt,
        lease_token,
        extension,
        Uuid::new_v4(),
    )
}

fn build_attempt_staging_location_with_id(
    output_prefix_uri: &str,
    export_id: &str,
    task_id: &str,
    assignment_attempt: u64,
    lease_token: &str,
    extension: &str,
    upload_id: Uuid,
) -> Result<AttemptStagingLocation> {
    validate_component("export ID", export_id)?;
    validate_component("task ID", task_id)?;
    validate_extension(extension)?;
    if lease_token.is_empty() {
        return Err(HailError::InvalidFormat(
            "Lease token must not be empty".to_string(),
        ));
    }

    let mut url = validate_output_prefix_uri(output_prefix_uri)?;
    let output_prefix = Path::from_url_path(url.path()).map_err(|error| {
        HailError::InvalidFormat(format!("Invalid output prefix path: {error}"))
    })?;

    let lease_identity_sha256 = lease_identity_sha256(lease_token)?;

    let path = output_prefix
        .child("_genohype")
        .child("staging")
        .child(export_id)
        .child(task_id)
        .child(format!("attempt-{assignment_attempt:020}"))
        .child(format!("lease-{lease_identity_sha256}"))
        .child(format!("upload-{upload_id}"))
        .child(format!("part.{extension}"));

    url.set_path(&format!("/{path}"));
    Ok(AttemptStagingLocation {
        uri: url.to_string(),
        path,
        lease_identity_sha256,
        upload_id,
    })
}

/// Domain-separated identity for a lease capability. Raw lease tokens must
/// never be persisted in reports, receipts, paths, or logs.
pub fn lease_identity_sha256(lease_token: &str) -> Result<String> {
    if lease_token.is_empty() {
        return Err(HailError::InvalidFormat(
            "Lease token must not be empty".to_string(),
        ));
    }
    let lease_len = u64::try_from(lease_token.len())
        .map_err(|_| HailError::InvalidFormat("Lease token length exceeds u64".to_string()))?;
    let mut lease_hash = Sha256::new();
    lease_hash.update(LEASE_HASH_DOMAIN);
    lease_hash.update(lease_len.to_be_bytes());
    lease_hash.update(lease_token.as_bytes());
    Ok(format!("{:x}", lease_hash.finalize()))
}

/// Derive the only valid final path for a digest and extension.
pub fn content_addressed_path(output_prefix: &Path, sha256: &str, extension: &str) -> Result<Path> {
    validate_digest(sha256)?;
    validate_extension(extension)?;
    Ok(output_prefix
        .child("_genohype")
        .child("objects")
        .child("sha256")
        .child(&sha256[..2])
        .child(format!("{sha256}.{extension}")))
}

/// Atomically publish a staged object to its digest-derived immutable path.
///
/// This never falls back to an overwrite-capable copy and never cleans up the
/// staging object. An existing final object is accepted only after a
/// generation/ETag-conditioned, bounded-memory full hash.
pub fn publish_content_addressed(
    store: Arc<dyn ObjectStore>,
    output_prefix: &Path,
    staging_path: &Path,
    extension: &str,
    staged: &StreamingWriteResult,
) -> Result<PublishedObject> {
    validate_digest(&staged.sha256)?;
    validate_extension(extension)?;
    let expected_size = usize::try_from(staged.byte_count).map_err(|_| {
        HailError::InvalidFormat("Staged byte count exceeds platform usize".to_string())
    })?;
    let final_path = content_addressed_path(output_prefix, &staged.sha256, extension)?;

    IO_RUNTIME.block_on(async move {
        let staging_meta = store.head(staging_path).await?;
        validate_size("staging", &staging_meta, expected_size)?;
        validate_put_identity("staging", &staged.put_result, &staging_meta)?;

        match store.copy_if_not_exists(staging_path, &final_path).await {
            Ok(()) => {
                let final_meta = store.head(&final_path).await?;
                validate_size("published", &final_meta, expected_size)?;
                Ok(PublishedObject {
                    path: final_path,
                    byte_count: staged.byte_count,
                    sha256: staged.sha256.clone(),
                    put_result: metadata_identity(&final_meta),
                    disposition: PublicationDisposition::Created,
                })
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                validate_existing(
                    store.as_ref(),
                    final_path,
                    expected_size,
                    staged.byte_count,
                    &staged.sha256,
                )
                .await
            }
            Err(error) => Err(error.into()),
        }
    })
}

/// Create a small immutable object, accepting an existing object only after a
/// generation/ETag-conditioned full-byte comparison.
///
/// A transport error after a successful create is deliberately returned. An
/// exact retry converges through the `AlreadyExists` path without overwriting.
pub fn put_immutable(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    bytes: bytes::Bytes,
) -> Result<PublishedObject> {
    let byte_count = u64::try_from(bytes.len())
        .map_err(|_| HailError::InvalidFormat("Immutable object size exceeds u64".to_string()))?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let expected_size = bytes.len();
    let path = path.clone();

    IO_RUNTIME.block_on(async move {
        match store
            .put_opts(&path, bytes.into(), PutMode::Create.into())
            .await
        {
            Ok(created) => {
                let meta = store.head(&path).await?;
                validate_size("created immutable", &meta, expected_size)?;
                validate_put_identity("created immutable", &created, &meta)?;
                Ok(PublishedObject {
                    path,
                    byte_count,
                    sha256,
                    put_result: metadata_identity(&meta),
                    disposition: PublicationDisposition::Created,
                })
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                validate_existing(store.as_ref(), path, expected_size, byte_count, &sha256).await
            }
            Err(error) => Err(error.into()),
        }
    })
}

async fn validate_existing(
    store: &dyn ObjectStore,
    final_path: Path,
    expected_size: usize,
    byte_count: u64,
    expected_sha256: &str,
) -> Result<PublishedObject> {
    let head = store.head(&final_path).await?;
    validate_size("existing published", &head, expected_size)?;
    if head.e_tag.is_none() && head.version.is_none() {
        return Err(conflict(format!(
            "existing immutable object {final_path} has no identity for a conditioned read"
        )));
    }

    let options = GetOptions {
        if_match: head.e_tag.clone(),
        version: head.version.clone(),
        ..Default::default()
    };
    let result = store.get_opts(&final_path, options).await?;
    validate_size(
        "conditioned existing published",
        &result.meta,
        expected_size,
    )?;
    validate_meta_identity("existing published", &head, &result.meta)?;
    let conditioned_identity = metadata_identity(&result.meta);

    let mut stream = result.into_stream();
    let mut digest = Sha256::new();
    let mut observed = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        observed = observed
            .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                conflict("existing immutable object chunk length exceeds u64".to_string())
            })?)
            .ok_or_else(|| conflict("existing immutable object byte count overflow".to_string()))?;
        if observed > byte_count {
            return Err(conflict(format!(
                "existing immutable object {final_path} exceeds expected size"
            )));
        }
        digest.update(&chunk);
    }
    let observed_sha256 = format!("{:x}", digest.finalize());
    if observed != byte_count || observed_sha256 != expected_sha256 {
        return Err(conflict(format!(
            "immutable publication conflict at {final_path}: existing bytes do not match digest"
        )));
    }

    Ok(PublishedObject {
        path: final_path,
        byte_count,
        sha256: expected_sha256.to_string(),
        put_result: conditioned_identity,
        disposition: PublicationDisposition::ExistingIdentical,
    })
}

fn validate_output_prefix_uri(value: &str) -> Result<Url> {
    let lower = value.to_ascii_lowercase();
    if value.contains('\\') || lower.contains("%2f") || lower.contains("%5c") {
        return Err(HailError::InvalidFormat(
            "Output prefix contains a path separator escape".to_string(),
        ));
    }
    let path_without_suffix = value.split(['?', '#']).next().unwrap_or(value);
    let raw_path = path_without_suffix
        .split_once("://")
        .map(|(_, rest)| rest.split_once('/').map(|(_, path)| path).unwrap_or(""))
        .unwrap_or("");
    if raw_path.split('/').any(is_dot_segment) {
        return Err(HailError::InvalidFormat(
            "Output prefix contains path traversal".to_string(),
        ));
    }

    let url = Url::parse(value)
        .map_err(|error| HailError::InvalidFormat(format!("Invalid output prefix URI: {error}")))?;
    if !matches!(url.scheme(), "gs" | "s3") {
        return Err(HailError::InvalidFormat(format!(
            "Unsupported output prefix scheme: {}",
            url.scheme()
        )));
    }
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(HailError::InvalidFormat(
            "Output prefix must contain only a cloud scheme, bucket, and path".to_string(),
        ));
    }
    Ok(url)
}

fn is_dot_segment(segment: &str) -> bool {
    let decoded_dots = segment.to_ascii_lowercase().replace("%2e", ".");
    decoded_dots == "." || decoded_dots == ".."
}

fn validate_component(name: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(HailError::InvalidFormat(format!(
            "{name} must be one safe path component"
        )));
    }
    Ok(())
}

fn validate_extension(extension: &str) -> Result<()> {
    if extension.is_empty()
        || !extension
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(HailError::InvalidFormat(
            "Extension must be lowercase ASCII alphanumeric".to_string(),
        ));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HailError::InvalidFormat(
            "SHA-256 must be exactly 64 lowercase hexadecimal characters".to_string(),
        ));
    }
    Ok(())
}

fn validate_size(label: &str, meta: &ObjectMeta, expected: usize) -> Result<()> {
    if meta.size != expected {
        return Err(conflict(format!(
            "{label} object {} has size {}, expected {expected}",
            meta.location, meta.size
        )));
    }
    Ok(())
}

fn validate_put_identity(label: &str, put: &PutResult, meta: &ObjectMeta) -> Result<()> {
    if put
        .e_tag
        .as_ref()
        .is_some_and(|value| meta.e_tag.as_ref() != Some(value))
        || put
            .version
            .as_ref()
            .is_some_and(|value| meta.version.as_ref() != Some(value))
    {
        return Err(conflict(format!(
            "{label} object {} identity changed before publication",
            meta.location
        )));
    }
    Ok(())
}

fn validate_meta_identity(label: &str, expected: &ObjectMeta, actual: &ObjectMeta) -> Result<()> {
    if expected
        .e_tag
        .as_ref()
        .is_some_and(|value| actual.e_tag.as_ref() != Some(value))
        || expected
            .version
            .as_ref()
            .is_some_and(|value| actual.version.as_ref() != Some(value))
    {
        return Err(conflict(format!(
            "{label} object {} changed during conditioned read",
            expected.location
        )));
    }
    Ok(())
}

fn metadata_identity(meta: &ObjectMeta) -> PutResult {
    PutResult {
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
    }
}

fn conflict(message: String) -> HailError {
    HailError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        message,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::StreamingCloudWriter;
    use futures::stream::BoxStream;
    use object_store::{ListResult, MultipartUpload, PutMultipartOpts, PutOptions, PutPayload};
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    use std::sync::Barrier;

    #[derive(Debug)]
    struct TestStore {
        inner: object_store::memory::InMemory,
        copy_mode: AtomicU8,
        put_mode: AtomicU8,
        mutate_conditioned_get: AtomicBool,
    }

    impl TestStore {
        fn new(copy_mode: u8) -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                copy_mode: AtomicU8::new(copy_mode),
                put_mode: AtomicU8::new(0),
                mutate_conditioned_get: AtomicBool::new(false),
            }
        }

        fn with_put_mode(put_mode: u8) -> Self {
            let store = Self::new(0);
            store.put_mode.store(put_mode, Ordering::SeqCst);
            store
        }
    }

    impl std::fmt::Display for TestStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "publication-test-store")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.put_mode.load(Ordering::SeqCst) == 2 {
                self.inner.put_opts(location, payload, options).await?;
                self.put_mode.store(0, Ordering::SeqCst);
                return Err(object_store::Error::Generic {
                    store: "publication-test-store",
                    source: "injected lost put response".into(),
                });
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if !options.head
                && (options.if_match.is_some() || options.version.is_some())
                && self.mutate_conditioned_get.swap(false, Ordering::SeqCst)
            {
                self.inner
                    .put(
                        location,
                        bytes::Bytes::from_static(b"changed-during-read").into(),
                    )
                    .await?;
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            match self.copy_mode.load(Ordering::SeqCst) {
                1 => Err(object_store::Error::NotSupported {
                    source: "injected atomic-copy failure".into(),
                }),
                2 => {
                    self.inner.copy_if_not_exists(from, to).await?;
                    self.copy_mode.store(0, Ordering::SeqCst);
                    Err(object_store::Error::Generic {
                        store: "publication-test-store",
                        source: "injected lost copy response".into(),
                    })
                }
                _ => self.inner.copy_if_not_exists(from, to).await,
            }
        }
    }

    fn staged(store: Arc<dyn ObjectStore>, path: &Path, bytes: &[u8]) -> StreamingWriteResult {
        let mut writer = StreamingCloudWriter::from_store(store, path.clone(), 3).unwrap();
        for chunk in bytes.chunks(2) {
            writer.write_all(chunk).unwrap();
        }
        writer.finish().unwrap()
    }

    #[test]
    fn staging_path_is_safe_unique_and_domain_hashed() {
        let first = build_attempt_staging_location(
            "gs://bucket/prefix",
            "export-1",
            "task_2",
            7,
            "super/secret?lease",
            "parquet",
        )
        .unwrap();
        let second = build_attempt_staging_location(
            "gs://bucket/prefix",
            "export-1",
            "task_2",
            7,
            "super/secret?lease",
            "parquet",
        )
        .unwrap();
        assert_ne!(first.upload_id, second.upload_id);
        assert!(!first.uri.contains("super/secret"));
        assert!(first
            .path
            .as_ref()
            .contains("export-1/task_2/attempt-00000000000000000007/lease-"));
        assert!(first.path.as_ref().ends_with("/part.parquet"));
        assert_eq!(first.lease_identity_sha256.len(), 64);

        for invalid in ["", ".", "..", "a/b", "a\\b", "a?b"] {
            assert!(build_attempt_staging_location(
                "gs://bucket/prefix",
                invalid,
                "task",
                1,
                "lease",
                "parquet"
            )
            .is_err());
        }
        for uri in [
            "file:///tmp/out",
            "gs://bucket/a/../out",
            "gs://bucket/a%2Fout",
            "gs://bucket/out?x=1",
            "gs://bucket/out#frag",
        ] {
            assert!(
                build_attempt_staging_location(uri, "export", "task", 1, "lease", "parquet")
                    .is_err(),
                "{uri}"
            );
        }
    }

    #[test]
    fn created_and_idempotent_publication_preserve_staging() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let staging = Path::from("staging/one");
        let bytes = b"immutable bytes";
        let identity = staged(store.clone(), &staging, bytes);
        let prefix = Path::from("output");

        let created =
            publish_content_addressed(store.clone(), &prefix, &staging, "parquet", &identity)
                .unwrap();
        assert_eq!(created.disposition, PublicationDisposition::Created);
        assert_eq!(created.byte_count, bytes.len() as u64);
        assert_eq!(created.sha256, identity.sha256);
        assert_eq!(
            created.path,
            content_addressed_path(&prefix, &identity.sha256, "parquet").unwrap()
        );
        let final_bytes = IO_RUNTIME.block_on(store.get(&created.path)).unwrap();
        assert_eq!(
            IO_RUNTIME.block_on(final_bytes.bytes()).unwrap(),
            &bytes[..]
        );
        assert!(IO_RUNTIME.block_on(store.head(&staging)).is_ok());

        let existing =
            publish_content_addressed(store.clone(), &prefix, &staging, "parquet", &identity)
                .unwrap();
        assert_eq!(
            existing.disposition,
            PublicationDisposition::ExistingIdentical
        );
        assert_eq!(existing.put_result, created.put_result);
    }

    #[test]
    fn atomic_race_creates_exactly_one_final_object() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let first_path = Path::from("staging/race-one");
        let second_path = Path::from("staging/race-two");
        let first = staged(store.clone(), &first_path, b"race bytes");
        let second = staged(store.clone(), &second_path, b"race bytes");
        let prefix = Path::from("race-output");
        let barrier = Arc::new(Barrier::new(2));

        let handles = [(first_path, first), (second_path, second)].map(|(path, identity)| {
            let store = store.clone();
            let prefix = prefix.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                publish_content_addressed(store, &prefix, &path, "bin", &identity).unwrap()
            })
        });
        let mut dispositions = handles.map(|handle| handle.join().unwrap().disposition);
        dispositions.sort_by_key(|value| match value {
            PublicationDisposition::Created => 0,
            PublicationDisposition::ExistingIdentical => 1,
        });
        assert_eq!(
            dispositions,
            [
                PublicationDisposition::Created,
                PublicationDisposition::ExistingIdentical
            ]
        );
    }

    #[test]
    fn conflict_and_staging_mutation_fail_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let prefix = Path::from("output");
        let staging = Path::from("staging/original");
        let identity = staged(store.clone(), &staging, b"same-size-a");
        let final_path = content_addressed_path(&prefix, &identity.sha256, "bin").unwrap();
        IO_RUNTIME
            .block_on(store.put(
                &final_path,
                bytes::Bytes::from_static(b"same-size-b").into(),
            ))
            .unwrap();
        assert!(
            publish_content_addressed(store.clone(), &prefix, &staging, "bin", &identity).is_err()
        );
        let final_bytes = IO_RUNTIME.block_on(store.get(&final_path)).unwrap();
        assert_eq!(
            IO_RUNTIME.block_on(final_bytes.bytes()).unwrap(),
            &b"same-size-b"[..]
        );
        assert!(IO_RUNTIME.block_on(store.head(&staging)).is_ok());
    }

    #[test]
    fn staging_identity_mutation_leaves_final_absent() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let prefix = Path::from("output");
        let staging = Path::from("staging/mutated");
        let identity = staged(store.clone(), &staging, b"original");
        IO_RUNTIME
            .block_on(store.put(&staging, bytes::Bytes::from_static(b"mutation").into()))
            .unwrap();
        let final_path = content_addressed_path(&prefix, &identity.sha256, "bin").unwrap();
        assert!(
            publish_content_addressed(store.clone(), &prefix, &staging, "bin", &identity).is_err()
        );
        assert!(IO_RUNTIME.block_on(store.head(&final_path)).is_err());
        assert!(IO_RUNTIME.block_on(store.head(&staging)).is_ok());
    }

    #[test]
    fn unsupported_atomic_copy_leaves_only_staging() {
        let concrete = Arc::new(TestStore::new(1));
        let store: Arc<dyn ObjectStore> = concrete;
        let staging = Path::from("staging/unsupported");
        let identity = staged(store.clone(), &staging, b"orphan");
        let prefix = Path::from("out");
        let final_path = content_addressed_path(&prefix, &identity.sha256, "bin").unwrap();
        let error = publish_content_addressed(store.clone(), &prefix, &staging, "bin", &identity)
            .unwrap_err();
        assert!(error.to_string().contains("not supported"));
        assert!(IO_RUNTIME.block_on(store.head(&staging)).is_ok());
        assert!(IO_RUNTIME.block_on(store.head(&final_path)).is_err());
    }

    #[test]
    fn uncertain_copy_response_recovers_as_existing_identical() {
        let concrete = Arc::new(TestStore::new(2));
        let store: Arc<dyn ObjectStore> = concrete;
        let staging = Path::from("staging/uncertain");
        let identity = staged(store.clone(), &staging, b"uncertain response");
        let prefix = Path::from("out");
        assert!(
            publish_content_addressed(store.clone(), &prefix, &staging, "bin", &identity).is_err()
        );
        let recovered =
            publish_content_addressed(store.clone(), &prefix, &staging, "bin", &identity).unwrap();
        assert_eq!(
            recovered.disposition,
            PublicationDisposition::ExistingIdentical
        );
    }

    #[test]
    fn retry_read_is_conditioned_on_head_identity() {
        let concrete = Arc::new(TestStore::new(0));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let staging = Path::from("staging/conditioned");
        let identity = staged(store.clone(), &staging, b"conditioned bytes");
        let prefix = Path::from("out");
        publish_content_addressed(store.clone(), &prefix, &staging, "bin", &identity).unwrap();
        concrete
            .mutate_conditioned_get
            .store(true, Ordering::SeqCst);
        let error =
            publish_content_addressed(store, &prefix, &staging, "bin", &identity).unwrap_err();
        assert!(error.to_string().contains("precondition"));
    }

    #[test]
    fn immutable_small_object_is_create_only_conditioned_and_exact() {
        let concrete = Arc::new(TestStore::new(0));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let path = Path::from("receipts/part.json");
        let bytes = bytes::Bytes::from_static(b"{\"receipt\":1}");

        let created = put_immutable(store.clone(), &path, bytes.clone()).unwrap();
        assert_eq!(created.disposition, PublicationDisposition::Created);
        let duplicate = put_immutable(store.clone(), &path, bytes).unwrap();
        assert_eq!(
            duplicate.disposition,
            PublicationDisposition::ExistingIdentical
        );
        assert_eq!(duplicate.put_result, created.put_result);

        assert!(put_immutable(
            store.clone(),
            &path,
            bytes::Bytes::from_static(b"{\"receipt\":2}")
        )
        .is_err());
        concrete
            .mutate_conditioned_get
            .store(true, Ordering::SeqCst);
        assert!(
            put_immutable(store, &path, bytes::Bytes::from_static(b"{\"receipt\":1}")).is_err()
        );
    }

    #[test]
    fn immutable_small_object_recovers_after_lost_create_response() {
        let store: Arc<dyn ObjectStore> = Arc::new(TestStore::with_put_mode(2));
        let path = Path::from("receipts/uncertain.json");
        let bytes = bytes::Bytes::from_static(b"{\"receipt\":1}");
        assert!(put_immutable(store.clone(), &path, bytes.clone()).is_err());
        let recovered = put_immutable(store, &path, bytes).unwrap();
        assert_eq!(
            recovered.disposition,
            PublicationDisposition::ExistingIdentical
        );
    }

    #[test]
    fn stale_attempt_is_only_a_durable_orphan_until_published() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let staging = Path::from("staging/orphan");
        let identity = staged(store.clone(), &staging, b"orphan");
        let final_path =
            content_addressed_path(&Path::from("out"), &identity.sha256, "bin").unwrap();
        assert!(IO_RUNTIME.block_on(store.head(&staging)).is_ok());
        assert!(IO_RUNTIME.block_on(store.head(&final_path)).is_err());
    }
}
