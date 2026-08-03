//! Validation and bootstrap metadata for opaque custom worker artifacts.

use crate::{HailError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

const ATTESTATION_SCHEMA_VERSION: u32 = 1;

/// Build-produced sidecar for a custom worker binary.
///
/// This is an integrity/identity assertion, not signed provenance. Genohype
/// independently verifies all byte-derived fields before provisioning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerArtifactAttestation {
    pub schema_version: u32,
    pub sha256: String,
    pub size: u64,
    pub revision: String,
    pub build_identity: String,
    pub build_version: String,
    pub platform: String,
    pub arch: String,
}

/// An artifact whose sidecar and exact bytes have been validated together.
#[derive(Debug, Clone)]
pub struct ValidatedWorkerArtifact {
    pub attestation: WorkerArtifactAttestation,
    pub bytes: Vec<u8>,
}

/// A validated custom worker staged at its immutable content-addressed URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedWorkerArtifact {
    pub gcs_url: String,
    pub attestation: WorkerArtifactAttestation,
}

/// Immutable values supplied to every VM in one custom-worker deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedWorkerBootstrap {
    pub artifact: StagedWorkerArtifact,
    pub deployment_id: String,
}

impl ExpectedWorkerBootstrap {
    pub fn new(artifact: StagedWorkerArtifact) -> Self {
        Self {
            artifact,
            deployment_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

fn invalid(message: impl Into<String>) -> HailError {
    HailError::InvalidFormat(format!("invalid worker attestation: {}", message.into()))
}

fn is_lower_hex(value: &str, lengths: &[usize]) -> bool {
    lengths.contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_deployment_safe(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._/+,:=-".contains(&byte))
}

fn validate_attestation_document(attestation: &WorkerArtifactAttestation) -> Result<()> {
    if attestation.schema_version != ATTESTATION_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported schema_version {}; expected {}",
            attestation.schema_version, ATTESTATION_SCHEMA_VERSION
        )));
    }
    if !is_lower_hex(&attestation.sha256, &[64]) {
        return Err(invalid(
            "sha256 must be 64 lowercase hexadecimal characters",
        ));
    }
    if !is_lower_hex(&attestation.revision, &[40, 64]) {
        return Err(invalid(
            "revision must be a full lowercase 40- or 64-hex object ID",
        ));
    }
    if !is_deployment_safe(&attestation.build_identity)
        || !attestation.build_identity.contains(&attestation.revision)
        || attestation.build_identity.contains("-dirty")
        || attestation.build_identity.contains("development-build")
    {
        return Err(invalid(
            "build_identity must be clean, deployment-safe, and bound to revision",
        ));
    }
    if attestation.build_version != attestation.build_identity {
        return Err(invalid("build_version must exactly equal build_identity"));
    }
    if attestation.platform != "linux" {
        return Err(invalid("platform must be linux"));
    }
    if attestation.arch != "x86_64" {
        return Err(invalid("arch must be x86_64"));
    }
    Ok(())
}

/// Read and validate the identity intent in a build-produced sidecar without
/// reading, hashing, staging, or building the corresponding local binary.
pub fn read_worker_attestation_intent(
    attestation_path: &Path,
) -> Result<WorkerArtifactAttestation> {
    let sidecar = std::fs::read(attestation_path).map_err(HailError::Io)?;
    let attestation: WorkerArtifactAttestation = serde_json::from_slice(&sidecar)?;
    validate_attestation_document(&attestation)?;
    Ok(attestation)
}

/// Read a sidecar and verify it against the exact custom-worker bytes.
pub fn validate_worker_artifact(
    binary_path: &Path,
    attestation_path: &Path,
) -> Result<ValidatedWorkerArtifact> {
    let attestation = read_worker_attestation_intent(attestation_path)?;
    let bytes = std::fs::read(binary_path).map_err(HailError::Io)?;
    if bytes.len() as u64 != attestation.size {
        return Err(invalid(format!(
            "size mismatch: sidecar={}, actual={}",
            attestation.size,
            bytes.len()
        )));
    }
    let actual_sha256 = format!("{:x}", Sha256::digest(&bytes));
    if actual_sha256 != attestation.sha256 {
        return Err(invalid(format!(
            "sha256 mismatch: sidecar={}, actual={actual_sha256}",
            attestation.sha256
        )));
    }

    // The supported bootstrap installs Linux x86_64 executables. Check the ELF
    // header rather than trusting only the descriptive sidecar fields.
    if bytes.len() < 20
        || &bytes[..4] != b"\x7fELF"
        || bytes[4] != 2
        || bytes[5] != 1
        || u16::from_le_bytes([bytes[18], bytes[19]]) != 62
    {
        return Err(invalid("binary is not a 64-bit little-endian x86_64 ELF"));
    }
    if !bytes
        .windows(attestation.build_identity.len())
        .any(|window| window == attestation.build_identity.as_bytes())
    {
        return Err(invalid("binary does not embed build_identity"));
    }

    Ok(ValidatedWorkerArtifact { attestation, bytes })
}

/// Deterministic object path below the pool database directory.
pub fn content_addressed_worker_url(
    pool_db_path: &str,
    attestation: &WorkerArtifactAttestation,
) -> Result<String> {
    if !pool_db_path.starts_with("gs://") {
        return Err(invalid(
            "attested custom workers require a gs:// pool_db_path",
        ));
    }
    let base = if pool_db_path.ends_with(".db") {
        pool_db_path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .ok_or_else(|| invalid("pool_db_path has no bucket-relative directory"))?
    } else {
        pool_db_path.trim_end_matches('/')
    };
    Ok(format!(
        "{base}/bin/custom-workers/{}/{}/genohype-worker",
        attestation.revision, attestation.sha256
    ))
}

/// Create the immutable binary and canonical sidecar, accepting an existing
/// object only when its bytes are identical.
pub fn stage_validated_worker_artifact(
    artifact: &ValidatedWorkerArtifact,
    pool_db_path: &str,
) -> Result<StagedWorkerArtifact> {
    use genohype_core::io::CloudWriter;

    let gcs_url = content_addressed_worker_url(pool_db_path, &artifact.attestation)?;
    let sidecar_url = format!("{gcs_url}.attestation.json");
    let canonical_sidecar = serde_json::to_vec(&artifact.attestation)?;

    let mut binary_writer = CloudWriter::new(&gcs_url)?;
    binary_writer
        .write_all(&artifact.bytes)
        .map_err(HailError::Io)?;
    binary_writer.finish_create_only()?;

    let mut sidecar_writer = CloudWriter::new(&sidecar_url)?;
    sidecar_writer
        .write_all(&canonical_sidecar)
        .map_err(HailError::Io)?;
    sidecar_writer.finish_create_only()?;

    Ok(StagedWorkerArtifact {
        gcs_url,
        attestation: artifact.attestation.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, Write};

    fn artifact() -> (
        tempfile::NamedTempFile,
        tempfile::NamedTempFile,
        WorkerArtifactAttestation,
    ) {
        let revision = "a".repeat(40);
        let identity = format!("fixture/{revision}/x86_64-linux-release");
        let mut bytes = vec![0_u8; 20];
        bytes[..6].copy_from_slice(b"\x7fELF\x02\x01");
        bytes[18..20].copy_from_slice(&62_u16.to_le_bytes());
        bytes.extend_from_slice(identity.as_bytes());
        let attestation = WorkerArtifactAttestation {
            schema_version: 1,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            size: bytes.len() as u64,
            revision,
            build_identity: identity.clone(),
            build_version: identity,
            platform: "linux".into(),
            arch: "x86_64".into(),
        };
        let mut binary = tempfile::NamedTempFile::new().unwrap();
        binary.write_all(&bytes).unwrap();
        let mut sidecar = tempfile::NamedTempFile::new().unwrap();
        serde_json::to_writer(&mut sidecar, &attestation).unwrap();
        (binary, sidecar, attestation)
    }

    #[test]
    fn validates_exact_bytes_and_identity() {
        let (binary, sidecar, expected) = artifact();
        let validated = validate_worker_artifact(binary.path(), sidecar.path()).unwrap();
        assert_eq!(validated.attestation, expected);
        assert_eq!(validated.bytes.len() as u64, expected.size);
    }

    #[test]
    fn rejects_distinct_build_version_and_identity() {
        let (binary, mut sidecar, mut attestation) = artifact();
        attestation.build_version = "different-build".into();
        sidecar.as_file_mut().set_len(0).unwrap();
        sidecar.rewind().unwrap();
        serde_json::to_writer(&mut sidecar, &attestation).unwrap();
        let error = validate_worker_artifact(binary.path(), sidecar.path()).unwrap_err();
        assert!(error
            .to_string()
            .contains("build_version must exactly equal build_identity"));
    }

    #[test]
    fn identity_intent_read_does_not_require_binary_bytes() {
        let (_binary, sidecar, expected) = artifact();
        assert_eq!(
            read_worker_attestation_intent(sidecar.path()).unwrap(),
            expected
        );
    }

    #[test]
    fn rejects_byte_mismatch() {
        let (mut binary, sidecar, _) = artifact();
        binary.write_all(b"changed").unwrap();
        let error = validate_worker_artifact(binary.path(), sidecar.path()).unwrap_err();
        assert!(error.to_string().contains("size mismatch"));
    }

    #[test]
    fn uses_revision_and_digest_in_pool_scoped_url() {
        let (_, _, attestation) = artifact();
        let url =
            content_addressed_worker_url("gs://bucket/runs/demo/ops.db", &attestation).unwrap();
        assert_eq!(
            url,
            format!(
                "gs://bucket/runs/demo/bin/custom-workers/{}/{}/genohype-worker",
                attestation.revision, attestation.sha256
            )
        );
    }
}
