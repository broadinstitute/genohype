use crate::layout::canonical_assembly;
use crate::manifest::{validate_schema_version, AssociationThresholdV1, SCHEMA_VERSION_V1};
use crate::reduction::MAX_TOP_K;
use crate::JSON_SAFE_INTEGER_MAX;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;

pub const STORE_SCHEMA_VERSION_V1: u32 = 1;
pub const MAX_MAPPING_FIELD_BYTES: usize = 256;

/// Typed effective raster background included in the private session identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionBackgroundV1 {
    White,
    Transparent,
    Color(String),
}
impl SessionBackgroundV1 {
    pub fn validate(&self) -> Result<(), SessionError> {
        if let Self::Color(value) = self {
            let valid = value.strip_prefix('#').is_some_and(|hex| {
                hex.len() == 6
                    && hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'A'..=b'F'))
            });
            if !valid {
                return Err(SessionError::InvalidSessionInput);
            }
        }
        Ok(())
    }
}

/// Canonical source-field mapping. Assembly is session metadata, not a row field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NormalizedFieldMappingV1 {
    #[schemars(length(min = 1, max = 256))]
    pub contig: String,
    #[schemars(length(min = 1, max = 256))]
    pub position: String,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub ref_allele: Option<String>,
    #[serde(rename = "alt", skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub alt_allele: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub pvalue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub neg_log10_p: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub beta: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub se: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub af: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub ac: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub gene: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub consequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub series: Option<String>,
}

impl NormalizedFieldMappingV1 {
    pub fn validate(&self) -> Result<(), SessionError> {
        let values = [
            Some(self.contig.as_str()),
            Some(self.position.as_str()),
            self.ref_allele.as_deref(),
            self.alt_allele.as_deref(),
            self.pvalue.as_deref(),
            self.neg_log10_p.as_deref(),
            self.beta.as_deref(),
            self.se.as_deref(),
            self.af.as_deref(),
            self.ac.as_deref(),
            self.gene.as_deref(),
            self.consequence.as_deref(),
            self.series.as_deref(),
        ];
        if self.pvalue.is_none() && self.neg_log10_p.is_none() {
            return Err(SessionError::MissingSignificanceMapping);
        }
        let mut seen = HashSet::new();
        for value in values.into_iter().flatten() {
            if value.trim() != value || value.is_empty() || value.len() > MAX_MAPPING_FIELD_BYTES {
                return Err(SessionError::InvalidMappingField);
            }
            if !seen.insert(value) {
                return Err(SessionError::DuplicateSourceField(value.to_string()));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SessionKeyInputsV1 {
    pub association_schema_version: String,
    pub store_schema_version: u32,
    pub source_fingerprint: String,
    pub mapping: NormalizedFieldMappingV1,
    #[schemars(length(min = 1, max = 64))]
    pub assembly: String,
    pub thresholds: Vec<AssociationThresholdV1>,
    #[schemars(range(max = 1000))]
    pub top_hits: usize,
    #[schemars(range(min = 1))]
    pub width: u32,
    #[schemars(range(min = 1))]
    pub height: u32,
    #[schemars(length(min = 1, max = 128))]
    pub layout_hash: String,
    #[schemars(range(min = 0.0, max = 100.0))]
    pub point_radius: f32,
    pub point_alpha: u8,
    pub background: SessionBackgroundV1,
    #[schemars(length(min = 1, max = 128))]
    pub renderer_version: String,
}

impl SessionKeyInputsV1 {
    pub fn validate(&self) -> Result<(), SessionError> {
        validate_schema_version(&self.association_schema_version)
            .map_err(|_| SessionError::InvalidSchemaVersion)?;
        if self.store_schema_version != STORE_SCHEMA_VERSION_V1 {
            return Err(SessionError::InvalidStoreSchemaVersion);
        }
        validate_sha256(&self.source_fingerprint)?;
        self.mapping.validate()?;
        if !matches!(self.assembly.as_str(), "GRCh37" | "GRCh38")
            || canonical_assembly(&self.assembly) != self.assembly
            || self.width == 0
            || self.height == 0
        {
            return Err(SessionError::InvalidSessionInput);
        }
        if self.top_hits > MAX_TOP_K
            || self.layout_hash.is_empty()
            || self.layout_hash.len() > 128
            || !self.point_radius.is_finite()
            || self.point_radius <= 0.0
            || self.point_radius > 100.0
            || self.background.validate().is_err()
            || self.renderer_version.is_empty()
            || self.renderer_version.len() > 128
        {
            return Err(SessionError::InvalidSessionInput);
        }
        if self.thresholds.len() > 100
            || self.thresholds.iter().any(|threshold| {
                threshold.name.trim().is_empty()
                    || threshold.name.trim() != threshold.name
                    || threshold.name.len() > 128
                    || !threshold.pvalue.is_finite()
                    || threshold.pvalue <= 0.0
                    || threshold.pvalue > 1.0
            })
        {
            return Err(SessionError::InvalidSessionInput);
        }
        Ok(())
    }

    pub fn session_key(&self) -> Result<String, SessionError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| SessionError::Serialization)?;
        let mut digest = Sha256::new();
        digest.update(b"genohype.association.session-key.v1\0");
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
        Ok(format!("sha256:{:x}", digest.finalize()))
    }
}

/// Browser-safe opaque dataset identifier. V1 uses canonical lowercase UUID-v4 text.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct DatasetIdV1(
    #[schemars(regex(
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ))]
    String,
);

impl DatasetIdV1 {
    pub fn new(value: impl Into<String>) -> Result<Self, SessionError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let hyphens = [8, 13, 18, 23];
        let valid = bytes.len() == 36
            && bytes.iter().enumerate().all(|(index, byte)| {
                if hyphens.contains(&index) {
                    *byte == b'-'
                } else {
                    byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
                }
            })
            && bytes[14] == b'4'
            && matches!(bytes[19], b'8' | b'9' | b'a' | b'b');
        if !valid {
            return Err(SessionError::InvalidDatasetId);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DatasetIdV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}
impl fmt::Display for DatasetIdV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompletedArtifactV1 {
    #[schemars(range(max = 9007199254740991_u64))]
    pub byte_size: u64,
    #[schemars(length(min = 71, max = 71))]
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationSessionCompletionV1 {
    pub schema_version: String,
    pub dataset_id: DatasetIdV1,
    pub session_key: String,
    pub source_fingerprint: String,
    pub store_schema_version: u32,
    pub database: CompletedArtifactV1,
    pub raster: CompletedArtifactV1,
    pub manifest: CompletedArtifactV1,
    pub raster_width: u32,
    pub raster_height: u32,
    #[schemars(length(min = 1, max = 64))]
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64))]
    pub expires_at: Option<String>,
}

impl AssociationSessionCompletionV1 {
    pub fn validate(&self) -> Result<(), SessionError> {
        if self.schema_version != SCHEMA_VERSION_V1
            || self.store_schema_version != STORE_SCHEMA_VERSION_V1
            || self.raster_width == 0
            || self.raster_height == 0
            || self.created_at.is_empty()
            || self.created_at.len() > 64
            || self
                .expires_at
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 64)
        {
            return Err(SessionError::InvalidCompletion);
        }
        validate_sha256(&self.session_key)?;
        validate_sha256(&self.source_fingerprint)?;
        for artifact in [&self.database, &self.raster, &self.manifest] {
            if artifact.byte_size > JSON_SAFE_INTEGER_MAX {
                return Err(SessionError::InvalidCompletion);
            }
            validate_sha256(&artifact.checksum)?;
        }
        Ok(())
    }
}

fn validate_sha256(value: &str) -> Result<(), SessionError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(SessionError::InvalidDigest);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(SessionError::InvalidDigest);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    MissingSignificanceMapping,
    InvalidMappingField,
    DuplicateSourceField(String),
    InvalidSchemaVersion,
    InvalidStoreSchemaVersion,
    InvalidSessionInput,
    InvalidDatasetId,
    InvalidDigest,
    InvalidCompletion,
    Serialization,
}
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSignificanceMapping => write!(f, "mapping requires pvalue or neg_log10_p"),
            Self::InvalidMappingField => {
                write!(f, "mapping fields must be non-empty, trimmed, and bounded")
            }
            Self::DuplicateSourceField(value) => {
                write!(f, "source field {value:?} is mapped more than once")
            }
            Self::InvalidSchemaVersion => write!(f, "unsupported association schema version"),
            Self::InvalidStoreSchemaVersion => {
                write!(f, "unsupported association store schema version")
            }
            Self::InvalidSessionInput => write!(f, "invalid association session input"),
            Self::InvalidDatasetId => write!(f, "dataset ID must be a canonical lowercase UUID-v4"),
            Self::InvalidDigest => write!(f, "digest must be lowercase sha256 text"),
            Self::InvalidCompletion => write!(f, "invalid completed-session metadata"),
            Self::Serialization => write!(f, "could not serialize association session inputs"),
        }
    }
}
impl Error for SessionError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn mapping() -> NormalizedFieldMappingV1 {
        NormalizedFieldMappingV1 {
            contig: "CHR".into(),
            position: "POS".into(),
            ref_allele: None,
            alt_allele: None,
            pvalue: Some("P".into()),
            neg_log10_p: None,
            beta: None,
            se: None,
            af: None,
            ac: None,
            gene: None,
            consequence: None,
            series: None,
        }
    }
    #[test]
    fn mapping_requires_unique_bounded_required_fields() {
        assert!(mapping().validate().is_ok());
        let mut bad = mapping();
        bad.pvalue = Some("CHR".into());
        assert!(matches!(
            bad.validate(),
            Err(SessionError::DuplicateSourceField(_))
        ));
    }
    #[test]
    fn dataset_ids_are_uuid_v4_only() {
        assert!(DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").is_ok());
        assert!(DatasetIdV1::new("../secret").is_err());
    }
    #[test]
    fn session_inputs_require_canonical_assembly_and_bounded_thresholds() {
        let mut inputs = session_inputs();
        inputs.assembly = "hg38".into();
        assert_eq!(inputs.validate(), Err(SessionError::InvalidSessionInput));

        let mut inputs = session_inputs();
        inputs.thresholds = (0..101)
            .map(|index| AssociationThresholdV1 {
                name: format!("t{index}"),
                pvalue: 0.5,
            })
            .collect();
        assert_eq!(inputs.validate(), Err(SessionError::InvalidSessionInput));

        let mut inputs = session_inputs();
        inputs.thresholds = vec![AssociationThresholdV1 {
            name: "   ".into(),
            pvalue: 0.5,
        }];
        assert_eq!(inputs.validate(), Err(SessionError::InvalidSessionInput));
    }

    #[test]
    fn session_hash_is_stable_and_domain_separated() {
        let inputs = session_inputs();
        assert_eq!(inputs.session_key().unwrap(), inputs.session_key().unwrap());
    }

    #[test]
    fn page_and_render_inputs_invalidate_session_identity() {
        let baseline = session_inputs().session_key().unwrap();
        let mut changed = session_inputs();
        changed.point_radius = 3.0;
        assert_ne!(baseline, changed.session_key().unwrap());
        let mut changed = session_inputs();
        changed.point_alpha = 177;
        assert_ne!(baseline, changed.session_key().unwrap());
        let mut changed = session_inputs();
        changed.background = SessionBackgroundV1::Color("#FFFFFF".into());
        assert_ne!(baseline, changed.session_key().unwrap());
        let mut invalid = session_inputs();
        invalid.background = SessionBackgroundV1::Color("#ffffff".into());
        assert_eq!(invalid.validate(), Err(SessionError::InvalidSessionInput));
    }

    fn session_inputs() -> SessionKeyInputsV1 {
        SessionKeyInputsV1 {
            association_schema_version: "1.0".into(),
            store_schema_version: 1,
            source_fingerprint: format!("sha256:{}", "a".repeat(64)),
            mapping: mapping(),
            assembly: "GRCh38".into(),
            thresholds: vec![],
            top_hits: 100,
            width: 3000,
            height: 800,
            layout_hash: "layout".into(),
            point_radius: 2.5,
            point_alpha: 178,
            background: SessionBackgroundV1::White,
            renderer_version: "test".into(),
        }
    }
}
