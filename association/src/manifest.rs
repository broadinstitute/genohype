use crate::layout::{canonical_assembly, GenomeLayoutV1};
use crate::model::AssociationPointV1;
use crate::reduction::{compare_association_points, MAX_TOP_K};
use crate::JSON_SAFE_INTEGER_MAX;
use schemars::{schema_for, JsonSchema, Schema};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::error::Error;
use std::fmt;

pub const SCHEMA_VERSION_V1: &str = "1.0";
pub const COORDINATE_CONVENTION_V1: &str = "1-based-inclusive";

/// Visualization kinds defined by schema major version 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlotKindV1 {
    Manhattan,
    Locus,
}

/// One named p-value threshold shown on a visualization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationThresholdV1 {
    #[schemars(length(min = 1))]
    pub name: String,
    #[schemars(range(min = 0.0, max = 1.0))]
    pub pvalue: f64,
}

/// Browser-fetchable artifact or API route. URLs must be relative and credential-free.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResourceDescriptorV1 {
    #[schemars(regex(pattern = "^[^/\\\\?:#@%][^\\\\?:#@%]*$"))]
    pub url: String,
    #[schemars(length(min = 1))]
    pub media_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(max = 9007199254740991_u64))]
    pub byte_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl ResourceDescriptorV1 {
    pub fn validate(&self) -> Result<(), ManifestError> {
        let url = self.url.trim();
        let first_component = url.split('/').next().unwrap_or_default();
        if url.is_empty()
            || url != self.url
            || url.starts_with('/')
            || url.starts_with('\\')
            || url.contains(['?', '#', '@', '%', '\\'])
            || first_component.contains(':')
            || url
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
            || url.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(ManifestError::UnsafeResourceUrl(self.url.clone()));
        }
        if self.media_type.trim().is_empty() {
            return Err(ManifestError::MissingMediaType);
        }
        if self
            .byte_size
            .is_some_and(|size| size > JSON_SAFE_INTEGER_MAX)
        {
            return Err(ManifestError::BrowserUnsafeInteger("resource byte_size"));
        }
        Ok(())
    }
}

/// Resources associated with one visualization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationVisualizationResourcesV1 {
    pub raster: ResourceDescriptorV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_api: Option<ResourceDescriptorV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_api: Option<ResourceDescriptorV1>,
}

/// Canonical, bounded association visualization manifest.
///
/// Unknown fields are intentionally accepted by Serde so compatible V1 minor
/// additions do not break readers. The schema-version deserializer rejects any
/// unsupported major version before the value reaches consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationVisualizationManifestV1 {
    #[serde(deserialize_with = "deserialize_v1_schema_version")]
    #[schemars(regex(pattern = "^1\\.[0-9]+$"))]
    pub schema_version: String,
    pub plot_id: String,
    pub kind: PlotKindV1,
    pub title: String,
    pub assembly: String,
    pub coordinate_convention: String,
    pub source_fingerprint: String,
    pub genohype_version: String,
    pub created_at: String,
    pub layout: GenomeLayoutV1,
    pub layout_hash: String,
    pub thresholds: Vec<AssociationThresholdV1>,
    #[schemars(range(max = 9007199254740991_u64))]
    pub row_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub valid_row_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub skipped_row_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub rendered_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub significant_hit_count: u64,
    #[schemars(length(max = 1_000))]
    pub top_hits: Vec<AssociationPointV1>,
    pub warnings: Vec<String>,
    pub resources: AssociationVisualizationResourcesV1,
}

impl AssociationVisualizationManifestV1 {
    pub fn validate(&self) -> Result<(), ManifestError> {
        validate_schema_version(&self.schema_version)?;
        if self.plot_id.trim().is_empty()
            || self.title.trim().is_empty()
            || self.assembly.trim().is_empty()
            || self.source_fingerprint.trim().is_empty()
            || self.genohype_version.trim().is_empty()
            || self.created_at.trim().is_empty()
        {
            return Err(ManifestError::MissingRequiredMetadata);
        }
        if self.coordinate_convention != COORDINATE_CONVENTION_V1 {
            return Err(ManifestError::UnsupportedCoordinateConvention(
                self.coordinate_convention.clone(),
            ));
        }
        for (field, value) in [
            ("row_count", self.row_count),
            ("valid_row_count", self.valid_row_count),
            ("skipped_row_count", self.skipped_row_count),
            ("rendered_point_count", self.rendered_point_count),
            ("significant_hit_count", self.significant_hit_count),
        ] {
            if value > JSON_SAFE_INTEGER_MAX {
                return Err(ManifestError::BrowserUnsafeInteger(field));
            }
        }
        if self.valid_row_count.checked_add(self.skipped_row_count) != Some(self.row_count) {
            return Err(ManifestError::InconsistentCounts);
        }
        if self.rendered_point_count > self.valid_row_count
            || self.significant_hit_count > self.valid_row_count
        {
            return Err(ManifestError::InconsistentCounts);
        }
        if self.top_hits.len() > MAX_TOP_K {
            return Err(ManifestError::TooManyTopHits(self.top_hits.len()));
        }
        if self.top_hits.len() as u64 > self.valid_row_count {
            return Err(ManifestError::InconsistentCounts);
        }
        for point in &self.top_hits {
            let (normalized, _) = point
                .clone()
                .normalize()
                .map_err(|error| ManifestError::InvalidTopHit(error.to_string()))?;
            if normalized != *point {
                return Err(ManifestError::NonCanonicalTopHit);
            }
        }
        if !self
            .top_hits
            .windows(2)
            .all(|pair| compare_association_points(&pair[0], &pair[1]).is_le())
        {
            return Err(ManifestError::UnsortedTopHits);
        }
        self.layout
            .validate()
            .map_err(|error| ManifestError::InvalidLayout(error.to_string()))?;
        let expected_hash = self.layout.layout_hash(&canonical_assembly(&self.assembly));
        if self.layout_hash != expected_hash {
            return Err(ManifestError::LayoutHashMismatch {
                expected: expected_hash,
                actual: self.layout_hash.clone(),
            });
        }
        for threshold in &self.thresholds {
            if threshold.name.trim().is_empty()
                || !threshold.pvalue.is_finite()
                || threshold.pvalue <= 0.0
                || threshold.pvalue > 1.0
            {
                return Err(ManifestError::InvalidThreshold);
            }
        }
        self.resources.raster.validate()?;
        if let Some(resource) = &self.resources.region_api {
            resource.validate()?;
        }
        if let Some(resource) = &self.resources.table_api {
            resource.validate()?;
        }
        Ok(())
    }
}

pub fn validate_schema_version(version: &str) -> Result<(), ManifestError> {
    let (major, minor) = version
        .split_once('.')
        .ok_or_else(|| ManifestError::MalformedSchemaVersion(version.to_string()))?;
    let major: u64 = major
        .parse()
        .map_err(|_| ManifestError::MalformedSchemaVersion(version.to_string()))?;
    let _: u64 = minor
        .parse()
        .map_err(|_| ManifestError::MalformedSchemaVersion(version.to_string()))?;
    if major != 1 {
        return Err(ManifestError::UnsupportedSchemaMajor(major));
    }
    Ok(())
}

fn deserialize_v1_schema_version<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let version = String::deserialize(deserializer)?;
    validate_schema_version(&version).map_err(de::Error::custom)?;
    Ok(version)
}

/// Generate the JSON Schema whose checked-in representation lives in `schema/`.
pub fn generate_manifest_schema() -> Schema {
    schema_for!(AssociationVisualizationManifestV1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    MalformedSchemaVersion(String),
    UnsupportedSchemaMajor(u64),
    UnsupportedCoordinateConvention(String),
    MissingRequiredMetadata,
    InconsistentCounts,
    TooManyTopHits(usize),
    InvalidTopHit(String),
    NonCanonicalTopHit,
    UnsortedTopHits,
    InvalidLayout(String),
    LayoutHashMismatch { expected: String, actual: String },
    InvalidThreshold,
    BrowserUnsafeInteger(&'static str),
    UnsafeResourceUrl(String),
    MissingMediaType,
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedSchemaVersion(version) => {
                write!(f, "malformed schema version {version:?}")
            }
            Self::UnsupportedSchemaMajor(major) => {
                write!(
                    f,
                    "unsupported association manifest schema major version {major}"
                )
            }
            Self::UnsupportedCoordinateConvention(value) => {
                write!(f, "unsupported coordinate convention {value:?}")
            }
            Self::MissingRequiredMetadata => {
                write!(f, "required manifest metadata cannot be empty")
            }
            Self::InconsistentCounts => write!(f, "manifest row counts are inconsistent"),
            Self::TooManyTopHits(count) => {
                write!(f, "manifest has {count} top hits; maximum is {MAX_TOP_K}")
            }
            Self::InvalidTopHit(error) => write!(f, "invalid top hit: {error}"),
            Self::NonCanonicalTopHit => write!(f, "top hits must use canonical normalized values"),
            Self::UnsortedTopHits => write!(f, "top hits are not in canonical order"),
            Self::InvalidLayout(error) => write!(f, "invalid visualization layout: {error}"),
            Self::LayoutHashMismatch { expected, actual } => {
                write!(f, "layout hash mismatch: expected {expected}, got {actual}")
            }
            Self::InvalidThreshold => write!(f, "threshold names and p-values must be valid"),
            Self::BrowserUnsafeInteger(field) => {
                write!(f, "{field} exceeds the browser-safe integer maximum")
            }
            Self::UnsafeResourceUrl(url) => {
                write!(f, "resource URL must be safe and relative: {url}")
            }
            Self::MissingMediaType => write!(f, "resource media type cannot be empty"),
        }
    }
}

impl Error for ManifestError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn version_policy_rejects_major_and_accepts_minor_unknown_fields() {
        assert!(validate_schema_version("1.0").is_ok());
        assert!(validate_schema_version("1.99").is_ok());
        assert_eq!(
            validate_schema_version("2.0"),
            Err(ManifestError::UnsupportedSchemaMajor(2))
        );
        assert!(validate_schema_version("v1").is_err());

        let compatible = json!({
            "schema_version": "1.7",
            "plot_id": "p",
            "kind": "manhattan",
            "title": "t",
            "assembly": "GRCh38",
            "coordinate_convention": "1-based-inclusive",
            "source_fingerprint": "sha256:x",
            "genohype_version": "test",
            "created_at": "2026-09-03T00:00:00Z",
            "layout": {
                "width": 10, "height": 10, "contig_gap": 0,
                "contigs": [{"name":"1", "length":10, "x_start":0.0, "x_end":10.0, "color":"#404040"}],
                "y_scale": {"height":10,"log_threshold":10.0,"linear_fraction":0.6,"max_neg_log_p":350.0}
            },
            "layout_hash": "ignored-for-parse",
            "thresholds": [],
            "row_count": 0, "valid_row_count": 0, "skipped_row_count": 0,
            "rendered_point_count": 0, "significant_hit_count": 0,
            "top_hits": [], "warnings": [],
            "resources": {"raster":{"url":"plot.png","media_type":"image/png"}},
            "future_minor_field": {"is": "ignored"}
        });
        assert!(serde_json::from_value::<AssociationVisualizationManifestV1>(compatible).is_ok());

        let incompatible = json!({"schema_version":"2.0"});
        let error = serde_json::from_value::<AssociationVisualizationManifestV1>(incompatible)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported association manifest schema major version 2"));
    }

    #[test]
    fn resource_urls_are_confined() {
        for unsafe_url in [
            "",
            "/absolute.png",
            "\\\\server\\plot.png",
            "https://example.test/a",
            "file:plot.png",
            "a/../secret",
            "plot.png?token=secret",
            "user@example.test/plot.png",
        ] {
            let resource = ResourceDescriptorV1 {
                url: unsafe_url.into(),
                media_type: "image/png".into(),
                byte_size: None,
                checksum: None,
                expires_at: None,
            };
            assert!(matches!(
                resource.validate(),
                Err(ManifestError::UnsafeResourceUrl(_))
            ));
        }
    }
}
