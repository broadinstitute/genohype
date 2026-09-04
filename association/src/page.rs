use crate::layout::canonical_contig;
use crate::manifest::validate_schema_version;
use crate::model::AssociationPointV1;
use crate::session::DatasetIdV1;
use crate::JSON_SAFE_INTEGER_MAX;
use schemars::{schema_for, JsonSchema, Schema};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::error::Error;
use std::fmt;

pub const ASSOCIATION_PAGE_SCHEMA_VERSION_V1: &str = "1.0";
pub const DEFAULT_PAGE_SIZE: u32 = 100;
pub const MAX_PAGE_SIZE: u32 = 1_000;
pub const MAX_SEARCH_BYTES: usize = 256;
pub const MAX_PAGE_POINT_TEXT_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AssociationTableSortV1 {
    Pvalue,
    Locus,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AssociationSortDirectionV1 {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationTablePageRequestV1 {
    pub dataset_id: DatasetIdV1,
    pub offset: u64,
    pub limit: u32,
    pub sort: AssociationTableSortV1,
    pub direction: AssociationSortDirectionV1,
    pub search: Option<String>,
}
impl AssociationTablePageRequestV1 {
    pub fn validate(&self) -> Result<(), PageError> {
        validate_window(self.offset, self.limit)?;
        if self.search.as_ref().is_some_and(|search| {
            search.len() > MAX_SEARCH_BYTES || search.bytes().any(|byte| byte.is_ascii_control())
        }) {
            return Err(PageError::InvalidSearch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationRegionRequestV1 {
    pub dataset_id: DatasetIdV1,
    pub contig: String,
    pub start: i64,
    pub end: i64,
    pub offset: u64,
    pub limit: u32,
}
impl AssociationRegionRequestV1 {
    pub fn validate(&self) -> Result<String, PageError> {
        validate_window(self.offset, self.limit)?;
        if self.start < 1
            || self.end < self.start
            || self.end as u64 > JSON_SAFE_INTEGER_MAX
            || self.contig.len() > 4_096
        {
            return Err(PageError::InvalidRegion);
        }
        canonical_contig(&self.contig).ok_or(PageError::InvalidRegion)
    }
}

fn validate_window(offset: u64, limit: u32) -> Result<(), PageError> {
    if offset > JSON_SAFE_INTEGER_MAX || limit == 0 || limit > MAX_PAGE_SIZE {
        return Err(PageError::InvalidWindow);
    }
    Ok(())
}

/// Versioned bounded envelope shared by table and inclusive-region APIs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationPageV1 {
    #[serde(deserialize_with = "deserialize_page_schema_version")]
    #[schemars(regex(pattern = "^1\\.[0-9]+$"))]
    pub schema_version: String,
    pub dataset_id: DatasetIdV1,
    #[schemars(length(max = 1000))]
    pub points: Vec<AssociationPointV1>,
    #[schemars(range(max = 9007199254740991_u64))]
    pub offset: u64,
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u32,
    #[schemars(range(max = 9007199254740991_u64))]
    pub total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<AssociationTableSortV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<AssociationSortDirectionV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub search: Option<String>,
}

impl AssociationPageV1 {
    pub fn validate(&self) -> Result<(), PageError> {
        validate_schema_version(&self.schema_version)
            .map_err(|_| PageError::InvalidSchemaVersion)?;
        validate_window(self.offset, self.limit)?;
        if self.total > JSON_SAFE_INTEGER_MAX
            || self.points.len() > self.limit as usize
            || self.points.len() > MAX_PAGE_SIZE as usize
        {
            return Err(PageError::InconsistentPage);
        }
        if !self.points.is_empty()
            && (self.offset >= self.total
                || self.offset.checked_add(self.points.len() as u64).is_none()
                || self.offset + self.points.len() as u64 > self.total)
        {
            return Err(PageError::InconsistentPage);
        }
        if self.sort.is_some() != self.direction.is_some()
            || (self.sort.is_none() && self.search.is_some())
        {
            return Err(PageError::InconsistentPage);
        }
        if self
            .search
            .as_ref()
            .is_some_and(|value| value.len() > MAX_SEARCH_BYTES)
        {
            return Err(PageError::InvalidSearch);
        }
        let mut text_bytes = 0_usize;
        for point in &self.points {
            let (normalized, _) = point
                .clone()
                .normalize()
                .map_err(|_| PageError::InvalidPoint)?;
            if normalized != *point {
                return Err(PageError::InvalidPoint);
            }
            text_bytes = text_bytes
                .checked_add(point_text_bytes(point))
                .ok_or(PageError::PayloadTooLarge)?;
        }
        if text_bytes > MAX_PAGE_POINT_TEXT_BYTES {
            return Err(PageError::PayloadTooLarge);
        }
        Ok(())
    }
}

fn point_text_bytes(point: &AssociationPointV1) -> usize {
    [
        Some(point.contig.as_str()),
        point.ref_allele.as_deref(),
        point.alt_allele.as_deref(),
        point.gene_symbol.as_deref(),
        point.consequence.as_deref(),
        point.series.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::len)
    .sum::<usize>()
}

fn deserialize_page_schema_version<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    let version = String::deserialize(deserializer)?;
    validate_schema_version(&version).map_err(de::Error::custom)?;
    Ok(version)
}
pub fn generate_association_page_schema() -> Schema {
    schema_for!(AssociationPageV1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    InvalidSchemaVersion,
    InvalidWindow,
    InvalidSearch,
    InvalidRegion,
    InconsistentPage,
    InvalidPoint,
    PayloadTooLarge,
}
impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchemaVersion => write!(f, "unsupported association page schema version"),
            Self::InvalidWindow => write!(f, "page offset or limit is outside the supported range"),
            Self::InvalidSearch => write!(
                f,
                "search text is invalid or exceeds {MAX_SEARCH_BYTES} UTF-8 bytes"
            ),
            Self::InvalidRegion => write!(f, "region must be 1-based and inclusive"),
            Self::InconsistentPage => write!(f, "association page fields are inconsistent"),
            Self::InvalidPoint => write!(f, "association page contains a non-canonical point"),
            Self::PayloadTooLarge => write!(
                f,
                "association page point text exceeds {MAX_PAGE_POINT_TEXT_BYTES} UTF-8 bytes"
            ),
        }
    }
}
impl Error for PageError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requests_are_bounded() {
        let id = DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap();
        let request = AssociationTablePageRequestV1 {
            dataset_id: id,
            offset: 0,
            limit: 1000,
            sort: AssociationTableSortV1::Pvalue,
            direction: AssociationSortDirectionV1::Ascending,
            search: Some("x".repeat(256)),
        };
        assert!(request.validate().is_ok());
        let mut bad = request.clone();
        bad.limit = 1001;
        assert!(bad.validate().is_err());
    }
    #[test]
    fn nonempty_pages_cannot_extend_beyond_total() {
        let point = AssociationPointV1 {
            contig: "1".into(),
            position: 1,
            ref_allele: None,
            alt_allele: None,
            pvalue: 0.5,
            neg_log10_p: -0.5_f64.log10(),
            beta: None,
            standard_error: None,
            allele_frequency: None,
            allele_count: None,
            gene_symbol: None,
            consequence: None,
            series: None,
            source_ordinal: 0,
        };
        let page = AssociationPageV1 {
            schema_version: "1.0".into(),
            dataset_id: DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap(),
            points: vec![point],
            offset: 1,
            limit: 100,
            total: 1,
            sort: Some(AssociationTableSortV1::Pvalue),
            direction: Some(AssociationSortDirectionV1::Ascending),
            search: None,
        };
        assert_eq!(page.validate(), Err(PageError::InconsistentPage));
    }

    #[test]
    fn regions_are_inclusive_and_canonicalized() {
        let request = AssociationRegionRequestV1 {
            dataset_id: DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap(),
            contig: "chr1".into(),
            start: 1,
            end: 1,
            offset: 0,
            limit: 100,
        };
        assert_eq!(request.validate().unwrap(), "1");
    }
}
