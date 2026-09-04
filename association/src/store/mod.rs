mod sqlite;

use crate::layout::canonical_contig;
use crate::model::AssociationPointV1;
use crate::page::{AssociationPageV1, AssociationRegionRequestV1, AssociationTablePageRequestV1};
use crate::session::DatasetIdV1;
use crate::JSON_SAFE_INTEGER_MAX;
use std::error::Error;
use std::fmt;

pub use sqlite::SqliteAssociationStore;

pub const MAX_INSERT_BATCH_SIZE: usize = 1_000;

/// Backend-neutral request for a one-pass 1-based-inclusive region scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationRegionScanRequestV1 {
    pub dataset_id: DatasetIdV1,
    pub contig: String,
    pub start: i64,
    pub end: i64,
}

impl AssociationRegionScanRequestV1 {
    pub fn validate(&self) -> Result<String, AssociationStoreError> {
        if self.start < 1
            || self.end < self.start
            || self.start as u64 > JSON_SAFE_INTEGER_MAX
            || self.end as u64 > JSON_SAFE_INTEGER_MAX
            || self.contig.len() > 4_096
        {
            return Err(AssociationStoreError::InvalidRequest(
                "region must use browser-safe 1-based inclusive bounds".into(),
            ));
        }
        canonical_contig(&self.contig)
            .ok_or_else(|| AssociationStoreError::InvalidRequest("region contig is invalid".into()))
    }
}

/// Exact result of a successful one-pass region scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationRegionScanSummaryV1 {
    pub dataset_id: DatasetIdV1,
    pub contig: String,
    pub start: i64,
    pub end: i64,
    pub point_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationBuildV1 {
    pub dataset_id: DatasetIdV1,
    pub created_at: String,
}

/// Synchronous backend-neutral indexed association storage contract.
/// Implementations must keep backend-specific connection, row, and error types private.
pub trait AssociationStore {
    fn begin_build(&mut self, build: &AssociationBuildV1) -> Result<(), AssociationStoreError>;
    fn insert_points(
        &mut self,
        dataset_id: &DatasetIdV1,
        points: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError>;
    fn replace_top_hits(
        &mut self,
        dataset_id: &DatasetIdV1,
        points: &[AssociationPointV1],
    ) -> Result<(), AssociationStoreError>;
    /// Atomically installs the source identity finalized at EOF and marks the dataset complete.
    fn complete_build(
        &mut self,
        dataset_id: &DatasetIdV1,
        source_fingerprint: &str,
    ) -> Result<(), AssociationStoreError>;
    fn abort_build(&mut self, dataset_id: &DatasetIdV1) -> Result<(), AssociationStoreError>;
    fn is_complete(&self, dataset_id: &DatasetIdV1) -> Result<bool, AssociationStoreError>;
    fn top_hits(
        &self,
        dataset_id: &DatasetIdV1,
        limit: u32,
    ) -> Result<Vec<AssociationPointV1>, AssociationStoreError>;
    fn table_page(
        &self,
        request: &AssociationTablePageRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError>;
    fn region_page(
        &self,
        request: &AssociationRegionRequestV1,
    ) -> Result<AssociationPageV1, AssociationStoreError>;
    /// Visit every canonical point in one indexed pass without paging or materializing the region.
    ///
    /// Points are visited in deterministic position/ref/alt/source-ordinal order. Returning an
    /// error from `visitor` stops the scan immediately and returns that exact error. A summary is
    /// returned only after every matching point has been visited successfully.
    fn scan_region(
        &self,
        _request: &AssociationRegionScanRequestV1,
        _visitor: &mut dyn FnMut(&AssociationPointV1) -> Result<(), AssociationStoreError>,
    ) -> Result<AssociationRegionScanSummaryV1, AssociationStoreError> {
        Err(AssociationStoreError::Backend(
            "association store does not support region scanning".into(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssociationStoreError {
    ReadOnly,
    InvalidBatchSize,
    InvalidPoint(String),
    InvalidRequest(String),
    InvalidBuildState,
    DatasetNotFound,
    Sink(String),
    Schema(String),
    Backend(String),
}
impl fmt::Display for AssociationStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "association store is read-only"),
            Self::InvalidBatchSize => write!(
                f,
                "association insert batch must contain 1..={MAX_INSERT_BATCH_SIZE} rows"
            ),
            Self::InvalidPoint(message) => write!(f, "invalid association point: {message}"),
            Self::InvalidRequest(message) => write!(f, "invalid association query: {message}"),
            Self::InvalidBuildState => {
                write!(f, "association dataset is not in the required build state")
            }
            Self::DatasetNotFound => write!(f, "association dataset was not found"),
            Self::Sink(message) => write!(f, "association point sink failed: {message}"),
            Self::Schema(message) => write!(f, "association store schema error: {message}"),
            Self::Backend(message) => write!(f, "association store backend error: {message}"),
        }
    }
}
impl Error for AssociationStoreError {}
