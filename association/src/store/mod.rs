mod sqlite;

use crate::model::AssociationPointV1;
use crate::page::{AssociationPageV1, AssociationRegionRequestV1, AssociationTablePageRequestV1};
use crate::session::DatasetIdV1;
use std::error::Error;
use std::fmt;

pub use sqlite::SqliteAssociationStore;

pub const MAX_INSERT_BATCH_SIZE: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationBuildV1 {
    pub dataset_id: DatasetIdV1,
    pub source_fingerprint: String,
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
    fn complete_build(&mut self, dataset_id: &DatasetIdV1) -> Result<(), AssociationStoreError>;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssociationStoreError {
    ReadOnly,
    InvalidBatchSize,
    InvalidPoint(String),
    InvalidRequest(String),
    InvalidBuildState,
    DatasetNotFound,
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
            Self::Schema(message) => write!(f, "association store schema error: {message}"),
            Self::Backend(message) => write!(f, "association store backend error: {message}"),
        }
    }
}
impl Error for AssociationStoreError {}
