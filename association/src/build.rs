use crate::model::{AssociationDatasetSummaryV1, AssociationPointV1};
use crate::reduction::{DEFAULT_TOP_K, MAX_TOP_K};
use crate::store::MAX_INSERT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

pub const DEFAULT_INSERT_BATCH_SIZE: usize = 1_000;

/// Filesystem- and runtime-neutral controls for a one-pass association build.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationBuildOptionsV1 {
    #[schemars(range(max = 1000))]
    pub top_hits: usize,
    #[schemars(range(min = 1, max = 1000))]
    pub insert_batch_size: usize,
    #[schemars(range(min = 0.0, max = 1.0))]
    pub significance_threshold: f64,
}
impl Default for AssociationBuildOptionsV1 {
    fn default() -> Self {
        Self {
            top_hits: DEFAULT_TOP_K,
            insert_batch_size: DEFAULT_INSERT_BATCH_SIZE,
            significance_threshold: 5e-8,
        }
    }
}
impl AssociationBuildOptionsV1 {
    pub fn validate(&self) -> Result<(), BuildContractError> {
        if self.top_hits > MAX_TOP_K
            || self.insert_batch_size == 0
            || self.insert_batch_size > MAX_INSERT_BATCH_SIZE
            || !self.significance_threshold.is_finite()
            || self.significance_threshold <= 0.0
            || self.significance_threshold > 1.0
        {
            return Err(BuildContractError::InvalidOptions);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationBuildProgressV1 {
    pub rows_seen: u64,
    pub valid_rows: u64,
    pub rejected_rows: u64,
    pub rows_persisted: u64,
    pub batches_committed: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationBuildOutcomeV1 {
    pub summary: AssociationDatasetSummaryV1,
    pub top_hits: Vec<AssociationPointV1>,
    pub source_fingerprint: String,
}

/// Source-neutral cancellation probe suitable for synchronous builders.
pub trait AssociationBuildCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}
#[derive(Debug, Default)]
pub struct NeverCancel;
impl AssociationBuildCancellation for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildContractError {
    InvalidOptions,
}
impl fmt::Display for BuildContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "association build options are outside bounded V1 limits")
    }
}
impl Error for BuildContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_are_bounded() {
        assert!(AssociationBuildOptionsV1::default().validate().is_ok());
        let mut bad = AssociationBuildOptionsV1::default();
        bad.insert_batch_size = 1001;
        assert!(bad.validate().is_err());
    }
}
