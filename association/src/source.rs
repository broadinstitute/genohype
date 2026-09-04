use crate::model::{AssociationPointV1, WarningCode};
use crate::session::NormalizedFieldMappingV1;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

pub const MAX_REJECTION_EXAMPLE_BYTES: usize = 80;
pub const MAX_REJECTION_EXAMPLES_PER_REASON: usize = 5;
pub const MAX_REJECTION_REASON_CATEGORIES: usize = 100;
const REJECTION_OVERFLOW_REASON: &str = "other_rejections";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AssociationSourceKindV1 {
    Hail,
    Csv,
    Tsv,
    Vcf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationSourceFieldV1 {
    #[schemars(length(min = 1, max = 256))]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128))]
    pub data_type: Option<String>,
}

/// Non-path metadata safe to carry across the source/domain boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationSourceMetadataV1 {
    pub kind: AssociationSourceKindV1,
    #[schemars(length(max = 4096))]
    pub observed_fields: Vec<AssociationSourceFieldV1>,
    pub normalized_mapping: NormalizedFieldMappingV1,
    #[schemars(length(min = 1, max = 64))]
    pub assembly: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub display_name: Option<String>,
}

impl AssociationSourceMetadataV1 {
    pub fn validate(&self) -> Result<(), AssociationSourceError> {
        if self.observed_fields.len() > 4_096
            || self.assembly.is_empty()
            || self.assembly.len() > 64
            || self
                .display_name
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 256)
            || self.observed_fields.iter().any(|field| {
                field.name.is_empty()
                    || field.name.len() > 256
                    || field
                        .data_type
                        .as_ref()
                        .is_some_and(|value| value.is_empty() || value.len() > 128)
            })
            || self.normalized_mapping.validate().is_err()
        {
            return Err(AssociationSourceError::new(
                "invalid_source_metadata",
                "source metadata is incomplete or exceeds bounded V1 limits",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationSourceRejectionV1 {
    #[schemars(range(max = 9007199254740991_u64))]
    pub source_ordinal: u64,
    #[schemars(length(min = 1, max = 128))]
    pub reason_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 256))]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 80))]
    pub bounded_example: Option<String>,
}

impl AssociationSourceRejectionV1 {
    pub fn new(
        source_ordinal: u64,
        reason_code: impl Into<String>,
        field: Option<String>,
        example: Option<&str>,
    ) -> Self {
        let reason_code = reason_code.into();
        Self {
            source_ordinal,
            reason_code: if reason_code.is_empty() {
                "rejected".into()
            } else {
                truncate_utf8(&reason_code, 128)
            },
            field: field.map(|value| truncate_utf8(&value, 256)),
            bounded_example: example.map(|value| truncate_utf8(value, MAX_REJECTION_EXAMPLE_BYTES)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssociationSourceItem {
    Point {
        point: AssociationPointV1,
        warnings: Vec<WarningCode>,
    },
    Rejected(AssociationSourceRejectionV1),
}

/// One-pass source contract. Recoverable row errors are `Rejected`; framing,
/// decoding, and I/O failures are returned as fatal errors.
pub trait AssociationSource {
    fn metadata(&self) -> &AssociationSourceMetadataV1;
    fn next_item(&mut self) -> Result<Option<AssociationSourceItem>, AssociationSourceError>;
    /// Available only after `next_item` has returned `Ok(None)`.
    fn finalized_source_fingerprint(&self) -> Option<&str>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationSourceError {
    pub code: String,
    pub message: String,
}
impl AssociationSourceError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}
impl fmt::Display for AssociationSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl Error for AssociationSourceError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RejectionDiagnostics {
    pub total: u64,
    pub reasons: BTreeMap<String, RejectionReasonDiagnostics>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RejectionReasonDiagnostics {
    pub count: u64,
    pub examples: Vec<String>,
}
impl RejectionDiagnostics {
    pub fn record(&mut self, rejection: &AssociationSourceRejectionV1) {
        self.total = self.total.saturating_add(1);
        let reason_code = if self.reasons.contains_key(&rejection.reason_code) {
            rejection.reason_code.as_str()
        } else if self.reasons.len() < MAX_REJECTION_REASON_CATEGORIES.saturating_sub(1) {
            rejection.reason_code.as_str()
        } else {
            REJECTION_OVERFLOW_REASON
        };
        let reason = self.reasons.entry(reason_code.to_string()).or_default();
        reason.count = reason.count.saturating_add(1);
        if reason.examples.len() < MAX_REJECTION_EXAMPLES_PER_REASON {
            if let Some(example) = &rejection.bounded_example {
                reason
                    .examples
                    .push(truncate_utf8(example, MAX_REJECTION_EXAMPLE_BYTES));
            }
        }
    }
}

fn truncate_utf8(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_string();
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejection_examples_are_bounded_without_breaking_utf8() {
        let rejection = AssociationSourceRejectionV1::new(1, "bad", None, Some(&"é".repeat(100)));
        assert!(rejection.bounded_example.unwrap().len() <= 80);
    }
    #[test]
    fn diagnostics_bound_examples_and_reason_categories() {
        let mut diagnostics = RejectionDiagnostics::default();
        for ordinal in 0..200 {
            diagnostics.record(&AssociationSourceRejectionV1::new(
                ordinal,
                format!("bad_{ordinal}"),
                None,
                Some("value"),
            ));
        }
        assert_eq!(diagnostics.total, 200);
        assert!(diagnostics.reasons.len() <= MAX_REJECTION_REASON_CATEGORIES);
        assert!(diagnostics.reasons.contains_key(REJECTION_OVERFLOW_REASON));
        assert!(diagnostics
            .reasons
            .values()
            .all(|reason| reason.examples.len() <= MAX_REJECTION_EXAMPLES_PER_REASON));
    }
}
