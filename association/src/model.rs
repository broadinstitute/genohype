use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

use crate::layout::canonical_contig;
use crate::scale::MAX_NEG_LOG_P;

/// Stable warning codes emitted during association normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WarningCode {
    /// A zero raw p-value was treated as floating-point underflow.
    PvalueUnderflow,
    /// A supplied significance exceeded the canonical display cap.
    NegLog10PDisplayCapped,
}

impl WarningCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PvalueUnderflow => "pvalue_underflow",
            Self::NegLog10PDisplayCapped => "neg_log10_p_display_capped",
        }
    }
}

/// Result of applying the canonical p-value rules.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedPValue {
    pub pvalue: f64,
    pub neg_log10_p: f64,
    pub warnings: Vec<WarningCode>,
}

/// Why a p-value pair cannot be represented by the canonical contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PValueError {
    InvalidPvalue,
    InvalidNegLog10P,
}

impl fmt::Display for PValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPvalue => write!(f, "pvalue must be finite and in the range 0..=1"),
            Self::InvalidNegLog10P => {
                write!(f, "neg_log10_p must be finite and non-negative")
            }
        }
    }
}

impl Error for PValueError {}

/// Normalize a raw p-value and an optional source-provided `-log10(p)` value.
///
/// A source-provided value is preferred because it preserves significance when
/// the raw value has underflowed. Zero is valid underflow, while negative,
/// non-finite, and greater-than-one p-values are rejected. Effective significance
/// is never capped here: values above 350 remain available for deterministic
/// ordering, while the warning tells renderers that the Y-scale will clip them.
pub fn normalize_pvalue(
    pvalue: f64,
    supplied_neg_log10_p: Option<f64>,
) -> Result<NormalizedPValue, PValueError> {
    if !pvalue.is_finite() || !(0.0..=1.0).contains(&pvalue) {
        return Err(PValueError::InvalidPvalue);
    }
    if supplied_neg_log10_p.is_some_and(|value| !value.is_finite() || value < 0.0) {
        return Err(PValueError::InvalidNegLog10P);
    }

    let mut warnings = Vec::new();
    let significance = if pvalue == 0.0 {
        warnings.push(WarningCode::PvalueUnderflow);
        supplied_neg_log10_p.unwrap_or(MAX_NEG_LOG_P)
    } else {
        supplied_neg_log10_p.unwrap_or_else(|| -pvalue.log10())
    };
    if significance > MAX_NEG_LOG_P {
        warnings.push(WarningCode::NegLog10PDisplayCapped);
    }

    Ok(NormalizedPValue {
        pvalue,
        neg_log10_p: significance,
        warnings,
    })
}

/// Canonical normalized single-variant association row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationPointV1 {
    #[schemars(length(min = 1))]
    pub contig: String,
    #[schemars(range(min = 1))]
    pub position: i64,
    pub ref_allele: Option<String>,
    pub alt_allele: Option<String>,
    #[schemars(range(min = 0.0, max = 1.0))]
    pub pvalue: f64,
    #[schemars(range(min = 0.0))]
    pub neg_log10_p: f64,
    pub beta: Option<f64>,
    pub standard_error: Option<f64>,
    #[schemars(range(min = 0.0, max = 1.0))]
    pub allele_frequency: Option<f64>,
    #[schemars(range(min = 0.0))]
    pub allele_count: Option<f64>,
    pub gene_symbol: Option<String>,
    pub consequence: Option<String>,
    pub series: Option<String>,
    pub source_ordinal: u64,
}

impl AssociationPointV1 {
    /// Validate fields and canonicalize the contig and p-value representation.
    pub fn normalize(mut self) -> Result<(Self, Vec<WarningCode>), ValidationError> {
        if self.position < 1 {
            return Err(ValidationError::InvalidPosition);
        }
        let contig = canonical_contig(&self.contig).ok_or(ValidationError::InvalidContig)?;
        let normalized = normalize_pvalue(self.pvalue, Some(self.neg_log10_p))?;
        validate_optional_finite(self.beta)?;
        validate_optional_finite(self.standard_error)?;
        validate_optional_finite(self.allele_frequency)?;
        validate_optional_finite(self.allele_count)?;
        if self
            .allele_frequency
            .is_some_and(|frequency| !(0.0..=1.0).contains(&frequency))
        {
            return Err(ValidationError::InvalidAlleleFrequency);
        }
        if self.allele_count.is_some_and(|count| count < 0.0) {
            return Err(ValidationError::InvalidAlleleCount);
        }

        self.contig = contig;
        self.neg_log10_p = normalized.neg_log10_p;
        Ok((self, normalized.warnings))
    }
}

fn validate_optional_finite(value: Option<f64>) -> Result<(), ValidationError> {
    if value.is_some_and(|number| !number.is_finite()) {
        Err(ValidationError::NonFiniteOptionalStatistic)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationError {
    InvalidContig,
    InvalidPosition,
    InvalidAlleleFrequency,
    InvalidAlleleCount,
    NonFiniteOptionalStatistic,
    InvalidPvalue(PValueError),
}

impl From<PValueError> for ValidationError {
    fn from(value: PValueError) -> Self {
        Self::InvalidPvalue(value)
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContig => write!(f, "contig cannot be empty"),
            Self::InvalidPosition => write!(f, "position must be 1-based and positive"),
            Self::InvalidAlleleFrequency => write!(f, "allele_frequency must be in 0..=1"),
            Self::InvalidAlleleCount => write!(f, "allele_count must be non-negative"),
            Self::NonFiniteOptionalStatistic => write!(f, "optional statistics must be finite"),
            Self::InvalidPvalue(error) => error.fmt(f),
        }
    }
}

impl Error for ValidationError {}

/// Canonical counts and warnings accumulated while scanning a dataset.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationDatasetSummaryV1 {
    pub row_count: u64,
    pub valid_row_count: u64,
    pub skipped_row_count: u64,
    pub rendered_point_count: u64,
    pub significant_hit_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl AssociationDatasetSummaryV1 {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.valid_row_count + self.skipped_row_count != self.row_count {
            return Err("valid_row_count + skipped_row_count must equal row_count");
        }
        if self.rendered_point_count > self.valid_row_count {
            return Err("rendered_point_count cannot exceed valid_row_count");
        }
        if self.significant_hit_count > self.valid_row_count {
            return Err("significant_hit_count cannot exceed valid_row_count");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point() -> AssociationPointV1 {
        AssociationPointV1 {
            contig: "chr1".into(),
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
        }
    }

    #[test]
    fn pvalue_boundaries_and_underflow_are_exact() {
        assert_eq!(normalize_pvalue(1.0, None).unwrap().neg_log10_p, 0.0);
        assert_eq!(normalize_pvalue(1e-8, None).unwrap().neg_log10_p, 8.0);
        let zero = normalize_pvalue(0.0, None).unwrap();
        assert_eq!(zero.neg_log10_p, MAX_NEG_LOG_P);
        assert_eq!(zero.warnings, vec![WarningCode::PvalueUnderflow]);
        let preserved = normalize_pvalue(0.0, Some(123.0)).unwrap();
        assert_eq!(preserved.neg_log10_p, 123.0);
        assert_eq!(preserved.warnings, vec![WarningCode::PvalueUnderflow]);
    }

    #[test]
    fn pvalue_invalid_values_are_rejected() {
        for pvalue in [
            -1.0,
            1.000_000_1,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert_eq!(
                normalize_pvalue(pvalue, None),
                Err(PValueError::InvalidPvalue)
            );
        }
        for supplied in [-1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                normalize_pvalue(0.0, Some(supplied)),
                Err(PValueError::InvalidNegLog10P)
            );
        }
    }

    #[test]
    fn significance_above_display_cap_is_preserved_with_warning() {
        let normalized = normalize_pvalue(0.0, Some(999.0)).unwrap();
        assert_eq!(normalized.neg_log10_p, 999.0);
        assert_eq!(
            normalized.warnings,
            vec![
                WarningCode::PvalueUnderflow,
                WarningCode::NegLog10PDisplayCapped
            ]
        );
    }

    #[test]
    fn point_normalization_validates_domain_fields() {
        let (normalized, warnings) = point().normalize().unwrap();
        assert_eq!(normalized.contig, "1");
        assert!(warnings.is_empty());

        let mut invalid = point();
        invalid.allele_frequency = Some(1.1);
        assert_eq!(
            invalid.normalize().unwrap_err(),
            ValidationError::InvalidAlleleFrequency
        );
    }
}
