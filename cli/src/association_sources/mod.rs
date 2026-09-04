pub mod delimited;
pub mod hail;
pub mod inspect;
pub mod mapping;
pub mod vcf;

use genohype_association::AssociationSourceKindV1;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedAssociationSource {
    Hail,
    Vcf,
    Delimited { delimiter: u8, gzip: bool },
}

impl DetectedAssociationSource {
    pub const fn source_kind(self) -> AssociationSourceKindV1 {
        match self {
            Self::Hail => AssociationSourceKindV1::Hail,
            Self::Vcf => AssociationSourceKindV1::Vcf,
            Self::Delimited {
                delimiter: b',', ..
            } => AssociationSourceKindV1::Csv,
            Self::Delimited { .. } => AssociationSourceKindV1::Tsv,
        }
    }

    pub const fn label(self) -> &'static str {
        match self.source_kind() {
            AssociationSourceKindV1::Hail => "hail",
            AssociationSourceKindV1::Csv => "csv",
            AssociationSourceKindV1::Tsv => "tsv",
            AssociationSourceKindV1::Vcf => "vcf",
        }
    }
}

pub fn detect_source(source: &str) -> Result<DetectedAssociationSource, SourceDetectionError> {
    let normalized = source.trim_end_matches('/').to_ascii_lowercase();
    if normalized.ends_with(".csv.gz") {
        return Ok(DetectedAssociationSource::Delimited {
            delimiter: b',',
            gzip: true,
        });
    }
    if normalized.ends_with(".tsv.gz") {
        return Ok(DetectedAssociationSource::Delimited {
            delimiter: b'\t',
            gzip: true,
        });
    }
    if normalized.ends_with(".csv") {
        return Ok(DetectedAssociationSource::Delimited {
            delimiter: b',',
            gzip: false,
        });
    }
    if normalized.ends_with(".tsv") {
        return Ok(DetectedAssociationSource::Delimited {
            delimiter: b'\t',
            gzip: false,
        });
    }
    if normalized.ends_with(".vcf")
        || normalized.ends_with(".vcf.gz")
        || normalized.ends_with(".vcf.bgz")
    {
        return Ok(DetectedAssociationSource::Vcf);
    }
    if normalized.ends_with(".ht") {
        return Ok(DetectedAssociationSource::Hail);
    }
    if normalized.ends_with(".parquet") || normalized.ends_with(".pq") {
        return Err(SourceDetectionError::new(
            "Parquet association input is not implemented; use Hail, VCF, CSV, or TSV",
        ));
    }
    if normalized.ends_with(".gz") || normalized.ends_with(".bgz") {
        return Err(SourceDetectionError::new(
            "generic gzip input is ambiguous; use .csv.gz, .tsv.gz, .vcf.gz, or .vcf.bgz",
        ));
    }
    Err(SourceDetectionError::new(
        "unsupported association source; expected .ht, .vcf, .vcf.gz, .vcf.bgz, .csv, .tsv, .csv.gz, or .tsv.gz",
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDetectionError {
    message: String,
}
impl SourceDetectionError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
impl fmt::Display for SourceDetectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for SourceDetectionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detection_is_explicit_and_fail_closed() {
        assert_eq!(
            detect_source("x.ht").unwrap(),
            DetectedAssociationSource::Hail
        );
        assert_eq!(
            detect_source("x.vcf.gz").unwrap(),
            DetectedAssociationSource::Vcf
        );
        assert_eq!(
            detect_source("x.CSV.GZ").unwrap(),
            DetectedAssociationSource::Delimited {
                delimiter: b',',
                gzip: true
            }
        );
        assert!(detect_source("x.gz")
            .unwrap_err()
            .to_string()
            .contains("generic gzip"));
        assert!(detect_source("x.parquet")
            .unwrap_err()
            .to_string()
            .contains("Parquet"));
        assert!(detect_source("x.txt").is_err());
    }
}
