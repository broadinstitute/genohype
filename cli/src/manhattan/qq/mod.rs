//! QQ Plot Data Export.
//!
//! Processes expected p-values from Hail tables into Parquet format for QQ plots.
//! This module handles the export of observed vs expected p-values for quality
//! control visualization.

use crate::manhattan::data::{QQPointRow, QQStats};
use crate::manhattan::qq_writer::QQPointWriter;
use crate::manhattan::reference::normalize_contig_name;
use crate::Result;
use genohype_core::codec::EncodedValue;
use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
use genohype_core::query::QueryEngine;

/// Result of scanning a QQ table to Parquet.
pub struct QQScanResult {
    /// Total number of rows written
    pub total_rows: usize,
    /// Lambda GC statistics from globals
    pub stats: QQStats,
}

/// Scan a variant_exp_p table and export to Parquet for QQ plots.
///
/// # Arguments
/// * `path` - Path to the variant_exp_p Hail table
/// * `phenotype` - Phenotype identifier
/// * `ancestry` - Ancestry group
/// * `sequencing_type` - "exomes" or "genomes"
/// * `output_path` - Path to write the Parquet file
///
/// # Returns
/// A `QQScanResult` containing row count and lambda stats.
pub fn scan_qq_to_parquet(
    path: &str,
    phenotype: &str,
    ancestry: &str,
    sequencing_type: &str,
    output_path: &str,
) -> Result<QQScanResult> {
    let engine = QueryEngine::open_path(path)?;

    // Extract lambda stats from globals
    let stats = extract_qq_stats(&engine);

    // Initialize writer
    let mut writer: Box<dyn QQWriterTrait> = if is_cloud_path(output_path) {
        let cloud_writer = StreamingCloudWriter::new(output_path)?;
        Box::new(CloudQQWriter {
            writer: QQPointWriter::from_writer(cloud_writer)?,
        })
    } else {
        Box::new(LocalQQWriter {
            writer: QQPointWriter::new(output_path)?,
        })
    };

    let iter = engine.query_iter(&[])?;
    let mut total_rows = 0;

    for row_res in iter {
        let row = row_res?;
        if let EncodedValue::Struct(fields) = row {
            // Helper closures
            let get_str = |k: &str| -> Option<String> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| v.as_string())
            };
            let get_f64 = |k: &str| -> Option<f64> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| encoded_as_f64(v))
            };
            let get_i32 = |k: &str| -> Option<i32> {
                fields
                    .iter()
                    .find(|(n, _)| n == k)
                    .and_then(|(_, v)| v.as_i32())
            };

            // Extract p-values (required)
            let pvalue_log10 = get_f64("Pvalue_log10");
            let pvalue_expected_log10 = get_f64("Pvalue_expected_log10");

            // Skip rows without valid p-values
            let (pv_obs, pv_exp) = match (pvalue_log10, pvalue_expected_log10) {
                (Some(obs), Some(exp)) if obs.is_finite() && exp.is_finite() => (obs, exp),
                _ => continue,
            };

            // Extract location - try locus first, then CHR/POS
            let (contig, position) = extract_locus_fields(&fields)
                .unwrap_or_else(|| {
                    let chr = get_str("CHR").unwrap_or_default();
                    let pos = get_i32("POS").unwrap_or(0);
                    (chr, pos)
                });

            // Extract alleles
            let (ref_allele, alt_allele) = extract_alleles(&fields);

            let row_out = QQPointRow {
                phenotype: phenotype.to_string(),
                ancestry: ancestry.to_string(),
                sequencing_type: sequencing_type.to_string(),
                contig: normalize_contig_name(&contig),
                position,
                ref_allele,
                alt_allele,
                pvalue_log10: pv_obs,
                pvalue_expected_log10: pv_exp,
            };

            writer.write(row_out)?;
            total_rows += 1;
        }
    }

    writer.finish()?;

    Ok(QQScanResult { total_rows, stats })
}

/// Helper to extract f64 from EncodedValue.
fn encoded_as_f64(val: &EncodedValue) -> Option<f64> {
    match val {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        EncodedValue::Int64(i) => Some(*i as f64),
        EncodedValue::Int32(i) => Some(*i as f64),
        _ => None,
    }
}

/// Extract locus fields from a row.
fn extract_locus_fields(fields: &[(String, EncodedValue)]) -> Option<(String, i32)> {
    if let Some((_, EncodedValue::Struct(locus))) = fields.iter().find(|(n, _)| n == "locus") {
        let contig = locus
            .iter()
            .find(|(n, _)| n == "contig")
            .and_then(|(_, v)| match v {
                EncodedValue::Binary(b) => Some(String::from_utf8_lossy(b).to_string()),
                _ => v.as_string(),
            })?;
        let position = locus
            .iter()
            .find(|(n, _)| n == "position")
            .and_then(|(_, v)| v.as_i32())?;
        Some((contig, position))
    } else {
        None
    }
}

/// Extract ref/alt alleles from the alleles array.
fn extract_alleles(fields: &[(String, EncodedValue)]) -> (String, String) {
    if let Some((_, EncodedValue::Array(alleles))) = fields.iter().find(|(n, _)| n == "alleles") {
        let ref_allele = alleles
            .first()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let alt_allele = alleles
            .get(1)
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        (ref_allele, alt_allele)
    } else {
        (String::new(), String::new())
    }
}

/// Extract QQ statistics from table globals.
///
/// TODO: Implement globals reading from Hail table metadata.
/// For now, returns empty stats - lambda values would need to be
/// read from the globals/part-0-... file in the Hail table directory.
fn extract_qq_stats(_engine: &QueryEngine) -> QQStats {
    // Lambda statistics are stored in the table globals, which requires
    // additional implementation to read. For now, return empty stats.
    QQStats {
        lambda_gc: None,
        lambda_q0_5: None,
        lambda_q0_1: None,
        lambda_q0_01: None,
        lambda_q0_001: None,
    }
}

// =============================================================================
// QQ Writer abstraction for local vs cloud output
// =============================================================================

/// Trait to abstract over local and cloud QQ writers.
trait QQWriterTrait {
    fn write(&mut self, row: QQPointRow) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<usize>;
}

/// Local file writer wrapper.
struct LocalQQWriter {
    writer: QQPointWriter<std::fs::File>,
}

impl QQWriterTrait for LocalQQWriter {
    fn write(&mut self, row: QQPointRow) -> Result<()> {
        self.writer.write(row)
    }

    fn finish(self: Box<Self>) -> Result<usize> {
        self.writer.finish()
    }
}

/// Cloud writer wrapper.
struct CloudQQWriter {
    writer: QQPointWriter<StreamingCloudWriter>,
}

impl QQWriterTrait for CloudQQWriter {
    fn write(&mut self, row: QQPointRow) -> Result<()> {
        self.writer.write(row)
    }

    fn finish(self: Box<Self>) -> Result<usize> {
        let cloud_writer = self.writer.into_inner()?;
        let rows = cloud_writer.bytes_written();
        cloud_writer.finish()?;
        Ok(rows)
    }
}
