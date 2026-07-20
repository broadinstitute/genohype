//! Parquet writers for locus definition and locus variant rows.
//!
//! This module provides writers for the consolidated loci output:
//! - `LocusDefinitionWriter` for `loci.parquet` (one row per locus)
//! - `LocusVariantWriter` for `loci_variants.parquet` (all variants in all loci)

use crate::manhattan::data::{LocusDefinitionRow, LocusVariantRow};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use genohype_core::error::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

// =============================================================================
// Locus Definition Writer
// =============================================================================

/// Writer for locus definition rows (loci.parquet).
///
/// Schema:
/// - locus_id: string (unique identifier)
/// - phenotype: string
/// - ancestry: string
/// - contig: string (chr-prefixed)
/// - start: int32
/// - stop: int32
/// - xstart: int64
/// - xstop: int64
/// - source: string ("exome", "genome", or "both")
/// - lead_variant: string
/// - lead_pvalue: float64
/// - exome_count: uint32
/// - genome_count: uint32
pub struct LocusDefinitionWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    buffer: Vec<LocusDefinitionRow>,
    batch_size: usize,
    total_rows: usize,
}

impl LocusDefinitionWriter<File> {
    /// Create a new LocusDefinitionWriter writing to a local file.
    pub fn new(path: &str) -> Result<Self> {
        Self::with_batch_size(path, 1024)
    }

    /// Create a new LocusDefinitionWriter with a custom batch size.
    pub fn with_batch_size(path: &str, batch_size: usize) -> Result<Self> {
        let file = File::create(path)?;
        Self::from_writer_with_batch_size(file, batch_size)
    }
}

impl<W: Write + Send> LocusDefinitionWriter<W> {
    /// Create a LocusDefinitionWriter from any Write implementation.
    pub fn from_writer(writer: W) -> Result<Self> {
        Self::from_writer_with_batch_size(writer, 1024)
    }

    /// Create a LocusDefinitionWriter from a writer with a custom batch size.
    pub fn from_writer_with_batch_size(writer: W, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(locus_definition_schema());

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_size(10_000)
            .build();

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            total_rows: 0,
        })
    }

    /// Write a locus definition row.
    pub fn write(&mut self, row: LocusDefinitionRow) -> Result<()> {
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Write multiple locus definition rows.
    pub fn write_batch(&mut self, rows: &[LocusDefinitionRow]) -> Result<()> {
        for row in rows {
            self.write(row.clone())?;
        }
        Ok(())
    }

    /// Flush buffered rows to the Parquet file.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = self.build_batch()?;
        self.writer.write(&batch)?;
        self.total_rows += self.buffer.len();
        self.buffer.clear();

        Ok(())
    }

    /// Close the writer and finalize the Parquet file.
    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total_rows)
    }

    /// Finish writing and return the underlying writer.
    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.into_inner()?)
    }

    /// Get the number of rows written so far.
    #[allow(dead_code)]
    pub fn rows_written(&self) -> usize {
        self.total_rows + self.buffer.len()
    }

    /// Build a RecordBatch from the buffered rows.
    fn build_batch(&self) -> Result<RecordBatch> {
        let mut locus_id_builder = StringBuilder::new();
        let mut phenotype_builder = StringBuilder::new();
        let mut ancestry_builder = StringBuilder::new();
        let mut contig_builder = StringBuilder::new();
        let mut start_builder = Int32Builder::new();
        let mut stop_builder = Int32Builder::new();
        let mut xstart_builder = Int64Builder::new();
        let mut xstop_builder = Int64Builder::new();
        let mut source_builder = StringBuilder::new();
        let mut lead_variant_builder = StringBuilder::new();
        let mut lead_pvalue_builder = Float64Builder::new();
        let mut exome_count_builder = UInt32Builder::new();
        let mut genome_count_builder = UInt32Builder::new();

        for row in &self.buffer {
            locus_id_builder.append_value(&row.locus_id);
            phenotype_builder.append_value(&row.phenotype);
            ancestry_builder.append_value(&row.ancestry);
            contig_builder.append_value(&row.contig);
            start_builder.append_value(row.start);
            stop_builder.append_value(row.stop);
            xstart_builder.append_value(row.xstart);
            xstop_builder.append_value(row.xstop);
            source_builder.append_value(&row.source);
            lead_variant_builder.append_value(&row.lead_variant);
            lead_pvalue_builder.append_value(row.lead_pvalue);
            exome_count_builder.append_value(row.exome_count);
            genome_count_builder.append_value(row.genome_count);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(locus_id_builder.finish()),
            Arc::new(phenotype_builder.finish()),
            Arc::new(ancestry_builder.finish()),
            Arc::new(contig_builder.finish()),
            Arc::new(start_builder.finish()),
            Arc::new(stop_builder.finish()),
            Arc::new(xstart_builder.finish()),
            Arc::new(xstop_builder.finish()),
            Arc::new(source_builder.finish()),
            Arc::new(lead_variant_builder.finish()),
            Arc::new(lead_pvalue_builder.finish()),
            Arc::new(exome_count_builder.finish()),
            Arc::new(genome_count_builder.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(batch)
    }
}

/// Create the Arrow schema for locus definition rows.
pub fn locus_definition_schema() -> Schema {
    Schema::new(vec![
        Field::new("locus_id", DataType::Utf8, false),
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        Field::new("contig", DataType::Utf8, false),
        Field::new("start", DataType::Int32, false),
        Field::new("stop", DataType::Int32, false),
        Field::new("xstart", DataType::Int64, false),
        Field::new("xstop", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("lead_variant", DataType::Utf8, false),
        Field::new("lead_pvalue", DataType::Float64, false),
        Field::new("exome_count", DataType::UInt32, false),
        Field::new("genome_count", DataType::UInt32, false),
    ])
}

// =============================================================================
// Locus Variant Writer
// =============================================================================

/// Writer for locus variant rows (loci_variants.parquet).
///
/// Schema:
/// - locus_id: string (links to loci.parquet)
/// - phenotype: string
/// - ancestry: string
/// - sequencing_type: string ("exome" or "genome")
/// - contig: string (chr-prefixed)
/// - xpos: int64
/// - position: int32
/// - pvalue: float64
/// - neg_log10_p: float32
/// - is_significant: boolean
pub struct LocusVariantWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    buffer: Vec<LocusVariantRow>,
    batch_size: usize,
    total_rows: usize,
}

impl LocusVariantWriter<File> {
    /// Create a new LocusVariantWriter writing to a local file.
    pub fn new(path: &str) -> Result<Self> {
        Self::with_batch_size(path, 4096)
    }

    /// Create a new LocusVariantWriter with a custom batch size.
    pub fn with_batch_size(path: &str, batch_size: usize) -> Result<Self> {
        let file = File::create(path)?;
        Self::from_writer_with_batch_size(file, batch_size)
    }
}

impl<W: Write + Send> LocusVariantWriter<W> {
    /// Create a LocusVariantWriter from any Write implementation.
    pub fn from_writer(writer: W) -> Result<Self> {
        Self::from_writer_with_batch_size(writer, 4096)
    }

    /// Create a LocusVariantWriter from a writer with a custom batch size.
    pub fn from_writer_with_batch_size(writer: W, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(locus_variant_schema());

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_size(100_000)
            .build();

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            total_rows: 0,
        })
    }

    /// Write a locus variant row.
    pub fn write(&mut self, row: LocusVariantRow) -> Result<()> {
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Write multiple locus variant rows.
    pub fn write_batch(&mut self, rows: &[LocusVariantRow]) -> Result<()> {
        for row in rows {
            self.write(row.clone())?;
        }
        Ok(())
    }

    /// Flush buffered rows to the Parquet file.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = self.build_batch()?;
        self.writer.write(&batch)?;
        self.total_rows += self.buffer.len();
        self.buffer.clear();

        Ok(())
    }

    /// Close the writer and finalize the Parquet file.
    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total_rows)
    }

    /// Finish writing and return the underlying writer.
    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.into_inner()?)
    }

    /// Get the number of rows written so far.
    #[allow(dead_code)]
    pub fn rows_written(&self) -> usize {
        self.total_rows + self.buffer.len()
    }

    /// Build a RecordBatch from the buffered rows.
    fn build_batch(&self) -> Result<RecordBatch> {
        let mut locus_id_builder = StringBuilder::new();
        let mut phenotype_builder = StringBuilder::new();
        let mut ancestry_builder = StringBuilder::new();
        let mut sequencing_type_builder = StringBuilder::new();
        let mut contig_builder = StringBuilder::new();
        let mut xpos_builder = Int64Builder::new();
        let mut position_builder = Int32Builder::new();
        let mut ref_builder = StringBuilder::new();
        let mut alt_builder = StringBuilder::new();
        let mut pvalue_builder = Float64Builder::new();
        let mut neg_log10_p_builder = Float32Builder::new();
        let mut is_significant_builder = BooleanBuilder::new();
        let mut beta_builder = Float64Builder::new();
        let mut se_builder = Float64Builder::new();
        let mut af_builder = Float64Builder::new();
        let mut ac_cases_builder = Float64Builder::new();
        let mut ac_controls_builder = Float64Builder::new();
        let mut af_cases_builder = Float64Builder::new();
        let mut af_controls_builder = Float64Builder::new();
        // Trait-level association stats
        let mut association_ac_builder = Float64Builder::new();

        for row in &self.buffer {
            locus_id_builder.append_value(&row.locus_id);
            phenotype_builder.append_value(&row.phenotype);
            ancestry_builder.append_value(&row.ancestry);
            sequencing_type_builder.append_value(&row.sequencing_type);
            contig_builder.append_value(&row.contig);
            xpos_builder.append_value(row.xpos);
            position_builder.append_value(row.position);
            ref_builder.append_value(&row.ref_allele);
            alt_builder.append_value(&row.alt_allele);
            pvalue_builder.append_value(row.pvalue);
            neg_log10_p_builder.append_value(row.neg_log10_p);
            is_significant_builder.append_value(row.is_significant);
            beta_builder.append_option(row.beta);
            se_builder.append_option(row.se);
            af_builder.append_option(row.af);
            ac_cases_builder.append_option(row.ac_cases);
            ac_controls_builder.append_option(row.ac_controls);
            af_cases_builder.append_option(row.af_cases);
            af_controls_builder.append_option(row.af_controls);
            association_ac_builder.append_option(row.association_ac);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(locus_id_builder.finish()),
            Arc::new(phenotype_builder.finish()),
            Arc::new(ancestry_builder.finish()),
            Arc::new(sequencing_type_builder.finish()),
            Arc::new(contig_builder.finish()),
            Arc::new(xpos_builder.finish()),
            Arc::new(position_builder.finish()),
            Arc::new(ref_builder.finish()),
            Arc::new(alt_builder.finish()),
            Arc::new(pvalue_builder.finish()),
            Arc::new(neg_log10_p_builder.finish()),
            Arc::new(is_significant_builder.finish()),
            Arc::new(beta_builder.finish()),
            Arc::new(se_builder.finish()),
            Arc::new(af_builder.finish()),
            Arc::new(ac_cases_builder.finish()),
            Arc::new(ac_controls_builder.finish()),
            Arc::new(af_cases_builder.finish()),
            Arc::new(af_controls_builder.finish()),
            Arc::new(association_ac_builder.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(batch)
    }
}

/// Create the Arrow schema for locus variant rows.
pub fn locus_variant_schema() -> Schema {
    Schema::new(vec![
        Field::new("locus_id", DataType::Utf8, false),
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        Field::new("sequencing_type", DataType::Utf8, false),
        Field::new("contig", DataType::Utf8, false),
        Field::new("xpos", DataType::Int64, false),
        Field::new("position", DataType::Int32, false),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("pvalue", DataType::Float64, false),
        Field::new("neg_log10_p", DataType::Float32, false),
        Field::new("is_significant", DataType::Boolean, false),
        Field::new("beta", DataType::Float64, true), // nullable
        Field::new("se", DataType::Float64, true),   // nullable
        Field::new("af", DataType::Float64, true),   // nullable
        Field::new("ac_cases", DataType::Float64, true), // nullable
        Field::new("ac_controls", DataType::Float64, true), // nullable
        Field::new("af_cases", DataType::Float64, true), // nullable
        Field::new("af_controls", DataType::Float64, true), // nullable
        Field::new("association_ac", DataType::Float64, true), // nullable
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_locus_definition_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut writer = LocusDefinitionWriter::with_batch_size(path, 2).unwrap();

        writer
            .write(LocusDefinitionRow {
                locus_id: "chr1_100000_200000".to_string(),
                phenotype: "height".to_string(),
                ancestry: "meta".to_string(),
                contig: "chr1".to_string(),
                start: 100000,
                stop: 200000,
                xstart: 1_000_100_000,
                xstop: 1_000_200_000,
                source: "exome".to_string(),
                lead_variant: "chr1:150000:A:G".to_string(),
                lead_pvalue: 1e-10,
                exome_count: 50,
                genome_count: 200,
            })
            .unwrap();

        writer
            .write(LocusDefinitionRow {
                locus_id: "chr2_300000_400000".to_string(),
                phenotype: "height".to_string(),
                ancestry: "meta".to_string(),
                contig: "chr2".to_string(),
                start: 300000,
                stop: 400000,
                xstart: 2_000_300_000,
                xstop: 2_000_400_000,
                source: "genome".to_string(),
                lead_variant: "chr2:350000:C:T".to_string(),
                lead_pvalue: 5e-9,
                exome_count: 30,
                genome_count: 150,
            })
            .unwrap();

        let rows = writer.finish().unwrap();
        assert_eq!(rows, 2);

        // Verify file was created
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_locus_variant_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut writer = LocusVariantWriter::with_batch_size(path, 2).unwrap();

        writer
            .write(LocusVariantRow {
                locus_id: "chr1_100000_200000".to_string(),
                phenotype: "height".to_string(),
                ancestry: "meta".to_string(),
                sequencing_type: "exome".to_string(),
                contig: "chr1".to_string(),
                xpos: 1_000_150_000,
                position: 150000,
                ref_allele: "A".to_string(),
                alt_allele: "G".to_string(),
                pvalue: 1e-10,
                neg_log10_p: 10.0,
                is_significant: true,
                beta: Some(-0.5),
                se: Some(0.1),
                af: Some(0.02),
                ac_cases: Some(10.0),
                ac_controls: Some(5.0),
                af_cases: Some(0.02),
                af_controls: Some(0.005),
                association_ac: Some(15.0),
            })
            .unwrap();

        writer
            .write(LocusVariantRow {
                locus_id: "chr1_100000_200000".to_string(),
                phenotype: "height".to_string(),
                ancestry: "meta".to_string(),
                sequencing_type: "genome".to_string(),
                contig: "chr1".to_string(),
                xpos: 1_000_160_000,
                position: 160000,
                ref_allele: "C".to_string(),
                alt_allele: "T".to_string(),
                pvalue: 1e-5,
                neg_log10_p: 5.0,
                is_significant: false,
                beta: None,
                se: None,
                af: Some(0.15),
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            })
            .unwrap();

        let rows = writer.finish().unwrap();
        assert_eq!(rows, 2);

        // Verify file was created
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);
    }
}
