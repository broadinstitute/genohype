//! Parquet writer for significant hit rows during Manhattan scan phase.
//!
//! This writer is optimized for the flat schema used by `SigHitRow` and writes
//! directly to Parquet without going through Hail's EncodedType system.

use crate::manhattan::data::SigHitRow;
use arrow::array::{ArrayRef, Float64Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use genohype_core::error::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

/// A writer for significant hit rows during the scan phase.
///
/// Buffers rows and writes them as Parquet with a flat schema:
/// - phenotype: string (identity)
/// - ancestry: string (identity)
/// - sequencing_type: string (identity: "exome" or "genome")
/// - xpos: int64 (pre-computed for efficient ordering)
/// - contig: string (chr-prefixed)
/// - position: int32
/// - ref: string
/// - alt: string
/// - pvalue: float64
/// - beta: float64 (nullable)
/// - se: float64 (nullable)
/// - af: float64 (nullable)
///
/// Note: Gene and consequence annotations are not included during scan phase.
/// These can be added during locus plot generation in the aggregate phase.
pub struct SigHitWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    buffer: Vec<SigHitRow>,
    batch_size: usize,
    total_rows: usize,
}

impl SigHitWriter<File> {
    /// Create a new SigHitWriter writing to a local file.
    pub fn new(path: &str) -> Result<Self> {
        Self::with_batch_size(path, 4096)
    }

    /// Create a new SigHitWriter with a custom batch size.
    pub fn with_batch_size(path: &str, batch_size: usize) -> Result<Self> {
        let file = File::create(path)?;
        Self::from_writer_with_batch_size(file, batch_size)
    }
}

impl<W: Write + Send> SigHitWriter<W> {
    /// Create a SigHitWriter from any Write implementation.
    pub fn from_writer(writer: W) -> Result<Self> {
        Self::from_writer_with_batch_size(writer, 4096)
    }

    /// Create a SigHitWriter from a writer with a custom batch size.
    pub fn from_writer_with_batch_size(writer: W, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(sig_hit_schema());

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

    /// Write a significant hit row.
    pub fn write(&mut self, row: SigHitRow) -> Result<()> {
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            self.flush()?;
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
    ///
    /// This allows accessing the writer after Parquet finalization, which is
    /// necessary for CloudWriter to trigger the upload.
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
        // Identity fields
        let mut phenotype_builder = StringBuilder::new();
        let mut ancestry_builder = StringBuilder::new();
        let mut sequencing_type_builder = StringBuilder::new();

        // Variant key fields
        let mut xpos_builder = Int64Builder::new();
        let mut contig_builder = StringBuilder::new();
        let mut position_builder = Int32Builder::new();
        let mut ref_builder = StringBuilder::new();
        let mut alt_builder = StringBuilder::new();

        // Association stats
        let mut pvalue_builder = Float64Builder::new();
        let mut beta_builder = Float64Builder::new();
        let mut se_builder = Float64Builder::new();
        let mut af_builder = Float64Builder::new();

        // Case/control breakdown
        let mut ac_cases_builder = Float64Builder::new();
        let mut ac_controls_builder = Float64Builder::new();
        let mut af_cases_builder = Float64Builder::new();
        let mut af_controls_builder = Float64Builder::new();
        // Trait-level association stats
        let mut association_ac_builder = Float64Builder::new();

        for row in &self.buffer {
            // Identity fields
            phenotype_builder.append_value(&row.phenotype);
            ancestry_builder.append_value(&row.ancestry);
            sequencing_type_builder.append_value(&row.sequencing_type);

            // Variant key fields
            xpos_builder.append_value(row.xpos);
            contig_builder.append_value(&row.contig);
            position_builder.append_value(row.position);
            ref_builder.append_value(&row.ref_allele);
            alt_builder.append_value(&row.alt_allele);

            // Association stats
            pvalue_builder.append_value(row.pvalue);

            match row.beta {
                Some(v) => beta_builder.append_value(v),
                None => beta_builder.append_null(),
            }
            match row.se {
                Some(v) => se_builder.append_value(v),
                None => se_builder.append_null(),
            }
            match row.af {
                Some(v) => af_builder.append_value(v),
                None => af_builder.append_null(),
            }

            // Case/control breakdown
            match row.ac_cases {
                Some(v) => ac_cases_builder.append_value(v),
                None => ac_cases_builder.append_null(),
            }
            match row.ac_controls {
                Some(v) => ac_controls_builder.append_value(v),
                None => ac_controls_builder.append_null(),
            }
            match row.af_cases {
                Some(v) => af_cases_builder.append_value(v),
                None => af_cases_builder.append_null(),
            }
            match row.af_controls {
                Some(v) => af_controls_builder.append_value(v),
                None => af_controls_builder.append_null(),
            }

            // Trait-level association stats
            match row.association_ac {
                Some(v) => association_ac_builder.append_value(v),
                None => association_ac_builder.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            // Identity fields
            Arc::new(phenotype_builder.finish()),
            Arc::new(ancestry_builder.finish()),
            Arc::new(sequencing_type_builder.finish()),
            // Variant key fields
            Arc::new(xpos_builder.finish()),
            Arc::new(contig_builder.finish()),
            Arc::new(position_builder.finish()),
            Arc::new(ref_builder.finish()),
            Arc::new(alt_builder.finish()),
            // Association stats
            Arc::new(pvalue_builder.finish()),
            Arc::new(beta_builder.finish()),
            Arc::new(se_builder.finish()),
            Arc::new(af_builder.finish()),
            // Case/control breakdown
            Arc::new(ac_cases_builder.finish()),
            Arc::new(ac_controls_builder.finish()),
            Arc::new(af_cases_builder.finish()),
            Arc::new(af_controls_builder.finish()),
            // Trait-level association stats
            Arc::new(association_ac_builder.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(batch)
    }
}

/// Create the Arrow schema for significant hit rows.
pub fn sig_hit_schema() -> Schema {
    Schema::new(vec![
        // Identity fields (for ClickHouse partitioning)
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        Field::new("sequencing_type", DataType::Utf8, false),
        // Variant key fields
        Field::new("xpos", DataType::Int64, false),
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        // Association stats
        Field::new("pvalue", DataType::Float64, false),
        Field::new("beta", DataType::Float64, true),
        Field::new("se", DataType::Float64, true),
        Field::new("af", DataType::Float64, true),
        // Case/control breakdown
        Field::new("ac_cases", DataType::Float64, true),
        Field::new("ac_controls", DataType::Float64, true),
        Field::new("af_cases", DataType::Float64, true),
        Field::new("af_controls", DataType::Float64, true),
        // Trait-level association stats
        Field::new("association_ac", DataType::Float64, true),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sig_hit_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut writer = SigHitWriter::with_batch_size(path, 2).unwrap();

        writer
            .write(SigHitRow {
                phenotype: "height".to_string(),
                ancestry: "meta".to_string(),
                sequencing_type: "exome".to_string(),
                contig: "chr1".to_string(),
                position: 12345,
                ref_allele: "A".to_string(),
                alt_allele: "G".to_string(),
                xpos: 1_000_012_345,
                pvalue: 1e-10,
                beta: Some(0.5),
                se: Some(0.1),
                af: Some(0.01),
                ac_cases: Some(10.0),
                ac_controls: Some(5.0),
                af_cases: Some(0.02),
                af_controls: Some(0.005),
                association_ac: Some(15.0),
            })
            .unwrap();

        writer
            .write(SigHitRow {
                phenotype: "height".to_string(),
                ancestry: "meta".to_string(),
                sequencing_type: "genome".to_string(),
                contig: "chr2".to_string(),
                position: 67890,
                ref_allele: "C".to_string(),
                alt_allele: "T".to_string(),
                xpos: 2_000_067_890,
                pvalue: 5e-9,
                beta: None,
                se: None,
                af: None,
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
