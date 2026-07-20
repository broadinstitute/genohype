//! Parquet writer for QQ plot point rows.
//!
//! This writer produces Parquet files containing observed and expected p-values
//! for QQ plot rendering on the frontend. Includes variant key fields for
//! annotation joins.

use crate::manhattan::data::QQPointRow;
use arrow::array::{ArrayRef, Float64Builder, Int32Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use genohype_core::error::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

/// A writer for QQ plot point rows to Parquet.
pub struct QQPointWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    buffer: Vec<QQPointRow>,
    batch_size: usize,
    total_rows: usize,
}

impl QQPointWriter<File> {
    /// Create a new QQPointWriter writing to a local file.
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Self::from_writer(file)
    }
}

impl<W: Write + Send> QQPointWriter<W> {
    /// Create a QQPointWriter from any Write implementation.
    pub fn from_writer(writer: W) -> Result<Self> {
        let schema = Arc::new(qq_point_schema());

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_size(100_000)
            .build();

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            buffer: Vec::with_capacity(4096),
            batch_size: 4096,
            total_rows: 0,
        })
    }

    /// Write a QQ point row.
    pub fn write(&mut self, row: QQPointRow) -> Result<()> {
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
    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.into_inner()?)
    }

    /// Build a RecordBatch from the buffered rows.
    fn build_batch(&self) -> Result<RecordBatch> {
        // Identity fields
        let mut phenotype = StringBuilder::new();
        let mut ancestry = StringBuilder::new();
        let mut sequencing_type = StringBuilder::new();

        // Variant key fields
        let mut contig = StringBuilder::new();
        let mut position = Int32Builder::new();
        let mut ref_allele = StringBuilder::new();
        let mut alt_allele = StringBuilder::new();

        // QQ values
        let mut pvalue_log10 = Float64Builder::new();
        let mut pvalue_expected_log10 = Float64Builder::new();

        for row in &self.buffer {
            phenotype.append_value(&row.phenotype);
            ancestry.append_value(&row.ancestry);
            sequencing_type.append_value(&row.sequencing_type);

            contig.append_value(&row.contig);
            position.append_value(row.position);
            ref_allele.append_value(&row.ref_allele);
            alt_allele.append_value(&row.alt_allele);

            pvalue_log10.append_value(row.pvalue_log10);
            pvalue_expected_log10.append_value(row.pvalue_expected_log10);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(phenotype.finish()),
            Arc::new(ancestry.finish()),
            Arc::new(sequencing_type.finish()),
            Arc::new(contig.finish()),
            Arc::new(position.finish()),
            Arc::new(ref_allele.finish()),
            Arc::new(alt_allele.finish()),
            Arc::new(pvalue_log10.finish()),
            Arc::new(pvalue_expected_log10.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(batch)
    }
}

/// Create the Arrow schema for QQ plot points.
pub fn qq_point_schema() -> Schema {
    Schema::new(vec![
        // Identity fields
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        Field::new("sequencing_type", DataType::Utf8, false),
        // Variant key fields (for annotation joins)
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("ref_allele", DataType::Utf8, false),
        Field::new("alt_allele", DataType::Utf8, false),
        // QQ values
        Field::new("pvalue_log10", DataType::Float64, false),
        Field::new("pvalue_expected_log10", DataType::Float64, false),
    ])
}
