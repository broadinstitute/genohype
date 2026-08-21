//! Parquet writer for gene association rows.
//!
//! This writer produces Parquet files that match the ClickHouse `gene_associations`
//! table schema for efficient ingestion into the data store.

use crate::manhattan::data::GeneAssociationRow;
use arrow::array::{ArrayRef, Float64Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use genohype_core::error::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

/// A writer for gene association rows to Parquet.
///
/// Buffers rows and writes them with a schema matching ClickHouse:
/// - gene_id: string (key)
/// - gene_symbol: string (key)
/// - annotation: string (key, e.g., "pLoF", "missenseLC")
/// - max_maf: float64 (key, MAF threshold)
/// - phenotype: string
/// - ancestry: string
/// - pvalue: float64 (nullable, SKAT-O combined)
/// - pvalue_burden: float64 (nullable)
/// - pvalue_skat: float64 (nullable)
/// - beta_burden: float64 (nullable)
/// - mac: int64 (nullable, minor allele count)
/// - mac_case: int64 (nullable, minor allele count among cases)
/// - mac_control: int64 (nullable, minor allele count among controls)
/// - contig: string (chromosome)
/// - gene_start_position: int32 (TSS for Manhattan)
/// - xpos: int64 (pre-computed for efficient ordering)
pub struct GeneAssociationWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    buffer: Vec<GeneAssociationRow>,
    batch_size: usize,
    total_rows: usize,
}

impl GeneAssociationWriter<File> {
    /// Create a new GeneAssociationWriter writing to a local file.
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Self::from_writer(file)
    }
}

impl<W: Write + Send> GeneAssociationWriter<W> {
    /// Create a GeneAssociationWriter from any Write implementation.
    pub fn from_writer(writer: W) -> Result<Self> {
        let schema = Arc::new(gene_association_schema());

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

    /// Write a gene association row.
    pub fn write(&mut self, row: GeneAssociationRow) -> Result<()> {
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
        // Key fields
        let mut gene_id = StringBuilder::new();
        let mut gene_symbol = StringBuilder::new();
        let mut annotation = StringBuilder::new();
        let mut max_maf = Float64Builder::new();
        let mut phenotype = StringBuilder::new();
        let mut ancestry = StringBuilder::new();

        // Stats (nullable)
        let mut pvalue = Float64Builder::new();
        let mut pvalue_burden = Float64Builder::new();
        let mut pvalue_skat = Float64Builder::new();
        let mut beta_burden = Float64Builder::new();
        let mut mac = Int64Builder::new();
        let mut mac_case = Int64Builder::new();
        let mut mac_control = Int64Builder::new();

        // Location
        let mut contig = StringBuilder::new();
        let mut gene_start_position = Int32Builder::new();
        let mut xpos = Int64Builder::new();

        for row in &self.buffer {
            // Key fields
            gene_id.append_value(&row.gene_id);
            gene_symbol.append_value(&row.gene_symbol);
            annotation.append_value(&row.annotation);
            max_maf.append_value(row.max_maf);
            phenotype.append_value(&row.phenotype);
            ancestry.append_value(&row.ancestry);

            // Stats (nullable)
            pvalue.append_option(row.pvalue);
            pvalue_burden.append_option(row.pvalue_burden);
            pvalue_skat.append_option(row.pvalue_skat);
            beta_burden.append_option(row.beta_burden);
            mac.append_option(row.mac);
            mac_case.append_option(row.mac_case);
            mac_control.append_option(row.mac_control);

            // Location
            contig.append_value(&row.contig);
            gene_start_position.append_value(row.gene_start_position);
            xpos.append_value(row.xpos);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(gene_id.finish()),
            Arc::new(gene_symbol.finish()),
            Arc::new(annotation.finish()),
            Arc::new(max_maf.finish()),
            Arc::new(phenotype.finish()),
            Arc::new(ancestry.finish()),
            Arc::new(pvalue.finish()),
            Arc::new(pvalue_burden.finish()),
            Arc::new(pvalue_skat.finish()),
            Arc::new(beta_burden.finish()),
            Arc::new(mac.finish()),
            Arc::new(mac_case.finish()),
            Arc::new(mac_control.finish()),
            Arc::new(contig.finish()),
            Arc::new(gene_start_position.finish()),
            Arc::new(xpos.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(batch)
    }
}

/// Create the Arrow schema for gene associations.
///
/// This schema matches the ClickHouse `gene_associations` table.
pub fn gene_association_schema() -> Schema {
    Schema::new(vec![
        // Key fields (non-nullable)
        Field::new("gene_id", DataType::Utf8, false),
        Field::new("gene_symbol", DataType::Utf8, false),
        Field::new("annotation", DataType::Utf8, false),
        Field::new("max_maf", DataType::Float64, false),
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        // Stats (nullable - some tests may not run for all genes)
        Field::new("pvalue", DataType::Float64, true),
        Field::new("pvalue_burden", DataType::Float64, true),
        Field::new("pvalue_skat", DataType::Float64, true),
        Field::new("beta_burden", DataType::Float64, true),
        Field::new("mac", DataType::Int64, true),
        Field::new("mac_case", DataType::Int64, true),
        Field::new("mac_control", DataType::Int64, true),
        // Location (non-nullable)
        Field::new("contig", DataType::Utf8, false),
        Field::new("gene_start_position", DataType::Int32, false),
        Field::new("xpos", DataType::Int64, false),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::io::Cursor;

    fn row(mac_case: Option<i64>, mac_control: Option<i64>) -> GeneAssociationRow {
        GeneAssociationRow {
            gene_id: "ENSG000001".to_string(),
            gene_symbol: "GENE1".to_string(),
            annotation: "pLoF".to_string(),
            max_maf: 0.001,
            phenotype: "1001".to_string(),
            ancestry: "meta".to_string(),
            pvalue: Some(0.01),
            pvalue_burden: None,
            pvalue_skat: None,
            beta_burden: None,
            mac: Some(4),
            mac_case,
            mac_control,
            contig: "chr1".to_string(),
            gene_start_position: 123,
            xpos: 1_000_000_123,
        }
    }

    #[test]
    fn gene_mac_parquet_schema_and_values_preserve_zero_and_null() {
        let mut writer = GeneAssociationWriter::from_writer(Cursor::new(Vec::new())).unwrap();
        writer.write(row(Some(0), Some(12))).unwrap();
        writer.write(row(None, None)).unwrap();
        let bytes = writer.into_inner().unwrap().into_inner();

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
        let schema = builder.schema();
        for name in ["mac_case", "mac_control"] {
            let field = schema.field_with_name(name).unwrap();
            assert_eq!(field.data_type(), &DataType::Int64);
            assert!(field.is_nullable());
        }

        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        let cases = batch
            .column_by_name("mac_case")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let controls = batch
            .column_by_name("mac_control")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(cases.value(0), 0);
        assert!(cases.is_null(1));
        assert_eq!(controls.value(0), 12);
        assert!(controls.is_null(1));
    }
}
