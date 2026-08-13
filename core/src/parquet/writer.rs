//! Parquet writer retaining the exact recursive Arrow plan across flushes.

use crate::codec::EncodedValue;
use crate::error::{HailError, Result};
use crate::parquet::builder::ColumnBuilder;
use crate::schema::ArrowSchemaPlan;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetWriteStats {
    pub row_count: u64,
    pub row_group_count: u64,
}

pub struct ParquetWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    builders: Vec<ColumnBuilder>,
    plan: Arc<ArrowSchemaPlan>,
    batch_size: usize,
    current_count: usize,
    total_rows: usize,
}

impl ParquetWriter<File> {
    pub fn new(path: &str, plan: Arc<ArrowSchemaPlan>) -> Result<Self> {
        Self::with_batch_size(path, plan, 4096)
    }

    /// Legacy export writer with deterministic project-owned schema metadata.
    /// Safe export deliberately continues using `new`/`from_writer`, whose
    /// embedded Arrow schema participates in its existing completion gates.
    pub fn new_deterministic(path: &str, plan: Arc<ArrowSchemaPlan>) -> Result<Self> {
        Self::from_writer_with_batch_size_and_metadata(File::create(path)?, plan, 4096, true)
    }

    pub fn with_batch_size(
        path: &str,
        plan: Arc<ArrowSchemaPlan>,
        batch_size: usize,
    ) -> Result<Self> {
        Self::from_writer_with_batch_size(File::create(path)?, plan, batch_size)
    }
}

impl<W: Write + Send> ParquetWriter<W> {
    pub fn from_writer(writer: W, plan: Arc<ArrowSchemaPlan>) -> Result<Self> {
        Self::from_writer_with_batch_size(writer, plan, 4096)
    }

    pub fn from_writer_deterministic(writer: W, plan: Arc<ArrowSchemaPlan>) -> Result<Self> {
        Self::from_writer_with_batch_size_and_metadata(writer, plan, 4096, true)
    }

    pub fn from_writer_with_batch_size(
        writer: W,
        plan: Arc<ArrowSchemaPlan>,
        batch_size: usize,
    ) -> Result<Self> {
        Self::from_writer_with_batch_size_and_metadata(writer, plan, batch_size, false)
    }

    fn from_writer_with_batch_size_and_metadata(
        writer: W,
        plan: Arc<ArrowSchemaPlan>,
        batch_size: usize,
        deterministic_metadata: bool,
    ) -> Result<Self> {
        if batch_size == 0 {
            return Err(HailError::InvalidFormat(
                "Parquet batch size must be greater than zero".to_string(),
            ));
        }
        let props_builder = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_size(100_000);
        let arrow_writer = if deterministic_metadata {
            // Arrow 53 serializes field HashMaps into `ARROW:schema` in their
            // randomized iteration order. Keep a canonical, sorted descriptor
            // instead and omit only that nondeterministic compatibility blob.
            // The physical Parquet schema and all row values are unchanged.
            let descriptor =
                String::from_utf8(plan.descriptor().canonical_bytes()?).map_err(|error| {
                    HailError::InvalidFormat(format!(
                        "canonical Arrow schema descriptor is not UTF-8: {error}"
                    ))
                })?;
            let props = props_builder
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "genohype.arrow_schema_descriptor.v1".to_string(),
                    Some(descriptor),
                )]))
                .build();
            ArrowWriter::try_new_with_options(
                writer,
                plan.schema().clone(),
                ArrowWriterOptions::new()
                    .with_properties(props)
                    .with_skip_arrow_metadata(true),
            )?
        } else {
            ArrowWriter::try_new(writer, plan.schema().clone(), Some(props_builder.build()))?
        };
        let builders = builders(&plan)?;
        Ok(Self {
            writer: arrow_writer,
            builders,
            plan,
            batch_size,
            current_count: 0,
            total_rows: 0,
        })
    }

    pub fn write_row(&mut self, row: &EncodedValue) -> Result<()> {
        let EncodedValue::Struct(fields) = row else {
            return Err(HailError::TypeMismatch {
                expected: "Struct".to_string(),
                actual: format!("{row:?}"),
            });
        };
        for (index, builder) in self.builders.iter_mut().enumerate() {
            if let Some((_, value)) = fields.get(index) {
                builder.append(value)?;
            } else {
                builder.append_null()?;
            }
        }
        self.current_count += 1;
        self.total_rows += 1;
        if self.current_count >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.current_count == 0 {
            return Ok(());
        }
        let arrays = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        let batch = RecordBatch::try_new(self.plan.schema().clone(), arrays)?;
        self.writer.write(&batch)?;
        // Recreate from the retained plan. Never infer EType from Arrow and never
        // fall back unknown Arrow types to EBinary.
        self.builders = builders(&self.plan)?;
        self.current_count = 0;
        Ok(())
    }

    pub fn close(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total_rows)
    }

    pub fn rows_written(&self) -> usize {
        self.total_rows
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.plan.schema().as_ref() {
            return Err(HailError::InvalidFormat(
                "record batch schema differs from retained Parquet plan".to_string(),
            ));
        }
        self.writer.write(batch)?;
        self.total_rows += batch.num_rows();
        Ok(())
    }

    pub fn schema(&self) -> &Arc<arrow::datatypes::Schema> {
        self.plan.schema()
    }

    pub fn plan(&self) -> &Arc<ArrowSchemaPlan> {
        &self.plan
    }

    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.into_inner()?)
    }

    /// Finalize the footer and return the underlying sink plus authoritative
    /// file-level counts measured from Parquet metadata.
    pub fn finish_into_inner(mut self) -> Result<(W, ParquetWriteStats)> {
        self.flush()?;
        self.writer.flush()?;
        let row_count =
            self.writer
                .flushed_row_groups()
                .iter()
                .try_fold(0_u64, |total, group| {
                    let rows = u64::try_from(group.num_rows()).map_err(|_| {
                        HailError::InvalidFormat(
                            "Parquet row group contains a negative row count".to_string(),
                        )
                    })?;
                    total.checked_add(rows).ok_or_else(|| {
                        HailError::InvalidFormat("Parquet row count overflow".to_string())
                    })
                })?;
        let row_group_count =
            u64::try_from(self.writer.flushed_row_groups().len()).map_err(|_| {
                HailError::InvalidFormat("Parquet row-group count exceeds u64".to_string())
            })?;
        // `into_inner` writes the footer from these exact flushed row-group
        // metadata records and returns the sink only after finalization.
        let sink = self.writer.into_inner()?;
        Ok((
            sink,
            ParquetWriteStats {
                row_count,
                row_group_count,
            },
        ))
    }
}

fn builders(plan: &ArrowSchemaPlan) -> Result<Vec<ColumnBuilder>> {
    plan.fields()
        .iter()
        .map(|field| ColumnBuilder::new(&field.plan))
        .collect()
}

pub fn build_record_batch(rows: &[EncodedValue], plan: &ArrowSchemaPlan) -> Result<RecordBatch> {
    let mut builders = builders(plan)?;
    for row in rows {
        let EncodedValue::Struct(fields) = row else {
            return Err(HailError::TypeMismatch {
                expected: "Struct".to_string(),
                actual: format!("{row:?}"),
            });
        };
        for (index, builder) in builders.iter_mut().enumerate() {
            if let Some((_, value)) = fields.get(index) {
                builder.append(value)?;
            } else {
                builder.append_null()?;
            }
        }
    }
    let arrays = builders.iter_mut().map(ColumnBuilder::finish).collect();
    Ok(RecordBatch::try_new(plan.schema().clone(), arrays)?)
}

pub struct InMemoryParquetWriter {
    inner: ParquetWriter<std::io::Cursor<Vec<u8>>>,
}

impl InMemoryParquetWriter {
    pub fn new(plan: Arc<ArrowSchemaPlan>) -> Result<Self> {
        Ok(Self {
            inner: ParquetWriter::from_writer(std::io::Cursor::new(Vec::new()), plan)?,
        })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn finish(self) -> Result<Vec<u8>> {
        Ok(self.inner.into_inner()?.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{ETypeParser, EncodedValue};
    use crate::schema::PairedSchema;
    use arrow::array::{Array, BinaryArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::NamedTempFile;

    fn deterministic_locus_parquet() -> Vec<u8> {
        let paired = PairedSchema::from_raw_vtype(
            ETypeParser::parse(
                "+EBaseStruct{locus:+EBaseStruct{contig:+EBinary,position:+EInt32}}",
            )
            .unwrap(),
            "Struct{locus:Locus(GRCh38)}",
        )
        .unwrap();
        let plan = Arc::new(paired.arrow_plan().unwrap());
        let mut writer =
            ParquetWriter::from_writer_deterministic(std::io::Cursor::new(Vec::new()), plan)
                .unwrap();
        writer
            .write_row(&EncodedValue::Struct(vec![(
                "locus".into(),
                EncodedValue::Struct(vec![
                    ("contig".into(), EncodedValue::Binary(b"chr1".to_vec())),
                    ("position".into(), EncodedValue::Int32(123)),
                ]),
            )]))
            .unwrap();
        writer.into_inner().unwrap().into_inner()
    }

    #[test]
    fn deterministic_metadata_produces_identical_bytes_and_canonical_descriptor() {
        let first = deterministic_locus_parquet();
        let second = deterministic_locus_parquet();
        assert_eq!(first, second);

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(first)).unwrap();
        let metadata = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        assert!(metadata
            .iter()
            .any(|entry| entry.key == "genohype.arrow_schema_descriptor.v1"));
        assert!(!metadata.iter().any(|entry| entry.key == "ARROW:schema"));
    }

    #[test]
    fn default_writer_retains_embedded_arrow_schema_for_safe_export() {
        let paired = PairedSchema::from_raw_vtype(
            ETypeParser::parse("+EBaseStruct{x:+EInt32}").unwrap(),
            "Struct{x:Int32}",
        )
        .unwrap();
        let plan = Arc::new(paired.arrow_plan().unwrap());
        let mut writer =
            ParquetWriter::from_writer(std::io::Cursor::new(Vec::new()), plan).unwrap();
        writer
            .write_row(&EncodedValue::Struct(vec![(
                "x".into(),
                EncodedValue::Int32(1),
            )]))
            .unwrap();
        let bytes = writer.into_inner().unwrap().into_inner();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
        let metadata = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        assert!(metadata.iter().any(|entry| entry.key == "ARROW:schema"));
        assert!(!metadata
            .iter()
            .any(|entry| entry.key == "genohype.arrow_schema_descriptor.v1"));
    }

    #[test]
    fn finalized_stats_come_from_parquet_footer() {
        let paired = PairedSchema::from_raw_vtype(
            ETypeParser::parse("+EBaseStruct{x:+EInt32}").unwrap(),
            "Struct{x:Int32}",
        )
        .unwrap();
        let plan = Arc::new(paired.arrow_plan().unwrap());
        let mut writer =
            ParquetWriter::from_writer(std::io::Cursor::new(Vec::new()), plan).unwrap();
        writer
            .write_row(&EncodedValue::Struct(vec![(
                "x".into(),
                EncodedValue::Int32(1),
            )]))
            .unwrap();
        let (bytes, stats) = writer.finish_into_inner().unwrap();
        assert!(!bytes.into_inner().is_empty());
        assert_eq!(stats.row_count, 1);
        assert_eq!(stats.row_group_count, 1);
    }

    #[test]
    fn forced_flush_keeps_binary_plan_and_bytes() {
        let paired = PairedSchema::from_raw_vtype(
            ETypeParser::parse("+EBaseStruct{x:EBinary}").unwrap(),
            "Struct{x:Binary}",
        )
        .unwrap();
        let plan = Arc::new(paired.arrow_plan().unwrap());
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let mut writer = ParquetWriter::with_batch_size(path, plan, 1).unwrap();
        for bytes in [vec![0xff], vec![0, 0x80]] {
            writer
                .write_row(&EncodedValue::Struct(vec![(
                    "x".into(),
                    EncodedValue::Binary(bytes),
                )]))
                .unwrap();
        }
        writer.close().unwrap();

        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let first = reader.next().unwrap().unwrap();
        let values = first
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), &[0xff]);
        assert_eq!(values.value(1), &[0, 0x80]);
        assert!(!values.is_null(0));
    }
}
