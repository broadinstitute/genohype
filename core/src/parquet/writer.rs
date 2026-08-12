//! Parquet writer retaining the exact recursive Arrow plan across flushes.

use crate::codec::EncodedValue;
use crate::error::{HailError, Result};
use crate::parquet::builder::ColumnBuilder;
use crate::schema::ArrowSchemaPlan;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

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

    pub fn from_writer_with_batch_size(
        writer: W,
        plan: Arc<ArrowSchemaPlan>,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            return Err(HailError::InvalidFormat(
                "Parquet batch size must be greater than zero".to_string(),
            ));
        }
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_size(100_000)
            .build();
        let arrow_writer = ArrowWriter::try_new(writer, plan.schema().clone(), Some(props))?;
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
