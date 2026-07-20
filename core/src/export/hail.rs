//! Hail Table Writer
//!
//! Exports data to Hail Table (.ht) format, enabling round-trip
//! conversions and filtered table exports.

use crate::buffer::{
    BlockingOutputBuffer, LEB128OutputBuffer, OutputBuffer, StreamBlockOutputBuffer,
};
use crate::codec::{EncodedType, EncodedValue, Encoder};
use crate::error::{HailError, Result};
use crate::metadata::{Interval, RVDComponentSpec};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

/// Writer for Hail Table (.ht) format
pub struct HailTableWriter {
    output_path: PathBuf,
    row_type: EncodedType,
    key_fields: Vec<String>,
    part_files: Vec<String>,
    range_bounds: Vec<Interval>,
    partition_row_counts: Vec<usize>,
}

impl HailTableWriter {
    /// Create a new Hail table writer
    ///
    /// # Arguments
    /// * `path` - Output directory path (will be created)
    /// * `row_type` - Schema for rows
    /// * `key_fields` - Key field names
    /// * `source_rvd` - Optional source RVD spec (for preserving metadata)
    pub fn new(
        path: &str,
        row_type: &EncodedType,
        key_fields: &[String],
        _source_rvd: Option<&RVDComponentSpec>,
    ) -> Result<Self> {
        let output_path = PathBuf::from(path);

        // Create directory structure
        fs::create_dir_all(&output_path)?;
        fs::create_dir_all(output_path.join("rows/parts"))?;
        fs::create_dir_all(output_path.join("globals/parts"))?;

        let writer = Self {
            output_path,
            row_type: row_type.clone(),
            key_fields: key_fields.to_vec(),
            part_files: Vec::new(),
            range_bounds: Vec::new(),
            partition_row_counts: Vec::new(),
        };

        // Write globals (empty struct for now)
        writer.write_globals()?;

        Ok(writer)
    }

    /// Write globals (empty struct for MVP)
    fn write_globals(&self) -> Result<()> {
        // Write an empty globals partition
        let globals_path = self.output_path.join("globals/parts/part-0");

        // For Hail globals, we need: [present: true][empty struct bitmap]
        // with the full encoding stack (LEB128 -> Blocking -> Zstd -> StreamBlock)
        let mut encoded_data = Vec::new();
        encoded_data.push(1); // present = true (row is present)
        encoded_data.push(0); // empty bitmap (no nullable fields in empty struct)

        // Compress with zstd
        let compressed = zstd::encode_all(encoded_data.as_slice(), 3)?;

        // Build block with decompressed size prefix: [4-byte decompressed_len][zstd_data]
        let mut block_data = Vec::with_capacity(4 + compressed.len());
        block_data.extend_from_slice(&(encoded_data.len() as i32).to_le_bytes());
        block_data.extend_from_slice(&compressed);

        // Write as stream block
        let file = File::create(&globals_path)?;
        let mut stream_writer = StreamBlockOutputBuffer::new(BufWriter::new(file));
        stream_writer.write_block(&block_data)?;

        // Write EOF marker: a zstd block with decompressed_len=0
        let empty_compressed = zstd::encode_all(&[][..], 3)?;
        let mut eof_block = Vec::with_capacity(4 + empty_compressed.len());
        eof_block.extend_from_slice(&0i32.to_le_bytes()); // decompressed_len = 0
        eof_block.extend_from_slice(&empty_compressed);
        stream_writer.write_block(&eof_block)?;

        stream_writer.flush()?;

        // Write globals metadata
        let globals_metadata_path = self.output_path.join("globals/metadata.json.gz");
        self.write_globals_metadata(&globals_metadata_path)?;

        Ok(())
    }

    /// Write globals metadata
    fn write_globals_metadata(&self, path: &Path) -> Result<()> {
        let metadata = json!({
            "name": "OrderedRVDSpec2",
            "_key": [],
            "_codecSpec": {
                "name": "TypedCodecSpec",
                "_eType": "+EBaseStruct{}",
                "_vType": "Struct{}",
                "_bufferSpec": {
                    "name": "LEB128BufferSpec",
                    "child": {
                        "name": "BlockingBufferSpec",
                        "blockSize": 65536,
                        "child": {
                            "name": "ZstdBlockBufferSpec",
                            "blockSize": 65536,
                            "child": {
                                "name": "StreamBlockBufferSpec"
                            }
                        }
                    }
                }
            },
            "_partFiles": ["part-0"],
            "_jRangeBounds": [{
                "start": {},
                "end": {},
                "includeStart": true,
                "includeEnd": true
            }],
            "_attrs": {}
        });

        let file = File::create(path)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        serde_json::to_writer(&mut encoder, &metadata)?;
        encoder.finish()?;

        Ok(())
    }

    /// Write a partition of rows
    ///
    /// # Arguments
    /// * `partition_idx` - Index of the partition
    /// * `rows` - Iterator of rows to write
    pub fn write_partition<I>(&mut self, partition_idx: usize, rows: I) -> Result<()>
    where
        I: IntoIterator<Item = EncodedValue>,
    {
        let part_name = format!("part-{}", partition_idx);
        let part_path = self.output_path.join("rows/parts").join(&part_name);

        // Collect rows and encode
        let rows: Vec<EncodedValue> = rows.into_iter().collect();

        let row_count = rows.len();

        if rows.is_empty() {
            // Write empty partition
            let file = File::create(&part_path)?;
            let mut stream_writer = StreamBlockOutputBuffer::new(BufWriter::new(file));
            stream_writer.write_block(&[])?;
            stream_writer.flush()?;
        } else {
            // Encode rows with LEB128 buffer stack
            let mut buffer = LEB128OutputBuffer::new(BlockingOutputBuffer::with_default_size());

            for row in &rows {
                // Write row presence byte (required even for non-nullable row types)
                buffer.write_bool(true)?;
                // Write row struct data (skip the nullable handling since we wrote presence above)
                Encoder::write_present_value(&mut buffer, &self.row_type, row)?;
            }

            // Write "no more rows" marker (row presence = false)
            buffer.write_bool(false)?;

            buffer.flush()?;
            let encoded_data = buffer.into_inner().into_data();

            // Compress with zstd
            let compressed = zstd::encode_all(encoded_data.as_slice(), 3)?;

            // Build block with decompressed size prefix: [4-byte decompressed_len][zstd_data]
            let mut block_data = Vec::with_capacity(4 + compressed.len());
            block_data.extend_from_slice(&(encoded_data.len() as i32).to_le_bytes());
            block_data.extend_from_slice(&compressed);

            // Write as stream block
            let file = File::create(&part_path)?;
            let mut stream_writer = StreamBlockOutputBuffer::new(BufWriter::new(file));
            stream_writer.write_block(&block_data)?;

            // Write EOF marker: a zstd block with decompressed_len=0
            // Format: [4-byte decompLen=0][zstd compressed empty data]
            let empty_compressed = zstd::encode_all(&[][..], 3)?;
            let mut eof_block = Vec::with_capacity(4 + empty_compressed.len());
            eof_block.extend_from_slice(&0i32.to_le_bytes()); // decompressed_len = 0
            eof_block.extend_from_slice(&empty_compressed);
            stream_writer.write_block(&eof_block)?;

            stream_writer.flush()?;

            // Track range bounds from first and last rows
            if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
                let start_key = self.extract_key(first)?;
                let end_key = self.extract_key(last)?;

                self.range_bounds.push(Interval {
                    start: start_key,
                    end: end_key,
                    include_start: true,
                    include_end: true,
                });
            }
        }

        self.part_files.push(part_name);
        self.partition_row_counts.push(row_count);

        Ok(())
    }

    /// Extract key fields from a row as JSON value
    fn extract_key(&self, row: &EncodedValue) -> Result<Value> {
        if let EncodedValue::Struct(fields) = row {
            let mut key_obj = serde_json::Map::new();

            for key_name in &self.key_fields {
                if let Some((_, value)) = fields.iter().find(|(n, _)| n == key_name) {
                    key_obj.insert(key_name.clone(), encoded_value_to_json(value));
                }
            }

            Ok(Value::Object(key_obj))
        } else {
            Err(HailError::InvalidFormat("Row must be a struct".to_string()))
        }
    }

    /// Finish writing the table
    pub fn finish(self) -> Result<()> {
        // Write rows metadata
        self.write_rows_metadata()?;

        // Write top-level metadata
        self.write_table_metadata()?;

        // Write _SUCCESS file
        let success_path = self.output_path.join("_SUCCESS");
        File::create(success_path)?;

        Ok(())
    }

    /// Write rows metadata
    fn write_rows_metadata(&self) -> Result<()> {
        let metadata_path = self.output_path.join("rows/metadata.json.gz");

        let e_type = format_etype(&self.row_type);
        let v_type = format_vtype(&self.row_type);

        let metadata = json!({
            "name": "OrderedRVDSpec2",
            "_key": self.key_fields,
            "_codecSpec": {
                "name": "TypedCodecSpec",
                "_eType": e_type,
                "_vType": v_type,
                "_bufferSpec": {
                    "name": "LEB128BufferSpec",
                    "child": {
                        "name": "BlockingBufferSpec",
                        "blockSize": 65536,
                        "child": {
                            "name": "ZstdBlockBufferSpec",
                            "blockSize": 65536,
                            "child": {
                                "name": "StreamBlockBufferSpec"
                            }
                        }
                    }
                }
            },
            "_partFiles": self.part_files,
            "_jRangeBounds": self.range_bounds,
            "_attrs": {}
        });

        let file = File::create(&metadata_path)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        serde_json::to_writer(&mut encoder, &metadata)?;
        encoder.finish()?;

        Ok(())
    }

    /// Write table-level metadata
    fn write_table_metadata(&self) -> Result<()> {
        let metadata_path = self.output_path.join("metadata.json.gz");

        // Format table_type as Hail expects: "Table{global:Struct{},key:[...],row:Struct{...}}"
        let key_str = format!("[{}]", self.key_fields.join(","));
        let table_type_str = format!(
            "Table{{global:Struct{{}},key:{},row:{}}}",
            key_str,
            format_vtype(&self.row_type)
        );

        // Use actual partition row counts
        let partition_counts = &self.partition_row_counts;

        let metadata = json!({
            "name": "TableSpec",
            "file_version": 67328,
            "hail_version": "0.2.134-952ae203dbbe",
            "references_rel_path": "references",
            "table_type": table_type_str,
            "components": {
                "globals": {
                    "name": "RVDComponentSpec",
                    "rel_path": "globals"
                },
                "rows": {
                    "name": "RVDComponentSpec",
                    "rel_path": "rows"
                },
                "partition_counts": {
                    "name": "PartitionCountsComponentSpec",
                    "counts": partition_counts
                },
                "properties": {
                    "name": "PropertiesSpec",
                    "properties": {}
                }
            }
        });

        let file = File::create(&metadata_path)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        serde_json::to_writer(&mut encoder, &metadata)?;
        encoder.finish()?;

        // Write references (empty)
        let refs_path = self.output_path.join("references");
        fs::create_dir_all(&refs_path)?;

        Ok(())
    }
}

/// Convert EncodedValue to JSON Value
fn encoded_value_to_json(value: &EncodedValue) -> Value {
    match value {
        EncodedValue::Null => Value::Null,
        EncodedValue::Binary(b) => Value::String(String::from_utf8_lossy(b).to_string()),
        EncodedValue::Int32(i) => Value::Number((*i).into()),
        EncodedValue::Int64(i) => Value::Number((*i).into()),
        EncodedValue::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        EncodedValue::Float64(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        EncodedValue::Boolean(b) => Value::Bool(*b),
        EncodedValue::Struct(fields) => {
            let obj: serde_json::Map<String, Value> = fields
                .iter()
                .map(|(k, v)| (k.clone(), encoded_value_to_json(v)))
                .collect();
            Value::Object(obj)
        }
        EncodedValue::Array(elements) => {
            Value::Array(elements.iter().map(encoded_value_to_json).collect())
        }
    }
}

/// Format EncodedType as Hail EType string (e.g., "+EBaseStruct{field:+EInt32}")
fn format_etype(ty: &EncodedType) -> String {
    let prefix = if ty.is_required() { "+" } else { "" };

    match ty {
        EncodedType::EBinary { .. } => format!("{}EBinary", prefix),
        EncodedType::EInt32 { .. } => format!("{}EInt32", prefix),
        EncodedType::EInt64 { .. } => format!("{}EInt64", prefix),
        EncodedType::EFloat32 { .. } => format!("{}EFloat32", prefix),
        EncodedType::EFloat64 { .. } => format!("{}EFloat64", prefix),
        EncodedType::EBoolean { .. } => format!("{}EBoolean", prefix),
        EncodedType::EArray { element, .. } => {
            format!("{}EArray[{}]", prefix, format_etype(element))
        }
        EncodedType::EBaseStruct { fields, .. } => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name, format_etype(&f.encoded_type)))
                .collect();
            format!("{}EBaseStruct{{{}}}", prefix, field_strs.join(","))
        }
    }
}

/// Format EncodedType as Hail VType string (e.g., "Struct{field:Int32}")
fn format_vtype(ty: &EncodedType) -> String {
    match ty {
        EncodedType::EBinary { .. } => "String".to_string(),
        EncodedType::EInt32 { .. } => "Int32".to_string(),
        EncodedType::EInt64 { .. } => "Int64".to_string(),
        EncodedType::EFloat32 { .. } => "Float32".to_string(),
        EncodedType::EFloat64 { .. } => "Float64".to_string(),
        EncodedType::EBoolean { .. } => "Boolean".to_string(),
        EncodedType::EArray { element, .. } => {
            format!("Array[{}]", format_vtype(element))
        }
        EncodedType::EBaseStruct { fields, .. } => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name, format_vtype(&f.encoded_type)))
                .collect();
            format!("Struct{{{}}}", field_strs.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_etype() {
        let ty = EncodedType::EInt32 { required: true };
        assert_eq!(format_etype(&ty), "+EInt32");

        let ty = EncodedType::EInt32 { required: false };
        assert_eq!(format_etype(&ty), "EInt32");
    }

    #[test]
    fn test_format_vtype() {
        let ty = EncodedType::EInt32 { required: true };
        assert_eq!(format_vtype(&ty), "Int32");

        let ty = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EBinary { required: true }),
        };
        assert_eq!(format_vtype(&ty), "Array[String]");
    }

    #[test]
    fn test_encoded_value_to_json() {
        let value = EncodedValue::Struct(vec![
            ("name".to_string(), EncodedValue::Binary(b"test".to_vec())),
            ("count".to_string(), EncodedValue::Int32(42)),
        ]);

        let json = encoded_value_to_json(&value);
        assert_eq!(json["name"], "test");
        assert_eq!(json["count"], 42);
    }
}
