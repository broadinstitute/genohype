//! VCF Writer
//!
//! Converts EncodedValue rows back to VCF format.
//! This is the inverse of `src/vcf/codec.rs`.

use crate::codec::{EncodedType, EncodedValue};
use crate::{HailError, Result};
use noodles::bgzf;
use noodles::vcf::header::record::value::map::{format, info};
use noodles::vcf::header::record::value::Map;
use noodles::vcf::header::FileFormat;
use noodles::vcf::variant::io::Write as VcfWrite;
use noodles::vcf::variant::record::Ids as IdsRecord;
use noodles::vcf::variant::record_buf::info::field::Value as InfoValue;
use noodles::vcf::variant::record_buf::samples::sample::value::Value as SampleValue;
use noodles::vcf::variant::record_buf::Samples;
use noodles::vcf::variant::RecordBuf;
use noodles::vcf::{self, Header};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Writer for VCF files from EncodedValue rows
pub struct VcfWriter<W: Write> {
    writer: vcf::io::Writer<W>,
    header: Header,
    sample_names: Vec<String>,
}

/// Type alias for plain file writer
pub type VcfFileWriter = VcfWriter<BufWriter<File>>;

/// Type alias for BGZF compressed writer
pub type BgzfVcfWriter = VcfWriter<bgzf::Writer<File>>;

impl VcfWriter<BufWriter<File>> {
    /// Create a new VCF writer for plain text output
    pub fn new<P: AsRef<Path>>(
        path: P,
        schema: &EncodedType,
        sample_names: Vec<String>,
    ) -> Result<Self> {
        let file = File::create(path)?;
        let buf_writer = BufWriter::new(file);
        let header = schema_to_header(schema, &sample_names)?;
        let mut writer = vcf::io::Writer::new(buf_writer);
        writer.write_header(&header)?;

        Ok(Self {
            writer,
            header,
            sample_names,
        })
    }
}

impl VcfWriter<bgzf::Writer<File>> {
    /// Create a new VCF writer for BGZF compressed output
    pub fn new_bgzf<P: AsRef<Path>>(
        path: P,
        schema: &EncodedType,
        sample_names: Vec<String>,
    ) -> Result<Self> {
        let file = File::create(path)?;
        let bgzf_writer = bgzf::Writer::new(file);
        let header = schema_to_header(schema, &sample_names)?;
        let mut writer = vcf::io::Writer::new(bgzf_writer);
        writer.write_header(&header)?;

        Ok(Self {
            writer,
            header,
            sample_names,
        })
    }
}

impl<W: Write> VcfWriter<W> {
    /// Write a row as a VCF record
    pub fn write_row(&mut self, row: &EncodedValue) -> Result<()> {
        let record = row_to_record(row, &self.sample_names)?;
        self.writer.write_variant_record(&self.header, &record)?;
        Ok(())
    }

    /// Get a reference to the header
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Finish writing and flush the output
    pub fn finish(self) -> Result<()> {
        // Writer will be flushed when dropped
        Ok(())
    }
}

/// Convert a Hail schema to a VCF header
fn schema_to_header(schema: &EncodedType, sample_names: &[String]) -> Result<Header> {
    let mut builder = Header::builder();

    // Set VCF version
    builder = builder.set_file_format(FileFormat::new(4, 3));

    // Extract INFO and FORMAT fields from schema
    if let EncodedType::EBaseStruct { fields, .. } = schema {
        // Find the info field
        if let Some(info_field) = fields.iter().find(|f| f.name == "info") {
            if let EncodedType::EBaseStruct {
                fields: info_fields,
                ..
            } = &info_field.encoded_type
            {
                for field in info_fields {
                    let (number, ty) = encoded_type_to_vcf_type(&field.encoded_type);
                    let mut info_map = Map::<info::Info>::new(number, ty, String::new());
                    // Set description if available
                    *info_map.description_mut() = format!("{} field", field.name);
                    builder = builder.add_info(field.name.clone(), info_map);
                }
            }
        }

        // Find the genotypes field and extract format fields
        if let Some(gt_field) = fields.iter().find(|f| f.name == "genotypes") {
            if let EncodedType::EArray { element, .. } = &gt_field.encoded_type {
                if let EncodedType::EBaseStruct {
                    fields: gt_fields, ..
                } = element.as_ref()
                {
                    for field in gt_fields {
                        // Skip sample_id, it's not a FORMAT field
                        if field.name == "sample_id" {
                            continue;
                        }
                        let (number, ty) = encoded_type_to_vcf_format_type(&field.encoded_type);
                        let mut format_map = Map::<format::Format>::new(number, ty, String::new());
                        *format_map.description_mut() = format!("{} field", field.name);
                        builder = builder.add_format(field.name.clone(), format_map);
                    }
                }
            }
        }
    }

    // Add sample names
    for name in sample_names {
        builder = builder.add_sample_name(name.clone());
    }

    Ok(builder.build())
}

/// Convert EncodedType to VCF INFO type
fn encoded_type_to_vcf_type(ty: &EncodedType) -> (info::Number, info::Type) {
    match ty {
        EncodedType::EInt32 { .. } => (info::Number::Count(1), info::Type::Integer),
        EncodedType::EInt64 { .. } => (info::Number::Count(1), info::Type::Integer),
        EncodedType::EFloat32 { .. } => (info::Number::Count(1), info::Type::Float),
        EncodedType::EFloat64 { .. } => (info::Number::Count(1), info::Type::Float),
        EncodedType::EBoolean { .. } => (info::Number::Count(0), info::Type::Flag),
        EncodedType::EBinary { .. } => (info::Number::Count(1), info::Type::String),
        EncodedType::EArray { element, .. } => {
            let (_, base_ty) = encoded_type_to_vcf_type(element);
            (info::Number::Unknown, base_ty)
        }
        EncodedType::EBaseStruct { .. } => (info::Number::Count(1), info::Type::String),
    }
}

/// Convert EncodedType to VCF FORMAT type
fn encoded_type_to_vcf_format_type(ty: &EncodedType) -> (format::Number, format::Type) {
    match ty {
        EncodedType::EInt32 { .. } => (format::Number::Count(1), format::Type::Integer),
        EncodedType::EInt64 { .. } => (format::Number::Count(1), format::Type::Integer),
        EncodedType::EFloat32 { .. } => (format::Number::Count(1), format::Type::Float),
        EncodedType::EFloat64 { .. } => (format::Number::Count(1), format::Type::Float),
        EncodedType::EBoolean { .. } => (format::Number::Count(1), format::Type::Integer),
        EncodedType::EBinary { .. } => (format::Number::Count(1), format::Type::String),
        EncodedType::EArray { element, .. } => {
            let (_, base_ty) = encoded_type_to_vcf_format_type(element);
            (format::Number::Unknown, base_ty)
        }
        EncodedType::EBaseStruct { .. } => (format::Number::Count(1), format::Type::String),
    }
}

/// Convert an EncodedValue row to a VCF RecordBuf
fn row_to_record(row: &EncodedValue, sample_names: &[String]) -> Result<RecordBuf> {
    let fields = match row {
        EncodedValue::Struct(f) => f,
        _ => return Err(HailError::InvalidFormat("Row must be a struct".to_string())),
    };

    // Extract locus
    let (chrom, pos) = extract_locus(fields)?;

    // Extract alleles
    let (reference, alternates) = extract_alleles(fields)?;

    // Create record builder
    let mut builder = RecordBuf::builder()
        .set_reference_sequence_name(chrom)
        .set_variant_start(noodles::core::Position::try_from(pos as usize).unwrap())
        .set_reference_bases(reference);

    // Set alternate alleles
    if !alternates.is_empty() {
        let alt_bases = noodles::vcf::variant::record_buf::AlternateBases::from(alternates.clone());
        builder = builder.set_alternate_bases(alt_bases);
    }

    // Extract and set rsid (IDs)
    if let Some((_, rsid_val)) = fields.iter().find(|(n, _)| n == "rsid") {
        if let EncodedValue::Binary(b) = rsid_val {
            let id_str = String::from_utf8_lossy(b);
            // IDs can be semicolon-separated
            let ids: noodles::vcf::variant::record_buf::Ids = id_str
                .split(';')
                .filter(|s| !s.is_empty())
                .filter_map(|id| id.parse().ok())
                .collect();
            if !IdsRecord::is_empty(&ids) {
                builder = builder.set_ids(ids);
            }
        }
    }

    // Extract and set quality score
    if let Some((_, qual_val)) = fields.iter().find(|(n, _)| n == "qual") {
        if let EncodedValue::Float64(q) = qual_val {
            builder = builder.set_quality_score((*q as f32).into());
        }
    }

    // Extract and set filters
    if let Some((_, filters_val)) = fields.iter().find(|(n, _)| n == "filters") {
        if let EncodedValue::Array(filters) = filters_val {
            if filters.is_empty() {
                // PASS
                builder = builder.set_filters(noodles::vcf::variant::record_buf::Filters::pass());
            } else {
                let filter_set: noodles::vcf::variant::record_buf::Filters = filters
                    .iter()
                    .filter_map(|f| {
                        if let EncodedValue::Binary(b) = f {
                            Some(String::from_utf8_lossy(b).to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                builder = builder.set_filters(filter_set);
            }
        }
    }

    // Extract and set INFO fields
    if let Some((_, info_val)) = fields.iter().find(|(n, _)| n == "info") {
        if let EncodedValue::Struct(info_fields) = info_val {
            let info = convert_info_to_vcf(info_fields)?;
            builder = builder.set_info(info);
        }
    }

    // Extract and set genotypes (samples)
    if let Some((_, gt_val)) = fields.iter().find(|(n, _)| n == "genotypes") {
        if let EncodedValue::Array(genotypes) = gt_val {
            let samples = convert_genotypes_to_vcf(genotypes, sample_names)?;
            builder = builder.set_samples(samples);
        }
    }

    Ok(builder.build())
}

/// Extract locus (chrom, position) from row fields
fn extract_locus(fields: &[(String, EncodedValue)]) -> Result<(String, i32)> {
    let locus = fields
        .iter()
        .find(|(n, _)| n == "locus")
        .ok_or_else(|| HailError::InvalidFormat("Missing locus field".to_string()))?;

    if let EncodedValue::Struct(locus_fields) = &locus.1 {
        let contig = locus_fields
            .iter()
            .find(|(n, _)| n == "contig")
            .and_then(|(_, v)| {
                if let EncodedValue::Binary(b) = v {
                    Some(String::from_utf8_lossy(b).to_string())
                } else {
                    None
                }
            })
            .ok_or_else(|| HailError::InvalidFormat("Missing contig in locus".to_string()))?;

        let position = locus_fields
            .iter()
            .find(|(n, _)| n == "position")
            .and_then(|(_, v)| {
                if let EncodedValue::Int32(p) = v {
                    Some(*p)
                } else {
                    None
                }
            })
            .ok_or_else(|| HailError::InvalidFormat("Missing position in locus".to_string()))?;

        Ok((contig, position))
    } else {
        Err(HailError::InvalidFormat(
            "Locus must be a struct".to_string(),
        ))
    }
}

/// Extract alleles (ref, alts) from row fields
fn extract_alleles(fields: &[(String, EncodedValue)]) -> Result<(String, Vec<String>)> {
    let alleles = fields
        .iter()
        .find(|(n, _)| n == "alleles")
        .ok_or_else(|| HailError::InvalidFormat("Missing alleles field".to_string()))?;

    if let EncodedValue::Array(allele_arr) = &alleles.1 {
        if allele_arr.is_empty() {
            return Err(HailError::InvalidFormat("Empty alleles array".to_string()));
        }

        let reference = if let EncodedValue::Binary(b) = &allele_arr[0] {
            String::from_utf8_lossy(b).to_string()
        } else {
            return Err(HailError::InvalidFormat(
                "Reference allele must be binary".to_string(),
            ));
        };

        let alternates: Vec<String> = allele_arr[1..]
            .iter()
            .filter_map(|a| {
                if let EncodedValue::Binary(b) = a {
                    Some(String::from_utf8_lossy(b).to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok((reference, alternates))
    } else {
        Err(HailError::InvalidFormat(
            "Alleles must be an array".to_string(),
        ))
    }
}

/// Convert info fields to VCF Info map
fn convert_info_to_vcf(
    info_fields: &[(String, EncodedValue)],
) -> Result<noodles::vcf::variant::record_buf::Info> {
    let mut info = noodles::vcf::variant::record_buf::Info::default();

    for (name, value) in info_fields {
        let vcf_value = match value {
            EncodedValue::Null => continue, // Skip null values
            EncodedValue::Int32(i) => Some(InfoValue::Integer(*i)),
            EncodedValue::Int64(i) => Some(InfoValue::Integer(*i as i32)),
            EncodedValue::Float32(f) => Some(InfoValue::Float(*f)),
            EncodedValue::Float64(f) => Some(InfoValue::Float(*f as f32)),
            EncodedValue::Boolean(b) => {
                if *b {
                    Some(InfoValue::Flag)
                } else {
                    continue;
                }
            }
            EncodedValue::Binary(b) => {
                Some(InfoValue::String(String::from_utf8_lossy(b).to_string()))
            }
            EncodedValue::Array(arr) => convert_info_array_to_vcf(arr),
            EncodedValue::Struct(_) => {
                // Serialize struct as JSON string
                Some(InfoValue::String(
                    serde_json::to_string(value).unwrap_or_default(),
                ))
            }
        };

        if let Some(v) = vcf_value {
            let key = name.parse().unwrap_or_else(|_| name.clone().into());
            info.insert(key, Some(v));
        }
    }

    Ok(info)
}

/// Convert an EncodedValue array to VCF Info array value
fn convert_info_array_to_vcf(arr: &[EncodedValue]) -> Option<InfoValue> {
    use noodles::vcf::variant::record_buf::info::field::value::Array;

    if arr.is_empty() {
        return None;
    }

    // Determine array type from first non-null element
    let first_non_null = arr.iter().find(|v| !matches!(v, EncodedValue::Null));

    match first_non_null {
        Some(EncodedValue::Int32(_)) | Some(EncodedValue::Int64(_)) => {
            let values: Vec<Option<i32>> = arr
                .iter()
                .map(|v| match v {
                    EncodedValue::Int32(i) => Some(*i),
                    EncodedValue::Int64(i) => Some(*i as i32),
                    EncodedValue::Null => None,
                    _ => None,
                })
                .collect();
            Some(InfoValue::Array(Array::Integer(values)))
        }
        Some(EncodedValue::Float32(_)) | Some(EncodedValue::Float64(_)) => {
            let values: Vec<Option<f32>> = arr
                .iter()
                .map(|v| match v {
                    EncodedValue::Float32(f) => Some(*f),
                    EncodedValue::Float64(f) => Some(*f as f32),
                    EncodedValue::Null => None,
                    _ => None,
                })
                .collect();
            Some(InfoValue::Array(Array::Float(values)))
        }
        Some(EncodedValue::Binary(_)) => {
            let values: Vec<Option<String>> = arr
                .iter()
                .map(|v| match v {
                    EncodedValue::Binary(b) => Some(String::from_utf8_lossy(b).to_string()),
                    EncodedValue::Null => None,
                    _ => None,
                })
                .collect();
            Some(InfoValue::Array(Array::String(values)))
        }
        _ => None,
    }
}

/// Convert genotypes to VCF Samples
fn convert_genotypes_to_vcf(
    genotypes: &[EncodedValue],
    _sample_names: &[String],
) -> Result<Samples> {
    use noodles::vcf::variant::record_buf::samples::Keys;

    if genotypes.is_empty() {
        return Ok(Samples::default());
    }

    // Collect all format keys from the first genotype struct
    let format_keys: Vec<String> = if let Some(EncodedValue::Struct(fields)) = genotypes.first() {
        fields
            .iter()
            .filter(|(name, _)| name != "sample_id")
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        return Ok(Samples::default());
    };

    if format_keys.is_empty() {
        return Ok(Samples::default());
    }

    // Build keys
    let keys: Keys = format_keys.iter().map(|k| k.parse().unwrap()).collect();

    // Build sample values
    let mut sample_values = Vec::new();
    for gt in genotypes {
        if let EncodedValue::Struct(fields) = gt {
            let mut values = Vec::new();
            for key in &format_keys {
                let value = fields
                    .iter()
                    .find(|(n, _)| n == key)
                    .map(|(_, v)| convert_sample_value_to_vcf(v, key))
                    .flatten();
                values.push(value);
            }
            sample_values.push(values);
        }
    }

    Ok(Samples::new(keys, sample_values))
}

/// Convert a sample value to VCF format
fn convert_sample_value_to_vcf(value: &EncodedValue, key: &str) -> Option<SampleValue> {
    match value {
        EncodedValue::Null => None,
        EncodedValue::Int32(i) => Some(SampleValue::Integer(*i)),
        EncodedValue::Int64(i) => Some(SampleValue::Integer(*i as i32)),
        EncodedValue::Float32(f) => Some(SampleValue::Float(*f)),
        EncodedValue::Float64(f) => Some(SampleValue::Float(*f as f32)),
        EncodedValue::Boolean(b) => Some(SampleValue::Integer(if *b { 1 } else { 0 })),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b).to_string();
            // Handle genotype field specially (GT) - just pass as string
            if key == "GT" {
                Some(SampleValue::String(s))
            } else {
                Some(SampleValue::String(s))
            }
        }
        EncodedValue::Array(arr) => convert_sample_array_to_vcf(arr),
        EncodedValue::Struct(_) => Some(SampleValue::String(
            serde_json::to_string(value).unwrap_or_default(),
        )),
    }
}

/// Convert sample array to VCF format
fn convert_sample_array_to_vcf(arr: &[EncodedValue]) -> Option<SampleValue> {
    use noodles::vcf::variant::record_buf::samples::sample::value::Array;

    if arr.is_empty() {
        return None;
    }

    let first_non_null = arr.iter().find(|v| !matches!(v, EncodedValue::Null));

    match first_non_null {
        Some(EncodedValue::Int32(_)) | Some(EncodedValue::Int64(_)) => {
            let values: Vec<Option<i32>> = arr
                .iter()
                .map(|v| match v {
                    EncodedValue::Int32(i) => Some(*i),
                    EncodedValue::Int64(i) => Some(*i as i32),
                    EncodedValue::Null => None,
                    _ => None,
                })
                .collect();
            Some(SampleValue::Array(Array::Integer(values)))
        }
        Some(EncodedValue::Float32(_)) | Some(EncodedValue::Float64(_)) => {
            let values: Vec<Option<f32>> = arr
                .iter()
                .map(|v| match v {
                    EncodedValue::Float32(f) => Some(*f),
                    EncodedValue::Float64(f) => Some(*f as f32),
                    EncodedValue::Null => None,
                    _ => None,
                })
                .collect();
            Some(SampleValue::Array(Array::Float(values)))
        }
        Some(EncodedValue::Binary(_)) => {
            let values: Vec<Option<String>> = arr
                .iter()
                .map(|v| match v {
                    EncodedValue::Binary(b) => Some(String::from_utf8_lossy(b).to_string()),
                    EncodedValue::Null => None,
                    _ => None,
                })
                .collect();
            Some(SampleValue::Array(Array::String(values)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_locus() {
        let fields = vec![(
            "locus".to_string(),
            EncodedValue::Struct(vec![
                ("contig".to_string(), EncodedValue::Binary(b"chr1".to_vec())),
                ("position".to_string(), EncodedValue::Int32(12345)),
            ]),
        )];

        let (chrom, pos) = extract_locus(&fields).unwrap();
        assert_eq!(chrom, "chr1");
        assert_eq!(pos, 12345);
    }

    #[test]
    fn test_extract_alleles() {
        let fields = vec![(
            "alleles".to_string(),
            EncodedValue::Array(vec![
                EncodedValue::Binary(b"A".to_vec()),
                EncodedValue::Binary(b"G".to_vec()),
                EncodedValue::Binary(b"T".to_vec()),
            ]),
        )];

        let (ref_allele, alts) = extract_alleles(&fields).unwrap();
        assert_eq!(ref_allele, "A");
        assert_eq!(alts, vec!["G", "T"]);
    }
}
