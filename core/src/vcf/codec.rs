//! VCF Record Codec
//!
//! Transforms noodles VCF records into Hail EncodedValue format.
//! This enables VCF data to be processed using the same pipeline as Hail Tables,
//! including Parquet conversion, JSON export, and HTTP streaming.

use crate::codec::EncodedValue;
use crate::{HailError, Result};
use noodles::vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles::vcf::variant::record::samples::series::value::Genotype as GenotypeRecord;
use noodles::vcf::variant::record::samples::Sample as SampleRecord;
use noodles::vcf::variant::record::AlternateBases as AlternateBasesRecord;
use noodles::vcf::variant::record::Filters as FiltersRecord;
use noodles::vcf::variant::record::Ids as IdsRecord;
use noodles::vcf::variant::record_buf::info::field::Value as InfoValueBuf;
use noodles::vcf::variant::record_buf::samples::sample::value::Value as SampleValueBuf;
use noodles::vcf::variant::RecordBuf;
use noodles::vcf::Header;
use noodles::vcf::Record;

/// Convert a VCF record to a Hail row (EncodedValue)
///
/// The output structure matches Hail's VCF import format:
/// - locus: { contig, position }
/// - alleles: [ref, alt1, alt2, ...]
/// - rsid: ID field (joined with semicolons if multiple)
/// - qual: quality score
/// - filters: array of filter strings
/// - info: struct with INFO fields
/// - genotypes: array of sample genotype data
pub fn record_to_row(header: &Header, record: &RecordBuf) -> Result<EncodedValue> {
    let mut fields = Vec::with_capacity(7);

    // 1. locus: Struct { contig: String, position: Int32 }
    let contig = record.reference_sequence_name().to_string();
    let position = record
        .variant_start()
        .map(|pos| usize::from(pos) as i32)
        .unwrap_or(0);

    fields.push((
        "locus".to_string(),
        EncodedValue::Struct(vec![
            (
                "contig".to_string(),
                EncodedValue::Binary(contig.into_bytes()),
            ),
            ("position".to_string(), EncodedValue::Int32(position)),
        ]),
    ));

    // 2. alleles: Array[String]
    let mut alleles = Vec::new();
    alleles.push(EncodedValue::Binary(
        record.reference_bases().to_string().into_bytes(),
    ));
    // AlternateBasesRecord::iter returns Result items
    for alt_result in AlternateBasesRecord::iter(record.alternate_bases()) {
        if let Ok(alt) = alt_result {
            alleles.push(EncodedValue::Binary(alt.to_string().into_bytes()));
        }
    }
    fields.push(("alleles".to_string(), EncodedValue::Array(alleles)));

    // 3. rsid: String (nullable)
    let ids = record.ids();
    let rsid = if IdsRecord::is_empty(&ids) {
        EncodedValue::Null
    } else {
        // IdsRecord::iter returns &str items (may be Result in some versions)
        let id_str: Vec<String> = IdsRecord::iter(&ids).map(|id| id.to_string()).collect();
        if id_str.is_empty() {
            EncodedValue::Null
        } else {
            EncodedValue::Binary(id_str.join(";").into_bytes())
        }
    };
    fields.push(("rsid".to_string(), rsid));

    // 4. qual: Float64 (nullable)
    let qual = match record.quality_score() {
        Some(q) => EncodedValue::Float64(f64::from(q)),
        None => EncodedValue::Null,
    };
    fields.push(("qual".to_string(), qual));

    // 5. filters: Array[String] (nullable)
    let filters = record.filters();
    let filter_val = if filters.is_pass() {
        // PASS is represented as empty array in Hail
        EncodedValue::Array(vec![])
    } else {
        // FiltersRecord::iter needs the header and returns Result items
        let filter_list: Vec<EncodedValue> = FiltersRecord::iter(&filters, header)
            .filter_map(|f_result| f_result.ok())
            .map(|f| EncodedValue::Binary(f.to_string().into_bytes()))
            .collect();
        if filter_list.is_empty() {
            EncodedValue::Null
        } else {
            EncodedValue::Array(filter_list)
        }
    };
    fields.push(("filters".to_string(), filter_val));

    // 6. info: Struct
    let info = convert_info(header, record)?;
    fields.push(("info".to_string(), info));

    // 7. genotypes: Array[Struct] (if samples exist)
    if !header.sample_names().is_empty() {
        let genotypes = convert_genotypes(header, record)?;
        fields.push(("genotypes".to_string(), genotypes));
    }

    Ok(EncodedValue::Struct(fields))
}

/// Convert a lazy VCF Record (from indexed query) to a Hail row
///
/// This handles the `Record` type returned by indexed query iterators.
/// The structure matches `record_to_row` but handles lazy/borrowed data.
pub fn record_to_row_lazy(header: &Header, record: &Record) -> Result<EncodedValue> {
    let mut fields = Vec::with_capacity(7);

    // 1. locus: Struct { contig: String, position: Int32 }
    // Record::reference_sequence_name() returns &str directly (no Result)
    let contig = record.reference_sequence_name().to_string();

    // Record::variant_start() returns Option<io::Result<Position>>
    let position = record
        .variant_start()
        .transpose()
        .map_err(|e| HailError::InvalidFormat(e.to_string()))?
        .map(|pos| usize::from(pos) as i32)
        .unwrap_or(0);

    fields.push((
        "locus".to_string(),
        EncodedValue::Struct(vec![
            (
                "contig".to_string(),
                EncodedValue::Binary(contig.into_bytes()),
            ),
            ("position".to_string(), EncodedValue::Int32(position)),
        ]),
    ));

    // 2. alleles: Array[String]
    // Record::reference_bases() returns &str directly
    let mut alleles_vec = Vec::new();
    let ref_bases = record.reference_bases();
    alleles_vec.push(EncodedValue::Binary(ref_bases.as_bytes().to_vec()));

    // Record::alternate_bases() returns AlternateBases<'_>
    let alt_bases = record.alternate_bases();
    for alt_result in alt_bases.iter() {
        if let Ok(alt) = alt_result {
            alleles_vec.push(EncodedValue::Binary(alt.as_bytes().to_vec()));
        }
    }
    fields.push(("alleles".to_string(), EncodedValue::Array(alleles_vec)));

    // 3. rsid: String (nullable)
    // Record::ids() returns Ids<'_>
    let ids = record.ids();
    let rsid = if ids.is_empty() {
        EncodedValue::Null
    } else {
        let id_str: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        if id_str.is_empty() {
            EncodedValue::Null
        } else {
            EncodedValue::Binary(id_str.join(";").into_bytes())
        }
    };
    fields.push(("rsid".to_string(), rsid));

    // 4. qual: Float64 (nullable)
    // Record::quality_score() returns Option<io::Result<f32>>
    let qual = match record.quality_score() {
        Some(Ok(q)) => EncodedValue::Float64(f64::from(q)),
        Some(Err(_)) | None => EncodedValue::Null,
    };
    fields.push(("qual".to_string(), qual));

    // 5. filters: Array[String] (nullable)
    // Record::filters() returns Filters<'_>
    let filters = record.filters();
    let is_pass = filters.is_pass(header).unwrap_or(false);
    let filter_val = if is_pass {
        EncodedValue::Array(vec![])
    } else {
        let filter_list: Vec<EncodedValue> = filters
            .iter(header)
            .filter_map(|f_result| f_result.ok())
            .map(|f| EncodedValue::Binary(f.to_string().into_bytes()))
            .collect();
        if filter_list.is_empty() {
            EncodedValue::Null
        } else {
            EncodedValue::Array(filter_list)
        }
    };
    fields.push(("filters".to_string(), filter_val));

    // 6. info: Struct - using lazy accessor
    let info = convert_info_lazy(header, record)?;
    fields.push(("info".to_string(), info));

    // 7. genotypes: Array[Struct] (if samples exist)
    if !header.sample_names().is_empty() {
        let genotypes = convert_genotypes_lazy(header, record)?;
        fields.push(("genotypes".to_string(), genotypes));
    }

    Ok(EncodedValue::Struct(fields))
}

/// Convert INFO fields from a lazy Record to EncodedValue struct
fn convert_info_lazy(header: &Header, record: &Record) -> Result<EncodedValue> {
    let mut fields = Vec::new();
    let info = record.info();

    // Iterate over header info definitions
    for (key, _) in header.infos() {
        let key_str = key.to_string();
        let val = match info.get(header, &key_str) {
            Some(Ok(Some(value))) => convert_info_value_lazy(&value),
            Some(Ok(None)) => EncodedValue::Boolean(true), // Flag present
            Some(Err(_)) | None => EncodedValue::Null,
        };
        fields.push((key_str, val));
    }

    Ok(EncodedValue::Struct(fields))
}

/// Convert a lazy INFO value to EncodedValue
fn convert_info_value_lazy(
    value: &noodles::vcf::variant::record::info::field::Value<'_>,
) -> EncodedValue {
    use noodles::vcf::variant::record::info::field::Value;

    match value {
        Value::Integer(i) => EncodedValue::Int32(*i),
        Value::Float(f) => EncodedValue::Float64(*f as f64),
        Value::Flag => EncodedValue::Boolean(true),
        Value::Character(c) => EncodedValue::Binary(c.to_string().into_bytes()),
        Value::String(s) => EncodedValue::Binary(s.to_string().into_bytes()),
        Value::Array(arr) => convert_info_array_lazy(arr),
    }
}

/// Convert a lazy INFO array to EncodedValue
fn convert_info_array_lazy(
    arr: &noodles::vcf::variant::record::info::field::value::Array<'_>,
) -> EncodedValue {
    use noodles::vcf::variant::record::info::field::value::Array;

    match arr {
        Array::Integer(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(i)) => EncodedValue::Int32(i),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Float(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(f)) => EncodedValue::Float64(f as f64),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Character(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(c)) => EncodedValue::Binary(c.to_string().into_bytes()),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::String(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(s)) => EncodedValue::Binary(s.to_string().into_bytes()),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
    }
}

/// Convert INFO fields to EncodedValue struct
fn convert_info(header: &Header, record: &RecordBuf) -> Result<EncodedValue> {
    let mut fields = Vec::new();
    let info = record.info();

    // Iterate over header info definitions to preserve order and handle missing fields
    for (key, _) in header.infos() {
        let key_str = key.to_string();
        let val = match info.get(key_str.as_str()) {
            Some(Some(value)) => convert_info_value(value),
            Some(None) => EncodedValue::Boolean(true), // Flag present without value
            None => EncodedValue::Null,
        };
        fields.push((key_str, val));
    }

    Ok(EncodedValue::Struct(fields))
}

/// Convert a single INFO value to EncodedValue
fn convert_info_value(value: &InfoValueBuf) -> EncodedValue {
    match value {
        InfoValueBuf::Integer(i) => EncodedValue::Int32(*i),
        InfoValueBuf::Float(f) => EncodedValue::Float64(*f as f64),
        InfoValueBuf::Flag => EncodedValue::Boolean(true),
        InfoValueBuf::Character(c) => EncodedValue::Binary(c.to_string().into_bytes()),
        InfoValueBuf::String(s) => EncodedValue::Binary(s.clone().into_bytes()),
        InfoValueBuf::Array(arr) => convert_info_array(arr),
    }
}

/// Convert INFO array value to EncodedValue
fn convert_info_array(
    arr: &noodles::vcf::variant::record_buf::info::field::value::Array,
) -> EncodedValue {
    use noodles::vcf::variant::record_buf::info::field::value::Array;

    match arr {
        Array::Integer(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(i) => EncodedValue::Int32(*i),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Float(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(f) => EncodedValue::Float64(*f as f64),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Character(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(c) => EncodedValue::Binary(c.to_string().into_bytes()),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::String(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(s) => EncodedValue::Binary(s.clone().into_bytes()),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
    }
}

/// Convert genotypes (sample data) to EncodedValue array
fn convert_genotypes(header: &Header, record: &RecordBuf) -> Result<EncodedValue> {
    let mut genotype_list = Vec::new();
    let sample_names = header.sample_names();
    let samples = record.samples();

    // Get keys as strings - use as_ref() to get the underlying slice
    let keys_slice = samples.keys().as_ref();
    let keys: Vec<String> = keys_slice.iter().map(|k| k.to_string()).collect();

    for (sample_idx, sample_name) in sample_names.iter().enumerate() {
        let mut fields = Vec::new();

        // Sample ID
        fields.push((
            "sample_id".to_string(),
            EncodedValue::Binary(sample_name.to_string().into_bytes()),
        ));

        // Format fields from header
        for (format_key, _) in header.formats() {
            let format_key_str = format_key.to_string();
            let val = if let Some(key_idx) = keys.iter().position(|k| k == &format_key_str) {
                // Get value for this sample at this key index
                if let Some(sample_values) = samples.get_index(sample_idx) {
                    // SampleRecord::get_index returns Option<Result<Option<Value>, Error>>
                    match SampleRecord::get_index(&sample_values, header, key_idx) {
                        Some(Ok(Some(value))) => convert_sample_value_from_series(&value),
                        Some(Ok(None)) | Some(Err(_)) | None => EncodedValue::Null,
                    }
                } else {
                    EncodedValue::Null
                }
            } else {
                EncodedValue::Null
            };
            fields.push((format_key_str, val));
        }

        genotype_list.push(EncodedValue::Struct(fields));
    }

    Ok(EncodedValue::Array(genotype_list))
}

/// Convert genotypes from a lazy Record to EncodedValue array.
/// Uses the concrete Samples type's `select` and `iter` methods for efficiency.
fn convert_genotypes_lazy(header: &Header, record: &Record) -> Result<EncodedValue> {
    let samples_data = record.samples();
    let sample_names = header.sample_names();
    let num_samples = sample_names.len();

    if samples_data.is_empty() || num_samples == 0 {
        return Ok(EncodedValue::Array(Vec::new()));
    }

    // Get the format keys present in this record
    let format_keys: Vec<&str> = samples_data.keys().iter().collect();

    // Collect format field names from the header (superset of what's in this record)
    let header_format_keys: Vec<String> = header.formats().keys().map(|k| k.to_string()).collect();

    // Iterate row-by-row using the Sample iterator
    let mut genotype_list = Vec::with_capacity(num_samples);

    for (sample_idx, sample_name) in sample_names.iter().enumerate() {
        let mut fields = Vec::with_capacity(header_format_keys.len() + 1);

        // Sample ID
        fields.push((
            "sample_id".to_string(),
            EncodedValue::Binary(sample_name.to_string().into_bytes()),
        ));

        // Get this sample's data
        let sample = samples_data.get_index(sample_idx);

        // Format fields - iterate over header format keys to match schema
        for header_key in &header_format_keys {
            let val = if let Some(ref s) = sample {
                // Find this key's index in the record's format keys
                if let Some(key_idx) = format_keys.iter().position(|k| k == header_key) {
                    match s.get_index(header, key_idx) {
                        Some(Some(Ok(value))) => convert_sample_value_from_series(&value),
                        _ => EncodedValue::Null,
                    }
                } else {
                    EncodedValue::Null
                }
            } else {
                EncodedValue::Null
            };
            fields.push((header_key.clone(), val));
        }

        genotype_list.push(EncodedValue::Struct(fields));
    }

    Ok(EncodedValue::Array(genotype_list))
}

/// Convert a sample value from a series to EncodedValue
fn convert_sample_value_from_series(
    value: &noodles::vcf::variant::record::samples::series::Value<'_>,
) -> EncodedValue {
    use noodles::vcf::variant::record::samples::series::Value;

    match value {
        Value::Integer(i) => EncodedValue::Int32(*i),
        Value::Float(f) => EncodedValue::Float64(*f as f64),
        Value::Character(c) => EncodedValue::Binary(c.to_string().into_bytes()),
        Value::String(s) => EncodedValue::Binary(s.to_string().into_bytes()),
        Value::Genotype(gt) => {
            // Convert genotype to string representation (e.g., "0/1", "1|1")
            let mut parts = Vec::new();
            for allele_result in GenotypeRecord::iter(gt.as_ref()) {
                if let Ok((idx, phasing)) = allele_result {
                    let allele_str = match idx {
                        Some(i) => i.to_string(),
                        None => ".".to_string(),
                    };
                    let phasing_char = match phasing {
                        Phasing::Phased => '|',
                        Phasing::Unphased => '/',
                    };
                    if parts.is_empty() {
                        parts.push(allele_str);
                    } else {
                        parts.push(format!("{}{}", phasing_char, allele_str));
                    }
                }
            }
            let gt_str = parts.join("");
            EncodedValue::Binary(gt_str.into_bytes())
        }
        Value::Array(arr) => convert_series_array(arr),
    }
}

/// Convert a series array value to EncodedValue
fn convert_series_array(
    arr: &noodles::vcf::variant::record::samples::series::value::Array<'_>,
) -> EncodedValue {
    use noodles::vcf::variant::record::samples::series::value::Array;

    match arr {
        Array::Integer(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(i)) => EncodedValue::Int32(i),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Float(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(f)) => EncodedValue::Float64(f as f64),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Character(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(c)) => EncodedValue::Binary(c.to_string().into_bytes()),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::String(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Ok(Some(s)) => EncodedValue::Binary(s.to_string().into_bytes()),
                    Ok(None) | Err(_) => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
    }
}

/// Convert a single sample (FORMAT) value to EncodedValue (for RecordBuf)
#[allow(dead_code)]
fn convert_sample_value(value: &SampleValueBuf) -> EncodedValue {
    match value {
        SampleValueBuf::Integer(i) => EncodedValue::Int32(*i),
        SampleValueBuf::Float(f) => EncodedValue::Float64(*f as f64),
        SampleValueBuf::Character(c) => EncodedValue::Binary(c.to_string().into_bytes()),
        SampleValueBuf::String(s) => EncodedValue::Binary(s.clone().into_bytes()),
        SampleValueBuf::Genotype(gt) => {
            // Convert genotype to string representation (e.g., "0/1", "1|1")
            let mut parts = Vec::new();
            for allele_result in GenotypeRecord::iter(&gt) {
                if let Ok((idx, phasing)) = allele_result {
                    let allele_str = match idx {
                        Some(i) => i.to_string(),
                        None => ".".to_string(),
                    };
                    let phasing_char = match phasing {
                        Phasing::Phased => '|',
                        Phasing::Unphased => '/',
                    };
                    if parts.is_empty() {
                        parts.push(allele_str);
                    } else {
                        parts.push(format!("{}{}", phasing_char, allele_str));
                    }
                }
            }
            let gt_str = parts.join("");
            EncodedValue::Binary(gt_str.into_bytes())
        }
        SampleValueBuf::Array(arr) => convert_sample_array(arr),
    }
}

/// Convert sample array value to EncodedValue
fn convert_sample_array(
    arr: &noodles::vcf::variant::record_buf::samples::sample::value::Array,
) -> EncodedValue {
    use noodles::vcf::variant::record_buf::samples::sample::value::Array;

    match arr {
        Array::Integer(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(i) => EncodedValue::Int32(*i),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Float(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(f) => EncodedValue::Float64(*f as f64),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::Character(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(c) => EncodedValue::Binary(c.to_string().into_bytes()),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
        Array::String(values) => {
            let elements: Vec<EncodedValue> = values
                .iter()
                .map(|v| match v {
                    Some(s) => EncodedValue::Binary(s.clone().into_bytes()),
                    None => EncodedValue::Null,
                })
                .collect();
            EncodedValue::Array(elements)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_to_row_basic() {
        // Create a minimal VCF
        let vcf_str = "\
##fileformat=VCFv4.3
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\trs123\tA\tG\t30\tPASS\t.
";
        let mut reader = noodles::vcf::io::Reader::new(vcf_str.as_bytes());
        let header = reader.read_header().unwrap();

        let mut record = RecordBuf::default();
        reader.read_record_buf(&header, &mut record).unwrap();

        let row = record_to_row(&header, &record).unwrap();

        // Verify basic structure
        if let EncodedValue::Struct(fields) = row {
            // Check locus
            let locus = fields.iter().find(|(k, _)| k == "locus").unwrap();
            if let EncodedValue::Struct(locus_fields) = &locus.1 {
                let contig = locus_fields.iter().find(|(k, _)| k == "contig").unwrap();
                assert_eq!(
                    contig.1,
                    EncodedValue::Binary("chr1".to_string().into_bytes())
                );
                let pos = locus_fields.iter().find(|(k, _)| k == "position").unwrap();
                assert_eq!(pos.1, EncodedValue::Int32(100));
            } else {
                panic!("Expected locus to be struct");
            }

            // Check alleles
            let alleles = fields.iter().find(|(k, _)| k == "alleles").unwrap();
            if let EncodedValue::Array(arr) = &alleles.1 {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], EncodedValue::Binary("A".to_string().into_bytes()));
                assert_eq!(arr[1], EncodedValue::Binary("G".to_string().into_bytes()));
            } else {
                panic!("Expected alleles to be array");
            }
        } else {
            panic!("Expected row to be struct");
        }
    }
}
