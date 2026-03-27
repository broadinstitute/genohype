//! VCF Schema Generation
//!
//! Converts a VCF Header into a Hail EncodedType schema.
//! This ensures VCF data can be represented using the same type system
//! as Hail Tables, enabling interoperability with Parquet conversion,
//! JSON schema validation, and the HTTP server.

use crate::codec::{EncodedField, EncodedType};
use crate::Result;
use noodles::vcf::header::record::value::map::info::{Number as InfoNumber, Type as InfoType};
use noodles::vcf::header::record::value::map::format::{Number as FormatNumber, Type as FormatType};
use noodles::vcf::Header;

/// Generate a Hail-compatible schema from a VCF header
///
/// The schema matches Hail's standard VCF import structure:
/// - `locus`: Struct { contig: String, position: Int32 }
/// - `alleles`: Array[String]
/// - `rsid`: String (nullable)
/// - `qual`: Float64 (nullable)
/// - `filters`: Array[String] (nullable)
/// - `info`: Struct with fields from INFO definitions
/// - `genotypes`: Array[Struct] with sample data (if samples exist)
pub fn extract_schema_from_header(header: &Header) -> Result<EncodedType> {
    let mut fields = Vec::new();
    let mut index = 0;

    // 1. locus: Struct { contig: String, position: Int32 }
    fields.push(EncodedField {
        name: "locus".to_string(),
        encoded_type: EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "contig".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "position".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 1,
                },
            ],
        },
        index,
    });
    index += 1;

    // 2. alleles: Array[String]
    fields.push(EncodedField {
        name: "alleles".to_string(),
        encoded_type: EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EBinary { required: true }),
        },
        index,
    });
    index += 1;

    // 3. rsid: String (nullable)
    fields.push(EncodedField {
        name: "rsid".to_string(),
        encoded_type: EncodedType::EBinary { required: false },
        index,
    });
    index += 1;

    // 4. qual: Float64 (nullable)
    fields.push(EncodedField {
        name: "qual".to_string(),
        encoded_type: EncodedType::EFloat64 { required: false },
        index,
    });
    index += 1;

    // 5. filters: Array[String] (nullable)
    // Hail represents Set[String] as Array[String] physically
    fields.push(EncodedField {
        name: "filters".to_string(),
        encoded_type: EncodedType::EArray {
            required: false,
            element: Box::new(EncodedType::EBinary { required: true }),
        },
        index,
    });
    index += 1;

    // 6. info: Struct (built from header INFO definitions)
    let info_type = build_info_struct(header)?;
    fields.push(EncodedField {
        name: "info".to_string(),
        encoded_type: info_type,
        index,
    });
    index += 1;

    // 7. genotypes: Array[Struct] (if samples exist)
    if !header.sample_names().is_empty() {
        let genotypes_type = build_genotypes_array(header)?;
        fields.push(EncodedField {
            name: "genotypes".to_string(),
            encoded_type: genotypes_type,
            index,
        });
    }

    Ok(EncodedType::EBaseStruct {
        required: true,
        fields,
    })
}

/// Build the INFO struct type from header definitions
fn build_info_struct(header: &Header) -> Result<EncodedType> {
    let mut fields = Vec::new();
    let mut index = 0;

    for (key, info) in header.infos() {
        let field_type = map_info_type_to_hail(info.ty(), info.number());

        fields.push(EncodedField {
            name: key.to_string(),
            encoded_type: field_type,
            index,
        });
        index += 1;
    }

    Ok(EncodedType::EBaseStruct {
        required: false, // Info struct is always present but individual fields may be null
        fields,
    })
}

/// Build the genotypes array type from header definitions
fn build_genotypes_array(header: &Header) -> Result<EncodedType> {
    let mut fields = Vec::new();
    let mut index = 0;

    // Add sample_id field
    fields.push(EncodedField {
        name: "sample_id".to_string(),
        encoded_type: EncodedType::EBinary { required: true },
        index,
    });
    index += 1;

    // Add format fields from header
    for (key, format) in header.formats() {
        let field_type = map_format_type_to_hail(format.ty(), format.number());

        fields.push(EncodedField {
            name: key.to_string(),
            encoded_type: field_type,
            index,
        });
        index += 1;
    }

    // Array of structs (one per sample)
    // Not required because indexed queries may skip genotype parsing
    Ok(EncodedType::EArray {
        required: false,
        element: Box::new(EncodedType::EBaseStruct {
            required: true,
            fields,
        }),
    })
}

/// Map VCF INFO type to Hail EncodedType
fn map_info_type_to_hail(ty: InfoType, number: InfoNumber) -> EncodedType {
    let is_array = match number {
        InfoNumber::Count(n) => n > 1,
        InfoNumber::Unknown => true,
        _ => true, // A, R, G are all array-like
    };

    let base_type = match ty {
        InfoType::Integer => EncodedType::EInt32 { required: false },
        InfoType::Float => EncodedType::EFloat64 { required: false },
        InfoType::Flag => EncodedType::EBoolean { required: false },
        InfoType::Character | InfoType::String => EncodedType::EBinary { required: false },
    };

    // Flags are never arrays
    if is_array && ty != InfoType::Flag {
        EncodedType::EArray {
            required: false,
            element: Box::new(base_type),
        }
    } else {
        base_type
    }
}

/// Map VCF FORMAT type to Hail EncodedType
fn map_format_type_to_hail(ty: FormatType, number: FormatNumber) -> EncodedType {
    let is_array = match number {
        FormatNumber::Count(n) => n > 1,
        FormatNumber::Unknown => true,
        _ => true, // A, R, G are all array-like
    };

    let base_type = match ty {
        FormatType::Integer => EncodedType::EInt32 { required: false },
        FormatType::Float => EncodedType::EFloat64 { required: false },
        FormatType::Character | FormatType::String => EncodedType::EBinary { required: false },
    };

    if is_array {
        EncodedType::EArray {
            required: false,
            element: Box::new(base_type),
        }
    } else {
        base_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_schema_basic() {
        // Create a minimal header
        let header: Header = "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"
            .parse()
            .unwrap();

        let schema = extract_schema_from_header(&header).unwrap();

        // Check it's a struct with expected fields
        if let EncodedType::EBaseStruct { fields, .. } = schema {
            assert!(fields.iter().any(|f| f.name == "locus"));
            assert!(fields.iter().any(|f| f.name == "alleles"));
            assert!(fields.iter().any(|f| f.name == "rsid"));
            assert!(fields.iter().any(|f| f.name == "qual"));
            assert!(fields.iter().any(|f| f.name == "filters"));
            assert!(fields.iter().any(|f| f.name == "info"));
            // No genotypes without samples
            assert!(!fields.iter().any(|f| f.name == "genotypes"));
        } else {
            panic!("Expected EBaseStruct");
        }
    }
}
