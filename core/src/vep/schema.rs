//! VEP annotation schema definition for EncodedType.
//!
//! Defines the nested struct schema for the `vep` field that gets appended
//! to each row by AnnotatingDataSource. Each element represents a flattened
//! (transcript, allele) annotation pair.

use crate::codec::{EncodedField, EncodedType};

/// Returns the EncodedField for the `vep` column: an array of annotation structs.
///
/// Each struct element represents one (transcript, allele) pair with fields like
/// gene_symbol, transcript_id, consequence, impact, hgvsc, hgvsp, etc.
pub fn vep_field() -> EncodedField {
    let element_fields = vec![
        str_field("allele", 0),
        str_field("consequence", 1),
        str_field("impact", 2),
        str_field("gene_symbol", 3),
        str_field("gene_id", 4),
        str_field("transcript_id", 5),
        str_field("biotype", 6),
        bool_field("canonical", 7),
        str_field("hgvsc", 8),
        str_field("hgvsp", 9),
        str_field("hgvsg", 10),
        str_field("amino_acids", 11),
        str_field("codons", 12),
        opt_str_field("protein_id", 13),
        opt_str_field("exon", 14),
        opt_str_field("intron", 15),
        opt_str_field("sift", 16),
        opt_str_field("polyphen", 17),
        opt_int_field("distance", 18),
        opt_str_field("mane_select", 19),
        opt_str_field("mane_plus_clinical", 20),
        opt_str_field("source", 21),
        opt_str_field("cdna_position", 22),
        opt_str_field("cds_position", 23),
        opt_str_field("protein_position", 24),
    ];

    EncodedField {
        name: "vep".to_string(),
        encoded_type: EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EBaseStruct {
                required: true,
                fields: element_fields,
            }),
        },
        index: 0, // Will be set by caller
    }
}

fn str_field(name: &str, index: usize) -> EncodedField {
    EncodedField {
        name: name.to_string(),
        encoded_type: EncodedType::EBinary { required: true },
        index,
    }
}

fn opt_str_field(name: &str, index: usize) -> EncodedField {
    EncodedField {
        name: name.to_string(),
        encoded_type: EncodedType::EBinary { required: false },
        index,
    }
}

fn bool_field(name: &str, index: usize) -> EncodedField {
    EncodedField {
        name: name.to_string(),
        encoded_type: EncodedType::EBoolean { required: true },
        index,
    }
}

fn opt_int_field(name: &str, index: usize) -> EncodedField {
    EncodedField {
        name: name.to_string(),
        encoded_type: EncodedType::EInt64 { required: false },
        index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vep_field_structure() {
        let field = vep_field();
        assert_eq!(field.name, "vep");
        match &field.encoded_type {
            EncodedType::EArray { element, .. } => match element.as_ref() {
                EncodedType::EBaseStruct { fields, .. } => {
                    assert!(fields.len() >= 20);
                    assert_eq!(fields[0].name, "allele");
                    assert_eq!(fields[1].name, "consequence");
                    assert_eq!(fields[7].name, "canonical");
                }
                _ => panic!("expected struct element"),
            },
            _ => panic!("expected array type"),
        }
    }
}
