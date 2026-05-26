//! VEP integration layer: convert genohype EncodedValue rows to fastVEP VariationFeature structs.

pub mod mapper;
pub mod schema;

use crate::codec::encoded_type::EncodedValue;
use crate::Result;
use fastvep_core::{Allele, GenomicPosition, Strand, VariantType};
use fastvep_io::variant::VariationFeature;

/// Extract locus and alleles from an EncodedValue row and build a VariationFeature
/// ready for annotation by fastVEP's AnnotationContext.
///
/// Handles chr-prefix stripping: if contig starts with "chr", the prefix is removed
/// for Ensembl GFF3 compatibility (e.g., "chr1" -> "1").
pub fn row_to_variation_feature(row: &EncodedValue) -> Result<VariationFeature> {
    let fields = match row {
        EncodedValue::Struct(f) => f,
        _ => {
            return Err(crate::HailError::InvalidFormat(
                "Expected struct row for VEP conversion".into(),
            ))
        }
    };

    // Extract locus.contig and locus.position
    let locus = fields
        .iter()
        .find(|(k, _)| k == "locus")
        .ok_or_else(|| crate::HailError::InvalidFormat("Missing 'locus' field".into()))?;
    let locus_fields = match &locus.1 {
        EncodedValue::Struct(f) => f,
        _ => {
            return Err(crate::HailError::InvalidFormat(
                "Expected locus to be a struct".into(),
            ))
        }
    };

    let contig = locus_fields
        .iter()
        .find(|(k, _)| k == "contig")
        .and_then(|(_, v)| v.as_string())
        .ok_or_else(|| crate::HailError::InvalidFormat("Missing 'locus.contig' field".into()))?;

    let position = locus_fields
        .iter()
        .find(|(k, _)| k == "position")
        .and_then(|(_, v)| v.as_i32())
        .ok_or_else(|| crate::HailError::InvalidFormat("Missing 'locus.position' field".into()))?
        as u64;

    // Strip chr prefix for Ensembl compatibility
    let chromosome = if let Some(stripped) = contig.strip_prefix("chr") {
        stripped.to_string()
    } else {
        contig
    };

    // Extract alleles array
    let alleles_val = fields
        .iter()
        .find(|(k, _)| k == "alleles")
        .ok_or_else(|| crate::HailError::InvalidFormat("Missing 'alleles' field".into()))?;
    let allele_strings: Vec<String> = match &alleles_val.1 {
        EncodedValue::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_string())
            .collect(),
        _ => {
            return Err(crate::HailError::InvalidFormat(
                "Expected alleles to be an array".into(),
            ))
        }
    };

    if allele_strings.is_empty() {
        return Err(crate::HailError::InvalidFormat("Empty alleles array".into()));
    }

    let ref_str = &allele_strings[0];
    let alt_strs: Vec<&str> = allele_strings[1..].iter().map(|s| s.as_str()).collect();

    // VCF-to-VEP coordinate conversion: strip shared first base for indels
    let mut start = position;
    let mut ref_allele_str = ref_str.clone();
    let mut alt_allele_strs: Vec<String> = alt_strs.iter().map(|s| s.to_string()).collect();

    if !alt_allele_strs.is_empty() {
        let all_share_first = !ref_allele_str.is_empty()
            && alt_allele_strs.iter().all(|alt| {
                !alt.is_empty()
                    && !alt.starts_with('<')
                    && alt.as_bytes()[0] == ref_allele_str.as_bytes()[0]
            });

        if all_share_first {
            ref_allele_str = if ref_allele_str.len() > 1 {
                ref_allele_str[1..].to_string()
            } else {
                "-".to_string()
            };
            start += 1;

            alt_allele_strs = alt_allele_strs
                .iter()
                .map(|alt| {
                    if alt.len() > 1 {
                        alt[1..].to_string()
                    } else {
                        "-".to_string()
                    }
                })
                .collect();
        }
    }

    // Calculate end position (Ensembl 1-based inclusive)
    let end = if ref_allele_str == "-" {
        start.saturating_sub(1) // Insertion: zero-length interval
    } else {
        start + ref_allele_str.len() as u64 - 1
    };

    // Build allele string: "REF/ALT1/ALT2"
    let allele_string = if alt_allele_strs.is_empty() {
        ref_allele_str.clone()
    } else {
        format!("{}/{}", ref_allele_str, alt_allele_strs.join("/"))
    };

    let ref_allele = Allele::from_str(&ref_allele_str);
    let alt_alleles: Vec<Allele> = alt_allele_strs.iter().map(|s| Allele::from_str(s)).collect();

    // Classify variant type
    let variant_type = classify_variant_type(&ref_allele, &alt_alleles);

    // Extract rsid if present
    let variation_name = fields
        .iter()
        .find(|(k, _)| k == "rsid")
        .and_then(|(_, v)| v.as_string())
        .filter(|s| s != ".");

    Ok(VariationFeature {
        position: GenomicPosition::new(&chromosome, start, end, Strand::Forward),
        allele_string,
        ref_allele,
        alt_alleles,
        variation_name,
        vcf_fields: None,
        transcript_variations: Vec::new(),
        existing_variants: Vec::new(),
        minimised: false,
        most_severe_consequence: None,
        variant_type,
        sv_end: None,
        sv_len: None,
        supplementary_annotations: Vec::new(),
        gene_annotations: Vec::new(),
    })
}

fn classify_variant_type(ref_allele: &Allele, alt_alleles: &[Allele]) -> VariantType {
    if alt_alleles.is_empty() {
        return VariantType::Unknown;
    }
    let first_alt = &alt_alleles[0];
    match (ref_allele, first_alt) {
        (Allele::Deletion, Allele::Sequence(_)) => VariantType::Insertion,
        (Allele::Sequence(_), Allele::Deletion) => VariantType::Deletion,
        (Allele::Sequence(r), Allele::Sequence(a)) => {
            if r.len() == 1 && a.len() == 1 {
                VariantType::Snv
            } else if r.len() == a.len() {
                VariantType::Mnv
            } else {
                VariantType::Indel
            }
        }
        _ => VariantType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_to_variation_feature_snv() {
        let row = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    (
                        "contig".to_string(),
                        EncodedValue::Binary("chr1".as_bytes().to_vec()),
                    ),
                    ("position".to_string(), EncodedValue::Int32(55039548)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary("G".as_bytes().to_vec()),
                    EncodedValue::Binary("A".as_bytes().to_vec()),
                ]),
            ),
        ]);

        let vf = row_to_variation_feature(&row).unwrap();
        assert_eq!(vf.position.chromosome, "1"); // chr stripped
        assert_eq!(vf.position.start, 55039548);
        assert_eq!(vf.position.end, 55039548);
        assert_eq!(vf.allele_string, "G/A");
        assert_eq!(vf.ref_allele, Allele::from_str("G"));
        assert_eq!(vf.alt_alleles, vec![Allele::from_str("A")]);
        assert_eq!(vf.variant_type, VariantType::Snv);
    }

    #[test]
    fn test_row_to_variation_feature_chr_stripping() {
        // Ensembl-style contig (no chr prefix) should pass through unchanged
        let row = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    (
                        "contig".to_string(),
                        EncodedValue::Binary("1".as_bytes().to_vec()),
                    ),
                    ("position".to_string(), EncodedValue::Int32(100)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary("C".as_bytes().to_vec()),
                    EncodedValue::Binary("T".as_bytes().to_vec()),
                ]),
            ),
        ]);

        let vf = row_to_variation_feature(&row).unwrap();
        assert_eq!(vf.position.chromosome, "1");
    }

    #[test]
    fn test_row_to_variation_feature_insertion() {
        // VCF: REF=A, ALT=AT at position 100 -> Ensembl: -/T at position 101
        let row = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    (
                        "contig".to_string(),
                        EncodedValue::Binary("chr2".as_bytes().to_vec()),
                    ),
                    ("position".to_string(), EncodedValue::Int32(100)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary("A".as_bytes().to_vec()),
                    EncodedValue::Binary("AT".as_bytes().to_vec()),
                ]),
            ),
        ]);

        let vf = row_to_variation_feature(&row).unwrap();
        assert_eq!(vf.position.chromosome, "2");
        assert_eq!(vf.position.start, 101);
        assert_eq!(vf.position.end, 100); // Insertion: end < start
        assert_eq!(vf.allele_string, "-/T");
        assert_eq!(vf.variant_type, VariantType::Insertion);
    }

    #[test]
    fn test_row_to_variation_feature_deletion() {
        // VCF: REF=AT, ALT=A at position 100 -> Ensembl: T/- at position 101
        let row = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    (
                        "contig".to_string(),
                        EncodedValue::Binary("chr3".as_bytes().to_vec()),
                    ),
                    ("position".to_string(), EncodedValue::Int32(100)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary("AT".as_bytes().to_vec()),
                    EncodedValue::Binary("A".as_bytes().to_vec()),
                ]),
            ),
        ]);

        let vf = row_to_variation_feature(&row).unwrap();
        assert_eq!(vf.position.start, 101);
        assert_eq!(vf.position.end, 101);
        assert_eq!(vf.allele_string, "T/-");
        assert_eq!(vf.variant_type, VariantType::Deletion);
    }
}
