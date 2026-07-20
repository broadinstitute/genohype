//! Maps fastVEP VariationFeature annotations back to EncodedValue for DataSource output.
//!
//! Flattens the nested (TranscriptVariation → AlleleAnnotation) structure into
//! one EncodedValue::Struct per (transcript, allele) pair, matching the schema
//! defined in `vep::schema::vep_field()`.

use crate::codec::EncodedValue;
use crate::Result;
use fastvep_io::variant::VariationFeature;

/// Append a `vep` field to an existing row using the annotated VariationFeature.
///
/// The row must be an `EncodedValue::Struct`. The `vep` field is an array of
/// structs, one per (transcript, allele) pair.
pub fn append_vep_to_row(row: EncodedValue, vf: &VariationFeature) -> Result<EncodedValue> {
    let mut fields = match row {
        EncodedValue::Struct(f) => f,
        _ => {
            return Err(crate::HailError::InvalidFormat(
                "Expected struct row for VEP append".into(),
            ))
        }
    };

    let vep_array = transcript_variations_to_encoded(vf);
    // Replace existing `vep` field if present (e.g. Canadian HT has a minimal vep struct),
    // otherwise append.
    if let Some(pos) = fields.iter().position(|(k, _)| k == "vep") {
        fields[pos] = ("vep".to_string(), vep_array);
    } else {
        fields.push(("vep".to_string(), vep_array));
    }
    Ok(EncodedValue::Struct(fields))
}

/// Convert all transcript variations from a VariationFeature into an EncodedValue::Array.
///
/// Each element is a flattened struct representing one (transcript, allele) pair.
fn transcript_variations_to_encoded(vf: &VariationFeature) -> EncodedValue {
    let mut elements = Vec::new();

    for tv in &vf.transcript_variations {
        for aa in &tv.allele_annotations {
            let consequences: String = aa
                .consequences
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join("&");

            let impact = format!("{:?}", aa.impact).to_uppercase();

            let amino_acids = aa
                .amino_acids
                .as_ref()
                .map(|(r, a)| format!("{}/{}", r, a))
                .unwrap_or_default();

            let codons = aa
                .codons
                .as_ref()
                .map(|(r, a)| format!("{}/{}", r, a))
                .unwrap_or_default();

            let exon = aa.exon.map(|(n, t)| format!("{}/{}", n, t));
            let intron = aa.intron.map(|(n, t)| format!("{}/{}", n, t));

            let cdna_pos = aa.cdna_position.map(|(s, e)| {
                if s == e {
                    format!("{}", s)
                } else {
                    format!("{}-{}", s, e)
                }
            });

            let cds_pos = aa.cds_position.map(|(s, e)| {
                if s == e {
                    format!("{}", s)
                } else {
                    format!("{}-{}", s, e)
                }
            });

            let protein_pos = aa.protein_position.map(|(s, e)| {
                if s == e {
                    format!("{}", s)
                } else {
                    format!("{}-{}", s, e)
                }
            });

            let element = EncodedValue::Struct(vec![
                ("allele".into(), str_val(&aa.allele.to_string())),
                ("consequence".into(), str_val(&consequences)),
                ("impact".into(), str_val(&impact)),
                (
                    "gene_symbol".into(),
                    str_val(tv.gene_symbol.as_deref().unwrap_or("-")),
                ),
                ("gene_id".into(), str_val(&tv.gene_id)),
                ("transcript_id".into(), str_val(&tv.transcript_id)),
                ("biotype".into(), str_val(&tv.biotype)),
                ("canonical".into(), EncodedValue::Boolean(tv.canonical)),
                ("hgvsc".into(), opt_str_val(aa.hgvsc.as_deref())),
                ("hgvsp".into(), opt_str_val(aa.hgvsp.as_deref())),
                ("hgvsg".into(), opt_str_val(aa.hgvsg.as_deref())),
                ("amino_acids".into(), str_val(&amino_acids)),
                ("codons".into(), str_val(&codons)),
                ("protein_id".into(), opt_str_val(tv.protein_id.as_deref())),
                ("exon".into(), opt_str_val(exon.as_deref())),
                ("intron".into(), opt_str_val(intron.as_deref())),
                ("sift".into(), opt_str_val(aa.sift.as_deref())),
                ("polyphen".into(), opt_str_val(aa.polyphen.as_deref())),
                (
                    "distance".into(),
                    aa.distance
                        .map(|d| EncodedValue::Int64(d))
                        .unwrap_or(EncodedValue::Null),
                ),
                ("mane_select".into(), opt_str_val(tv.mane_select.as_deref())),
                (
                    "mane_plus_clinical".into(),
                    opt_str_val(tv.mane_plus_clinical.as_deref()),
                ),
                ("source".into(), opt_str_val(tv.source.as_deref())),
                ("cdna_position".into(), opt_str_val(cdna_pos.as_deref())),
                ("cds_position".into(), opt_str_val(cds_pos.as_deref())),
                (
                    "protein_position".into(),
                    opt_str_val(protein_pos.as_deref()),
                ),
                (
                    "lof".into(),
                    opt_str_val(aa.loftee.as_ref().map(|l| l.confidence.as_str())),
                ),
                (
                    "lof_filter".into(),
                    opt_str_val(
                        aa.loftee
                            .as_ref()
                            .and_then(|l| {
                                if l.filters.is_empty() {
                                    None
                                } else {
                                    Some(l.filters.join(","))
                                }
                            })
                            .as_deref(),
                    ),
                ),
                (
                    "lof_flags".into(),
                    opt_str_val(
                        aa.loftee
                            .as_ref()
                            .and_then(|l| {
                                if l.flags.is_empty() {
                                    None
                                } else {
                                    Some(l.flags.join(","))
                                }
                            })
                            .as_deref(),
                    ),
                ),
            ]);

            elements.push(element);
        }
    }

    EncodedValue::Array(elements)
}

fn str_val(s: &str) -> EncodedValue {
    EncodedValue::Binary(s.as_bytes().to_vec())
}

fn opt_str_val(s: Option<&str>) -> EncodedValue {
    match s {
        Some(v) => EncodedValue::Binary(v.as_bytes().to_vec()),
        None => EncodedValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastvep_core::{Allele, Consequence, GenomicPosition, Impact, Strand};
    use fastvep_io::variant::{AlleleAnnotation, TranscriptVariation};

    fn make_test_vf() -> VariationFeature {
        let mut vf = VariationFeature {
            position: GenomicPosition::new("1", 55039548, 55039548, Strand::Forward),
            allele_string: "G/A".to_string(),
            ref_allele: Allele::from_str("G"),
            alt_alleles: vec![Allele::from_str("A")],
            variation_name: None,
            vcf_fields: None,
            transcript_variations: Vec::new(),
            existing_variants: Vec::new(),
            minimised: false,
            most_severe_consequence: None,
            variant_type: fastvep_core::VariantType::Snv,
            sv_end: None,
            sv_len: None,
            supplementary_annotations: Vec::new(),
            gene_annotations: Vec::new(),
        };

        vf.transcript_variations.push(TranscriptVariation {
            transcript_id: "ENST00000302118".into(),
            gene_id: "ENSG00000169174".into(),
            gene_symbol: Some("PCSK9".into()),
            biotype: "protein_coding".into(),
            allele_annotations: vec![AlleleAnnotation {
                allele: Allele::from_str("A"),
                consequences: vec![Consequence::MissenseVariant],
                impact: Impact::Moderate,
                cdna_position: Some((100, 100)),
                cds_position: Some((50, 50)),
                protein_position: Some((17, 17)),
                amino_acids: Some(("R".to_string(), "K".to_string())),
                codons: Some(("aGg".to_string(), "aAg".to_string())),
                exon: Some((2, 12)),
                intron: None,
                distance: None,
                hgvsc: Some("ENST00000302118.5:c.50G>A".to_string()),
                hgvsp: Some("ENSP00000303208.5:p.Arg17Lys".to_string()),
                hgvsg: Some("1:g.55039548G>A".to_string()),
                hgvs_offset: None,
                existing_variation: vec![],
                sift: Some("tolerated(0.5)".to_string()),
                polyphen: Some("benign(0.1)".to_string()),
                supplementary: Vec::new(),
                acmg_classification: None,
                loftee: None,
            }],
            canonical: true,
            strand: Strand::Forward,
            source: Some("pcsk9_transcripts.gff3".to_string()),
            protein_id: Some("ENSP00000303208".to_string()),
            mane_select: Some("NM_174936.4".to_string()),
            mane_plus_clinical: None,
            tsl: Some(1),
            appris: None,
            ccds: None,
            gencode_primary: false,
            symbol_source: None,
            hgnc_id: None,
            flags: Vec::new(),
        });

        vf
    }

    #[test]
    fn test_append_vep_to_row() {
        let row = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    ("contig".into(), EncodedValue::Binary(b"chr1".to_vec())),
                    ("position".into(), EncodedValue::Int32(55039548)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary(b"G".to_vec()),
                    EncodedValue::Binary(b"A".to_vec()),
                ]),
            ),
        ]);

        let vf = make_test_vf();
        let result = append_vep_to_row(row, &vf).unwrap();

        let fields = match result {
            EncodedValue::Struct(f) => f,
            _ => panic!("expected struct"),
        };

        // Should have locus, alleles, vep
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[2].0, "vep");

        let vep_arr = match &fields[2].1 {
            EncodedValue::Array(a) => a,
            _ => panic!("expected array"),
        };

        // One transcript × one allele = 1 element
        assert_eq!(vep_arr.len(), 1);

        let entry = match &vep_arr[0] {
            EncodedValue::Struct(f) => f,
            _ => panic!("expected struct element"),
        };

        // Check a few fields
        assert_eq!(entry[1].0, "consequence");
        assert_eq!(entry[1].1.as_string().unwrap(), "missense_variant");
        assert_eq!(entry[3].0, "gene_symbol");
        assert_eq!(entry[3].1.as_string().unwrap(), "PCSK9");
        assert_eq!(entry[7].0, "canonical");
        assert_eq!(entry[7].1, EncodedValue::Boolean(true));
    }
}
