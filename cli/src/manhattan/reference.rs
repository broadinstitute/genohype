//! Reference genome contig length helpers.
//!
//! Provides fallback chromosome lengths for GRCh37 and GRCh38 when the
//! table metadata doesn't include reference genome information.

use std::collections::HashMap;

use genohype_core::query::QueryEngine;

/// Try to determine contig lengths from the engine metadata, falling back
/// to GRCh38 defaults.
pub fn get_contig_lengths(_engine: &QueryEngine) -> Vec<(String, u32)> {
    // TODO: Try to extract from engine globals / reference genome metadata.
    // For now, fall back to GRCh38 standard lengths.
    get_grch38_autosomes_and_x()
}

/// GRCh38 chromosome lengths for autosomes + X.
/// Returns an ordered Vec suitable for layout.
fn get_grch38_autosomes_and_x() -> Vec<(String, u32)> {
    vec![
        ("1".into(), 248956422),
        ("2".into(), 242193529),
        ("3".into(), 198295559),
        ("4".into(), 190214555),
        ("5".into(), 181538259),
        ("6".into(), 170805979),
        ("7".into(), 159345973),
        ("8".into(), 145138636),
        ("9".into(), 138394717),
        ("10".into(), 133797422),
        ("11".into(), 135086622),
        ("12".into(), 133275309),
        ("13".into(), 114364328),
        ("14".into(), 107043718),
        ("15".into(), 101991189),
        ("16".into(), 90338345),
        ("17".into(), 83257441),
        ("18".into(), 80373285),
        ("19".into(), 58617616),
        ("20".into(), 64444167),
        ("21".into(), 46709983),
        ("22".into(), 50818468),
        ("X".into(), 156040895),
    ]
}

/// Build a lookup map from the ordered contig list, including "chr"-prefixed aliases.
#[allow(dead_code)]
pub fn contig_length_map(contigs: &[(String, u32)]) -> HashMap<String, u32> {
    let mut m = HashMap::new();
    for (name, len) in contigs {
        m.insert(name.clone(), *len);
        // Also register "chr" prefixed version for VCFs that use "chr1" format
        if !name.starts_with("chr") {
            m.insert(format!("chr{}", name), *len);
        }
    }
    m
}

/// Convert a contig name to its numeric index for xpos calculation.
///
/// - Strips "chr" prefix if present
/// - Maps "X" -> 23, "Y" -> 24, "M"/"MT" -> 25
/// - Parses numeric strings (1-22) directly
/// - Returns 0 for unknown contigs
pub fn contig_to_int(contig: &str) -> u8 {
    // Strip "chr" prefix if present
    let name = contig.strip_prefix("chr").unwrap_or(contig);

    match name {
        "X" => 23,
        "Y" => 24,
        "M" | "MT" => 25,
        _ => name.parse::<u8>().unwrap_or(0),
    }
}

/// Calculate xpos from contig name and position.
///
/// xpos is a single integer encoding both chromosome and position:
/// `xpos = contig_num * 1_000_000_000 + position`
///
/// This enables efficient ordering and range queries across the genome.
pub fn calculate_xpos(contig: &str, position: i32) -> i64 {
    let contig_num = contig_to_int(contig) as i64;
    contig_num * 1_000_000_000 + position as i64
}

/// Normalize a contig name to have the "chr" prefix.
///
/// - Returns unchanged if already prefixed
/// - Adds "chr" prefix for numeric or X/Y/M/MT contigs
pub fn normalize_contig_name(contig: &str) -> String {
    if contig.starts_with("chr") {
        contig.to_string()
    } else {
        format!("chr{}", contig)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contig_to_int() {
        // Basic numeric chromosomes
        assert_eq!(contig_to_int("1"), 1);
        assert_eq!(contig_to_int("22"), 22);
        assert_eq!(contig_to_int("chr1"), 1);
        assert_eq!(contig_to_int("chr22"), 22);

        // Sex chromosomes
        assert_eq!(contig_to_int("X"), 23);
        assert_eq!(contig_to_int("chrX"), 23);
        assert_eq!(contig_to_int("Y"), 24);
        assert_eq!(contig_to_int("chrY"), 24);

        // Mitochondrial
        assert_eq!(contig_to_int("M"), 25);
        assert_eq!(contig_to_int("MT"), 25);
        assert_eq!(contig_to_int("chrM"), 25);

        // Unknown
        assert_eq!(contig_to_int("unknown"), 0);
        assert_eq!(contig_to_int("GL000220.1"), 0);
    }

    #[test]
    fn test_calculate_xpos() {
        // Basic cases
        assert_eq!(calculate_xpos("1", 12345), 1_000_012_345);
        assert_eq!(calculate_xpos("chr1", 12345), 1_000_012_345);
        assert_eq!(calculate_xpos("22", 67890), 22_000_067_890);

        // Sex chromosomes
        assert_eq!(calculate_xpos("X", 100), 23_000_000_100);
        assert_eq!(calculate_xpos("chrX", 100), 23_000_000_100);
        assert_eq!(calculate_xpos("Y", 200), 24_000_000_200);

        // Mitochondrial
        assert_eq!(calculate_xpos("MT", 300), 25_000_000_300);
    }

    #[test]
    fn test_normalize_contig_name() {
        assert_eq!(normalize_contig_name("1"), "chr1");
        assert_eq!(normalize_contig_name("X"), "chrX");
        assert_eq!(normalize_contig_name("chr1"), "chr1");
        assert_eq!(normalize_contig_name("chrX"), "chrX");
    }
}
