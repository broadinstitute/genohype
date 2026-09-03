use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

use crate::scale::YScale;

const GRCH38_LENGTHS: [u64; 25] = [
    248_956_422,
    242_193_529,
    198_295_559,
    190_214_555,
    181_538_259,
    170_805_979,
    159_345_973,
    145_138_636,
    138_394_717,
    133_797_422,
    135_086_622,
    133_275_309,
    114_364_328,
    107_043_718,
    101_991_189,
    90_338_345,
    83_257_441,
    80_373_285,
    58_617_616,
    64_444_167,
    46_709_983,
    50_818_468,
    156_040_895,
    57_227_415,
    16_569,
];
const GRCH37_LENGTHS: [u64; 25] = [
    249_250_621,
    243_199_373,
    198_022_430,
    191_154_276,
    180_915_260,
    171_115_067,
    159_138_663,
    146_364_022,
    141_213_431,
    135_534_747,
    135_006_516,
    133_851_895,
    115_169_878,
    107_349_540,
    102_531_392,
    90_354_753,
    81_195_210,
    78_077_248,
    59_128_983,
    63_025_520,
    48_129_895,
    51_304_566,
    155_270_560,
    59_373_566,
    16_569,
];

/// Recognized reference assemblies. Unknown assembly labels remain supported in manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assembly {
    Grch37,
    Grch38,
}

/// Canonicalize common assembly aliases without guessing unknown values.
pub fn canonical_assembly(assembly: &str) -> String {
    match assembly.trim().to_ascii_lowercase().as_str() {
        "grch37" | "hg19" | "b37" => "GRCh37".to_string(),
        "grch38" | "hg38" | "b38" => "GRCh38".to_string(),
        _ => assembly.trim().to_string(),
    }
}

/// Return the canonical primary-contig dictionary in genomic order.
pub fn canonical_contig_lengths(assembly: Assembly) -> Vec<(String, u64)> {
    let lengths = match assembly {
        Assembly::Grch37 => &GRCH37_LENGTHS,
        Assembly::Grch38 => &GRCH38_LENGTHS,
    };
    (1..=22)
        .map(|number| number.to_string())
        .chain(["X".to_string(), "Y".to_string(), "MT".to_string()])
        .zip(lengths.iter().copied())
        .collect()
}

/// Canonicalize primary contig aliases (`chr1`/`1`, `chrM`/`MT`) and retain
/// non-empty alternate-contig names after removing a `chr` prefix.
pub fn canonical_contig(contig: &str) -> Option<String> {
    let trimmed = contig.trim();
    if trimmed.is_empty() {
        return None;
    }
    let bare = if trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("chr"))
    {
        &trimmed[3..]
    } else {
        trimmed
    };
    if bare.is_empty() {
        return None;
    }
    if let Ok(number) = bare.parse::<u8>() {
        if (1..=22).contains(&number) {
            return Some(number.to_string());
        }
    }
    match bare.to_ascii_uppercase().as_str() {
        "X" => Some("X".to_string()),
        "Y" => Some("Y".to_string()),
        "M" | "MT" => Some("MT".to_string()),
        _ => Some(bare.to_string()),
    }
}

fn canonical_rank(contig: &str) -> u16 {
    if let Ok(number) = contig.parse::<u16>() {
        if (1..=22).contains(&number) {
            return number;
        }
    }
    match contig {
        "X" => 23,
        "Y" => 24,
        "MT" => 25,
        _ => u16::MAX,
    }
}

/// Compare contigs in canonical primary order, then lexical alternate-contig order.
pub fn canonical_contig_cmp(left: &str, right: &str) -> Ordering {
    let left = canonical_contig(left).unwrap_or_default();
    let right = canonical_contig(right).unwrap_or_default();
    canonical_rank(&left)
        .cmp(&canonical_rank(&right))
        .then_with(|| left.cmp(&right))
}

/// One contig's raw-coordinate and pixel extent in a Manhattan layout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContigLayoutV1 {
    #[schemars(length(min = 1))]
    pub name: String,
    #[schemars(range(min = 1))]
    pub length: u64,
    #[schemars(range(min = 0.0))]
    pub x_start: f64,
    #[schemars(range(min = 0.0))]
    pub x_end: f64,
    #[schemars(regex(pattern = "^#[0-9A-Fa-f]{6}$"))]
    pub color: String,
}

/// Canonical genome-wide geometry. Public coordinates are 1-based inclusive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GenomeLayoutV1 {
    #[schemars(range(min = 1))]
    pub width: u32,
    #[schemars(range(min = 1))]
    pub height: u32,
    pub contig_gap: u32,
    pub contigs: Vec<ContigLayoutV1>,
    pub y_scale: YScale,
}

impl GenomeLayoutV1 {
    /// Construct a layout after alias normalization and canonical ordering.
    pub fn new<I, S>(
        contigs: I,
        width: u32,
        height: u32,
        contig_gap: u32,
    ) -> Result<Self, LayoutError>
    where
        I: IntoIterator<Item = (S, u64)>,
        S: Into<String>,
    {
        if width == 0 || height == 0 {
            return Err(LayoutError::ZeroDimension);
        }
        let mut normalized = Vec::new();
        let mut names = HashSet::new();
        for (name, length) in contigs {
            let original = name.into();
            let name = canonical_contig(&original).ok_or(LayoutError::InvalidContig)?;
            if length == 0 {
                return Err(LayoutError::ZeroContigLength(name));
            }
            if !names.insert(name.clone()) {
                return Err(LayoutError::DuplicateContig(name));
            }
            normalized.push((name, length));
        }
        if normalized.is_empty() {
            return Err(LayoutError::NoContigs);
        }
        normalized.sort_by(|left, right| canonical_contig_cmp(&left.0, &right.0));

        let gap_count = normalized.len().saturating_sub(1) as u64;
        let total_gap = u64::from(contig_gap)
            .checked_mul(gap_count)
            .ok_or(LayoutError::DimensionsOverflow)?;
        if total_gap >= u64::from(width) {
            return Err(LayoutError::GapsExceedWidth);
        }
        let total_bases: u64 = normalized
            .iter()
            .try_fold(0_u64, |sum, (_, length)| sum.checked_add(*length))
            .ok_or(LayoutError::DimensionsOverflow)?;
        let pixels_per_base = (u64::from(width) - total_gap) as f64 / total_bases as f64;
        let mut cursor = 0.0;
        let colors = ["#404040", "#4682B4"];
        let contigs = normalized
            .into_iter()
            .enumerate()
            .map(|(index, (name, length))| {
                let x_start = cursor;
                let x_end = x_start + length as f64 * pixels_per_base;
                cursor = x_end + f64::from(contig_gap);
                ContigLayoutV1 {
                    name,
                    length,
                    x_start,
                    x_end,
                    color: colors[index % colors.len()].to_string(),
                }
            })
            .collect();

        Ok(Self {
            width,
            height,
            contig_gap,
            contigs,
            y_scale: YScale::new(height),
        })
    }

    /// Ensure deserialized geometry is exactly the canonical V1 derivation.
    pub fn validate(&self) -> Result<(), LayoutError> {
        if self.y_scale != YScale::new(self.height) {
            return Err(LayoutError::NonCanonicalYScale);
        }
        let expected = Self::new(
            self.contigs
                .iter()
                .map(|contig| (contig.name.clone(), contig.length)),
            self.width,
            self.height,
            self.contig_gap,
        )?;
        if *self != expected {
            return Err(LayoutError::NonCanonicalGeometry);
        }
        Ok(())
    }

    /// Convert a 1-based position to the exact raster-space x coordinate.
    pub fn x_for(&self, contig: &str, position: i64) -> Option<f32> {
        let name = canonical_contig(contig)?;
        let info = self.contigs.iter().find(|info| info.name == name)?;
        let position = u64::try_from(position).ok()?;
        if position == 0 || position > info.length {
            return None;
        }
        let fraction = (position - 1) as f64 / info.length as f64;
        Some((info.x_start + fraction * (info.x_end - info.x_start)) as f32)
    }

    pub fn color_for(&self, contig: &str) -> Option<&str> {
        let name = canonical_contig(contig)?;
        self.contigs
            .iter()
            .find(|info| info.name == name)
            .map(|info| info.color.as_str())
    }

    /// Hash all geometry inputs with an explicit binary encoding.
    pub fn layout_hash(&self, assembly: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"genohype.association.layout.v1\0");
        update_string(&mut digest, &canonical_assembly(assembly));
        digest.update(self.width.to_be_bytes());
        digest.update(self.height.to_be_bytes());
        digest.update(self.contig_gap.to_be_bytes());
        digest.update((self.contigs.len() as u64).to_be_bytes());
        for contig in &self.contigs {
            update_string(&mut digest, &contig.name);
            digest.update(contig.length.to_be_bytes());
        }
        digest.update(self.y_scale.log_threshold.to_bits().to_be_bytes());
        digest.update(self.y_scale.linear_fraction.to_bits().to_be_bytes());
        digest.update(self.y_scale.max_neg_log_p.to_bits().to_be_bytes());
        format!("sha256:{:x}", digest.finalize())
    }

    pub fn contig_lengths(&self) -> HashMap<&str, u64> {
        self.contigs
            .iter()
            .map(|contig| (contig.name.as_str(), contig.length))
            .collect()
    }
}

fn update_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutError {
    ZeroDimension,
    NoContigs,
    InvalidContig,
    ZeroContigLength(String),
    DuplicateContig(String),
    GapsExceedWidth,
    DimensionsOverflow,
    NonCanonicalYScale,
    NonCanonicalGeometry,
}

impl fmt::Display for LayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDimension => write!(f, "layout dimensions must be non-zero"),
            Self::NoContigs => write!(f, "layout requires at least one contig"),
            Self::InvalidContig => write!(f, "contig cannot be empty"),
            Self::ZeroContigLength(name) => write!(f, "contig {name} has zero length"),
            Self::DuplicateContig(name) => write!(f, "duplicate canonical contig {name}"),
            Self::GapsExceedWidth => write!(f, "contig gaps consume the full layout width"),
            Self::DimensionsOverflow => write!(f, "layout dimensions overflowed"),
            Self::NonCanonicalYScale => write!(f, "layout must use the canonical V1 y scale"),
            Self::NonCanonicalGeometry => {
                write!(f, "layout geometry is not the canonical derivation")
            }
        }
    }
}

impl Error for LayoutError {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn aliases_and_order_are_canonical() {
        for (alias, expected) in [
            ("chr1", "1"),
            ("01", "1"),
            ("ChrX", "X"),
            ("chrM", "MT"),
            ("MT", "MT"),
        ] {
            assert_eq!(canonical_contig(alias).as_deref(), Some(expected));
        }
        let mut contigs = ["GL1", "MT", "2", "Y", "1", "X"];
        contigs.sort_by(|left, right| canonical_contig_cmp(left, right));
        assert_eq!(contigs, ["1", "2", "X", "Y", "MT", "GL1"]);
    }

    #[test]
    fn assembly_dictionaries_are_complete_and_authoritative() {
        let grch38 = canonical_contig_lengths(Assembly::Grch38);
        assert_eq!(grch38.len(), 25);
        assert_eq!(grch38[0], ("1".into(), 248_956_422));
        assert_eq!(grch38[24], ("MT".into(), 16_569));
        assert_eq!(canonical_assembly("hg19"), "GRCh37");
        assert_eq!(canonical_assembly("b38"), "GRCh38");
    }

    #[test]
    fn aliases_cannot_create_duplicate_layout_entries() {
        let error = GenomeLayoutV1::new([("1", 10), ("chr1", 10)], 100, 50, 4).unwrap_err();
        assert_eq!(error, LayoutError::DuplicateContig("1".into()));
    }

    #[test]
    fn layout_hash_is_stable_across_aliases_and_input_order() {
        let first = GenomeLayoutV1::new([("chr2", 20), ("chr1", 10)], 100, 50, 4).unwrap();
        let second = GenomeLayoutV1::new([("1", 10), ("2", 20)], 100, 50, 4).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.layout_hash("hg38"), second.layout_hash("GRCh38"));
        assert_eq!(
            first.layout_hash("GRCh38"),
            "sha256:3c7ee1a1070d72d6556653ae933790e0f2b36b1daac917e083f73a56fafd4c4a"
        );
    }

    #[test]
    fn deserialized_layout_must_preserve_canonical_geometry_and_scale() {
        let base = GenomeLayoutV1::new([("1", 10), ("2", 20)], 100, 50, 4).unwrap();
        assert!(base.validate().is_ok());

        let mut scale = base.clone();
        scale.y_scale.max_neg_log_p = 300.0;
        assert_eq!(scale.validate(), Err(LayoutError::NonCanonicalYScale));

        let mut extent = base.clone();
        extent.contigs[0].x_end += 1.0;
        assert_eq!(extent.validate(), Err(LayoutError::NonCanonicalGeometry));

        let mut order = base.clone();
        order.contigs.reverse();
        assert_eq!(order.validate(), Err(LayoutError::NonCanonicalGeometry));

        let mut alias = base.clone();
        alias.contigs[0].name = "chr1".into();
        assert_eq!(alias.validate(), Err(LayoutError::NonCanonicalGeometry));
    }

    #[test]
    fn layout_hash_covers_every_canonical_geometry_input() {
        let base = GenomeLayoutV1::new([("1", 10), ("2", 20)], 100, 50, 4).unwrap();
        let base_hash = base.layout_hash("GRCh38");
        assert_ne!(
            base_hash,
            GenomeLayoutV1::new([("1", 10), ("2", 20)], 101, 50, 4)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            base_hash,
            GenomeLayoutV1::new([("1", 10), ("2", 20)], 100, 51, 4)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            base_hash,
            GenomeLayoutV1::new([("1", 10), ("2", 20)], 100, 50, 5)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            base_hash,
            GenomeLayoutV1::new([("1", 11), ("2", 20)], 100, 50, 4)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(base_hash, base.layout_hash("GRCh37"));
        let mut different_scale = base.clone();
        different_scale.y_scale.linear_fraction = 0.5;
        assert_ne!(base_hash, different_scale.layout_hash("GRCh38"));
    }

    proptest! {
        #[test]
        fn mapped_primary_positions_stay_in_bounds(position in 1_i64..=1_000_000) {
            let layout = GenomeLayoutV1::new([("1", 1_000_000)], 777, 333, 0).unwrap();
            let x = layout.x_for("chr1", position).unwrap();
            prop_assert!((0.0..777.0).contains(&x));
        }
    }
}
