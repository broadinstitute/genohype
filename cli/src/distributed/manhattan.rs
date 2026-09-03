//! Bounded, deterministic reduction receipts produced by distributed Manhattan scans.

use crate::distributed::message::ManhattanSource;
use crate::manhattan::legacy_manifest::{
    hash_encoded_value, source_ordinal, source_partition_hasher, LegacyScanSummary,
};
use genohype_association::{
    compare_association_points, AssociationPointV1, TopK, WarningCode, DEFAULT_TOP_K,
};
use genohype_core::codec::EncodedValue;
use genohype_core::{HailError, Result};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const MANHATTAN_PARTITION_REDUCTION_SCHEMA_V1: &str = "1.0";
pub const MANHATTAN_PARTITION_TOP_HITS_MAX_V1: usize = DEFAULT_TOP_K;

/// Stable source identity stored in a partition reduction receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManhattanReductionSourceV1 {
    Exome,
    Genome,
}

impl From<ManhattanSource> for ManhattanReductionSourceV1 {
    fn from(source: ManhattanSource) -> Self {
        match source {
            ManhattanSource::Exome => Self::Exome,
            ManhattanSource::Genome => Self::Genome,
        }
    }
}

/// Bounded result of scanning one source partition.
///
/// The best points are kept in canonical association order and are hard-bounded
/// at 100. `source_digest` fingerprints every decoded row, including skipped
/// rows, while excluding input and output paths.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ManhattanPartitionReductionReceiptV1 {
    pub schema_version: String,
    pub partition_index: u64,
    pub source: ManhattanReductionSourceV1,
    pub row_count: u64,
    pub valid_row_count: u64,
    pub skipped_row_count: u64,
    pub rendered_point_count: u64,
    pub significant_hit_count: u64,
    pub top_hits: Vec<AssociationPointV1>,
    pub warnings: Vec<String>,
    pub source_digest: String,
}

#[derive(Deserialize)]
struct ManhattanPartitionReductionReceiptWireV1 {
    schema_version: String,
    partition_index: u64,
    source: ManhattanReductionSourceV1,
    row_count: u64,
    valid_row_count: u64,
    skipped_row_count: u64,
    rendered_point_count: u64,
    significant_hit_count: u64,
    top_hits: Vec<AssociationPointV1>,
    warnings: Vec<String>,
    source_digest: String,
}

impl<'de> Deserialize<'de> for ManhattanPartitionReductionReceiptV1 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ManhattanPartitionReductionReceiptWireV1::deserialize(deserializer)?;
        let receipt = Self {
            schema_version: wire.schema_version,
            partition_index: wire.partition_index,
            source: wire.source,
            row_count: wire.row_count,
            valid_row_count: wire.valid_row_count,
            skipped_row_count: wire.skipped_row_count,
            rendered_point_count: wire.rendered_point_count,
            significant_hit_count: wire.significant_hit_count,
            top_hits: wire.top_hits,
            warnings: wire.warnings,
            source_digest: wire.source_digest,
        };
        receipt.validate().map_err(D::Error::custom)?;
        Ok(receipt)
    }
}

impl ManhattanPartitionReductionReceiptV1 {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.schema_version != MANHATTAN_PARTITION_REDUCTION_SCHEMA_V1 {
            return Err(format!(
                "unsupported Manhattan partition reduction schema {:?}",
                self.schema_version
            ));
        }
        let expected_rows = self
            .valid_row_count
            .checked_add(self.skipped_row_count)
            .ok_or_else(|| "partition row counts overflowed".to_string())?;
        if expected_rows != self.row_count {
            return Err("valid_row_count + skipped_row_count must equal row_count".into());
        }
        if self.rendered_point_count > self.valid_row_count {
            return Err("rendered_point_count cannot exceed valid_row_count".into());
        }
        if self.significant_hit_count > self.valid_row_count {
            return Err("significant_hit_count cannot exceed valid_row_count".into());
        }
        let expected_top_hits =
            self.valid_row_count
                .min(MANHATTAN_PARTITION_TOP_HITS_MAX_V1 as u64) as usize;
        if self.top_hits.len() != expected_top_hits {
            return Err(format!(
                "top_hits must contain exactly min(valid_row_count, {}) = {} points, found {}",
                MANHATTAN_PARTITION_TOP_HITS_MAX_V1,
                expected_top_hits,
                self.top_hits.len()
            ));
        }
        let partition = u32::try_from(self.partition_index)
            .map_err(|_| "partition index exceeds source ordinal capacity".to_string())?;
        for point in &self.top_hits {
            let (normalized, _) = point
                .clone()
                .normalize()
                .map_err(|error| format!("invalid top hit: {error}"))?;
            if normalized != *point {
                return Err("top_hits must contain normalized association points".into());
            }
            if point.source_ordinal >> 32 != u64::from(partition) {
                return Err("top hit source ordinal belongs to another partition".into());
            }
        }
        if self
            .top_hits
            .windows(2)
            .any(|pair| compare_association_points(&pair[0], &pair[1]).is_gt())
        {
            return Err("top_hits are not in canonical order".into());
        }
        if self.warnings.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("warnings must be deduplicated and lexically ordered".into());
        }
        let digest = self
            .source_digest
            .strip_prefix("sha256:")
            .ok_or_else(|| "source_digest must use the sha256: prefix".to_string())?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("source_digest must contain 64 lowercase hexadecimal digits".into());
        }
        Ok(())
    }

    pub(crate) fn to_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate().map_err(HailError::InvalidFormat)?;
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| HailError::InvalidFormat(error.to_string()))?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

/// Streaming O(100) accumulator for one partition receipt.
pub(crate) struct ManhattanPartitionReductionAccumulatorV1 {
    partition_index: usize,
    source: ManhattanReductionSourceV1,
    row_count: u64,
    valid_row_count: u64,
    skipped_row_count: u64,
    rendered_point_count: u64,
    significant_hit_count: u64,
    top_hits: TopK,
    warnings: BTreeSet<String>,
    source_hasher: Sha256,
}

impl ManhattanPartitionReductionAccumulatorV1 {
    pub(crate) fn new(partition_index: usize, source: ManhattanSource) -> Result<Self> {
        // Fail before scanning if a stable partition+row ordinal cannot be formed.
        source_ordinal(partition_index, 0)?;
        let source = ManhattanReductionSourceV1::from(source);
        // Reuse the integrated/legacy partition domain so a later bounded merge can
        // produce the same source fingerprint regardless of execution mode.
        let source_hasher = source_partition_hasher(partition_index);
        Ok(Self {
            partition_index,
            source,
            row_count: 0,
            valid_row_count: 0,
            skipped_row_count: 0,
            rendered_point_count: 0,
            significant_hit_count: 0,
            top_hits: TopK::new(MANHATTAN_PARTITION_TOP_HITS_MAX_V1)
                .map_err(|error| HailError::InvalidFormat(error.to_string()))?,
            warnings: BTreeSet::new(),
            source_hasher,
        })
    }

    pub(crate) fn observe_source_row(&mut self, row: &EncodedValue) -> Result<()> {
        self.row_count = checked_add(self.row_count, "source row count")?;
        hash_encoded_value(&mut self.source_hasher, row);
        Ok(())
    }

    pub(crate) fn observe_skipped_row(&mut self) -> Result<()> {
        self.skipped_row_count = checked_add(self.skipped_row_count, "skipped row count")?;
        Ok(())
    }

    pub(crate) fn observe_valid_point(
        &mut self,
        point: AssociationPointV1,
        warnings: Vec<WarningCode>,
        rendered: bool,
        significant: bool,
    ) -> Result<()> {
        self.valid_row_count = checked_add(self.valid_row_count, "valid row count")?;
        if rendered {
            self.rendered_point_count =
                checked_add(self.rendered_point_count, "rendered point count")?;
        }
        if significant {
            self.significant_hit_count =
                checked_add(self.significant_hit_count, "significant hit count")?;
        }
        self.warnings.extend(
            warnings
                .into_iter()
                .map(|warning| warning.as_str().to_string()),
        );
        self.top_hits
            .insert(point)
            .map_err(|error| HailError::InvalidFormat(error.to_string()))?;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<ManhattanPartitionReductionReceiptV1> {
        let receipt = ManhattanPartitionReductionReceiptV1 {
            schema_version: MANHATTAN_PARTITION_REDUCTION_SCHEMA_V1.into(),
            partition_index: self.partition_index as u64,
            source: self.source,
            row_count: self.row_count,
            valid_row_count: self.valid_row_count,
            skipped_row_count: self.skipped_row_count,
            rendered_point_count: self.rendered_point_count,
            significant_hit_count: self.significant_hit_count,
            top_hits: self.top_hits.into_sorted_vec(),
            warnings: self.warnings.into_iter().collect(),
            source_digest: format!("sha256:{:x}", self.source_hasher.finalize()),
        };
        receipt.validate().map_err(HailError::InvalidFormat)?;
        Ok(receipt)
    }
}

/// Merge a complete set of bounded partition receipts in stable partition order.
///
/// The result uses the same source fingerprint construction as local/integrated
/// scans and retains at most the canonical top 100 points.
pub(crate) fn merge_partition_reduction_receipts_in_order<I>(
    expected_source: ManhattanReductionSourceV1,
    expected_partition_count: usize,
    receipts: I,
) -> Result<LegacyScanSummary>
where
    I: IntoIterator<Item = Result<ManhattanPartitionReductionReceiptV1>>,
{
    if expected_partition_count == 0 {
        return Err(HailError::InvalidFormat(
            "distributed Manhattan source has no expected partitions".into(),
        ));
    }
    let mut row_count = 0_u64;
    let mut valid_row_count = 0_u64;
    let mut skipped_row_count = 0_u64;
    let mut rendered_point_count = 0_u64;
    let mut significant_hit_count = 0_u64;
    let mut top_hits =
        TopK::new(DEFAULT_TOP_K).map_err(|error| HailError::InvalidFormat(error.to_string()))?;
    let mut warnings = BTreeSet::new();
    let mut source_hasher = Sha256::new();
    source_hasher.update(b"genohype.association.source.v1\0");
    source_hasher.update((expected_partition_count as u64).to_be_bytes());

    let mut receipt_count = 0usize;
    for (expected_index, receipt) in receipts.into_iter().enumerate() {
        receipt_count = expected_index + 1;
        let receipt = receipt?;
        receipt.validate().map_err(HailError::InvalidFormat)?;
        if receipt.source != expected_source {
            return Err(HailError::InvalidFormat(format!(
                "Manhattan partition {} has wrong source identity",
                receipt.partition_index
            )));
        }
        if receipt.partition_index != expected_index as u64 {
            return Err(HailError::InvalidFormat(format!(
                "missing Manhattan partition reduction receipt {expected_index}"
            )));
        }

        row_count = checked_sum(row_count, receipt.row_count, "source row count")?;
        valid_row_count = checked_sum(valid_row_count, receipt.valid_row_count, "valid row count")?;
        skipped_row_count = checked_sum(
            skipped_row_count,
            receipt.skipped_row_count,
            "skipped row count",
        )?;
        rendered_point_count = checked_sum(
            rendered_point_count,
            receipt.rendered_point_count,
            "rendered point count",
        )?;
        significant_hit_count = checked_sum(
            significant_hit_count,
            receipt.significant_hit_count,
            "significant hit count",
        )?;
        warnings.extend(receipt.warnings);
        for point in receipt.top_hits {
            top_hits
                .insert(point)
                .map_err(|error| HailError::InvalidFormat(error.to_string()))?;
        }

        source_hasher.update(receipt.partition_index.to_be_bytes());
        source_hasher.update(receipt.row_count.to_be_bytes());
        source_hasher.update(parse_source_digest(&receipt.source_digest)?);
    }

    if receipt_count != expected_partition_count {
        return Err(HailError::InvalidFormat(format!(
            "expected {expected_partition_count} Manhattan partition reduction receipts, found {receipt_count}"
        )));
    }
    if valid_row_count.checked_add(skipped_row_count) != Some(row_count) {
        return Err(HailError::InvalidFormat(
            "distributed Manhattan scan counts are inconsistent".into(),
        ));
    }

    Ok(LegacyScanSummary {
        row_count,
        valid_row_count,
        skipped_row_count,
        rendered_point_count,
        significant_hit_count,
        top_hits: top_hits.into_sorted_vec(),
        warnings: warnings.into_iter().collect(),
        source_fingerprint: format!("sha256:{:x}", source_hasher.finalize()),
    })
}

pub(crate) fn merge_partition_reduction_receipts(
    expected_source: ManhattanReductionSourceV1,
    expected_partition_count: usize,
    mut receipts: Vec<ManhattanPartitionReductionReceiptV1>,
) -> Result<LegacyScanSummary> {
    receipts.sort_by_key(|receipt| receipt.partition_index);
    if receipts
        .windows(2)
        .any(|pair| pair[0].partition_index == pair[1].partition_index)
    {
        return Err(HailError::InvalidFormat(
            "duplicate Manhattan partition reduction receipt".into(),
        ));
    }
    merge_partition_reduction_receipts_in_order(
        expected_source,
        expected_partition_count,
        receipts.into_iter().map(Ok),
    )
}

fn parse_source_digest(digest: &str) -> Result<[u8; 32]> {
    let hex = digest.strip_prefix("sha256:").ok_or_else(|| {
        HailError::InvalidFormat("source_digest must use the sha256: prefix".into())
    })?;
    if hex.len() != 64 {
        return Err(HailError::InvalidFormat(
            "source_digest must contain 64 hexadecimal digits".into(),
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair)
            .map_err(|error| HailError::InvalidFormat(error.to_string()))?;
        bytes[index] = u8::from_str_radix(text, 16)
            .map_err(|_| HailError::InvalidFormat("invalid source_digest hexadecimal".into()))?;
    }
    Ok(bytes)
}

fn checked_sum(left: u64, right: u64, name: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| HailError::InvalidFormat(format!("{name} overflowed")))
}

fn checked_add(value: u64, name: &str) -> Result<u64> {
    checked_sum(value, 1, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(source_ordinal: u64, pvalue: f64) -> AssociationPointV1 {
        AssociationPointV1 {
            contig: "1".into(),
            position: source_ordinal as i64 + 1,
            ref_allele: Some("A".into()),
            alt_allele: Some("C".into()),
            pvalue,
            neg_log10_p: -pvalue.log10(),
            beta: None,
            standard_error: None,
            allele_frequency: None,
            allele_count: None,
            gene_symbol: None,
            consequence: None,
            series: None,
            source_ordinal,
        }
    }

    #[test]
    fn receipt_round_trips_with_stable_wire_names() {
        let mut accumulator =
            ManhattanPartitionReductionAccumulatorV1::new(0, ManhattanSource::Exome).unwrap();
        accumulator
            .observe_source_row(&EncodedValue::Int32(7))
            .unwrap();
        accumulator
            .observe_valid_point(point(0, 1e-9), Vec::new(), true, true)
            .unwrap();
        let receipt = accumulator.finish().unwrap();
        let json = serde_json::to_string(&receipt).unwrap();
        assert!(json.contains("\"source\":\"exome\""));
        assert!(json.contains("\"schema_version\":\"1.0\""));
        let decoded: ManhattanPartitionReductionReceiptV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, receipt);
    }

    #[test]
    fn receipt_is_bounded_and_counts_every_observation() {
        let mut accumulator =
            ManhattanPartitionReductionAccumulatorV1::new(3, ManhattanSource::Genome).unwrap();
        for row in 0..150_u32 {
            accumulator
                .observe_source_row(&EncodedValue::Int32(row as i32))
                .unwrap();
            let ordinal = source_ordinal(3, row as usize).unwrap();
            accumulator
                .observe_valid_point(
                    point(ordinal, f64::from(row + 1) * 1e-12),
                    vec![],
                    true,
                    row < 5,
                )
                .unwrap();
        }
        accumulator.observe_source_row(&EncodedValue::Null).unwrap();
        accumulator.observe_skipped_row().unwrap();

        let receipt = accumulator.finish().unwrap();
        assert_eq!(receipt.row_count, 151);
        assert_eq!(receipt.valid_row_count, 150);
        assert_eq!(receipt.skipped_row_count, 1);
        assert_eq!(receipt.rendered_point_count, 150);
        assert_eq!(receipt.significant_hit_count, 5);
        assert_eq!(receipt.top_hits.len(), 100);
        assert!(receipt
            .top_hits
            .windows(2)
            .all(|pair| !compare_association_points(&pair[0], &pair[1]).is_gt()));
    }

    #[test]
    fn complete_receipts_merge_deterministically_with_exact_counts() {
        let receipt = |partition_index: u64, pvalues: &[f64]| {
            let mut top_hits = pvalues
                .iter()
                .enumerate()
                .map(|(row, &pvalue)| point((partition_index << 32) | row as u64, pvalue))
                .collect::<Vec<_>>();
            top_hits.sort_by(compare_association_points);
            ManhattanPartitionReductionReceiptV1 {
                schema_version: MANHATTAN_PARTITION_REDUCTION_SCHEMA_V1.into(),
                partition_index,
                source: ManhattanReductionSourceV1::Exome,
                row_count: top_hits.len() as u64 + 1,
                valid_row_count: top_hits.len() as u64,
                skipped_row_count: 1,
                rendered_point_count: top_hits.len() as u64,
                significant_hit_count: pvalues.iter().filter(|&&p| p < 5e-8).count() as u64,
                top_hits,
                warnings: vec!["pvalue_underflow".into()],
                source_digest: format!("sha256:{:064x}", partition_index + 1),
            }
        };
        let first = receipt(0, &[1e-6, 1e-12]);
        let second = receipt(1, &[1e-12, 1e-9]);

        let forward = merge_partition_reduction_receipts(
            ManhattanReductionSourceV1::Exome,
            2,
            vec![first.clone(), second.clone()],
        )
        .unwrap();
        let reverse = merge_partition_reduction_receipts(
            ManhattanReductionSourceV1::Exome,
            2,
            vec![second, first],
        )
        .unwrap();

        assert_eq!(forward.source_fingerprint, reverse.source_fingerprint);
        assert_eq!(forward.top_hits, reverse.top_hits);
        assert_eq!(forward.row_count, 6);
        assert_eq!(forward.valid_row_count, 4);
        assert_eq!(forward.skipped_row_count, 2);
        assert_eq!(forward.rendered_point_count, 4);
        assert_eq!(forward.significant_hit_count, 3);
        assert_eq!(forward.warnings, ["pvalue_underflow"]);
        assert_eq!(
            forward
                .top_hits
                .iter()
                .map(|hit| hit.source_ordinal)
                .collect::<Vec<_>>(),
            [1, 1_u64 << 32, (1_u64 << 32) | 1, 0]
        );
    }

    #[test]
    fn receipt_merge_rejects_missing_duplicate_and_wrong_source() {
        let receipt = |partition_index, source| ManhattanPartitionReductionReceiptV1 {
            schema_version: MANHATTAN_PARTITION_REDUCTION_SCHEMA_V1.into(),
            partition_index,
            source,
            row_count: 0,
            valid_row_count: 0,
            skipped_row_count: 0,
            rendered_point_count: 0,
            significant_hit_count: 0,
            top_hits: vec![],
            warnings: vec![],
            source_digest: format!("sha256:{:064x}", partition_index + 1),
        };
        let zero = receipt(0, ManhattanReductionSourceV1::Exome);
        assert!(merge_partition_reduction_receipts(
            ManhattanReductionSourceV1::Exome,
            2,
            vec![zero.clone()]
        )
        .unwrap_err()
        .to_string()
        .contains("expected 2"));
        assert!(merge_partition_reduction_receipts(
            ManhattanReductionSourceV1::Exome,
            2,
            vec![zero.clone(), zero.clone()]
        )
        .unwrap_err()
        .to_string()
        .contains("duplicate"));
        assert!(merge_partition_reduction_receipts(
            ManhattanReductionSourceV1::Genome,
            1,
            vec![zero]
        )
        .unwrap_err()
        .to_string()
        .contains("wrong source"));
    }

    #[test]
    fn deserialization_rejects_unbounded_or_inconsistent_receipts() {
        let mut accumulator =
            ManhattanPartitionReductionAccumulatorV1::new(0, ManhattanSource::Exome).unwrap();
        accumulator
            .observe_source_row(&EncodedValue::Int32(1))
            .unwrap();
        accumulator.observe_skipped_row().unwrap();
        let receipt = accumulator.finish().unwrap();
        let mut value = serde_json::to_value(receipt).unwrap();

        value["row_count"] = 2.into();
        assert!(
            serde_json::from_value::<ManhattanPartitionReductionReceiptV1>(value.clone())
                .unwrap_err()
                .to_string()
                .contains("must equal")
        );

        value["row_count"] = 101.into();
        value["valid_row_count"] = 101.into();
        value["skipped_row_count"] = 0.into();
        value["top_hits"] = serde_json::to_value(
            (0..101_u64)
                .map(|ordinal| point(ordinal, (ordinal + 1) as f64 * 1e-12))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(
            serde_json::from_value::<ManhattanPartitionReductionReceiptV1>(value)
                .unwrap_err()
                .to_string()
                .contains("exactly")
        );
    }

    #[test]
    fn deserialization_rejects_truncated_and_noncanonical_top_hits() {
        let mut accumulator =
            ManhattanPartitionReductionAccumulatorV1::new(0, ManhattanSource::Exome).unwrap();
        for row in 0..3 {
            accumulator
                .observe_source_row(&EncodedValue::Int32(row))
                .unwrap();
            accumulator
                .observe_valid_point(
                    point(row as u64, 1e-9 * f64::from(row + 1)),
                    vec![],
                    true,
                    true,
                )
                .unwrap();
        }
        let receipt = accumulator.finish().unwrap();
        let mut truncated = serde_json::to_value(&receipt).unwrap();
        truncated["top_hits"].as_array_mut().unwrap().pop();
        assert!(
            serde_json::from_value::<ManhattanPartitionReductionReceiptV1>(truncated)
                .unwrap_err()
                .to_string()
                .contains("exactly")
        );

        let mut unordered = serde_json::to_value(receipt).unwrap();
        unordered["top_hits"].as_array_mut().unwrap().swap(0, 2);
        assert!(
            serde_json::from_value::<ManhattanPartitionReductionReceiptV1>(unordered)
                .unwrap_err()
                .to_string()
                .contains("canonical order")
        );
    }
}
