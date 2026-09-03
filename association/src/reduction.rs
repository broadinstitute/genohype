use crate::layout::canonical_contig_cmp;
use crate::model::{AssociationPointV1, ValidationError, WarningCode};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt;

pub const DEFAULT_TOP_K: usize = 100;
pub const MAX_TOP_K: usize = 1_000;

/// Canonical deterministic top-hit order.
///
/// Earlier means: greater effective significance, canonical contig, position,
/// reference allele, alternate allele, then source row ordinal.
pub fn compare_association_points(
    left: &AssociationPointV1,
    right: &AssociationPointV1,
) -> Ordering {
    right
        .neg_log10_p
        .total_cmp(&left.neg_log10_p)
        .then_with(|| canonical_contig_cmp(&left.contig, &right.contig))
        .then_with(|| left.position.cmp(&right.position))
        .then_with(|| left.ref_allele.cmp(&right.ref_allele))
        .then_with(|| left.alt_allele.cmp(&right.alt_allele))
        .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
}

#[derive(Debug, Clone)]
struct HeapEntry(AssociationPointV1);

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        compare_association_points(&self.0, &other.0) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // In canonical sort order, Greater means worse. BinaryHeap therefore
        // exposes the worst retained hit for O(log K) replacement.
        compare_association_points(&self.0, &other.0)
    }
}

/// Streaming deterministic reducer with O(K) memory and O(log K) insertion.
#[derive(Debug, Clone)]
pub struct TopK {
    limit: usize,
    heap: BinaryHeap<HeapEntry>,
    observed: u64,
}

impl Default for TopK {
    fn default() -> Self {
        Self::new(DEFAULT_TOP_K).expect("the default top-K limit is valid")
    }
}

impl TopK {
    pub fn new(limit: usize) -> Result<Self, TopKError> {
        if limit > MAX_TOP_K {
            return Err(TopKError::LimitExceedsMaximum {
                requested: limit,
                maximum: MAX_TOP_K,
            });
        }
        Ok(Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            observed: 0,
        })
    }

    /// Validate, normalize, and consider one source row. Returns normalization warnings.
    pub fn insert(&mut self, point: AssociationPointV1) -> Result<Vec<WarningCode>, TopKError> {
        let (point, warnings) = point.normalize().map_err(TopKError::InvalidPoint)?;
        self.observed = self
            .observed
            .checked_add(1)
            .ok_or(TopKError::ObservationCountOverflow)?;
        self.consider_normalized(point);
        Ok(warnings)
    }

    /// Merge another reduction with the same limit without retaining source rows.
    ///
    /// Keeping each shard's best K points is sufficient to recover the global
    /// best K, so this remains O(K) regardless of the number of source rows.
    pub fn merge(&mut self, other: Self) -> Result<(), TopKError> {
        if self.limit != other.limit {
            return Err(TopKError::LimitMismatch {
                left: self.limit,
                right: other.limit,
            });
        }
        self.observed = self
            .observed
            .checked_add(other.observed)
            .ok_or(TopKError::ObservationCountOverflow)?;
        for entry in other.heap {
            self.consider_normalized(entry.0);
        }
        Ok(())
    }

    fn consider_normalized(&mut self, point: AssociationPointV1) {
        if self.limit == 0 {
            return;
        }
        if self.heap.len() < self.limit {
            self.heap.push(HeapEntry(point));
            return;
        }
        let should_replace = self
            .heap
            .peek()
            .is_some_and(|worst| compare_association_points(&point, &worst.0) == Ordering::Less);
        if should_replace {
            self.heap.pop();
            self.heap.push(HeapEntry(point));
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }

    pub const fn observed(&self) -> u64 {
        self.observed
    }

    pub fn is_truncated(&self) -> bool {
        self.observed > self.heap.len() as u64
    }

    pub fn into_sorted_vec(self) -> Vec<AssociationPointV1> {
        let mut points: Vec<_> = self.heap.into_iter().map(|entry| entry.0).collect();
        points.sort_by(compare_association_points);
        points
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopKError {
    LimitExceedsMaximum { requested: usize, maximum: usize },
    LimitMismatch { left: usize, right: usize },
    ObservationCountOverflow,
    InvalidPoint(ValidationError),
}

impl fmt::Display for TopKError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceedsMaximum { requested, maximum } => {
                write!(f, "top-K limit {requested} exceeds hard maximum {maximum}")
            }
            Self::LimitMismatch { left, right } => {
                write!(f, "cannot merge top-K limits {left} and {right}")
            }
            Self::ObservationCountOverflow => write!(f, "top-K observation count overflowed"),
            Self::InvalidPoint(error) => write!(f, "invalid association point: {error}"),
        }
    }
}

impl Error for TopKError {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn point(contig: &str, position: i64, significance: f64, ordinal: u64) -> AssociationPointV1 {
        AssociationPointV1 {
            contig: contig.into(),
            position,
            ref_allele: Some("A".into()),
            alt_allele: Some("C".into()),
            pvalue: 10_f64.powf(-significance),
            neg_log10_p: significance,
            beta: None,
            standard_error: None,
            allele_frequency: None,
            allele_count: None,
            gene_symbol: None,
            consequence: None,
            series: None,
            source_ordinal: ordinal,
        }
    }

    #[test]
    fn default_and_hard_maximum_are_enforced() {
        assert_eq!(TopK::default().limit(), 100);
        assert!(TopK::new(1_000).is_ok());
        assert_eq!(
            TopK::new(1_001).unwrap_err(),
            TopKError::LimitExceedsMaximum {
                requested: 1_001,
                maximum: 1_000
            }
        );
    }

    #[test]
    fn reducer_is_bounded_and_keeps_best_hits() {
        let mut reducer = TopK::new(10).unwrap();
        for ordinal in 0..100_000 {
            reducer
                .insert(point(
                    "1",
                    ordinal as i64 + 1,
                    ordinal as f64 / 1_000.0,
                    ordinal,
                ))
                .unwrap();
            assert!(reducer.len() <= 10);
        }
        let points = reducer.into_sorted_vec();
        assert_eq!(points.len(), 10);
        assert_eq!(points[0].source_ordinal, 99_999);
        assert_eq!(points[9].source_ordinal, 99_990);
    }

    #[test]
    fn ties_follow_every_canonical_key() {
        let mut points = [
            point("chrX", 2, 8.0, 5),
            point("2", 1, 8.0, 4),
            point("chr1", 2, 8.0, 3),
            point("1", 1, 8.0, 2),
            point("1", 1, 8.0, 1),
            point("1", 1, 9.0, 9),
        ];
        points.sort_by(compare_association_points);
        assert_eq!(
            points
                .iter()
                .map(|point| point.source_ordinal)
                .collect::<Vec<_>>(),
            vec![9, 1, 2, 3, 4, 5]
        );
    }

    proptest! {
        #[test]
        fn shard_merges_match_full_sort_in_any_order(
            rows in prop::collection::vec((0_u8..3, 1_u32..1_000_000, 0_u16..=3000), 0..200),
            limit in 0_usize..25,
            reverse_merge_order in any::<bool>(),
        ) {
            let points: Vec<_> = rows
                .into_iter()
                .enumerate()
                .map(|(ordinal, (contig, position, significance_tenths))| {
                    let contig = match contig {
                        0 => "chr1",
                        1 => "2",
                        _ => "chrX",
                    };
                    point(
                        contig,
                        i64::from(position),
                        f64::from(significance_tenths) / 10.0,
                        ordinal as u64,
                    )
                })
                .collect();

            let mut expected: Vec<_> = points
                .iter()
                .cloned()
                .map(|point| point.normalize().unwrap().0)
                .collect();
            expected.sort_by(compare_association_points);
            expected.truncate(limit);

            let mut shards = [TopK::new(limit).unwrap(), TopK::new(limit).unwrap(), TopK::new(limit).unwrap()];
            for (index, point) in points.iter().cloned().enumerate() {
                let shard_index = index % shards.len();
                shards[shard_index].insert(point).unwrap();
                prop_assert!(shards[shard_index].len() <= limit);
            }
            if reverse_merge_order {
                shards.reverse();
            }

            let mut merged = TopK::new(limit).unwrap();
            for shard in shards {
                merged.merge(shard).unwrap();
                prop_assert!(merged.len() <= limit);
            }
            prop_assert_eq!(merged.observed(), points.len() as u64);
            prop_assert_eq!(merged.into_sorted_vec(), expected);
        }
    }
}
