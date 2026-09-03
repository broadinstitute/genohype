use genohype_association::{compare_association_points, AssociationPointV1, TopK, WarningCode};
use proptest::prelude::*;

fn point(contig: &str, position: i64, significance: f64, ordinal: u64) -> AssociationPointV1 {
    AssociationPointV1 {
        contig: contig.into(),
        position,
        ref_allele: Some("A".into()),
        alt_allele: Some("C".into()),
        pvalue: if significance > 300.0 {
            0.0
        } else {
            10_f64.powf(-significance)
        },
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

fn shard_reduce(points: &[AssociationPointV1], limit: usize, reverse: bool) -> Vec<TopK> {
    let mut shards: Vec<_> = (0..4).map(|_| TopK::new(limit).unwrap()).collect();
    let iter: Box<dyn Iterator<Item = &AssociationPointV1>> = if reverse {
        Box::new(points.iter().rev())
    } else {
        Box::new(points.iter())
    };
    for point in iter {
        let shard = point.source_ordinal as usize % shards.len();
        shards[shard].insert(point.clone()).unwrap();
        assert!(shards[shard].len() <= limit);
    }
    shards
}

fn merge_shards(mut shards: Vec<TopK>, order: &[usize], limit: usize) -> TopK {
    let mut slots: Vec<_> = shards.drain(..).map(Some).collect();
    let mut merged = TopK::new(limit).unwrap();
    for &index in order {
        merged.merge(slots[index].take().unwrap()).unwrap();
        assert!(merged.len() <= limit);
    }
    merged
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn shard_and_merge_order_matches_full_sort(
        rows in prop::collection::vec((0_u8..6, 1_i64..1_000, 0_u16..5_001), 0..160),
        limit in 0_usize..51,
    ) {
        let contigs = ["1", "chr1", "2", "X", "MT", "GL0001"];
        let points: Vec<_> = rows
            .into_iter()
            .enumerate()
            .map(|(ordinal, (contig, position, score))| {
                point(contigs[contig as usize], position, f64::from(score) / 10.0, ordinal as u64)
            })
            .collect();

        let mut expected: Vec<_> = points
            .iter()
            .cloned()
            .map(|point| point.normalize().unwrap().0)
            .collect();
        expected.sort_by(compare_association_points);
        expected.truncate(limit);

        let forward = merge_shards(shard_reduce(&points, limit, false), &[0, 1, 2, 3], limit);
        let reverse = merge_shards(shard_reduce(&points, limit, true), &[3, 2, 1, 0], limit);
        prop_assert_eq!(forward.observed(), points.len() as u64);
        prop_assert_eq!(reverse.observed(), points.len() as u64);
        prop_assert_eq!(forward.into_sorted_vec(), expected.clone());
        prop_assert_eq!(reverse.into_sorted_vec(), expected);
    }
}

#[test]
fn exact_ties_use_all_canonical_keys() {
    let mut points = [
        point("2", 1, 8.0, 8),
        point("1", 2, 8.0, 7),
        point("1", 1, 8.0, 6),
        point("1", 1, 9.0, 5),
        point("1", 1, 8.0, 4),
        point("1", 1, 8.0, 3),
        point("1", 1, 8.0, 2),
    ];
    points[2].ref_allele = Some("C".into());
    points[4].ref_allele = None;
    points[5].alt_allele = None;
    points[6].source_ordinal = 1;
    points.sort_by(compare_association_points);
    assert_eq!(
        points
            .iter()
            .map(|point| point.source_ordinal)
            .collect::<Vec<_>>(),
        vec![5, 4, 3, 1, 6, 7, 8]
    );
}

#[test]
fn significance_above_display_cap_controls_top_k_without_being_clamped() {
    let mut top = TopK::new(2).unwrap();
    let warnings = top.insert(point("1", 1, 400.0, 1)).unwrap();
    assert!(warnings.contains(&WarningCode::NegLog10PDisplayCapped));
    top.insert(point("1", 2, 999.0, 2)).unwrap();
    let points = top.into_sorted_vec();
    assert_eq!(points[0].neg_log10_p, 999.0);
    assert_eq!(points[1].neg_log10_p, 400.0);
}
