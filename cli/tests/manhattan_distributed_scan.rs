use genohype_cli::distributed::manhattan::ManhattanPartitionReductionReceiptV1;
use genohype_cli::distributed::message::{ManhattanScanSpec, ManhattanSource};
use genohype_cli::distributed::worker::handlers::manhattan::process_manhattan_scan_v2;
use genohype_cli::manhattan::config::ResolvedStyle;
use genohype_cli::manhattan::layout::{ChromosomeLayout, YScale};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

fn scan(output: &Path) -> ManhattanPartitionReductionReceiptV1 {
    let contigs = vec![
        ("1".to_string(), 250_000_000),
        ("4".to_string(), 200_000_000),
    ];
    let spec = ManhattanScanSpec {
        phenotype: "synthetic-phenotype".into(),
        ancestry: "meta".into(),
        source: ManhattanSource::Exome,
        table_path: fixture_path().to_string_lossy().into_owned(),
        output_path: output.to_string_lossy().into_owned(),
        threshold: 5e-8,
        y_field: "Pvalue".into(),
        layout: ChromosomeLayout::new(&contigs, 800, 5),
        y_scale: YScale::new(400, 400.0),
        width: 800,
        height: 400,
        contig_lengths: contigs.into_iter().collect::<HashMap<_, _>>(),
        style: ResolvedStyle::default(),
    };

    let (rows, cached) = process_manhattan_scan_v2(None, &[0], &spec, None).unwrap();
    assert_eq!(rows, 12);
    assert!(cached.is_some());

    let source_dir = output.join("exome");
    let png = source_dir.join("part-00000.png");
    let sig = source_dir.join("part-00000-sig.parquet");
    let receipt = source_dir.join("part-00000-reduction.json");
    assert!(fs::metadata(png).unwrap().len() > 0);
    assert!(fs::metadata(sig).unwrap().len() > 0);
    serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap()
}

#[test]
fn local_worker_scan_publishes_canonical_bounded_receipt_last() {
    let temp = tempfile::tempdir().unwrap();
    let first = scan(&temp.path().join("first"));

    assert_eq!(first.partition_index, 0);
    assert_eq!(first.row_count, 12);
    assert_eq!(first.valid_row_count, 10);
    assert_eq!(first.skipped_row_count, 2);
    assert_eq!(first.rendered_point_count, 10);
    assert_eq!(first.significant_hit_count, 5);
    assert_eq!(first.top_hits.len(), 10);
    assert_eq!(first.warnings, ["pvalue_underflow"]);
    assert!(first.top_hits.iter().all(|point| point.source_ordinal < 12));

    // Output location and a fresh worker engine cannot alter source identity or ordering.
    let second = scan(&temp.path().join("second"));
    assert_eq!(second.source_digest, first.source_digest);
    assert_eq!(second.top_hits, first.top_hits);
}
