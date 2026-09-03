use genohype_association::{
    canonical_contig_lengths, Assembly, AssociationVisualizationManifestV1,
};
use genohype_cli::distributed::message::{
    ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource,
};
use genohype_cli::distributed::worker::handlers::manhattan::{
    process_manhattan_aggregate, process_manhattan_scan_v2, verify_and_checkpoint,
};
use genohype_cli::manhattan::aggregate::run_aggregation;
use genohype_cli::manhattan::config::{ManhattanConfig, PlotType};
use genohype_cli::manhattan::layout::{ChromosomeLayout, YScale};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/test/association/single-variant.ht")
}

fn canonical_scan_geometry() -> (ChromosomeLayout, YScale, HashMap<String, u32>) {
    let contigs: Vec<(String, u32)> = canonical_contig_lengths(Assembly::Grch38)
        .into_iter()
        .map(|(name, length)| (name, length as u32))
        .collect();
    (
        ChromosomeLayout::new(&contigs, 800, 4),
        YScale::new(400, 350.0),
        contigs.into_iter().collect(),
    )
}

fn scan(output: &Path, source: ManhattanSource) {
    let (layout, y_scale, contig_lengths) = canonical_scan_geometry();
    let config = ManhattanConfig::default();
    let plot_type = match source {
        ManhattanSource::Exome => PlotType::Exome,
        ManhattanSource::Genome => PlotType::Genome,
    };
    let spec = ManhattanScanSpec {
        phenotype: "synthetic-phenotype".into(),
        ancestry: "meta".into(),
        source,
        table_path: fixture_path().to_string_lossy().into_owned(),
        output_path: output.to_string_lossy().into_owned(),
        threshold: 1e-7,
        y_field: "Pvalue".into(),
        layout,
        y_scale,
        width: 800,
        height: 400,
        contig_lengths,
        style: config.resolve(plot_type),
    };
    let (rows, _) = process_manhattan_scan_v2(None, &[0], &spec, None).unwrap();
    assert_eq!(rows, 12);
}

fn aggregate_spec(output: &Path, exome: bool, genome: bool) -> ManhattanAggregateSpec {
    let (layout, y_scale, _) = canonical_scan_geometry();
    let input = fixture_path().to_string_lossy().into_owned();
    ManhattanAggregateSpec {
        output_path: output.to_string_lossy().into_owned(),
        phenotype_id: Some("synthetic-phenotype".into()),
        ancestry: Some("meta".into()),
        exome_results: exome.then(|| input.clone()),
        genome_results: genome.then_some(input),
        exome_partitions: exome.then_some(1),
        genome_partitions: genome.then_some(1),
        gene_burden: None,
        gene_associations_only: false,
        exome_exp_p: None,
        genome_exp_p: None,
        exome_annotations: None,
        genome_annotations: None,
        genes: None,
        threshold: 1e-7,
        gene_threshold: 2.5e-6,
        locus_threshold: 0.01,
        locus_window: 1_000_000,
        locus_plots: false,
        min_variants_per_locus: 1,
        width: 800,
        height: 400,
        layout,
        y_scale,
        cleanup: false,
        styling: ManhattanConfig::default(),
    }
}

fn assert_canonical(output: &Path, source: &str) -> AssociationVisualizationManifestV1 {
    let plots = output.join("plots");
    let raster_name = format!("{source}_manhattan.png");
    let raster = fs::read(plots.join(&raster_name)).unwrap();
    let manifest: AssociationVisualizationManifestV1 = serde_json::from_slice(
        &fs::read(plots.join(format!("{source}_manhattan.manifest.json"))).unwrap(),
    )
    .unwrap();
    manifest.validate().unwrap();
    assert_eq!(manifest.plot_id, format!("{source}_manhattan"));
    assert_eq!(manifest.assembly, "GRCh38");
    assert_eq!(manifest.layout.contigs.len(), 25);
    assert_eq!(manifest.layout.y_scale.max_neg_log_p, 350.0);
    assert_eq!(manifest.thresholds[0].pvalue, 1e-7);
    assert_eq!(manifest.row_count, 12);
    assert_eq!(manifest.valid_row_count, 10);
    assert_eq!(manifest.skipped_row_count, 2);
    assert_eq!(manifest.rendered_point_count, 10);
    assert_eq!(manifest.significant_hit_count, 6);
    assert_eq!(manifest.top_hits.len(), 10);
    assert_eq!(manifest.warnings, ["pvalue_underflow"]);
    assert_eq!(manifest.resources.raster.url, raster_name);
    assert_eq!(
        manifest.resources.raster.byte_size,
        Some(raster.len() as u64)
    );
    assert_eq!(
        manifest.resources.raster.checksum,
        Some(format!("sha256:{:x}", Sha256::digest(&raster)))
    );
    let tied: Vec<_> = manifest
        .top_hits
        .iter()
        .filter(|hit| hit.pvalue == 1e-12)
        .map(|hit| (hit.contig.as_str(), hit.position, hit.source_ordinal))
        .collect();
    assert_eq!(tied, [("1", 101_003, 2), ("4", 68_000_010, 6)]);
    manifest
}

#[test]
fn local_distributed_aggregate_publishes_exome_and_genome_manifests_and_root_inventory() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("manhattans/meta/synthetic");
    scan(&output, ManhattanSource::Exome);
    scan(&output, ManhattanSource::Genome);
    let spec = aggregate_spec(&output, true, true);

    let (rows, _) = process_manhattan_aggregate(&spec).unwrap();
    assert_eq!(rows, 20);
    let exome = assert_canonical(&output, "exome");
    let genome = assert_canonical(&output, "genome");
    assert_eq!(exome.source_fingerprint, genome.source_fingerprint);
    assert!(output.join("manifest.json").is_file());
    assert!(output.join("chroms/chr1/exome_manhattan.png").is_file());
    assert!(output.join("chroms/chr1/genome_manhattan.png").is_file());
    assert!(temp
        .path()
        .join("manhattans/.completed_phenos/meta_synthetic")
        .is_file());
}

#[test]
fn aggregate_rejects_missing_duplicate_and_corrupt_receipts() {
    for case in ["missing", "duplicate", "corrupt"] {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join(format!("manhattans/meta/{case}"));
        scan(&output, ManhattanSource::Exome);
        let receipt = output.join("exome/part-00000-reduction.json");
        match case {
            "missing" => fs::remove_file(receipt).unwrap(),
            "duplicate" => {
                fs::copy(receipt, output.join("exome/part-00001-reduction.json")).unwrap();
            }
            "corrupt" => fs::write(receipt, b"{not-json").unwrap(),
            _ => unreachable!(),
        }
        let error = run_aggregation(&aggregate_spec(&output, true, false)).unwrap_err();
        assert!(
            error.to_string().contains("reduction receipt")
                || error.to_string().contains("expected 1 Manhattan"),
            "{case}: {error}"
        );
        assert!(!temp
            .path()
            .join(format!("manhattans/.completed_phenos/meta_{case}"))
            .exists());
    }
}

#[test]
fn completion_verification_is_fail_closed_for_manifests_rasters_and_marker_writes() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("manhattans/meta/verify");
    scan(&output, ManhattanSource::Exome);
    let spec = aggregate_spec(&output, true, false);
    process_manhattan_aggregate(&spec).unwrap();
    assert_canonical(&output, "exome");

    let marker_dir = temp.path().join("manhattans/.completed_phenos");
    let marker = marker_dir.join("meta_verify");
    fs::remove_file(&marker).unwrap();
    let manifest_path = output.join("plots/exome_manhattan.manifest.json");
    let raster_path = output.join("plots/exome_manhattan.png");
    let original_manifest = fs::read(&manifest_path).unwrap();
    let original_raster = fs::read(&raster_path).unwrap();

    fs::remove_file(&manifest_path).unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
    fs::write(&manifest_path, &original_manifest).unwrap();

    fs::remove_file(&raster_path).unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
    fs::write(&raster_path, &original_raster).unwrap();

    let mut invalid: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
    invalid["resources"]["raster"]["url"] = serde_json::json!("../escape.png");
    fs::write(&manifest_path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());

    let mut mismatch: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
    mismatch["resources"]["raster"]["checksum"] =
        serde_json::json!(format!("sha256:{}", "0".repeat(64)));
    fs::write(&manifest_path, serde_json::to_vec(&mismatch).unwrap()).unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
    fs::write(&manifest_path, &original_manifest).unwrap();

    fs::remove_dir(&marker_dir).unwrap();
    fs::write(&marker_dir, b"blocks checkpoint directory").unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
}
