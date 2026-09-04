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
        let mut spec = aggregate_spec(&output, true, false);
        match case {
            "missing" => fs::remove_file(receipt).unwrap(),
            "duplicate" => {
                fs::copy(&receipt, output.join("exome/part-00001-reduction.json")).unwrap();
                fs::copy(
                    output.join("exome/part-00000.png"),
                    output.join("exome/part-00001.png"),
                )
                .unwrap();
                spec.exome_partitions = Some(2);
            }
            "corrupt" => fs::write(receipt, b"{not-json").unwrap(),
            _ => unreachable!(),
        }
        let error = run_aggregation(&spec).unwrap_err();
        assert!(
            error.to_string().contains("reduction receipt")
                || error.to_string().contains("expected 1 Manhattan")
                || error.to_string().contains("declares partition")
                || error.to_string().contains("No such file"),
            "{case}: {error}"
        );
        assert!(!temp
            .path()
            .join(format!("manhattans/.completed_phenos/meta_{case}"))
            .exists());
    }
}

#[test]
fn aggregation_requires_exact_raster_partition_inventory_and_bounded_receipts() {
    for case in ["missing-png", "extra-png", "oversized-receipt"] {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join(format!("manhattans/meta/{case}"));
        scan(&output, ManhattanSource::Exome);
        match case {
            "missing-png" => fs::remove_file(output.join("exome/part-00000.png")).unwrap(),
            "extra-png" => {
                fs::copy(
                    output.join("exome/part-00000.png"),
                    output.join("exome/part-00001.png"),
                )
                .unwrap();
            }
            "oversized-receipt" => fs::write(
                output.join("exome/part-00000-reduction.json"),
                vec![b' '; 1_048_577],
            )
            .unwrap(),
            _ => unreachable!(),
        }
        let error = run_aggregation(&aggregate_spec(&output, true, false)).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("partial PNG")
                || message.contains("No part-*.png")
                || message.contains("byte limit")
                || message.contains("No such file"),
            "{case}: {message}"
        );
        assert!(!output.join("plots/exome_manhattan.manifest.json").exists());
    }
}

#[test]
fn aggregation_rejects_same_name_same_dimensions_raster_substitution_and_stale_bytes() {
    for case in ["substitution", "stale-bytes"] {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join(format!("manhattans/meta/{case}"));
        scan(&output, ManhattanSource::Exome);
        let raster_path = output.join("exome/part-00000.png");
        let mut replacement = tiny_skia::Pixmap::new(800, 400).unwrap();
        let color = if case == "substitution" {
            tiny_skia::Color::from_rgba8(255, 0, 0, 255)
        } else {
            tiny_skia::Color::from_rgba8(0, 0, 255, 255)
        };
        replacement.fill(color);
        let replacement = replacement.encode_png().unwrap();
        let decoded = tiny_skia::Pixmap::decode_png(&replacement).unwrap();
        assert_eq!((decoded.width(), decoded.height()), (800, 400));
        fs::write(&raster_path, replacement).unwrap();

        let error = run_aggregation(&aggregate_spec(&output, true, false)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match its reduction receipt"),
            "{case}: {error}"
        );
        assert!(!output.join("plots/exome_manhattan.manifest.json").exists());
    }
}

#[test]
fn aggregation_rejects_unbound_receipts_and_noncanonical_warning_growth() {
    for case in ["unbound", "bogus-warning"] {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join(format!("manhattans/meta/{case}"));
        scan(&output, ManhattanSource::Exome);
        let receipt_path = output.join("exome/part-00000-reduction.json");
        let mut receipt: serde_json::Value =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        match case {
            "unbound" => {
                receipt.as_object_mut().unwrap().remove("raster");
            }
            "bogus-warning" => {
                receipt["warnings"] = serde_json::json!(["worker-00000-private-warning"]);
            }
            _ => unreachable!(),
        }
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();

        let error = run_aggregation(&aggregate_spec(&output, true, false)).unwrap_err();
        assert!(
            error.to_string().contains("not bound") || error.to_string().contains("non-canonical"),
            "{case}: {error}"
        );
        assert!(!output.join("plots/exome_manhattan.manifest.json").exists());
    }
}

#[test]
fn zero_hit_publication_replaces_stale_compatibility_artifacts_with_truthful_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("manhattans/meta/zero-hits");
    scan(&output, ManhattanSource::Exome);
    let receipt_path = output.join("exome/part-00000-reduction.json");
    let mut receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["significant_hit_count"] = serde_json::json!(0);
    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
    genohype_cli::manhattan::sig_writer::SigHitWriter::new(
        output
            .join("exome/part-00000-sig.parquet")
            .to_str()
            .unwrap(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let spec = aggregate_spec(&output, true, false);
    process_manhattan_aggregate(&spec).unwrap();

    let root: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("manifest.json")).unwrap()).unwrap();
    assert!(root["significant_hits"]["exome"].is_null());
    let marker = temp
        .path()
        .join("manhattans/.completed_phenos/meta_zero-hits");
    assert!(marker.is_file());

    fs::write(
        output.join("exome_significant.parquet"),
        b"stale nonzero artifact",
    )
    .unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
}

#[test]
fn failed_rerun_invalidates_prior_completion_marker_before_aggregation() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("manhattans/meta/rerun");
    scan(&output, ManhattanSource::Exome);
    let spec = aggregate_spec(&output, true, false);
    process_manhattan_aggregate(&spec).unwrap();
    let marker = temp.path().join("manhattans/.completed_phenos/meta_rerun");
    assert!(marker.is_file());

    fs::remove_file(output.join("exome/part-00000-reduction.json")).unwrap();
    assert!(process_manhattan_aggregate(&spec).is_err());
    assert!(!marker.exists());
}

#[test]
fn compatibility_significant_hits_are_exactly_verified_before_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("manhattans/meta/significant-integrity");
    scan(&output, ManhattanSource::Exome);
    scan(&output, ManhattanSource::Genome);
    let spec = aggregate_spec(&output, true, false);
    process_manhattan_aggregate(&spec).unwrap();

    let marker = temp
        .path()
        .join("manhattans/.completed_phenos/meta_significant-integrity");
    let root_path = output.join("manifest.json");
    let canonical_path = output.join("plots/exome_manhattan.manifest.json");
    let significant_path = output.join("exome_significant.parquet");
    let original_root = fs::read(&root_path).unwrap();
    let original_canonical = fs::read(&canonical_path).unwrap();
    let original_significant = fs::read(&significant_path).unwrap();
    let genome_content = fs::read(output.join("genome/part-00000-sig.parquet")).unwrap();

    for case in [
        "removed-entry",
        "mismatched-count",
        "wrong-path",
        "corrupt",
        "removed-artifact",
        "wrong-top-hit",
        "wrong-content-source",
        "false-nonzero-entry",
    ] {
        fs::write(&root_path, &original_root).unwrap();
        fs::write(&canonical_path, &original_canonical).unwrap();
        fs::write(&significant_path, &original_significant).unwrap();
        verify_and_checkpoint(&spec).unwrap();
        assert!(marker.is_file());

        match case {
            "removed-entry" => {
                let mut root: serde_json::Value = serde_json::from_slice(&original_root).unwrap();
                root["significant_hits"]
                    .as_object_mut()
                    .unwrap()
                    .remove("exome");
                fs::write(&root_path, serde_json::to_vec(&root).unwrap()).unwrap();
            }
            "mismatched-count" => {
                let mut root: serde_json::Value = serde_json::from_slice(&original_root).unwrap();
                root["significant_hits"]["exome"]["count"] = serde_json::json!(7);
                fs::write(&root_path, serde_json::to_vec(&root).unwrap()).unwrap();
            }
            "wrong-path" => {
                let decoy = output.join("readable-decoy.parquet");
                fs::write(&decoy, &original_significant).unwrap();
                let mut root: serde_json::Value = serde_json::from_slice(&original_root).unwrap();
                root["significant_hits"]["exome"]["path"] =
                    serde_json::json!(decoy.to_string_lossy());
                fs::write(&root_path, serde_json::to_vec(&root).unwrap()).unwrap();
            }
            "corrupt" => fs::write(&significant_path, b"not parquet").unwrap(),
            "removed-artifact" => fs::remove_file(&significant_path).unwrap(),
            "wrong-top-hit" => {
                let mut root: serde_json::Value = serde_json::from_slice(&original_root).unwrap();
                root["significant_hits"]["exome"]["top_hit"]["id"] =
                    serde_json::json!("chr22:1:A:T");
                fs::write(&root_path, serde_json::to_vec(&root).unwrap()).unwrap();
            }
            "wrong-content-source" => fs::write(&significant_path, &genome_content).unwrap(),
            "false-nonzero-entry" => {
                let mut canonical: serde_json::Value =
                    serde_json::from_slice(&original_canonical).unwrap();
                canonical["significant_hit_count"] = serde_json::json!(0);
                fs::write(&canonical_path, serde_json::to_vec(&canonical).unwrap()).unwrap();
            }
            _ => unreachable!(),
        }

        let error = verify_and_checkpoint(&spec).unwrap_err();
        assert!(!marker.exists(), "{case} left a completion marker: {error}");
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
    assert!(
        marker.is_file(),
        "regression must begin with a stale success marker"
    );
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

    let root_manifest_path = output.join("manifest.json");
    let original_root = fs::read(&root_manifest_path).unwrap();
    fs::write(&root_manifest_path, b"{}").unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
    fs::write(&root_manifest_path, original_root).unwrap();

    let mut wrong_media: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
    wrong_media["resources"]["raster"]["media_type"] = serde_json::json!("image/jpeg");
    fs::write(&manifest_path, serde_json::to_vec(&wrong_media).unwrap()).unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());

    let wrong_dimensions = tiny_skia::Pixmap::new(10, 10)
        .unwrap()
        .encode_png()
        .unwrap();
    let mut wrong_geometry: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
    wrong_geometry["resources"]["raster"]["byte_size"] = serde_json::json!(wrong_dimensions.len());
    wrong_geometry["resources"]["raster"]["checksum"] =
        serde_json::json!(format!("sha256:{:x}", Sha256::digest(&wrong_dimensions)));
    fs::write(&manifest_path, serde_json::to_vec(&wrong_geometry).unwrap()).unwrap();
    fs::write(&raster_path, wrong_dimensions).unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
    fs::write(&manifest_path, &original_manifest).unwrap();
    fs::write(&raster_path, &original_raster).unwrap();

    fs::remove_dir(&marker_dir).unwrap();
    fs::write(&marker_dir, b"blocks checkpoint directory").unwrap();
    assert!(verify_and_checkpoint(&spec).is_err());
    assert!(!marker.exists());
}
