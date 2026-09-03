//! Manhattan plot handlers.
//!
//! Processes Manhattan scan (Phase 1) and aggregate (Phase 2) jobs.

use crate::distributed::manhattan::{
    ManhattanPartitionReductionAccumulatorV1, ManhattanPartitionReductionReceiptV1,
};
use crate::distributed::message::{
    CoreTaskInfo, ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource,
};
use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::query::QueryEngine;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;

enum ScanSigHitWriter {
    Local(crate::manhattan::sig_writer::SigHitWriter<std::fs::File>),
    Cloud(crate::manhattan::sig_writer::SigHitWriter<genohype_core::io::CloudWriter>),
}

impl ScanSigHitWriter {
    fn new(path: &str) -> Result<Self> {
        if genohype_core::io::is_cloud_path(path) {
            let writer = genohype_core::io::CloudWriter::new(path)?;
            Ok(Self::Cloud(
                crate::manhattan::sig_writer::SigHitWriter::from_writer(writer)?,
            ))
        } else {
            Ok(Self::Local(
                crate::manhattan::sig_writer::SigHitWriter::new(path)?,
            ))
        }
    }

    fn write(&mut self, row: crate::manhattan::data::SigHitRow) -> Result<()> {
        match self {
            Self::Local(writer) => writer.write(row),
            Self::Cloud(writer) => writer.write(row),
        }
    }

    fn finish(self) -> Result<()> {
        match self {
            Self::Local(writer) => {
                writer.finish()?;
            }
            Self::Cloud(writer) => {
                writer.into_inner()?.finish()?;
            }
        }
        Ok(())
    }
}

fn write_partition_reduction_receipt(
    path: &str,
    receipt: &ManhattanPartitionReductionReceiptV1,
) -> Result<()> {
    let bytes = receipt.to_json_bytes()?;
    if genohype_core::io::is_cloud_path(path) {
        let mut writer = genohype_core::io::StreamingCloudWriter::new(path)?;
        writer.write_all(&bytes)?;
        writer.finish()?;
        return Ok(());
    }

    let path = std::path::Path::new(path);
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            crate::HailError::InvalidFormat("reduction receipt has no UTF-8 basename".into())
        })?;
    let temporary = parent.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Process partitions for Manhattan scan phase (V2 pipeline).
///
/// This is the Phase 1 worker task. For each partition, it:
/// 1. Renders plot points to a partial PNG (transparent background)
/// 2. Writes significant hits (below threshold) to a Parquet file
/// 3. Publishes a bounded reduction receipt after all partition artifacts succeed
///
/// Output structure:
/// - {output_path}/{source}/part-{id}.png
/// - {output_path}/{source}/part-{id}-sig.parquet
/// - {output_path}/{source}/part-{id}-reduction.json
pub fn process_manhattan_scan_v2(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    spec: &ManhattanScanSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::manhattan::data::{extract_association_point, SigHitRow};
    use crate::manhattan::layout::ChromosomeLayout;
    use crate::manhattan::reference::{calculate_xpos, normalize_contig_name};
    use crate::manhattan::render::ManhattanRenderer;
    use genohype_core::io::{is_cloud_path, StreamingCloudWriter};
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::io::Write;

    let source_name = match spec.source {
        ManhattanSource::Exome => "exome",
        ManhattanSource::Genome => "genome",
    };

    // Get sequencing_type string for SigHitRow
    let sequencing_type = source_name.to_string();

    println!(
        "Processing {} partitions for Manhattan scan ({})...",
        partitions.len(),
        source_name
    );

    let layout = &spec.layout;
    let y_scale = &spec.y_scale;
    let table_path = &spec.table_path;

    let engine = if let Some((cached_path, cached_eng)) = cached_engine {
        if cached_path == *table_path {
            cached_eng
        } else {
            QueryEngine::open_path(table_path)?
        }
    } else {
        QueryEngine::open_path(table_path)?
    };
    let engine_ref = &engine;
    let output_base = format!("{}/{}", spec.output_path.trim_end_matches('/'), source_name);
    let width = spec.width;
    let height = spec.height;
    let threshold = spec.threshold;
    let y_field = &spec.y_field;
    let phenotype = &spec.phenotype;
    let ancestry = &spec.ancestry;
    let contig_lengths = &spec.contig_lengths;
    let style = &spec.style;

    // Update telemetry with first partition
    if let Some(ref ts) = telemetry {
        if let Some(&part_id) = partitions.first() {
            ts.active_partition.store(part_id, Ordering::Relaxed);
        }
    }

    // Parallel scan+render+encode: each partition produces PNG + sig.parquet
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Track the active partition for this Rayon thread (RAII guard)
            // Include phenotype as label so dashboard shows which phenotype each core is scanning
            let _core_guard = telemetry.as_ref().map(|ts| {
                CoreTaskGuard::new(
                    ts,
                    CoreTaskInfo {
                        task_type: "partition".to_string(),
                        task_id: partition_id.to_string(),
                        label: Some(phenotype.clone()),
                        parent: None,
                    },
                )
            });

            let iter = engine_ref.scan_partition_iter(partition_id, &[])?;

            // Each thread has its own renderer with transparent background.
            let mut renderer = ManhattanRenderer::new_transparent(width, height);
            let mut chrom_renderers: HashMap<String, ManhattanRenderer> = HashMap::new();
            let mut chrom_layouts: HashMap<String, ChromosomeLayout> = HashMap::new();
            let mut reduction =
                ManhattanPartitionReductionAccumulatorV1::new(partition_id, spec.source)?;

            // Significant rows are streamed in bounded batches rather than retained in memory.
            let sig_file = format!("{}/part-{:05}-sig.parquet", output_base, partition_id);
            if !is_cloud_path(&sig_file) {
                std::fs::create_dir_all(&output_base)?;
            }
            let mut sig_writer = ScanSigHitWriter::new(&sig_file)?;

            for (row_index, row_result) in iter.enumerate() {
                let row = row_result?;
                let ordinal =
                    crate::manhattan::legacy_manifest::source_ordinal(partition_id, row_index)?;
                reduction.observe_source_row(&row)?;

                let (point, warnings) = match extract_association_point(&row, y_field, ordinal) {
                    Ok(adapted) => adapted,
                    Err(_) => {
                        reduction.observe_skipped_row()?;
                        continue;
                    }
                };

                // Legacy raster and Parquet formats use i32 positions. This matches the
                // integrated path and fails rather than silently narrowing a valid point.
                let position = i32::try_from(point.position).map_err(|_| {
                    crate::HailError::InvalidFormat(format!(
                        "position {} cannot be represented by distributed Manhattan outputs",
                        point.position
                    ))
                })?;
                let contig_for_layout = point.contig.as_str();
                let significant = point.pvalue < threshold;

                // A point is counted as rendered only when it appears in the whole-genome layout.
                let rendered = if let Some(x) = layout.get_x(contig_for_layout, position) {
                    let y = y_scale.get_y(point.neg_log10_p);
                    let color = layout.get_color(contig_for_layout);
                    renderer.render_point_with_radius(
                        x,
                        y,
                        color,
                        style.point_alpha,
                        style.point_radius,
                    );
                    true
                } else {
                    false
                };

                // Compatibility per-chromosome partials remain transparent and keep their
                // existing output names and scales.
                let normalized_contig = normalize_contig_name(&point.contig);
                if let Some(&len) = contig_lengths
                    .get(contig_for_layout)
                    .or_else(|| contig_lengths.get(&normalized_contig))
                {
                    let chrom_layout = chrom_layouts
                        .entry(normalized_contig.clone())
                        .or_insert_with(|| {
                            ChromosomeLayout::new(&[(contig_for_layout.to_string(), len)], width, 0)
                        });
                    let chrom_renderer = chrom_renderers
                        .entry(normalized_contig.clone())
                        .or_insert_with(|| ManhattanRenderer::new_transparent(width, height));
                    if let Some(x) = chrom_layout.get_x(contig_for_layout, position) {
                        let y = y_scale.get_y(point.neg_log10_p);
                        let color = layout.get_color(contig_for_layout);
                        chrom_renderer.render_point_with_radius(
                            x,
                            y,
                            color,
                            style.point_alpha,
                            style.point_radius,
                        );
                    }
                }

                if significant {
                    sig_writer.write(SigHitRow {
                        phenotype: phenotype.clone(),
                        ancestry: ancestry.clone(),
                        sequencing_type: sequencing_type.clone(),
                        contig: normalized_contig,
                        position,
                        ref_allele: point.ref_allele.clone().unwrap_or_default(),
                        alt_allele: point.alt_allele.clone().unwrap_or_default(),
                        xpos: calculate_xpos(&point.contig, position),
                        pvalue: point.pvalue,
                        beta: point.beta,
                        se: point.standard_error,
                        af: point.allele_frequency,
                        ac_cases: None,
                        ac_controls: None,
                        af_cases: None,
                        af_controls: None,
                        association_ac: point.allele_count,
                    })?;
                }

                reduction.observe_valid_point(point, warnings, rendered, significant)?;
            }

            // Encode WG PNG
            let png_data = renderer.encode_png()?;

            // Write WG PNG for this partition
            let png_file = format!("{}/part-{:05}.png", output_base, partition_id);
            if is_cloud_path(&png_file) {
                let mut writer = StreamingCloudWriter::new(&png_file)?;
                writer.write_all(&png_data)?;
                writer.finish()?;
            } else {
                std::fs::create_dir_all(&output_base)?;
                std::fs::write(&png_file, &png_data)?;
            }

            // Write Per-Chromosome PNGs
            // Structure: {output_root}/chroms/{chrom}/{source}/part-{id}.png
            let root = spec.output_path.trim_end_matches('/');
            for (chrom, chrom_renderer) in chrom_renderers {
                let chrom_png_data = chrom_renderer.encode_png()?;
                let chrom_path = format!(
                    "{}/chroms/{}/{}/part-{:05}.png",
                    root, chrom, source_name, partition_id
                );

                if is_cloud_path(&chrom_path) {
                    let mut writer = StreamingCloudWriter::new(&chrom_path)?;
                    writer.write_all(&chrom_png_data)?;
                    writer.finish()?;
                } else {
                    if let Some(parent) = std::path::Path::new(&chrom_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&chrom_path, &chrom_png_data)?;
                }
            }

            // Finalize Parquet before publishing the receipt. Any writer error aborts the
            // partition and leaves no successful reduction receipt behind.
            sig_writer.finish()?;
            let receipt = reduction.finish()?;
            let receipt_file = format!("{}/part-{:05}-reduction.json", output_base, partition_id);
            write_partition_reduction_receipt(&receipt_file, &receipt)?;

            usize::try_from(receipt.row_count).map_err(|_| {
                crate::HailError::InvalidFormat("partition row count exceeds usize".into())
            })
        })
        .collect();

    // Aggregate row counts
    let mut total_rows = 0usize;
    for result in results {
        total_rows += result?;
    }

    // Update telemetry
    if let Some(ref ts) = telemetry {
        ts.total_rows.fetch_add(total_rows, Ordering::Relaxed);
    }

    println!(
        "  Manhattan scan ({}) partitions {:?} complete: {} rows",
        source_name, partitions, total_rows
    );

    Ok((total_rows, Some((spec.table_path.clone(), engine))))
}

/// Extract fields for a significant hit from an encoded row.
pub fn extract_sig_hit_fields(
    row: &genohype_core::codec::EncodedValue,
) -> (String, String, Option<f64>, Option<f64>, Option<f64>) {
    use genohype_core::codec::EncodedValue;

    // Helper to get nested field
    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
        let mut current = value;
        for &field_name in path {
            if let EncodedValue::Struct(fields) = current {
                current = fields
                    .iter()
                    .find(|(n, _)| n == field_name)
                    .map(|(_, v)| v)?;
            } else {
                return None;
            }
        }
        Some(current)
    }

    // Extract alleles
    let (ref_allele, alt_allele) =
        if let Some(EncodedValue::Array(alleles)) = get_field(row, &["alleles"]) {
            let ref_a = alleles
                .first()
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let alt_a = alleles
                .get(1)
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            (ref_a, alt_a)
        } else {
            (String::new(), String::new())
        };

    // Extract BETA
    let beta = get_field(row, &["BETA"]).and_then(|v| match v {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        _ => None,
    });

    // Extract SE
    let se = get_field(row, &["SE"]).and_then(|v| match v {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        _ => None,
    });

    // Extract AF (AF_Allele2)
    let af = get_field(row, &["AF_Allele2"]).and_then(|v| match v {
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Float32(f) => Some(*f as f64),
        _ => None,
    });

    (ref_allele, alt_allele, beta, se, af)
}

/// Process Manhattan aggregate phase (V2 pipeline).
///
/// This is the Phase 2 worker task. It:
/// 1. Composites partial PNGs into final Manhattan plots
/// 2. Processes gene burden table (if provided)
/// 3. Merges pre-annotated significant hits
/// 4. Generates locus plots for significant regions
/// 5. Writes manifest.json
/// 6. Verifies outputs and writes to checkpoint
pub fn process_manhattan_aggregate(
    spec: &ManhattanAggregateSpec,
) -> Result<(usize, serde_json::Value)> {
    use crate::manhattan::aggregate::run_aggregation;

    let (rows, summary) = run_aggregation(spec)?;

    // The narrow gene export intentionally has no Manhattan manifest/checkpoint.
    if !spec.gene_associations_only {
        // Verify expected outputs exist and write to checkpoint
        if let Err(e) = verify_and_checkpoint(spec) {
            // Log but don't fail - aggregation succeeded
            eprintln!("Warning: checkpoint update failed: {}", e);
        }
    }

    Ok((rows, summary))
}

/// Verify expected outputs exist and write checkpoint marker.
#[allow(unused_variables)]
fn verify_and_checkpoint(spec: &ManhattanAggregateSpec) -> Result<()> {
    let output_base = spec.output_path.trim_end_matches('/');

    // Build list of expected files
    let mut expected = vec!["manifest.json"];
    if spec.exome_results.is_some() {
        expected.push("plots/exome_manhattan.png");
    }
    if spec.genome_results.is_some() {
        expected.push("plots/genome_manhattan.png");
    }

    // Verify expected files exist (log warnings instead of failing)
    for file in &expected {
        let file_url = format!("{}/{}", output_base, file);
        if genohype_core::io::is_cloud_path(&file_url) {
            if let Err(e) = genohype_core::io::get_file_size(&file_url) {
                eprintln!(
                    "  Warning: Expected output file missing or inaccessible: {} ({})",
                    file_url, e
                );
            }
        } else if !std::path::Path::new(&file_url).exists() {
            eprintln!("  Warning: Expected output file missing: {}", file_url);
        }
    }

    // Derive checkpoint path and relative phenotype path
    // output_path: gs://bucket/manhattans/meta/1234
    // checkpoint:  gs://bucket/manhattans/.completed_phenos/meta_1234
    let parts: Vec<&str> = output_base.rsplitn(3, '/').collect();
    if parts.len() < 3 {
        return Ok(()); // Can't determine checkpoint path
    }

    let id = parts[0];
    let ancestry = parts[1];
    let base_dir = parts[2];

    let marker_url = format!("{}/.completed_phenos/{}_{}", base_dir, ancestry, id);

    // Write the marker using the unified CloudWriter (uses global IO_RUNTIME)
    if genohype_core::io::is_cloud_path(&marker_url) {
        use genohype_core::io::CloudWriter;
        use std::io::Write;

        let mut writer = CloudWriter::new(&marker_url).map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create CloudWriter for marker: {}", e),
            ))
        })?;

        writer.write_all(b"")?;
        writer.finish()?;

        println!(
            "  Checkpoint: added {}_{} to .completed_phenos",
            ancestry, id
        );
    } else {
        // Local file system fallback
        let marker_dir = format!("{}/.completed_phenos", base_dir);
        std::fs::create_dir_all(&marker_dir)?;
        let marker_path = format!("{}/{}_{}", marker_dir, ancestry, id);
        std::fs::write(&marker_path, b"")?;
        println!(
            "  Checkpoint: added {}_{} to .completed_phenos",
            ancestry, id
        );
    }

    Ok(())
}
