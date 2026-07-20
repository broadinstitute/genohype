//! Manhattan plot handlers.
//!
//! Processes Manhattan scan (Phase 1) and aggregate (Phase 2) jobs.

use crate::distributed::message::{
    CoreTaskInfo, ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource,
};
use crate::distributed::worker::telemetry::{CoreTaskGuard, TelemetryState};
use crate::Result;
use genohype_core::query::QueryEngine;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Process partitions for Manhattan scan phase (V2 pipeline).
///
/// This is the Phase 1 worker task. For each partition, it:
/// 1. Renders plot points to a partial PNG (transparent background)
/// 2. Writes significant hits (below threshold) to a Parquet file
///
/// Output structure:
/// - {output_path}/{source}/part-{id}.png
/// - {output_path}/{source}/part-{id}-sig.parquet
pub fn process_manhattan_scan_v2(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    spec: &ManhattanScanSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::manhattan::data::{extract_plot_data, SigHitRow};
    use crate::manhattan::layout::ChromosomeLayout;
    use crate::manhattan::reference::{calculate_xpos, normalize_contig_name};
    use crate::manhattan::render::ManhattanRenderer;
    use crate::manhattan::sig_writer::SigHitWriter;
    use genohype_core::io::{is_cloud_path, CloudWriter, StreamingCloudWriter};
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

            // Each thread has its own renderer with transparent background
            let mut renderer = ManhattanRenderer::new_transparent(width, height);

            // Per-chromosome renderers (lazy init)
            let mut chrom_renderers: HashMap<String, ManhattanRenderer> = HashMap::new();
            // Per-chromosome layouts (lazy init)
            let mut chrom_layouts: HashMap<String, ChromosomeLayout> = HashMap::new();

            let mut rows = 0usize;

            // Collect significant hits for this partition
            let mut sig_hits: Vec<SigHitRow> = Vec::new();

            for row_result in iter {
                let row = row_result?;
                rows += 1;

                if let Some(point) = extract_plot_data(&row, y_field) {
                    // For layout lookups, strip "chr" prefix if present
                    let contig_for_layout = if point.contig.starts_with("chr") {
                        &point.contig[3..]
                    } else {
                        &point.contig
                    };

                    // 1. Whole Genome Plot: Map to pixel coordinates and render
                    if let Some(x) = layout.get_x(contig_for_layout, point.position) {
                        let y = y_scale.get_y(point.neg_log10_p);
                        let color = layout.get_color(contig_for_layout);
                        renderer.render_point_with_radius(
                            x,
                            y,
                            color,
                            style.point_alpha,
                            style.point_radius,
                        );
                    }

                    // 2. Per-Chromosome Plot
                    // Use normalized contig name for file/map keys (e.g., "chr1")
                    let normalized_contig = normalize_contig_name(&point.contig);

                    // Look up length using short name (e.g., "1") because that's what we have in map
                    if let Some(&len) = contig_lengths
                        .get(contig_for_layout)
                        .or_else(|| contig_lengths.get(&normalized_contig))
                    {
                        // Initialize renderer and layout for this chromosome if needed
                        let chrom_layout = chrom_layouts
                            .entry(normalized_contig.clone())
                            .or_insert_with(|| {
                                // Create a layout where this single chromosome fills the width
                                ChromosomeLayout::new(
                                    &[(contig_for_layout.to_string(), len)],
                                    width,
                                    0,
                                )
                            });

                        let chrom_renderer = chrom_renderers
                            .entry(normalized_contig.clone())
                            .or_insert_with(|| ManhattanRenderer::new_transparent(width, height));

                        if let Some(x) = chrom_layout.get_x(contig_for_layout, point.position) {
                            let y = y_scale.get_y(point.neg_log10_p);
                            // Use same color scheme as WG plot
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

                    // Check significance threshold
                    if point.pvalue < threshold {
                        // Extract additional fields for significant hit
                        let (ref_allele, alt_allele, beta, se, af) = extract_sig_hit_fields(&row);

                        // Normalize contig to chr-prefixed format for output
                        let contig_normalized = normalize_contig_name(&point.contig);
                        // Calculate xpos for efficient ordering
                        let xpos = calculate_xpos(&point.contig, point.position);

                        sig_hits.push(SigHitRow {
                            phenotype: phenotype.clone(),
                            ancestry: ancestry.clone(),
                            sequencing_type: sequencing_type.clone(),
                            contig: contig_normalized,
                            position: point.position,
                            ref_allele,
                            alt_allele,
                            xpos,
                            pvalue: point.pvalue,
                            beta,
                            se,
                            af,
                            // Case/control and association fields not available in distributed scan phase
                            ac_cases: None,
                            ac_controls: None,
                            af_cases: None,
                            af_controls: None,
                            association_ac: None,
                        });
                    }
                }
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

            // Write significant hits Parquet (even if empty - consistent output)
            let sig_file = format!("{}/part-{:05}-sig.parquet", output_base, partition_id);
            if is_cloud_path(&sig_file) {
                let cloud_writer = CloudWriter::new(&sig_file)?;
                let mut writer = SigHitWriter::from_writer(cloud_writer)?;
                for hit in sig_hits {
                    writer.write(hit)?;
                }
                // Get back the cloud writer and finish to trigger the upload
                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
            } else {
                let mut writer = SigHitWriter::new(&sig_file)?;
                for hit in sig_hits {
                    writer.write(hit)?;
                }
                writer.finish()?;
            }

            Ok(rows)
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

    // Verify expected outputs exist and write to checkpoint
    if let Err(e) = verify_and_checkpoint(spec) {
        // Log but don't fail - aggregation succeeded
        eprintln!("Warning: checkpoint update failed: {}", e);
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
