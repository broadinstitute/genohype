//! Manhattan plot handlers.
//!
//! Processes Manhattan scan (Phase 1) and aggregate (Phase 2) jobs.

use crate::distributed::message::{ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource};
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
    _cached_engine: Option<(String, QueryEngine)>,
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
            let _core_guard = telemetry.as_ref().map(|ts| CoreTaskGuard::partition(ts, partition_id));

            let engine = QueryEngine::open_path(table_path)?;
            let iter = engine.scan_partition_iter(partition_id, &[])?;

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
                        renderer.render_point_with_radius(x, y, color, style.point_alpha, style.point_radius);
                    }

                    // 2. Per-Chromosome Plot
                    // Use normalized contig name for file/map keys (e.g., "chr1")
                    let normalized_contig = normalize_contig_name(&point.contig);

                    // Look up length using short name (e.g., "1") because that's what we have in map
                    if let Some(&len) = contig_lengths.get(contig_for_layout).or_else(|| contig_lengths.get(&normalized_contig)) {
                        // Initialize renderer and layout for this chromosome if needed
                        let chrom_layout = chrom_layouts.entry(normalized_contig.clone()).or_insert_with(|| {
                            // Create a layout where this single chromosome fills the width
                            ChromosomeLayout::new(&[(contig_for_layout.to_string(), len)], width, 0)
                        });

                        let chrom_renderer = chrom_renderers.entry(normalized_contig.clone()).or_insert_with(|| {
                            ManhattanRenderer::new_transparent(width, height)
                        });

                        if let Some(x) = chrom_layout.get_x(contig_for_layout, point.position) {
                            let y = y_scale.get_y(point.neg_log10_p);
                            // Use same color scheme as WG plot
                            let color = layout.get_color(contig_for_layout);
                            chrom_renderer.render_point_with_radius(x, y, color, style.point_alpha, style.point_radius);
                        }
                    }

                    // Check significance threshold
                    if point.pvalue < threshold {
                        // Extract additional fields for significant hit
                        let (ref_allele, alt_allele, beta, se, af) =
                            extract_sig_hit_fields(&row);

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
                let chrom_path = format!("{}/chroms/{}/{}/part-{:05}.png", root, chrom, source_name, partition_id);

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

    Ok((total_rows, None))
}


/// Extract fields for a significant hit from an encoded row.
pub fn extract_sig_hit_fields(row: &genohype_core::codec::EncodedValue) -> (String, String, Option<f64>, Option<f64>, Option<f64>) {
    use genohype_core::codec::EncodedValue;

    // Helper to get nested field
    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
        let mut current = value;
        for &field_name in path {
            if let EncodedValue::Struct(fields) = current {
                current = fields.iter().find(|(n, _)| n == field_name).map(|(_, v)| v)?;
            } else {
                return None;
            }
        }
        Some(current)
    }

    // Extract alleles
    let (ref_allele, alt_allele) = if let Some(EncodedValue::Array(alleles)) = get_field(row, &["alleles"]) {
        let ref_a = alleles.first()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let alt_a = alleles.get(1)
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        (ref_a, alt_a)
    } else {
        (String::new(), String::new())
    };

    // Extract BETA
    let beta = get_field(row, &["BETA"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    // Extract SE
    let se = get_field(row, &["SE"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    // Extract AF (AF_Allele2)
    let af = get_field(row, &["AF_Allele2"])
        .and_then(|v| match v {
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

/// Verify expected outputs exist and append to checkpoint file.
#[allow(unused_variables)]
fn verify_and_checkpoint(spec: &ManhattanAggregateSpec) -> Result<()> {
    // Build list of expected files
    // Note: significant.parquet files are only created when there are significant hits,
    // so we don't require them here. The manifest.json and PNG files are always created.
    let mut expected = vec!["manifest.json"];
    if spec.exome_results.is_some() {
        expected.push("exome_manhattan.png");
    }
    if spec.genome_results.is_some() {
        expected.push("genome_manhattan.png");
    }

    let url = url::Url::parse(&spec.output_path).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid output URL: {}", e),
        ))
    })?;

    #[cfg(feature = "gcp")]
    use object_store::{ObjectStore, path::Path as ObjPath};

    #[cfg(feature = "gcp")]
    let store: std::sync::Arc<dyn ObjectStore> = {
        let bucket = url.host_str().ok_or_else(|| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing bucket in GCS URL",
            ))
        })?;
        genohype_core::io::get_gcs_client(bucket)?
    };

    #[cfg(not(feature = "gcp"))]
    return Ok(()); // Can't verify without GCS support

    #[cfg(feature = "gcp")]
    {
        let base_path = url.path().trim_start_matches('/');

        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            crate::HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Verify all expected files exist
        for file in &expected {
            let file_path = ObjPath::from(format!("{}/{}", base_path, file));
            rt.block_on(async {
                store.head(&file_path).await
            }).map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Missing expected output {}: {}", file, e),
                ))
            })?;
        }

        // Derive checkpoint path and relative phenotype path
        // output_path: gs://bucket/manhattans/meta/1234
        // checkpoint:  gs://bucket/manhattans/.completed
        // rel_path:    meta/1234
        let parts: Vec<&str> = base_path.rsplitn(3, '/').collect();
        if parts.len() < 3 {
            return Ok(()); // Can't determine checkpoint path
        }

        let phenotype_rel = format!("{}/{}", parts[1], parts[0]); // ancestry/id
        let base_dir = parts[2]; // everything before ancestry
        let checkpoint_path = ObjPath::from(format!("{}/.completed", base_dir));

        // Append to checkpoint file (read-modify-write with newline)
        let append_content = format!("{}\n", phenotype_rel);

        rt.block_on(async {
            // Try to read existing content
            let existing = match store.get(&checkpoint_path).await {
                Ok(result) => {
                    let bytes = result.bytes().await.unwrap_or_default();
                    String::from_utf8_lossy(&bytes).to_string()
                }
                Err(_) => String::new(),
            };

            // Check if already in checkpoint (idempotent)
            if existing.lines().any(|line| line.trim() == phenotype_rel) {
                return Ok::<(), object_store::Error>(());
            }

            // Append and write back
            let new_content = format!("{}{}", existing, append_content);
            store.put(&checkpoint_path, new_content.into()).await?;
            Ok(())
        }).map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to update checkpoint: {}", e),
            ))
        })?;

        println!("  Checkpoint: added {}", phenotype_rel);
        Ok(())
    }
}
