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

fn canonical_scan_layout(width: u32, height: u32) -> Result<genohype_association::GenomeLayoutV1> {
    genohype_association::GenomeLayoutV1::new(
        genohype_association::canonical_contig_lengths(genohype_association::Assembly::Grch38),
        width,
        height,
        4,
    )
    .map_err(|error| crate::HailError::InvalidFormat(error.to_string()))
}

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

    // Whole-genome shards use the exact canonical geometry later carried by
    // the V1 manifest. Legacy wire fields remain for compatibility products.
    let canonical_layout = canonical_scan_layout(spec.width, spec.height)?;
    let compatibility_layout = &spec.layout;
    let compatibility_y_scale = &spec.y_scale;
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
                let rendered =
                    if let Some(x) = canonical_layout.x_for(&point.contig, point.position) {
                        let y = canonical_layout.y_scale.y_for(point.neg_log10_p);
                        let color = canonical_layout.color_for(&point.contig).ok_or_else(|| {
                            crate::HailError::InvalidFormat(format!(
                                "canonical Manhattan layout has no color for {:?}",
                                point.contig
                            ))
                        })?;
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
                        let y = compatibility_y_scale.get_y(point.neg_log10_p);
                        let color = compatibility_layout.get_color(contig_for_layout);
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
            let raster_basename = format!("part-{partition_id:05}.png");
            let receipt = reduction.finish_with_finalized_raster(raster_basename, &png_data)?;
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

fn completion_marker_url(spec: &ManhattanAggregateSpec) -> Result<String> {
    let output_base = spec.output_path.trim_end_matches('/');
    let parts: Vec<&str> = output_base.rsplitn(3, '/').collect();
    if parts.len() < 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(crate::HailError::InvalidFormat(format!(
            "cannot derive completion checkpoint from output path {output_base:?}"
        )));
    }
    Ok(format!(
        "{}/.completed_phenos/{}_{}",
        parts[2], parts[1], parts[0]
    ))
}

fn clear_completion_marker(spec: &ManhattanAggregateSpec) -> Result<()> {
    let marker = completion_marker_url(spec)?;
    if !genohype_core::io::is_cloud_path(&marker) {
        return match std::fs::remove_file(&marker) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        };
    }

    #[cfg(feature = "gcp")]
    {
        use object_store::path::Path as ObjectPath;
        use object_store::ObjectStore;
        let url = url::Url::parse(&marker)
            .map_err(|error| crate::HailError::InvalidFormat(error.to_string()))?;
        if url.scheme() != "gs" {
            return Err(crate::HailError::InvalidFormat(format!(
                "unsupported completion marker URL: {marker}"
            )));
        }
        let bucket = url.host_str().ok_or_else(|| {
            crate::HailError::InvalidFormat(format!("completion marker has no bucket: {marker}"))
        })?;
        let location = ObjectPath::from(url.path().trim_start_matches('/'));
        let store = genohype_core::io::get_gcs_client(bucket)?;
        let runtime = tokio::runtime::Runtime::new()?;
        match runtime.block_on(store.delete(&location)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(crate::HailError::ObjectStore(error)),
        }
    }
    #[cfg(not(feature = "gcp"))]
    {
        Err(crate::HailError::InvalidFormat(
            "cloud completion marker removal requires the gcp feature".into(),
        ))
    }
}

fn verify_compatibility_significant_hits(
    output_base: &str,
    source: &str,
    entry: Option<&crate::manhattan::data::ManifestSigHits>,
    canonical: &genohype_association::AssociationVisualizationManifestV1,
    threshold: f64,
    expected_phenotype: &str,
    expected_ancestry: &str,
) -> Result<()> {
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};

    let expected_count = canonical.significant_hit_count;
    let expected_path = format!("{output_base}/{source}_significant.parquet");
    if expected_count == 0 {
        if entry.is_some() {
            return Err(crate::HailError::InvalidFormat(format!(
                "compatibility significant-hit entry for {source} must be absent when the canonical count is zero"
            )));
        }
        let batches = crate::manhattan::aggregate::io::read_parquet_file(&expected_path).map_err(
            |error| {
                crate::HailError::InvalidFormat(format!(
                    "invalid zero-hit compatibility Parquet {expected_path}: {error}"
                ))
            },
        )?;
        let expected_schema = crate::manhattan::sig_writer::sig_hit_schema();
        if batches
            .iter()
            .any(|batch| batch.schema().as_ref() != &expected_schema || batch.num_rows() != 0)
        {
            return Err(crate::HailError::InvalidFormat(format!(
                "zero-hit compatibility Parquet for {source} is stale or invalid"
            )));
        }
        return Ok(());
    }

    let entry = entry.ok_or_else(|| {
        crate::HailError::InvalidFormat(format!(
            "compatibility manifest is missing {source} significant hits for canonical count {expected_count}"
        ))
    })?;
    let canonical_top = canonical.top_hits.first().ok_or_else(|| {
        crate::HailError::InvalidFormat(format!(
            "canonical {source} manifest has significant hits but no top hit"
        ))
    })?;
    let canonical_id = format!(
        "{}:{}:{}:{}",
        crate::manhattan::reference::normalize_contig_name(&canonical_top.contig),
        canonical_top.position,
        canonical_top.ref_allele.as_deref().unwrap_or_default(),
        canonical_top.alt_allele.as_deref().unwrap_or_default()
    );
    if canonical_top.pvalue >= threshold {
        return Err(crate::HailError::InvalidFormat(format!(
            "canonical {source} top hit is not significant at the aggregate threshold"
        )));
    }
    if entry.path != expected_path || entry.count != expected_count {
        return Err(crate::HailError::InvalidFormat(format!(
            "compatibility {source} significant-hit path/count does not match the canonical manifest"
        )));
    }

    let batches =
        crate::manhattan::aggregate::io::read_parquet_file(&entry.path).map_err(|error| {
            crate::HailError::InvalidFormat(format!(
                "invalid compatibility significant-hit Parquet {}: {error}",
                entry.path
            ))
        })?;
    let expected_schema = crate::manhattan::sig_writer::sig_hit_schema();
    let mut actual_count = 0_u64;
    let mut canonical_top_present = false;
    for batch in &batches {
        if batch.schema().as_ref() != &expected_schema
            || expected_schema
                .fields()
                .iter()
                .enumerate()
                .any(|(index, field)| !field.is_nullable() && batch.column(index).null_count() != 0)
        {
            return Err(crate::HailError::InvalidFormat(format!(
                "compatibility {source} significant-hit Parquet has the wrong schema or nullability"
            )));
        }
        actual_count = actual_count
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit row count overflowed".into(),
                )
            })?;
        let phenotype_column = batch
            .column_by_name("phenotype")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit phenotype column is invalid".into(),
                )
            })?;
        let ancestry_column = batch
            .column_by_name("ancestry")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit ancestry column is invalid".into(),
                )
            })?;
        let source_column = batch
            .column_by_name("sequencing_type")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit sequencing_type column is invalid".into(),
                )
            })?;
        let pvalue_column = batch
            .column_by_name("pvalue")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit pvalue column is invalid".into(),
                )
            })?;
        let xpos_column = batch
            .column_by_name("xpos")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit xpos column is invalid".into(),
                )
            })?;
        let contig_column = batch
            .column_by_name("contig")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit contig column is invalid".into(),
                )
            })?;
        let position_column = batch
            .column_by_name("position")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit position column is invalid".into(),
                )
            })?;
        let ref_column = batch
            .column_by_name("ref")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit ref column is invalid".into(),
                )
            })?;
        let alt_column = batch
            .column_by_name("alt")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                crate::HailError::InvalidFormat(
                    "compatibility significant-hit alt column is invalid".into(),
                )
            })?;
        for row in 0..batch.num_rows() {
            let contig = contig_column.value(row);
            let position = position_column.value(row);
            let canonical_contig = genohype_association::canonical_contig(contig);
            if phenotype_column.value(row) != expected_phenotype
                || ancestry_column.value(row) != expected_ancestry
                || source_column.value(row) != source
                || canonical_contig.is_none()
                || crate::manhattan::reference::normalize_contig_name(
                    canonical_contig.as_deref().unwrap_or_default(),
                ) != contig
                || position <= 0
                || xpos_column.value(row)
                    != crate::manhattan::reference::calculate_xpos(contig, position)
                || pvalue_column.is_null(row)
                || !pvalue_column.value(row).is_finite()
                || pvalue_column.value(row) < 0.0
                || pvalue_column.value(row) > 1.0
                || pvalue_column.value(row) >= threshold
            {
                return Err(crate::HailError::InvalidFormat(format!(
                    "compatibility {source} significant-hit Parquet contains inconsistent row content"
                )));
            }
            let row_id = format!(
                "{}:{}:{}:{}",
                contig,
                position,
                ref_column.value(row),
                alt_column.value(row)
            );
            if row_id == canonical_id
                && pvalue_column.value(row).to_bits() == canonical_top.pvalue.to_bits()
            {
                canonical_top_present = true;
            }
        }
    }
    if actual_count != expected_count {
        return Err(crate::HailError::InvalidFormat(format!(
            "compatibility {source} significant-hit Parquet has {actual_count} rows, expected {expected_count}"
        )));
    }

    if !canonical_top_present {
        return Err(crate::HailError::InvalidFormat(format!(
            "compatibility {source} significant-hit Parquet does not contain the canonical top hit"
        )));
    }
    let compatibility_top = entry.top_hit.as_ref().ok_or_else(|| {
        crate::HailError::InvalidFormat(format!(
            "compatibility {source} significant-hit metadata has no top hit"
        ))
    })?;
    if compatibility_top.id != canonical_id
        || compatibility_top.pvalue.to_bits() != canonical_top.pvalue.to_bits()
        || compatibility_top.gene.is_some()
        || compatibility_top.consequence.is_some()
    {
        return Err(crate::HailError::InvalidFormat(format!(
            "compatibility {source} top-hit metadata does not match its Parquet content"
        )));
    }

    Ok(())
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

    if !spec.gene_associations_only {
        clear_completion_marker(spec)?;
    }
    let (rows, summary) = run_aggregation(spec)?;

    // The narrow gene export intentionally has no Manhattan manifest/checkpoint.
    if !spec.gene_associations_only {
        // Canonical publication and the completion marker are one fail-closed contract.
        verify_and_checkpoint(spec)?;
    }

    Ok((rows, summary))
}

/// Verify the compatibility inventory and every canonical raster manifest before
/// writing the completion marker. Any missing, malformed, or mismatched resource
/// fails the aggregate task and leaves completion unmarked.
pub fn verify_and_checkpoint(spec: &ManhattanAggregateSpec) -> Result<()> {
    use crate::manhattan::data::Manifest;
    use genohype_association::{
        canonical_contig_lengths, Assembly, AssociationVisualizationManifestV1, GenomeLayoutV1,
        PlotKindV1,
    };
    use sha2::{Digest, Sha256};

    fn read_all(path: &str) -> Result<Vec<u8>> {
        if genohype_core::io::is_cloud_path(path) {
            let size = genohype_core::io::get_file_size(path)?;
            let size = usize::try_from(size).map_err(|_| {
                crate::HailError::InvalidFormat(format!("output is too large to verify: {path}"))
            })?;
            genohype_core::io::range_read(path, 0, size)
        } else {
            std::fs::read(path).map_err(|error| {
                crate::HailError::Io(std::io::Error::new(
                    error.kind(),
                    format!("failed to read required output {path}: {error}"),
                ))
            })
        }
    }

    // Also invalidate when this verifier is called directly against an existing run.
    clear_completion_marker(spec)?;

    let output_base = spec.output_path.trim_end_matches('/');
    let root_manifest_path = format!("{output_base}/manifest.json");
    let root_manifest_bytes = read_all(&root_manifest_path)?;
    let root: Manifest = serde_json::from_slice(&root_manifest_bytes).map_err(|error| {
        crate::HailError::InvalidFormat(format!(
            "invalid compatibility manifest {root_manifest_path}: {error}"
        ))
    })?;
    if root.inputs.exome_results != spec.exome_results
        || root.inputs.genome_results != spec.genome_results
        || root.inputs.gene_burden != spec.gene_burden
        || spec
            .phenotype_id
            .as_ref()
            .is_some_and(|phenotype| root.phenotype != *phenotype)
        || spec
            .ancestry
            .as_ref()
            .is_some_and(|ancestry| root.ancestry.as_ref() != Some(ancestry))
        || root.manhattans.exome.is_some() != spec.exome_results.is_some()
        || root.manhattans.genome.is_some() != spec.genome_results.is_some()
        || root.manhattans.gene.is_some() != spec.gene_burden.is_some()
        || (spec.exome_results.is_none() && root.significant_hits.exome.is_some())
        || (spec.genome_results.is_none() && root.significant_hits.genome.is_some())
        || (spec.gene_burden.is_none() && root.significant_hits.gene.is_some())
    {
        return Err(crate::HailError::InvalidFormat(format!(
            "compatibility manifest source inventory mismatch: {root_manifest_path}"
        )));
    }

    let verify_png = |path: &str, expected_width: u32, expected_height: u32| -> Result<Vec<u8>> {
        let bytes = read_all(path)?;
        let pixmap = tiny_skia::Pixmap::decode_png(&bytes).map_err(|error| {
            crate::HailError::InvalidFormat(format!("invalid PNG {path}: {error}"))
        })?;
        if pixmap.width() != expected_width || pixmap.height() != expected_height {
            return Err(crate::HailError::InvalidFormat(format!(
                "PNG dimensions mismatch for {path}: {}x{}, expected {}x{}",
                pixmap.width(),
                pixmap.height(),
                expected_width,
                expected_height
            )));
        }
        Ok(bytes)
    };

    let expected_layout = GenomeLayoutV1::new(
        canonical_contig_lengths(Assembly::Grch38),
        spec.width,
        spec.height,
        4,
    )
    .map_err(|error| crate::HailError::InvalidFormat(error.to_string()))?;

    for source in [
        spec.exome_results.as_ref().map(|_| "exome"),
        spec.genome_results.as_ref().map(|_| "genome"),
    ]
    .into_iter()
    .flatten()
    {
        let plot_id = format!("{source}_manhattan");
        let raster_name = format!("{plot_id}.png");
        let manifest_path = format!("{output_base}/plots/{plot_id}.manifest.json");
        let manifest_bytes = read_all(&manifest_path)?;
        let manifest: AssociationVisualizationManifestV1 = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| {
                crate::HailError::InvalidFormat(format!(
                    "invalid canonical manifest {manifest_path}: {error}"
                ))
            })?;
        manifest.validate().map_err(|error| {
            crate::HailError::InvalidFormat(format!(
                "invalid canonical manifest {manifest_path}: {error}"
            ))
        })?;
        if manifest.plot_id != plot_id
            || manifest.kind != PlotKindV1::Manhattan
            || manifest.assembly != "GRCh38"
            || manifest.layout != expected_layout
            || manifest.resources.raster.url != raster_name
            || manifest.thresholds.len() != 1
            || manifest.thresholds[0].name != "genome_wide_significance"
            || manifest.thresholds[0].pvalue.to_bits() != spec.threshold.to_bits()
        {
            return Err(crate::HailError::InvalidFormat(format!(
                "canonical manifest identity or threshold mismatch: {manifest_path}"
            )));
        }
        let compatibility_entry = match source {
            "exome" => root.manhattans.exome.as_ref(),
            "genome" => root.manhattans.genome.as_ref(),
            _ => unreachable!(),
        }
        .ok_or_else(|| {
            crate::HailError::InvalidFormat(format!(
                "compatibility manifest is missing {source} Manhattan entry"
            ))
        })?;
        let expected_compatibility_png = format!("{output_base}/plots/{raster_name}");
        if compatibility_entry.png != expected_compatibility_png
            || compatibility_entry.count != manifest.rendered_point_count
        {
            return Err(crate::HailError::InvalidFormat(format!(
                "compatibility {source} raster/count does not match canonical manifest"
            )));
        }
        if manifest.resources.raster.media_type != "image/png" {
            return Err(crate::HailError::InvalidFormat(format!(
                "canonical raster media type is not image/png: {manifest_path}"
            )));
        }

        let fingerprint = manifest
            .source_fingerprint
            .strip_prefix("sha256:")
            .unwrap_or_default();
        if fingerprint.len() != 64
            || !fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(crate::HailError::InvalidFormat(format!(
                "invalid source fingerprint in {manifest_path}"
            )));
        }

        let raster_path = format!("{output_base}/plots/{raster_name}");
        let raster = verify_png(&raster_path, manifest.layout.width, manifest.layout.height)?;
        let actual_size = u64::try_from(raster.len()).map_err(|_| {
            crate::HailError::InvalidFormat(format!("raster is too large: {raster_path}"))
        })?;
        let actual_checksum = format!("sha256:{:x}", Sha256::digest(&raster));
        if manifest.resources.raster.byte_size != Some(actual_size)
            || manifest.resources.raster.checksum.as_deref() != Some(actual_checksum.as_str())
        {
            return Err(crate::HailError::InvalidFormat(format!(
                "canonical raster size or checksum mismatch: {raster_path}"
            )));
        }

        let compatibility_significant_hits = match source {
            "exome" => root.significant_hits.exome.as_ref(),
            "genome" => root.significant_hits.genome.as_ref(),
            _ => unreachable!(),
        };
        verify_compatibility_significant_hits(
            output_base,
            source,
            compatibility_significant_hits,
            &manifest,
            spec.threshold,
            spec.phenotype_id.as_deref().unwrap_or(&root.phenotype),
            spec.ancestry
                .as_deref()
                .or(root.ancestry.as_deref())
                .unwrap_or("meta"),
        )?;
    }

    // Validate every remaining compatibility artifact referenced by the root.
    if let Some(gene) = root.manhattans.gene.as_ref() {
        let expected = format!("{output_base}/plots/gene_manhattan.png");
        if gene.png != expected {
            return Err(crate::HailError::InvalidFormat(
                "compatibility gene Manhattan path mismatch".into(),
            ));
        }
        verify_png(&gene.png, spec.width, spec.height)?;
    }
    for (chrom, entries) in &root.chrom_manhattans {
        for (source, entry, enabled) in [
            (
                "exome",
                entries.exome.as_ref(),
                spec.exome_results.is_some(),
            ),
            (
                "genome",
                entries.genome.as_ref(),
                spec.genome_results.is_some(),
            ),
            ("gene", entries.gene.as_ref(), spec.gene_burden.is_some()),
        ] {
            if let Some(entry) = entry {
                if !enabled || entry.count != 0 {
                    return Err(crate::HailError::InvalidFormat(format!(
                        "invalid compatibility chromosome inventory for {chrom}/{source}"
                    )));
                }
                let expected = format!("{output_base}/chroms/{chrom}/{source}_manhattan.png");
                if entry.png != expected {
                    return Err(crate::HailError::InvalidFormat(format!(
                        "compatibility chromosome raster path mismatch: {}",
                        entry.png
                    )));
                }
                verify_png(&entry.png, spec.width, spec.height)?;
            }
        }
    }
    // Variant significant-hit entries were checked exactly against their canonical
    // manifests and Parquet content above. Preserve the existing gene compatibility
    // behavior because Phase 1 does not publish a canonical gene manifest.
    if let Some(hit) = root.significant_hits.gene.as_ref() {
        if spec.gene_burden.is_none() || hit.count == 0 {
            return Err(crate::HailError::InvalidFormat(
                "invalid compatibility significant-hit inventory for gene".into(),
            ));
        }
        read_all(&hit.path)?;
    }
    for locus in &root.loci {
        if let Some(plot) = locus.plot.as_deref() {
            verify_png(plot, spec.width, spec.height)?;
        }
    }

    let marker_url = completion_marker_url(spec)?;
    let marker_name = marker_url.rsplit('/').next().unwrap_or_default();

    if genohype_core::io::is_cloud_path(&marker_url) {
        use genohype_core::io::CloudWriter;
        let mut writer = CloudWriter::new(&marker_url).map_err(|error| {
            crate::HailError::Io(std::io::Error::other(format!(
                "failed to create completion marker: {error}"
            )))
        })?;
        writer.write_all(b"")?;
        writer.finish()?;
    } else {
        let marker_path = std::path::Path::new(&marker_url);
        let marker_dir = marker_path.parent().ok_or_else(|| {
            crate::HailError::InvalidFormat(format!(
                "completion marker has no parent: {marker_url}"
            ))
        })?;
        std::fs::create_dir_all(marker_dir)?;
        std::fs::write(marker_path, b"")?;
    }
    println!("  Checkpoint: added {marker_name} to .completed_phenos");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::canonical_scan_layout;

    #[test]
    fn distributed_whole_genome_geometry_uses_canonical_one_based_boundaries() {
        let layout = canonical_scan_layout(1_000, 400).unwrap();
        let contig = layout
            .contigs
            .iter()
            .find(|contig| contig.name == "1")
            .unwrap();
        assert_eq!(layout.x_for("chr1", 1), Some(contig.x_start as f32));
        let expected_end = contig.x_start
            + (contig.length - 1) as f64 / contig.length as f64 * (contig.x_end - contig.x_start);
        assert_eq!(
            layout.x_for("1", contig.length as i64),
            Some(expected_end as f32)
        );
        assert_eq!(layout.x_for("1", 0), None);
        assert_eq!(layout.x_for("1", contig.length as i64 + 1), None);
    }
}
