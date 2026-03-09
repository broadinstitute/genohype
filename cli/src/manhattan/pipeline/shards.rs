//! Shard aggregation for distributed execution (`--from-shards` mode).
//!
//! This module handles aggregating distributed scan results from multiple worker shards
//! and rendering the final Manhattan plot from the combined data.

use crate::manhattan::data::{
    ManhattanSidecar, PlotPoint, SidecarChromosome, SidecarImage, SidecarThreshold, SidecarYAxis,
    SignificantHit,
};
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::render::ManhattanRenderer;
use crate::HailError;
use crate::Result;
use genohype_core::io::{get_file_size, is_cloud_path, range_read, StreamingCloudWriter};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs;
use std::io::Write;

/// Result of a distributed scan containing extracted plot points.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedScanResult {
    /// All extracted plot points from scanned partitions
    pub points: Vec<PlotPoint>,
    /// Total rows processed
    pub rows_processed: usize,
}

/// Aggregate distributed scan shards and render final Manhattan plot.
///
/// This function reads all `part-*.json` files from the shards directory,
/// combines the PlotPoints, and renders the final PNG with sidecar JSON.
pub fn aggregate_shards_and_render(
    shards_path: &str,
    output_prefix: &str,
    width: u32,
    height: u32,
    threshold: f64,
) -> Result<()> {
    println!("Aggregating shards from: {}", shards_path);

    // 1. List and read all shard files
    let shard_files = list_shard_files(shards_path)?;
    println!("Found {} shard files", shard_files.len());

    if shard_files.is_empty() {
        return Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No part-*.json files found in {}", shards_path),
        )));
    }

    // 2. Read and aggregate all PlotPoints
    let mut all_points: Vec<PlotPoint> = Vec::new();
    let mut total_rows: usize = 0;

    let pb = ProgressBar::new(shard_files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} shards")
            .unwrap()
            .progress_chars("#>-"),
    );

    for shard_file in &shard_files {
        let result = read_shard_file(shard_file)?;
        all_points.extend(result.points);
        total_rows += result.rows_processed;
        pb.inc(1);
    }
    pb.finish_with_message("done");

    println!(
        "Aggregated {} plot points from {} total rows",
        all_points.len(),
        total_rows
    );

    // 3. Build chromosome layout from the data
    let (layout, y_scale, max_neg_log_p) = build_layout_from_points(&all_points, width, height);

    // 4. Render the Manhattan plot
    let mut renderer = ManhattanRenderer::new(width, height);

    // Draw threshold line
    let threshold_y = y_scale.get_y(-threshold.log10());
    renderer.render_threshold_line(threshold_y, width);

    // Draw all points
    let mut significant_hits: Vec<SignificantHit> = Vec::new();

    for point in &all_points {
        // Normalize contig name (strip "chr" prefix)
        let contig_name = if point.contig.starts_with("chr") {
            &point.contig[3..]
        } else {
            &point.contig
        };

        if let Some(x) = layout.get_x(contig_name, point.position) {
            let y = y_scale.get_y(point.neg_log10_p);
            let color = layout.get_color(contig_name);
            renderer.render_point(x, y, color, 0.6);

            // Track significant hits
            if point.pvalue < threshold {
                significant_hits.push(SignificantHit {
                    variant_id: format!("{}:{}", point.contig, point.position),
                    pvalue: point.pvalue,
                    x_px: x,
                    y_px: y,
                    x_normalized: x / width as f32,
                    y_normalized: y / height as f32,
                    annotations: serde_json::Value::Null,
                });
            }
        }
    }

    // 5. Write output PNG
    let png_path = format!("{}.png", output_prefix);
    let png_data = renderer.encode_png()?;

    if is_cloud_path(&png_path) {
        let mut writer = StreamingCloudWriter::new(&png_path)?;
        writer.write_all(&png_data)?;
        writer.finish()?;
    } else {
        fs::write(&png_path, &png_data)?;
    }
    println!("Wrote PNG: {}", png_path);

    // 6. Write sidecar JSON
    let sidecar = ManhattanSidecar {
        image: SidecarImage { width, height },
        chromosomes: layout
            .chromosome_info
            .iter()
            .map(|c| SidecarChromosome {
                name: c.name.clone(),
                x_start_px: c.x_start_px,
                x_end_px: c.x_end_px,
                color: c.color.clone(),
            })
            .collect(),
        threshold: SidecarThreshold {
            pvalue: threshold,
            y_px: threshold_y,
        },
        y_axis: SidecarYAxis {
            log_threshold: 10.0,
            linear_fraction: 0.5,
            max_neg_log_p,
        },
        significant_hits,
    };

    let json_path = format!("{}.json", output_prefix);
    let json_data = serde_json::to_string_pretty(&sidecar).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to serialize sidecar: {}", e),
        ))
    })?;

    if is_cloud_path(&json_path) {
        let mut writer = StreamingCloudWriter::new(&json_path)?;
        writer.write_all(json_data.as_bytes())?;
        writer.finish()?;
    } else {
        fs::write(&json_path, &json_data)?;
    }
    println!("Wrote sidecar: {}", json_path);

    println!(
        "Manhattan plot complete: {} significant hits (p < {})",
        sidecar.significant_hits.len(),
        threshold
    );

    Ok(())
}

/// List all part-*.json files in a directory (supports cloud and local).
fn list_shard_files(dir_path: &str) -> Result<Vec<String>> {
    if is_cloud_path(dir_path) {
        // Use gsutil to list files
        let dir = dir_path.trim_end_matches('/');
        let output = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/part-*.json", dir)])
            .output()
            .map_err(|e| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to run gsutil: {}", e),
                ))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("gsutil ls failed: {}", stderr),
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let files: Vec<String> = stdout
            .lines()
            .filter(|l| l.ends_with(".json"))
            .map(|s| s.to_string())
            .collect();

        Ok(files)
    } else {
        // Local directory
        let mut files = Vec::new();
        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("part-") && name.ends_with(".json") {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

/// Read a single shard file and deserialize to DistributedScanResult.
pub fn read_shard_file(path: &str) -> Result<DistributedScanResult> {
    let data = if is_cloud_path(path) {
        // Read entire cloud file
        let file_size = get_file_size(path)?;
        range_read(path, 0, file_size as usize)?
    } else {
        fs::read(path)?
    };

    serde_json::from_slice(&data).map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to parse shard {}: {}", path, e),
        ))
    })
}

/// Build chromosome layout and Y scale from aggregated PlotPoints.
pub fn build_layout_from_points(
    points: &[PlotPoint],
    width: u32,
    height: u32,
) -> (ChromosomeLayout, YScale, f64) {
    // Collect chromosome extents from the data
    let mut chrom_extents: HashMap<String, (i32, i32)> = HashMap::new();

    for point in points {
        // Normalize contig name
        let contig = if point.contig.starts_with("chr") {
            point.contig[3..].to_string()
        } else {
            point.contig.clone()
        };

        let entry = chrom_extents.entry(contig).or_insert((i32::MAX, i32::MIN));
        entry.0 = entry.0.min(point.position);
        entry.1 = entry.1.max(point.position);
    }

    // Sort chromosomes in standard order
    let mut chroms: Vec<(String, u32)> = chrom_extents
        .into_iter()
        .map(|(name, (_, max))| (name, max as u32))
        .collect();

    chroms.sort_by(|a, b| {
        let a_num: Option<u32> = a.0.parse().ok();
        let b_num: Option<u32> = b.0.parse().ok();
        match (a_num, b_num) {
            (Some(an), Some(bn)) => an.cmp(&bn),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.0.cmp(&b.0),
        }
    });

    // Filter to standard chromosomes (1-22, X, Y)
    let chroms: Vec<(String, u32)> = chroms
        .into_iter()
        .filter(|(name, _)| {
            name.len() <= 2
                || name == "X"
                || name == "Y"
                || name == "MT"
                || name.parse::<u32>().is_ok()
        })
        .collect();

    let layout = ChromosomeLayout::new(&chroms, width, 4);

    // Find max -log10(p) for Y scale
    let max_neg_log_p = points
        .iter()
        .map(|p| p.neg_log10_p)
        .fold(0.0_f64, |a, b| a.max(b));

    let y_scale = YScale::new(height, max_neg_log_p.max(10.0));

    (layout, y_scale, max_neg_log_p)
}
