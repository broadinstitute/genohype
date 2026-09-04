//! Image compositing utilities for partial PNGs.
//!
//! This module handles the final compositing step in distributed Manhattan plot rendering,
//! where multiple worker-rendered partial images are combined into a single final image.

use crate::manhattan::config::BackgroundStyle;
use crate::manhattan::layout::YScale;
use crate::manhattan::render::hex_to_color;
use crate::HailError;
use crate::Result;
use genohype_core::io::{get_file_size, is_cloud_path, range_read, StreamingCloudWriter};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::fs;
use std::io::Write;
use std::time::Instant;
use tiny_skia::Pixmap;

/// Incremental compositor for receipt-verified canonical shards.
///
/// Callers pass the exact byte slice they verified. Each shard is decoded and
/// blended immediately, so no mutable path is reread and memory is one final
/// canvas plus one shard.
pub(crate) struct VerifiedPngCompositor {
    canvas: Pixmap,
    width: u32,
    height: u32,
}

impl VerifiedPngCompositor {
    pub(crate) fn new(width: u32, height: u32, background: &BackgroundStyle) -> Result<Self> {
        let mut canvas = Pixmap::new(width, height).ok_or_else(|| {
            HailError::Io(std::io::Error::other("Failed to create canvas pixmap"))
        })?;
        match background {
            BackgroundStyle::Transparent => {}
            BackgroundStyle::White => canvas.fill(tiny_skia::Color::WHITE),
            BackgroundStyle::Color(hex) => canvas.fill(hex_to_color(hex, 1.0)),
        }
        Ok(Self {
            canvas,
            width,
            height,
        })
    }

    pub(crate) fn composite_verified_bytes(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        let partial = Pixmap::decode_png(bytes).map_err(|error| {
            HailError::InvalidFormat(format!("failed to decode verified PNG {name}: {error}"))
        })?;
        if partial.width() != self.width || partial.height() != self.height {
            return Err(HailError::InvalidFormat(format!(
                "Manhattan partial PNG {name} has dimensions {}x{}, expected {}x{}",
                partial.width(),
                partial.height(),
                self.width,
                self.height
            )));
        }
        self.canvas.draw_pixmap(
            0,
            0,
            partial.as_ref(),
            &tiny_skia::PixmapPaint::default(),
            tiny_skia::Transform::identity(),
            None,
        );
        Ok(())
    }

    /// Draw the threshold, encode, and write the exact returned bytes.
    pub(crate) fn finish(mut self, output_path: &str, threshold: f64) -> Result<Vec<u8>> {
        draw_threshold_line_on_pixmap(&mut self.canvas, self.width, threshold);
        let png_data = self.canvas.encode_png().map_err(|error| {
            HailError::Io(std::io::Error::other(format!(
                "Failed to encode PNG: {error}"
            )))
        })?;
        if is_cloud_path(output_path) {
            let mut writer = StreamingCloudWriter::new(output_path)?;
            writer.write_all(&png_data)?;
            writer.finish()?;
        } else {
            fs::write(output_path, &png_data)?;
        }
        Ok(png_data)
    }
}

/// Verify that the canonical shard directory has exactly the expected PNG names
/// without retaining an O(partition count) inventory.
pub(crate) fn verify_exact_partition_png_inventory(
    parts_dir: &str,
    expected_partition_count: usize,
) -> Result<()> {
    if is_cloud_path(parts_dir) {
        return verify_exact_cloud_partition_png_inventory(parts_dir, expected_partition_count);
    }

    let mut count = 0usize;
    for entry in fs::read_dir(parts_dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !name.starts_with("part-") || !name.ends_with(".png") {
            continue;
        }
        validate_partition_png_name(&name, expected_partition_count)?;
        count += 1;
    }
    if count != expected_partition_count {
        return Err(HailError::InvalidFormat(format!(
            "expected {expected_partition_count} Manhattan partial PNGs, found {count}"
        )));
    }
    Ok(())
}

fn validate_partition_png_name(name: &str, expected_partition_count: usize) -> Result<()> {
    let index = name
        .strip_prefix("part-")
        .and_then(|name| name.strip_suffix(".png"))
        .and_then(|index| index.parse::<usize>().ok())
        .filter(|index| name == format!("part-{index:05}.png"))
        .ok_or_else(|| {
            HailError::InvalidFormat(format!("unexpected Manhattan partial PNG {name:?}"))
        })?;
    if index >= expected_partition_count {
        return Err(HailError::InvalidFormat(format!(
            "unexpected Manhattan partial PNG {name:?}"
        )));
    }
    Ok(())
}

fn verify_exact_cloud_partition_png_inventory(
    parts_dir: &str,
    expected_partition_count: usize,
) -> Result<()> {
    use futures::StreamExt;
    use object_store::path::Path as ObjectPath;
    use object_store::ObjectStore;
    use std::sync::Arc;

    let url = url::Url::parse(parts_dir)
        .map_err(|error| HailError::InvalidFormat(format!("invalid cloud URL: {error}")))?;
    let bucket = url
        .host_str()
        .ok_or_else(|| HailError::InvalidFormat("cloud URL has no bucket".into()))?;
    let prefix = ObjectPath::from(format!(
        "{}/",
        url.path().trim_start_matches('/').trim_end_matches('/')
    ));
    let store: Arc<dyn ObjectStore> = match url.scheme() {
        #[cfg(feature = "gcp")]
        "gs" => genohype_core::io::get_gcs_client(bucket)?,
        #[cfg(feature = "aws")]
        "s3" => Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .build()
                .map_err(|error| HailError::InvalidFormat(error.to_string()))?,
        ),
        scheme => {
            return Err(HailError::InvalidFormat(format!(
                "unsupported cloud URL scheme: {scheme}"
            )))
        }
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let prefix_text = prefix.to_string();
    let count = runtime.block_on(async {
        let mut count = 0usize;
        let mut objects = store.list(Some(&prefix));
        while let Some(object) = objects.next().await {
            let object = object.map_err(HailError::ObjectStore)?;
            let location = object.location.to_string();
            let Some(name) = location.strip_prefix(&prefix_text) else {
                continue;
            };
            if name.contains('/') || !name.starts_with("part-") || !name.ends_with(".png") {
                continue;
            }
            validate_partition_png_name(name, expected_partition_count)?;
            count += 1;
        }
        Ok::<_, HailError>(count)
    })?;
    if count != expected_partition_count {
        return Err(HailError::InvalidFormat(format!(
            "expected {expected_partition_count} Manhattan partial PNGs, found {count}"
        )));
    }
    Ok(())
}

/// Composite multiple partial PNG images into a final Manhattan plot.
///
/// This function is called by the coordinator after all workers have finished
/// rendering their partial images. It overlays all non-background pixels onto
/// a single canvas to produce the final image.
pub fn composite_partial_pngs(
    parts_dir: &str,
    output_path: &str,
    width: u32,
    height: u32,
    threshold: f64,
) -> Result<()> {
    composite_partial_pngs_with_style(
        parts_dir,
        output_path,
        width,
        height,
        threshold,
        &BackgroundStyle::White,
    )
}

/// Composite multiple partial PNG images into a final Manhattan plot with configurable background.
pub fn composite_partial_pngs_with_style(
    parts_dir: &str,
    output_path: &str,
    width: u32,
    height: u32,
    threshold: f64,
    background: &BackgroundStyle,
) -> Result<()> {
    composite_partial_pngs_impl(
        parts_dir,
        output_path,
        width,
        height,
        threshold,
        background,
        None,
    )
}

pub(crate) fn composite_partial_pngs_with_style_for_partitions(
    parts_dir: &str,
    output_path: &str,
    width: u32,
    height: u32,
    threshold: f64,
    background: &BackgroundStyle,
    expected_partition_count: usize,
) -> Result<()> {
    composite_partial_pngs_impl(
        parts_dir,
        output_path,
        width,
        height,
        threshold,
        background,
        Some(expected_partition_count),
    )
}

fn composite_partial_pngs_impl(
    parts_dir: &str,
    output_path: &str,
    width: u32,
    height: u32,
    threshold: f64,
    background: &BackgroundStyle,
    expected_partition_count: Option<usize>,
) -> Result<()> {
    let start_time = Instant::now();

    println!("Compositing partial PNGs from: {}", parts_dir);

    // List all part-*.png files
    let list_start = Instant::now();
    let png_files = list_partial_png_files(parts_dir)?;
    let list_duration = list_start.elapsed();
    println!(
        "Found {} partial images (listed in {:.1}s)",
        png_files.len(),
        list_duration.as_secs_f64()
    );

    if png_files.is_empty() {
        return Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No part-*.png files found in {}", parts_dir),
        )));
    }

    if let Some(expected) = expected_partition_count {
        if png_files.len() != expected {
            return Err(HailError::InvalidFormat(format!(
                "expected {expected} Manhattan partial PNGs, found {}",
                png_files.len()
            )));
        }
        for (index, path) in png_files.iter().enumerate() {
            let name = path.rsplit('/').next().unwrap_or_default();
            if name != format!("part-{index:05}.png") {
                return Err(HailError::InvalidFormat(format!(
                    "unexpected Manhattan partial PNG {name:?}"
                )));
            }
        }
    }

    // Download all PNGs in parallel
    println!("Downloading {} images in parallel...", png_files.len());
    let download_start = Instant::now();
    let pb = ProgressBar::new(png_files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} downloading")
            .unwrap()
            .progress_chars("#>-"),
    );

    let downloaded: Vec<Result<Vec<u8>>> = png_files
        .par_iter()
        .map(|png_file| {
            let result = read_png_file_bytes(png_file);
            pb.inc(1);
            result
        })
        .collect();

    pb.finish_with_message("downloaded");
    let download_duration = download_start.elapsed();
    let total_bytes: usize = downloaded
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|v| v.len())
        .sum();
    println!(
        "Downloaded {:.1} MB in {:.1}s ({:.1} MB/s)",
        total_bytes as f64 / 1_000_000.0,
        download_duration.as_secs_f64(),
        total_bytes as f64 / 1_000_000.0 / download_duration.as_secs_f64()
    );

    // Create output canvas with configurable background
    let mut canvas = Pixmap::new(width, height).ok_or_else(|| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to create canvas pixmap",
        ))
    })?;

    // Fill canvas with configured background
    match background {
        BackgroundStyle::Transparent => {
            // Pixmap is already transparent (zeroed) by default
        }
        BackgroundStyle::White => {
            canvas.fill(tiny_skia::Color::WHITE);
        }
        BackgroundStyle::Color(hex) => {
            canvas.fill(hex_to_color(hex, 1.0));
        }
    }

    // Composite each partial image using tiny_skia's native alpha blending
    let num_images = downloaded.len();
    println!("Compositing {} images...", num_images);
    let composite_start = Instant::now();

    let paint = tiny_skia::PixmapPaint::default();
    let transform = tiny_skia::Transform::identity();

    for (i, png_data_result) in downloaded.into_iter().enumerate() {
        let png_data = png_data_result?;
        let partial = Pixmap::decode_png(&png_data).map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to decode PNG {}: {}", png_files[i], e),
            ))
        })?;

        if partial.width() != width || partial.height() != height {
            return Err(HailError::InvalidFormat(format!(
                "Manhattan partial PNG {} has dimensions {}x{}, expected {width}x{height}",
                png_files[i],
                partial.width(),
                partial.height()
            )));
        }

        // Draw partial onto canvas with proper alpha blending (handled by tiny_skia)
        canvas.draw_pixmap(0, 0, partial.as_ref(), &paint, transform, None);
    }

    let composite_duration = composite_start.elapsed();
    println!(
        "Composited {} images in {:.1}s",
        num_images,
        composite_duration.as_secs_f64()
    );

    // Draw threshold line on the composited image
    draw_threshold_line_on_pixmap(&mut canvas, width, threshold);

    // Encode final PNG
    let png_data = canvas.encode_png().map_err(|e| {
        HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to encode PNG: {}", e),
        ))
    })?;

    // Write output
    if is_cloud_path(output_path) {
        let mut writer = StreamingCloudWriter::new(output_path)?;
        writer.write_all(&png_data)?;
        writer.finish()?;
    } else {
        fs::write(output_path, &png_data)?;
    }

    let total_duration = start_time.elapsed();
    println!();
    println!("=== Composite Summary ===");
    println!("  Images:      {}", png_files.len());
    println!(
        "  Downloaded:  {:.1} MB in {:.1}s ({:.1} MB/s)",
        total_bytes as f64 / 1_000_000.0,
        download_duration.as_secs_f64(),
        total_bytes as f64 / 1_000_000.0 / download_duration.as_secs_f64()
    );
    println!("  Composited:  {:.1}s", composite_duration.as_secs_f64());
    println!("  Output:      {}", output_path);
    println!("  Total time:  {:.1}s", total_duration.as_secs_f64());
    println!();

    Ok(())
}

/// List all part-*.png files in a directory.
fn list_partial_png_files(dir_path: &str) -> Result<Vec<String>> {
    if is_cloud_path(dir_path) {
        let dir = dir_path.trim_end_matches('/');
        let output = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/part-*.png", dir)])
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
            .filter(|l| l.ends_with(".png"))
            .map(|s| s.to_string())
            .collect();

        Ok(files)
    } else {
        let mut files = Vec::new();
        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("part-") && name.ends_with(".png") {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

/// Read a PNG file and return raw bytes (for tiny_skia to decode).
fn read_png_file_bytes(path: &str) -> Result<Vec<u8>> {
    if is_cloud_path(path) {
        let file_size = get_file_size(path)?;
        range_read(path, 0, file_size as usize)
    } else {
        fs::read(path).map_err(|e| HailError::Io(e))
    }
}

/// Draw a horizontal threshold line on a Pixmap.
pub fn draw_threshold_line_on_pixmap(pixmap: &mut Pixmap, width: u32, threshold: f64) {
    use tiny_skia::{Color, Paint, PathBuilder, Stroke, Transform};

    let y_scale = YScale::new(pixmap.height(), 350.0); // Support p-values down to ~1e-350
    let y = y_scale.threshold_y(threshold);

    let mut paint = Paint::default();
    paint.set_color(Color::from_rgba8(255, 0, 0, 255));
    paint.anti_alias = true;

    let mut stroke = Stroke::default();
    stroke.width = 1.0;
    stroke.dash = tiny_skia::StrokeDash::new(vec![6.0, 4.0], 0.0);

    let mut pb = PathBuilder::new();
    pb.move_to(0.0, y);
    pb.line_to(width as f32, y);

    if let Some(path) = pb.finish() {
        pixmap.stroke_path(&path, &paint, &stroke, Transform::identity(), None);
    }
}
