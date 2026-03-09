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
    composite_partial_pngs_with_style(parts_dir, output_path, width, height, threshold, &BackgroundStyle::White)
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
    let start_time = Instant::now();

    println!("Compositing partial PNGs from: {}", parts_dir);

    // List all part-*.png files
    let list_start = Instant::now();
    let png_files = list_partial_png_files(parts_dir)?;
    let list_duration = list_start.elapsed();
    println!("Found {} partial images (listed in {:.1}s)", png_files.len(), list_duration.as_secs_f64());

    if png_files.is_empty() {
        return Err(HailError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No part-*.png files found in {}", parts_dir),
        )));
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
    let total_bytes: usize = downloaded.iter().filter_map(|r| r.as_ref().ok()).map(|v| v.len()).sum();
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
    println!("  Downloaded:  {:.1} MB in {:.1}s ({:.1} MB/s)",
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
