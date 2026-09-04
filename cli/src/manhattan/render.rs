//! Rendering engine using tiny-skia for Manhattan plot rasterization.

use crate::manhattan::config::BackgroundStyle;
use crate::HailError;
use crate::Result;
use tiny_skia::{Color, FillRule, Paint, PathBuilder, Pixmap, Stroke, Transform};

/// Rasterizes points onto a pixel buffer and encodes to PNG.
pub struct ManhattanRenderer {
    pixmap: Pixmap,
}

impl ManhattanRenderer {
    /// Create a new renderer with a white background.
    pub fn new(width: u32, height: u32) -> Self {
        Self::new_with_background(width, height, &BackgroundStyle::White)
    }

    /// Create a new renderer with a transparent background.
    /// Use this for distributed rendering where partial images will be composited.
    pub fn new_transparent(width: u32, height: u32) -> Self {
        Self::new_with_background(width, height, &BackgroundStyle::Transparent)
    }

    /// Create a new renderer with a configurable background.
    pub fn new_with_background(width: u32, height: u32, background: &BackgroundStyle) -> Self {
        let mut pixmap =
            Pixmap::new(width, height).expect("Failed to allocate pixmap (dimensions too large?)");

        match background {
            BackgroundStyle::Transparent => {
                // Pixmap is already transparent (zeroed) by default
            }
            BackgroundStyle::White => {
                pixmap.fill(Color::WHITE);
            }
            BackgroundStyle::Color(hex) => {
                pixmap.fill(hex_to_color(hex, 1.0));
            }
        }

        Self { pixmap }
    }

    /// Render a single variant point as an anti-aliased circle with default radius.
    pub fn render_point(&mut self, x: f32, y: f32, color_hex: &str, alpha: f32) {
        self.render_point_with_radius(x, y, color_hex, alpha, 2.5);
    }

    /// Render a single variant point as an anti-aliased circle with custom radius.
    pub fn render_point_with_radius(
        &mut self,
        x: f32,
        y: f32,
        color_hex: &str,
        alpha: f32,
        radius: f32,
    ) {
        let mut paint = Paint::default();
        paint.set_color(hex_to_color(color_hex, alpha));
        paint.anti_alias = true;

        if let Some(path) = PathBuilder::from_circle(x, y, radius) {
            self.pixmap.fill_path(
                &path,
                &paint,
                FillRule::Winding,
                Transform::identity(),
                None,
            );
        }
    }

    /// Draw a horizontal dashed threshold line with default color.
    pub fn render_threshold_line(&mut self, y: f32, width: u32) {
        self.render_threshold_line_styled(y, width, "#B71C1C", 0.5);
    }

    /// Draw a horizontal dashed threshold line with custom color and alpha.
    pub fn render_threshold_line_styled(&mut self, y: f32, width: u32, color: &str, alpha: f32) {
        let mut paint = Paint::default();
        paint.set_color(hex_to_color(color, alpha));
        paint.anti_alias = true;

        let mut stroke = Stroke::default();
        stroke.width = 1.0;
        stroke.dash = tiny_skia::StrokeDash::new(vec![6.0, 4.0], 0.0);

        let mut pb = PathBuilder::new();
        pb.move_to(0.0, y);
        pb.line_to(width as f32, y);
        if let Some(path) = pb.finish() {
            self.pixmap
                .stroke_path(&path, &paint, &stroke, Transform::identity(), None);
        }
    }

    /// Encode the rendered pixmap as a PNG byte vector.
    pub fn encode_png(&self) -> Result<Vec<u8>> {
        self.pixmap.encode_png().map_err(|e| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("PNG encoding failed: {}", e),
            ))
        })
    }
}

/// Parse a hex color string (e.g. "#4682B4") into a tiny-skia Color with alpha.
pub fn hex_to_color(hex: &str, alpha: f32) -> Color {
    let hex = hex.trim_start_matches('#');
    let r = u8::from_str_radix(&hex[0..2], 16).unwrap_or(0);
    let g = u8::from_str_radix(&hex[2..4], 16).unwrap_or(0);
    let b = u8::from_str_radix(&hex[4..6], 16).unwrap_or(0);

    Color::from_rgba8(r, g, b, (alpha * 255.0) as u8)
}
