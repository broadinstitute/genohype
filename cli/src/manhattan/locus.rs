//! Locus-level scatter plot renderer for zoomed-in LocusZoom-style views.
//!
//! This module renders a scatter plot for a specific genomic region, distinguishing
//! between Exome and Genome variants with different shapes and colors.

use crate::manhattan::config::BackgroundStyle;
use crate::manhattan::data::VariantSource;
use crate::manhattan::layout::YScale;
use crate::manhattan::render::hex_to_color;
use crate::HailError;
use crate::Result;
use tiny_skia::{Color, FillRule, Paint, PathBuilder, Pixmap, Stroke, Transform};

/// A variant prepared for rendering in the locus plot.
#[derive(Clone)]
pub struct RenderVariant {
    pub position: i32,
    pub ref_allele: String,
    pub alt_allele: String,
    pub pvalue: f64,
    pub beta: Option<f64>,
    pub se: Option<f64>,
    pub af: Option<f64>,
    pub source: VariantSource,
    pub is_significant: bool,
    // Case/control breakdown fields
    pub ac_cases: Option<f64>,
    pub ac_controls: Option<f64>,
    pub af_cases: Option<f64>,
    pub af_controls: Option<f64>,
    // Trait-level association stats
    pub association_ac: Option<f64>,
}

/// Configuration for the locus scatter plot.
pub struct LocusPlotConfig {
    pub width: u32,
    pub height: u32,
    /// Genomic start position of the window
    pub start_pos: i32,
    /// Genomic end position of the window
    pub end_pos: i32,
    /// Maximum -log10(p) value for Y-axis scaling
    pub y_max: f64,
}

impl LocusPlotConfig {
    /// Map genomic position to X-pixel coordinate.
    pub fn get_x(&self, pos: i32) -> f32 {
        let range = (self.end_pos - self.start_pos) as f64;
        if range <= 0.0 {
            return 0.0;
        }
        let relative = (pos - self.start_pos) as f64;
        let x_fraction = relative / range;

        // Map to full width [0, width]
        (x_fraction * self.width as f64) as f32
    }

    /// Map p-value to Y-pixel coordinate using hybrid linear-log scale.
    pub fn get_y(&self, pval: f64) -> f32 {
        let neg_log_p = -pval.log10();
        // Use YScale for consistent linear-log behavior
        let scale = YScale::new(self.height, self.y_max);
        scale.get_y(neg_log_p)
    }
}

/// Renderer for locus-level scatter plots.
pub struct LocusRenderer {
    pixmap: Pixmap,
    config: LocusPlotConfig,
    /// Exome variant color
    exome_color: String,
    /// Genome variant color
    genome_color: String,
    /// Point radius
    point_radius: f32,
}

impl LocusRenderer {
    /// Create a new renderer with a white background.
    pub fn new(config: LocusPlotConfig) -> Self {
        let mut pixmap = Pixmap::new(config.width, config.height)
            .expect("Failed to allocate locus pixmap (dimensions too large?)");
        pixmap.fill(Color::WHITE);
        Self {
            pixmap,
            config,
            exome_color: "#BF616A".to_string(), // Default exome color (red)
            genome_color: "#5E81AC".to_string(), // Default genome color (blue)
            point_radius: 3.0,
        }
    }

    /// Create a new renderer with custom styling.
    pub fn new_with_style(
        config: LocusPlotConfig,
        background: &BackgroundStyle,
        exome_color: String,
        genome_color: String,
        point_radius: f32,
    ) -> Self {
        let mut pixmap = Pixmap::new(config.width, config.height)
            .expect("Failed to allocate locus pixmap (dimensions too large?)");

        match background {
            BackgroundStyle::Transparent => {
                // Pixmap is already transparent by default
            }
            BackgroundStyle::White => {
                pixmap.fill(Color::WHITE);
            }
            BackgroundStyle::Color(hex) => {
                pixmap.fill(hex_to_color(hex, 1.0));
            }
        }

        Self {
            pixmap,
            config,
            exome_color,
            genome_color,
            point_radius,
        }
    }

    /// Draw a dashed horizontal line at the significance threshold.
    pub fn draw_threshold_line(&mut self, pvalue: f64) {
        let y = self.config.get_y(pvalue);

        let mut paint = Paint::default();
        paint.set_color_rgba8(183, 28, 28, 128); // Dark Red, semi-transparent
        paint.anti_alias = true;

        let mut stroke = Stroke::default();
        stroke.width = 1.0;
        stroke.dash = tiny_skia::StrokeDash::new(vec![6.0, 4.0], 0.0);

        let mut pb = PathBuilder::new();
        pb.move_to(0.0, y);
        pb.line_to(self.config.width as f32, y);

        if let Some(path) = pb.finish() {
            self.pixmap
                .stroke_path(&path, &paint, &stroke, Transform::identity(), None);
        }
    }

    /// Render variants as points.
    ///
    /// Draws Genome variants first (background), then Exome variants (foreground).
    /// This layering ensures sparser exome variants aren't buried under genome variants.
    pub fn draw_variants(&mut self, variants: &[RenderVariant]) {
        // Use configured colors
        let genome_color = hex_to_color(&self.genome_color, 0.7);
        let exome_color = hex_to_color(&self.exome_color, 0.8);

        let radius = self.point_radius;

        // Helper to draw variants for a given source
        let draw_source = |pixmap: &mut Pixmap,
                           config: &LocusPlotConfig,
                           source: VariantSource,
                           paint_color: Color,
                           radius: f32| {
            let mut paint = Paint::default();
            paint.set_color(paint_color);
            paint.anti_alias = true;

            for v in variants {
                if v.source != source {
                    continue;
                }

                let x = config.get_x(v.position);
                let y = config.get_y(v.pvalue);

                // Skip points far outside bounds
                if x < -5.0 || x > (config.width as f32 + 5.0) {
                    continue;
                }

                match source {
                    VariantSource::Genome => {
                        // Circle
                        if let Some(path) = PathBuilder::from_circle(x, y, radius) {
                            pixmap.fill_path(
                                &path,
                                &paint,
                                FillRule::Winding,
                                Transform::identity(),
                                None,
                            );
                        }
                    }
                    VariantSource::Exome => {
                        // Diamond (rotated square)
                        let mut pb = PathBuilder::new();
                        pb.move_to(x, y - radius);
                        pb.line_to(x + radius, y);
                        pb.line_to(x, y + radius);
                        pb.line_to(x - radius, y);
                        pb.close();

                        if let Some(path) = pb.finish() {
                            pixmap.fill_path(
                                &path,
                                &paint,
                                FillRule::Winding,
                                Transform::identity(),
                                None,
                            );
                        }
                    }
                }
            }
        };

        // Draw genome first (background), then exome (foreground)
        draw_source(
            &mut self.pixmap,
            &self.config,
            VariantSource::Genome,
            genome_color,
            radius,
        );
        draw_source(
            &mut self.pixmap,
            &self.config,
            VariantSource::Exome,
            exome_color,
            radius,
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_render_locus_plot() {
        // Setup a mock region: chr1:100,000-200,000
        let config = LocusPlotConfig {
            width: 800,
            height: 400,
            start_pos: 100_000,
            end_pos: 200_000,
            y_max: 20.0,
        };

        let mut renderer = LocusRenderer::new(config);

        // Mock Data
        let variants = vec![
            // Genome background (noise)
            RenderVariant {
                position: 110_000,
                ref_allele: "A".to_string(),
                alt_allele: "G".to_string(),
                pvalue: 1e-1,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Genome,
                is_significant: false,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
            RenderVariant {
                position: 120_000,
                ref_allele: "C".to_string(),
                alt_allele: "T".to_string(),
                pvalue: 1e-2,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Genome,
                is_significant: false,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
            RenderVariant {
                position: 130_000,
                ref_allele: "G".to_string(),
                alt_allele: "A".to_string(),
                pvalue: 1e-3,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Genome,
                is_significant: false,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
            // Genome Significant Hit
            RenderVariant {
                position: 150_000,
                ref_allele: "T".to_string(),
                alt_allele: "C".to_string(),
                pvalue: 1e-9,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Genome,
                is_significant: true,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
            // Exome Signal (should appear on top of genome)
            RenderVariant {
                position: 148_000,
                ref_allele: "A".to_string(),
                alt_allele: "T".to_string(),
                pvalue: 1e-5,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Exome,
                is_significant: false,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
            RenderVariant {
                position: 150_000,
                ref_allele: "G".to_string(),
                alt_allele: "C".to_string(),
                pvalue: 1e-12,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Exome,
                is_significant: true,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
            RenderVariant {
                position: 152_000,
                ref_allele: "C".to_string(),
                alt_allele: "A".to_string(),
                pvalue: 1e-6,
                beta: None,
                se: None,
                af: None,
                source: VariantSource::Exome,
                is_significant: false,
                ac_cases: None,
                ac_controls: None,
                af_cases: None,
                af_controls: None,
                association_ac: None,
            },
        ];

        // Render
        renderer.draw_threshold_line(5e-8);
        renderer.draw_variants(&variants);

        // Save output for visual inspection
        let png_data = renderer.encode_png().unwrap();
        let mut file = File::create("locus_test_render.png").unwrap();
        file.write_all(&png_data).unwrap();

        println!("Test plot saved to locus_test_render.png");
    }
}
