use crate::layout::{validate_raster_dimensions, MAX_ENCODED_RASTER_BYTES};
use crate::region::RegionLayoutV1;
use crate::{AssociationPointV1, GenomeLayoutV1};
use std::error::Error;
use std::fmt;
use tiny_skia::{Color, FillRule, Paint, PathBuilder, Pixmap, Stroke, Transform};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Background {
    White,
    Transparent,
    Color(String),
}

/// Deterministic tiny-skia rasterizer. It owns no source-specific state.
pub struct AssociationRasterizer {
    pixmap: Pixmap,
}

impl AssociationRasterizer {
    pub fn new(width: u32, height: u32, background: Background) -> Result<Self, RenderError> {
        validate_raster_dimensions(width, height)
            .map_err(|_| RenderError::InvalidDimensions { width, height })?;
        let mut pixmap =
            Pixmap::new(width, height).ok_or(RenderError::InvalidDimensions { width, height })?;
        match background {
            Background::White => pixmap.fill(Color::WHITE),
            Background::Transparent => {}
            Background::Color(value) => pixmap.fill(parse_hex_color(&value, 255)?),
        }
        Ok(Self { pixmap })
    }

    pub fn from_layout(
        layout: &GenomeLayoutV1,
        background: Background,
    ) -> Result<Self, RenderError> {
        Self::new(layout.width, layout.height, background)
    }

    pub fn width(&self) -> u32 {
        self.pixmap.width()
    }

    pub fn height(&self) -> u32 {
        self.pixmap.height()
    }

    /// Render one anti-aliased point and report whether it was inside the layout.
    pub fn render_association_point(
        &mut self,
        layout: &GenomeLayoutV1,
        point: &AssociationPointV1,
        radius: f32,
        alpha: u8,
    ) -> Result<bool, RenderError> {
        if !radius.is_finite() || radius <= 0.0 {
            return Err(RenderError::InvalidRadius);
        }
        let Some(x) = layout.x_for(&point.contig, point.position) else {
            return Ok(false);
        };
        let Some(color) = layout.color_for(&point.contig) else {
            return Ok(false);
        };
        let y = layout.y_scale.y_for(point.neg_log10_p);
        self.render_circle(x, y, radius, parse_hex_color(color, alpha)?);
        Ok(true)
    }

    /// Render an iterator in source order, returning the represented point count.
    pub fn render_points<'a, I>(
        &mut self,
        layout: &GenomeLayoutV1,
        points: I,
        radius: f32,
        alpha: u8,
    ) -> Result<u64, RenderError>
    where
        I: IntoIterator<Item = &'a AssociationPointV1>,
    {
        let mut rendered = 0;
        for point in points {
            rendered += u64::from(self.render_association_point(layout, point, radius, alpha)?);
        }
        Ok(rendered)
    }

    pub fn render_threshold(
        &mut self,
        layout: &GenomeLayoutV1,
        pvalue: f64,
        color: &str,
    ) -> Result<(), RenderError> {
        let y = layout
            .y_scale
            .threshold_y(pvalue)
            .ok_or(RenderError::InvalidThreshold)?;
        let mut paint = Paint::default();
        paint.set_color(parse_hex_color(color, 178)?);
        paint.anti_alias = true;
        let stroke = Stroke {
            width: 1.0,
            dash: tiny_skia::StrokeDash::new(vec![6.0, 4.0], 0.0),
            ..Stroke::default()
        };
        let mut builder = PathBuilder::new();
        builder.move_to(0.0, y);
        builder.line_to(layout.width as f32, y);
        if let Some(path) = builder.finish() {
            self.pixmap
                .stroke_path(&path, &paint, &stroke, Transform::identity(), None);
        }
        Ok(())
    }

    pub fn render_circle(&mut self, x: f32, y: f32, radius: f32, color: Color) {
        let mut paint = Paint::default();
        paint.set_color(color);
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

    /// Return tiny-skia's deterministic premultiplied RGBA pixel bytes.
    pub fn raster_bytes(&self) -> &[u8] {
        self.pixmap.data()
    }

    /// Encode the raster as standalone PNG bytes without touching the filesystem.
    pub fn png_bytes(&self) -> Result<Vec<u8>, RenderError> {
        let bytes = self
            .pixmap
            .encode_png()
            .map_err(|_| RenderError::PngEncoding)?;
        if bytes.len() as u64 > MAX_ENCODED_RASTER_BYTES {
            return Err(RenderError::EncodedRasterTooLarge);
        }
        Ok(bytes)
    }
}

/// Pure source-neutral region renderer. Every accepted point is drawn exactly
/// once and in iterator order; semantic overlay reduction is a separate step.
pub struct LocusRasterizer {
    pixmap: Pixmap,
}

impl LocusRasterizer {
    pub fn from_layout(
        layout: &RegionLayoutV1,
        background: Background,
    ) -> Result<Self, RenderError> {
        layout
            .validate()
            .map_err(|_| RenderError::InvalidRegionLayout)?;
        validate_raster_dimensions(layout.physical_width, layout.physical_height).map_err(
            |_| RenderError::InvalidDimensions {
                width: layout.physical_width,
                height: layout.physical_height,
            },
        )?;
        let mut pixmap = Pixmap::new(layout.physical_width, layout.physical_height).ok_or(
            RenderError::InvalidDimensions {
                width: layout.physical_width,
                height: layout.physical_height,
            },
        )?;
        match background {
            Background::White => pixmap.fill(Color::WHITE),
            Background::Transparent => {}
            Background::Color(value) => pixmap.fill(parse_hex_color(&value, 255)?),
        }
        Ok(Self { pixmap })
    }

    pub fn render_point(
        &mut self,
        layout: &RegionLayoutV1,
        point: &AssociationPointV1,
    ) -> Result<bool, RenderError> {
        if crate::canonical_contig(&point.contig).as_deref() != Some(layout.contig.as_str()) {
            return Ok(false);
        }
        let Some(x) = layout.x_for(point.position) else {
            return Ok(false);
        };
        let y = layout.y_for(point.neg_log10_p);
        let dpr = f32::from(layout.dpr);
        let radius = locus_point_radius(point.allele_frequency) * dpr;
        let color = locus_point_color(point.consequence.as_deref());
        self.render_circle(x, y, radius, color);
        Ok(true)
    }

    pub fn render_points<I>(
        &mut self,
        layout: &RegionLayoutV1,
        points: I,
    ) -> Result<u64, RenderError>
    where
        I: IntoIterator<Item = AssociationPointV1>,
    {
        let mut rendered = 0_u64;
        for point in points {
            rendered += u64::from(self.render_point(layout, &point)?);
        }
        Ok(rendered)
    }

    /// Draw the canonical gainsboro 3/3 dashed, 2 CSS-pixel threshold line.
    pub fn render_threshold(
        &mut self,
        layout: &RegionLayoutV1,
        pvalue: f64,
    ) -> Result<(), RenderError> {
        let y = layout
            .y_scale
            .threshold_y(pvalue)
            .ok_or(RenderError::InvalidThreshold)?;
        let dpr = f32::from(layout.dpr);
        let mut paint = Paint::default();
        paint.set_color(parse_hex_color("#DCDCDC", 255)?);
        paint.anti_alias = true;
        let stroke = Stroke {
            width: 2.0 * dpr,
            dash: tiny_skia::StrokeDash::new(vec![3.0 * dpr, 3.0 * dpr], 0.0),
            ..Stroke::default()
        };
        let mut builder = PathBuilder::new();
        builder.move_to(0.0, y);
        builder.line_to(layout.physical_width.saturating_sub(1) as f32, y);
        if let Some(path) = builder.finish() {
            self.pixmap
                .stroke_path(&path, &paint, &stroke, Transform::identity(), None);
        }
        Ok(())
    }

    fn render_circle(&mut self, x: f32, y: f32, radius: f32, color: Color) {
        let mut paint = Paint::default();
        paint.set_color(color);
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

    pub fn raster_bytes(&self) -> &[u8] {
        self.pixmap.data()
    }

    pub fn png_bytes(&self) -> Result<Vec<u8>, RenderError> {
        let bytes = self
            .pixmap
            .encode_png()
            .map_err(|_| RenderError::PngEncoding)?;
        if bytes.len() as u64 > MAX_ENCODED_RASTER_BYTES {
            return Err(RenderError::EncodedRasterTooLarge);
        }
        Ok(bytes)
    }
}

fn locus_point_color(consequence: Option<&str>) -> Color {
    let value = consequence.unwrap_or_default().trim();
    let (red, green, blue) = match value {
        "pLoF"
        | "lof"
        | "transcript_ablation"
        | "splice_acceptor_variant"
        | "splice_donor_variant"
        | "stop_gained"
        | "frameshift_variant"
        | "stop_lost"
        | "start_lost" => (255, 88, 63),
        "missense"
        | "missenseLC"
        | "missense_variant"
        | "protein_altering_variant"
        | "inframe_insertion"
        | "inframe_deletion" => (240, 201, 77),
        "synonymous" | "synonymous_variant" => (128, 128, 128),
        _ => (211, 211, 211),
    };
    Color::from_rgba8(red, green, blue, 178)
}

fn locus_point_radius(allele_frequency: Option<f64>) -> f32 {
    let Some(frequency) = allele_frequency.filter(|value| value.is_finite() && *value > 0.0) else {
        return 2.0;
    };
    let minimum = 1e-6_f64;
    let maximum = 0.1_f64;
    let normalized =
        (frequency.clamp(minimum, maximum).ln() - minimum.ln()) / (maximum.ln() - minimum.ln());
    (2.0 + normalized * 2.0) as f32
}

fn parse_hex_color(value: &str, alpha: u8) -> Result<Color, RenderError> {
    let value = value.strip_prefix('#').unwrap_or(value);
    if value.len() != 6 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RenderError::InvalidColor(value.to_string()));
    }
    let red = u8::from_str_radix(&value[0..2], 16)
        .map_err(|_| RenderError::InvalidColor(value.to_string()))?;
    let green = u8::from_str_radix(&value[2..4], 16)
        .map_err(|_| RenderError::InvalidColor(value.to_string()))?;
    let blue = u8::from_str_radix(&value[4..6], 16)
        .map_err(|_| RenderError::InvalidColor(value.to_string()))?;
    Ok(Color::from_rgba8(red, green, blue, alpha))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderError {
    InvalidDimensions { width: u32, height: u32 },
    InvalidRegionLayout,
    InvalidRadius,
    InvalidThreshold,
    InvalidColor(String),
    PngEncoding,
    EncodedRasterTooLarge,
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDimensions { width, height } => {
                write!(f, "cannot allocate {width}x{height} raster")
            }
            Self::InvalidRegionLayout => write!(f, "region layout is not canonical"),
            Self::InvalidRadius => write!(f, "point radius must be finite and positive"),
            Self::InvalidThreshold => write!(f, "threshold must be finite and in 0 < p <= 1"),
            Self::InvalidColor(color) => write!(f, "invalid six-digit RGB color {color:?}"),
            Self::PngEncoding => write!(f, "PNG encoding failed"),
            Self::EncodedRasterTooLarge => write!(f, "encoded PNG exceeds the hard byte limit"),
        }
    }
}

impl Error for RenderError {}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn point(contig: &str, position: i64, significance: f64, ordinal: u64) -> AssociationPointV1 {
        AssociationPointV1 {
            contig: contig.into(),
            position,
            ref_allele: None,
            alt_allele: None,
            pvalue: 10_f64.powf(-significance),
            neg_log10_p: significance,
            beta: None,
            standard_error: None,
            allele_frequency: None,
            allele_count: None,
            gene_symbol: None,
            consequence: None,
            series: None,
            source_ordinal: ordinal,
        }
    }

    #[test]
    fn raster_dimensions_bytes_and_digest_are_deterministic() {
        let layout = GenomeLayoutV1::new([("1", 100), ("2", 80)], 96, 48, 4).unwrap();
        let points = [
            point("chr1", 1, 2.0, 0),
            point("1", 50, 8.0, 1),
            point("2", 80, 20.0, 2),
        ];
        let mut raster = AssociationRasterizer::from_layout(&layout, Background::White).unwrap();
        assert_eq!(raster.render_points(&layout, &points, 2.5, 178).unwrap(), 3);
        raster.render_threshold(&layout, 5e-8, "#B71C1C").unwrap();
        assert_eq!((raster.width(), raster.height()), (96, 48));
        assert_eq!(raster.raster_bytes().len(), 96 * 48 * 4);
        let png = raster.png_bytes().unwrap();
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert_eq!(
            format!("{:x}", Sha256::digest(&png)),
            "bead756e06928952e169d4078a9e107ea6e1acc045888ca52994402bf2bfba1c"
        );
    }

    #[test]
    fn locus_raster_digests_are_stable_at_multiple_dprs() {
        let points = vec![
            point("1", 1, 0.0, 0),
            point("1", 50, 10.0, 1),
            point("1", 100, 400.0, 2),
        ];
        let mut digests = Vec::new();
        for dpr in [1, 2, 4] {
            let layout = RegionLayoutV1::new("1", 100, 1, 100, 100, 64, dpr).unwrap();
            let mut raster = LocusRasterizer::from_layout(&layout, Background::White).unwrap();
            assert_eq!(raster.render_points(&layout, points.clone()).unwrap(), 3);
            raster.render_threshold(&layout, 5e-8).unwrap();
            assert_eq!(
                raster.raster_bytes().len(),
                layout.physical_width as usize * layout.physical_height as usize * 4
            );
            digests.push(format!("{:x}", Sha256::digest(raster.png_bytes().unwrap())));
        }
        assert_eq!(
            digests,
            [
                "7ef8206a79056fcfe9ce7a595fdc50b906db377f5174cffc48ac0afedcd244ad",
                "a9db4004f888c2cdfcc244cb500840a80dd8ff3858cdba35b45b70f917992188",
                "6290bbafdc44c0db450574a5a0357d170bc943100e8338a2209e9209e0e8a361",
            ]
        );
    }

    #[test]
    fn locus_points_are_all_drawn_in_supplied_order() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 100, 64, 1).unwrap();
        let mut lof = point("1", 50, 12.0, 1);
        lof.consequence = Some("pLoF".into());
        let mut other = point("1", 50, 12.0, 2);
        other.consequence = Some("unknown".into());

        let render = |points: Vec<AssociationPointV1>| {
            let mut raster =
                LocusRasterizer::from_layout(&layout, Background::Transparent).unwrap();
            assert_eq!(raster.render_points(&layout, points).unwrap(), 2);
            format!("{:x}", Sha256::digest(raster.png_bytes().unwrap()))
        };
        assert_ne!(
            render(vec![lof.clone(), other.clone()]),
            render(vec![other, lof])
        );
    }

    #[test]
    fn locus_constructor_rejects_noncanonical_layouts() {
        let mut layout = RegionLayoutV1::new("1", 100, 1, 100, 100, 64, 1).unwrap();
        layout.physical_width += 1;
        assert!(matches!(
            LocusRasterizer::from_layout(&layout, Background::White),
            Err(RenderError::InvalidRegionLayout)
        ));
        let mut scale = RegionLayoutV1::new("1", 100, 1, 100, 100, 64, 1).unwrap();
        scale.y_scale.max_neg_log_p = 349.0;
        assert!(matches!(
            LocusRasterizer::from_layout(&scale, Background::White),
            Err(RenderError::InvalidRegionLayout)
        ));
    }

    #[test]
    fn locus_radius_scaling_has_documented_endpoints() {
        assert_eq!(locus_point_radius(None), 2.0);
        assert_eq!(locus_point_radius(Some(0.0)), 2.0);
        assert_eq!(locus_point_radius(Some(1e-6)), 2.0);
        assert_eq!(locus_point_radius(Some(0.1)), 4.0);
        assert_eq!(locus_point_radius(Some(1.0)), 4.0);
    }

    #[test]
    fn points_outside_declared_contigs_are_not_rendered() {
        let layout = GenomeLayoutV1::new([("1", 100)], 100, 50, 0).unwrap();
        let mut raster =
            AssociationRasterizer::from_layout(&layout, Background::Transparent).unwrap();
        assert!(!raster
            .render_association_point(&layout, &point("2", 1, 5.0, 0), 2.0, 255)
            .unwrap());
        assert!(!raster
            .render_association_point(&layout, &point("1", 101, 5.0, 0), 2.0, 255)
            .unwrap());
    }
}
