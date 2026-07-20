//! Configuration system for Manhattan plot styling and job settings.
//!
//! Supports TOML configuration files with per-plot-type styling overrides
//! and job-level settings for batch processing.

use crate::{HailError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

// =============================================================================
// Job Configuration (top-level config file)
// =============================================================================

/// Complete job configuration including inputs, outputs, and styling.
///
/// This is the top-level config loaded from a TOML file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManhattanJobConfig {
    /// Job-level settings (inputs, outputs, thresholds)
    #[serde(default)]
    pub job: JobSettings,

    /// Ingest settings for ClickHouse ingestion
    #[serde(default)]
    pub ingest: IngestSettings,

    /// Styling configuration
    #[serde(default)]
    pub styling: ManhattanConfig,

    /// Shorthand: styling settings can also be at top level
    #[serde(default, flatten)]
    pub styling_flat: ManhattanConfig,
}

impl ManhattanJobConfig {
    /// Load configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(HailError::Io)?;
        toml::from_str(&content).map_err(|e| HailError::Config(e.to_string()))
    }

    /// Get the effective styling config (merges top-level and [styling] section).
    pub fn styling(&self) -> ManhattanConfig {
        // If [styling] section exists and has non-default values, prefer it
        // Otherwise use top-level styling settings
        if self.styling != ManhattanConfig::default() {
            self.styling.clone()
        } else {
            self.styling_flat.clone()
        }
    }

    /// Get the effective input directory for ingestion.
    /// Falls back to job.output_dir if ingest.input_dir is not specified.
    pub fn ingest_input_dir(&self) -> Option<String> {
        self.ingest
            .input_dir
            .clone()
            .or(self.job.output_dir.clone())
    }
}

/// Job-level settings for Manhattan batch processing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobSettings {
    /// Path to assets JSON file
    pub assets_json: Option<String>,

    /// Output directory for generated plots
    pub output_dir: Option<String>,

    /// Filter to specific analysis IDs (comma-separated in TOML)
    #[serde(default)]
    pub analysis_ids: Vec<String>,

    /// Filter to specific ancestries
    #[serde(default)]
    pub ancestries: Vec<String>,

    /// Random sample fraction (0.0 to 1.0)
    pub sample: Option<f64>,

    /// Limit number of phenotypes to process
    pub limit: Option<usize>,

    /// Path to genes reference table
    pub genes: Option<String>,

    /// Path to exome annotations table
    pub exome_annotations: Option<String>,

    /// Path to genome annotations table
    pub genome_annotations: Option<String>,

    /// Path to phenotype metadata Hail table (for catalog enrichment)
    pub metadata_path: Option<String>,

    /// P-value threshold for genome-wide significance
    #[serde(default = "default_threshold")]
    pub threshold: f64,

    /// P-value threshold for gene burden significance
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,

    /// P-value threshold for locus plot inclusion
    #[serde(default = "default_locus_threshold")]
    pub locus_threshold: f64,

    /// Window size in bp for locus region expansion
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,

    /// Whether to generate locus plots
    #[serde(default)]
    pub locus_plots: bool,

    /// Minimum number of significant variants required to form a locus
    #[serde(default = "default_min_variants_per_locus")]
    pub min_variants_per_locus: usize,

    /// Image width in pixels
    #[serde(default = "default_width")]
    pub width: u32,

    /// Image height in pixels
    #[serde(default = "default_height")]
    pub height: u32,

    /// Field name for p-value in source tables
    #[serde(default = "default_y_field")]
    pub y_field: String,

    /// Run only the highly-parallel scan phase
    #[serde(default)]
    pub scan_only: bool,

    /// Run only the memory-intensive aggregate phase
    #[serde(default)]
    pub aggregate_only: bool,

    /// Start coordinator in idle mode without queueing phenotypes
    #[serde(default)]
    pub idle: bool,
}

/// Settings for ClickHouse ingestion.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestSettings {
    /// Input directory containing Manhattan output (defaults to job.output_dir)
    pub input_dir: Option<String>,

    /// ClickHouse HTTP URL (e.g., http://10.128.15.247:8123)
    pub clickhouse_url: Option<String>,

    /// ClickHouse database name
    #[serde(default = "default_database")]
    pub database: String,

    /// Table initialization strategy: "replace", "append", or "skip"
    #[serde(default = "default_init_strategy")]
    pub init_strategy: String,

    /// Number of concurrent upload workers
    pub concurrency: Option<usize>,
}

fn default_database() -> String {
    "default".to_string()
}

fn default_init_strategy() -> String {
    "replace".to_string()
}

fn default_threshold() -> f64 {
    5e-8
}

fn default_gene_threshold() -> f64 {
    2.5e-6
}

fn default_locus_threshold() -> f64 {
    0.01
}

fn default_locus_window() -> i32 {
    1_000_000
}

fn default_min_variants_per_locus() -> usize {
    1
}

fn default_width() -> u32 {
    3000
}

fn default_height() -> u32 {
    800
}

fn default_y_field() -> String {
    "Pvalue".to_string()
}

// =============================================================================
// Styling Configuration
// =============================================================================

/// Background style for rendered plots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BackgroundStyle {
    /// Transparent background (for compositing on any theme)
    Transparent,
    /// White background (default for final output)
    White,
    /// Custom hex color background
    #[serde(untagged)]
    Color(String),
}

impl Default for BackgroundStyle {
    fn default() -> Self {
        BackgroundStyle::White
    }
}

impl BackgroundStyle {
    /// Parse from string (CLI argument).
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "transparent" => BackgroundStyle::Transparent,
            "white" => BackgroundStyle::White,
            _ => BackgroundStyle::Color(s.to_string()),
        }
    }
}

/// Plot type for style resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlotType {
    /// Exome variant Manhattan plot
    Exome,
    /// Genome variant Manhattan plot
    Genome,
    /// Gene burden Manhattan plot
    GeneBurden,
    /// Locus zoom plot
    Locus,
}

/// Root configuration for Manhattan plot styling.
///
/// Loaded from TOML file or constructed with defaults.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManhattanConfig {
    /// Global defaults applied to all plots
    #[serde(default)]
    pub defaults: DefaultsConfig,
    /// Settings for variant plots (exome/genome)
    #[serde(default)]
    pub variant: VariantConfig,
    /// Settings for gene burden plots
    #[serde(default)]
    pub gene_burden: PlotStyleConfig,
    /// Settings for locus plots
    #[serde(default)]
    pub locus: LocusConfig,
}

impl Default for ManhattanConfig {
    fn default() -> Self {
        Self {
            defaults: DefaultsConfig::default(),
            variant: VariantConfig::default(),
            gene_burden: PlotStyleConfig {
                point_radius: Some(5.0),
                point_alpha: Some(0.9),
                chrom_colors: None,
                background: None,
            },
            locus: LocusConfig::default(),
        }
    }
}

impl ManhattanConfig {
    /// Load configuration from a TOML file.
    #[allow(dead_code)]
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(HailError::Io)?;
        toml::from_str(&content).map_err(|e| HailError::Config(e.to_string()))
    }

    /// Resolve styling for a specific plot type.
    ///
    /// Merges defaults → variant/gene_burden/locus → type-specific overrides.
    pub fn resolve(&self, plot_type: PlotType) -> ResolvedStyle {
        match plot_type {
            PlotType::Exome => self.resolve_variant(self.variant.exome.as_ref()),
            PlotType::Genome => self.resolve_variant(self.variant.genome.as_ref()),
            PlotType::GeneBurden => self.resolve_gene_burden(),
            PlotType::Locus => self.resolve_locus(),
        }
    }

    fn resolve_variant(&self, specific: Option<&PlotStyleConfig>) -> ResolvedStyle {
        // Start with defaults
        let mut style = ResolvedStyle {
            point_radius: self.defaults.point_radius,
            point_alpha: self.defaults.point_alpha,
            chrom_colors: self.defaults.chrom_colors.clone(),
            background: self.defaults.background.clone(),
            threshold_line_color: self.defaults.threshold_line_color.clone(),
            threshold_line_alpha: self.defaults.threshold_line_alpha,
        };

        // Apply variant base settings
        if let Some(radius) = self.variant.base.point_radius {
            style.point_radius = radius;
        }
        if let Some(alpha) = self.variant.base.point_alpha {
            style.point_alpha = alpha;
        }
        if let Some(ref colors) = self.variant.base.chrom_colors {
            style.chrom_colors = colors.clone();
        }
        if let Some(ref bg) = self.variant.base.background {
            style.background = bg.clone();
        }

        // Apply type-specific overrides (exome or genome)
        if let Some(specific) = specific {
            if let Some(radius) = specific.point_radius {
                style.point_radius = radius;
            }
            if let Some(alpha) = specific.point_alpha {
                style.point_alpha = alpha;
            }
            if let Some(ref colors) = specific.chrom_colors {
                style.chrom_colors = colors.clone();
            }
            if let Some(ref bg) = specific.background {
                style.background = bg.clone();
            }
        }

        style
    }

    fn resolve_gene_burden(&self) -> ResolvedStyle {
        let mut style = ResolvedStyle {
            point_radius: self.defaults.point_radius,
            point_alpha: self.defaults.point_alpha,
            chrom_colors: self.defaults.chrom_colors.clone(),
            background: self.defaults.background.clone(),
            threshold_line_color: self.defaults.threshold_line_color.clone(),
            threshold_line_alpha: self.defaults.threshold_line_alpha,
        };

        // Apply gene burden settings
        if let Some(radius) = self.gene_burden.point_radius {
            style.point_radius = radius;
        }
        if let Some(alpha) = self.gene_burden.point_alpha {
            style.point_alpha = alpha;
        }
        if let Some(ref colors) = self.gene_burden.chrom_colors {
            style.chrom_colors = colors.clone();
        }
        if let Some(ref bg) = self.gene_burden.background {
            style.background = bg.clone();
        }

        style
    }

    fn resolve_locus(&self) -> ResolvedStyle {
        let mut style = ResolvedStyle {
            point_radius: self.defaults.point_radius,
            point_alpha: self.defaults.point_alpha,
            chrom_colors: self.defaults.chrom_colors.clone(),
            background: self.defaults.background.clone(),
            threshold_line_color: self.defaults.threshold_line_color.clone(),
            threshold_line_alpha: self.defaults.threshold_line_alpha,
        };

        // Apply locus base settings
        if let Some(radius) = self.locus.base.point_radius {
            style.point_radius = radius;
        }
        if let Some(alpha) = self.locus.base.point_alpha {
            style.point_alpha = alpha;
        }
        if let Some(ref colors) = self.locus.base.chrom_colors {
            style.chrom_colors = colors.clone();
        }
        if let Some(ref bg) = self.locus.base.background {
            style.background = bg.clone();
        }

        style
    }

    /// Get locus-specific colors for exome vs genome points.
    pub fn locus_colors(&self) -> (String, String) {
        let exome = self
            .locus
            .exome_color
            .clone()
            .unwrap_or_else(|| "#BF616A".to_string());
        let genome = self
            .locus
            .genome_color
            .clone()
            .unwrap_or_else(|| "#5E81AC".to_string());
        (exome, genome)
    }
}

/// Global default settings applied to all plots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DefaultsConfig {
    /// Background style
    #[serde(default = "default_background")]
    pub background: BackgroundStyle,
    /// Alternating chromosome colors
    #[serde(default = "default_chrom_colors")]
    pub chrom_colors: Vec<String>,
    /// Color for threshold line
    #[serde(default = "default_threshold_line_color")]
    pub threshold_line_color: String,
    /// Alpha for threshold line
    #[serde(default = "default_threshold_line_alpha")]
    pub threshold_line_alpha: f32,
    /// Default point radius
    #[serde(default = "default_point_radius")]
    pub point_radius: f32,
    /// Default point alpha
    #[serde(default = "default_point_alpha")]
    pub point_alpha: f32,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            background: default_background(),
            chrom_colors: default_chrom_colors(),
            threshold_line_color: default_threshold_line_color(),
            threshold_line_alpha: default_threshold_line_alpha(),
            point_radius: default_point_radius(),
            point_alpha: default_point_alpha(),
        }
    }
}

/// Settings for variant plots with optional exome/genome overrides.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct VariantConfig {
    /// Base settings for all variant plots
    #[serde(flatten)]
    pub base: PlotStyleConfig,
    /// Exome-specific overrides
    #[serde(default)]
    pub exome: Option<PlotStyleConfig>,
    /// Genome-specific overrides
    #[serde(default)]
    pub genome: Option<PlotStyleConfig>,
}

/// Optional style settings that can override defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct PlotStyleConfig {
    /// Point radius in pixels
    pub point_radius: Option<f32>,
    /// Point opacity (0.0-1.0)
    pub point_alpha: Option<f32>,
    /// Alternating chromosome colors
    pub chrom_colors: Option<Vec<String>>,
    /// Background style
    pub background: Option<BackgroundStyle>,
}

/// Settings for locus zoom plots.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct LocusConfig {
    /// Base settings
    #[serde(flatten)]
    pub base: PlotStyleConfig,
    /// Color for exome variants
    pub exome_color: Option<String>,
    /// Color for genome variants
    pub genome_color: Option<String>,
}

/// Fully resolved styling with no Options - ready for rendering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedStyle {
    /// Point radius in pixels
    pub point_radius: f32,
    /// Point opacity (0.0-1.0)
    pub point_alpha: f32,
    /// Alternating chromosome colors
    pub chrom_colors: Vec<String>,
    /// Background style
    pub background: BackgroundStyle,
    /// Threshold line color
    pub threshold_line_color: String,
    /// Threshold line alpha
    pub threshold_line_alpha: f32,
}

impl Default for ResolvedStyle {
    fn default() -> Self {
        Self {
            point_radius: default_point_radius(),
            point_alpha: default_point_alpha(),
            chrom_colors: default_chrom_colors(),
            background: default_background(),
            threshold_line_color: default_threshold_line_color(),
            threshold_line_alpha: default_threshold_line_alpha(),
        }
    }
}

// Default value functions

fn default_background() -> BackgroundStyle {
    BackgroundStyle::White
}

fn default_chrom_colors() -> Vec<String> {
    vec!["#404040".to_string(), "#4682B4".to_string()]
}

fn default_threshold_line_color() -> String {
    "#B71C1C".to_string()
}

fn default_threshold_line_alpha() -> f32 {
    0.5
}

fn default_point_radius() -> f32 {
    2.5
}

fn default_point_alpha() -> f32 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ManhattanConfig::default();

        // Check defaults
        assert_eq!(config.defaults.point_radius, 2.5);
        assert_eq!(config.defaults.point_alpha, 1.0);
        assert_eq!(
            config.defaults.chrom_colors,
            vec!["#404040".to_string(), "#4682B4".to_string()]
        );
        assert_eq!(config.defaults.background, BackgroundStyle::White);

        // Check gene burden has larger points
        assert_eq!(config.gene_burden.point_radius, Some(5.0));
    }

    #[test]
    fn test_resolve_variant() {
        let config = ManhattanConfig::default();

        let exome_style = config.resolve(PlotType::Exome);
        assert_eq!(exome_style.point_radius, 2.5);
        assert_eq!(exome_style.point_alpha, 1.0);

        let genome_style = config.resolve(PlotType::Genome);
        assert_eq!(genome_style.point_radius, 2.5);
    }

    #[test]
    fn test_resolve_gene_burden() {
        let config = ManhattanConfig::default();

        let style = config.resolve(PlotType::GeneBurden);
        assert_eq!(style.point_radius, 5.0);
        assert_eq!(style.point_alpha, 0.9);
    }

    #[test]
    fn test_background_style_parse() {
        assert_eq!(
            BackgroundStyle::from_str("transparent"),
            BackgroundStyle::Transparent
        );
        assert_eq!(BackgroundStyle::from_str("white"), BackgroundStyle::White);
        assert_eq!(
            BackgroundStyle::from_str("#FF0000"),
            BackgroundStyle::Color("#FF0000".to_string())
        );
    }

    #[test]
    fn test_toml_parse() {
        let toml = r##"
[defaults]
background = "transparent"
chrom_colors = ["#5E81AC", "#88C0D0"]
point_radius = 3.0

[variant]
point_radius = 2.0

[variant.exome]
point_radius = 1.5

[gene_burden]
point_radius = 6.0
chrom_colors = ["#A3BE8C", "#B48EAD"]

[locus]
exome_color = "#BF616A"
genome_color = "#5E81AC"
"##;

        let config: ManhattanConfig = toml::from_str(toml).unwrap();

        assert_eq!(config.defaults.background, BackgroundStyle::Transparent);
        assert_eq!(config.defaults.point_radius, 3.0);
        assert_eq!(config.variant.base.point_radius, Some(2.0));
        assert_eq!(
            config.variant.exome.as_ref().unwrap().point_radius,
            Some(1.5)
        );
        assert_eq!(config.gene_burden.point_radius, Some(6.0));
        assert_eq!(config.locus.exome_color, Some("#BF616A".to_string()));

        // Test resolution
        let exome_style = config.resolve(PlotType::Exome);
        assert_eq!(exome_style.point_radius, 1.5); // Overridden by variant.exome
        assert_eq!(exome_style.background, BackgroundStyle::Transparent);

        let genome_style = config.resolve(PlotType::Genome);
        assert_eq!(genome_style.point_radius, 2.0); // Falls back to variant.base

        let gene_style = config.resolve(PlotType::GeneBurden);
        assert_eq!(gene_style.point_radius, 6.0);
        assert_eq!(
            gene_style.chrom_colors,
            vec!["#A3BE8C".to_string(), "#B48EAD".to_string()]
        );
    }
}
