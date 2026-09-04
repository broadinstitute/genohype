//! Generalized association input command arguments.

use clap::{Args, Subcommand, ValueEnum};
use genohype_association::{validate_raster_dimensions, MAX_RASTER_DIMENSION};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

pub const MAX_ASSOCIATION_RETENTION_HOURS: u64 = 8_760;
const SECONDS_PER_HOUR: u64 = 60 * 60;

#[derive(Subcommand, Debug)]
pub enum AssociationCommands {
    /// Inspect a source using bounded schema and sample reads
    Inspect(AssociationInspectArgs),
    /// Build or reuse an indexed local association dataset and open its viewer
    View(AssociationViewArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AssociationBuildArg {
    #[value(name = "GRCh37")]
    Grch37,
    #[value(name = "GRCh38")]
    Grch38,
}

impl AssociationBuildArg {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Grch37 => "GRCh37",
            Self::Grch38 => "GRCh38",
        }
    }
}

impl fmt::Display for AssociationBuildArg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Args, Debug, Clone, Default)]
pub struct AssociationMappingArgs {
    /// Map a logical field to a source field; repeat or comma-separate values
    #[arg(long = "map", value_name = "LOGICAL=SOURCE", value_delimiter = ',')]
    pub mappings: Vec<String>,

    /// Strict TOML mapping file
    #[arg(long, value_name = "FILE")]
    pub mapping_file: Option<PathBuf>,
}

#[derive(Args, Debug)]
#[command(
    after_help = "Example:\n  genohype association inspect data/test/association/single-variant.tsv --build GRCh38"
)]
pub struct AssociationInspectArgs {
    /// Hail, VCF, CSV, TSV, .csv.gz, or .tsv.gz source
    pub source: String,

    /// Reference assembly (never inferred)
    #[arg(long, value_enum)]
    pub build: Option<AssociationBuildArg>,

    #[command(flatten)]
    pub mapping: AssociationMappingArgs,

    /// Maximum logical data records to inspect (hard maximum: 100)
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..=100))]
    pub sample_rows: u64,

    /// Emit a deterministic JSON report
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
#[command(
    after_help = "Example:\n  genohype association view results.tsv --build GRCh38 --map contig=CHR,position=POS,pvalue=P"
)]
pub struct AssociationViewArgs {
    /// Hail, VCF, CSV, TSV, .csv.gz, or .tsv.gz source
    pub source: String,

    /// Reference assembly
    #[arg(long, value_enum, required = true)]
    pub build: AssociationBuildArg,

    #[command(flatten)]
    pub mapping: AssociationMappingArgs,

    /// Genome-wide significance threshold
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Number of inline top hits (maximum: 1000)
    #[arg(long, default_value_t = 100, value_parser = parse_top_hits)]
    pub top_hits: usize,

    /// Raster width in pixels
    #[arg(long, default_value_t = 3000, value_parser = parse_raster_dimension)]
    pub width: u32,

    /// Raster height in pixels
    #[arg(long, default_value_t = 800, value_parser = parse_raster_dimension)]
    pub height: u32,

    /// Do not open a browser automatically
    #[arg(long)]
    pub no_open: bool,

    /// Viewer port; omitted means 0 (OS-selected)
    #[arg(long, value_name = "PORT")]
    pub port: Option<u16>,

    /// Ignore a reusable completed session and rebuild
    #[arg(long)]
    pub rebuild: bool,

    /// Remove the completed session after the viewer exits
    #[arg(long)]
    pub ephemeral: bool,

    /// Retain a completed session for this many hours
    #[arg(long, default_value_t = 168, value_parser = parse_retention_hours)]
    pub retention_hours: u64,

    /// Remove expired and incomplete unlocked association sessions first
    #[arg(long)]
    pub cleanup_expired: bool,
}

pub fn validate_association_raster_dimensions(width: u32, height: u32) -> Result<(), String> {
    validate_raster_dimensions(width, height).map_err(|_| {
        format!(
            "association raster width and height must each be in 1..={MAX_RASTER_DIMENSION} and their product must not exceed {} pixels",
            genohype_association::MAX_RASTER_PIXELS
        )
    })
}

pub fn association_retention_duration(hours: u64) -> Result<Duration, String> {
    if hours == 0 || hours > MAX_ASSOCIATION_RETENTION_HOURS {
        return Err(format!(
            "retention hours must be in 1..={MAX_ASSOCIATION_RETENTION_HOURS}"
        ));
    }
    let seconds = hours
        .checked_mul(SECONDS_PER_HOUR)
        .ok_or_else(|| "retention duration exceeds the supported range".to_string())?;
    Ok(Duration::from_secs(seconds))
}

fn parse_raster_dimension(value: &str) -> Result<u32, String> {
    let value = value
        .parse::<u32>()
        .map_err(|_| "raster dimensions must be positive integers".to_string())?;
    if value == 0 || value > MAX_RASTER_DIMENSION {
        return Err(format!(
            "raster dimensions must be in 1..={MAX_RASTER_DIMENSION}"
        ));
    }
    Ok(value)
}

fn parse_retention_hours(value: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|_| "retention hours must be a positive integer".to_string())?;
    association_retention_duration(value).map(|_| value)
}

fn parse_top_hits(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| "top hits must be an integer".to_string())?;
    if value > 1_000 {
        return Err("top hits cannot exceed 1000".to_string());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Commands};
    use clap::{CommandFactory, Parser};

    #[test]
    fn stable_inspect_shape_parses_repeatable_and_comma_maps() {
        let cli = Cli::try_parse_from([
            "genohype",
            "association",
            "inspect",
            "results.tsv",
            "--build",
            "GRCh38",
            "--map",
            "contig=CHR,position=POS",
            "--map",
            "pvalue=P",
            "--sample-rows",
            "25",
            "--json",
        ])
        .unwrap();
        let Commands::Association { command } = cli.command else {
            panic!("expected association command");
        };
        let AssociationCommands::Inspect(args) = command else {
            panic!("expected inspect command");
        };
        assert_eq!(
            args.mapping.mappings,
            ["contig=CHR", "position=POS", "pvalue=P"]
        );
        assert_eq!(args.build, Some(AssociationBuildArg::Grch38));
        assert_eq!(args.sample_rows, 25);
        assert!(args.json);
    }

    #[test]
    fn stable_view_shape_and_bounds_parse() {
        let cli = Cli::try_parse_from([
            "genohype",
            "association",
            "view",
            "results.tsv.gz",
            "--build",
            "GRCh37",
            "--map",
            "contig=CHR,position=POS,neg_log10_p=LOGP",
            "--threshold",
            "1e-6",
            "--top-hits",
            "1000",
            "--width",
            "1200",
            "--height",
            "600",
            "--no-open",
            "--port",
            "0",
            "--rebuild",
            "--ephemeral",
            "--retention-hours",
            "24",
            "--cleanup-expired",
        ])
        .unwrap();
        let Commands::Association { command } = cli.command else {
            panic!("expected association command");
        };
        let AssociationCommands::View(args) = command else {
            panic!("expected view command");
        };
        assert_eq!(args.build, AssociationBuildArg::Grch37);
        assert_eq!(args.top_hits, 1000);
        assert!(args.no_open && args.rebuild && args.ephemeral && args.cleanup_expired);
    }

    #[test]
    fn view_requires_build_and_bounded_values() {
        assert!(Cli::try_parse_from([
            "genohype",
            "association",
            "view",
            "results.tsv",
            "--map",
            "contig=CHR,position=POS,pvalue=P"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "genohype",
            "association",
            "inspect",
            "results.tsv",
            "--sample-rows",
            "101"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "genohype",
            "association",
            "view",
            "results.tsv",
            "--build",
            "GRCh38",
            "--top-hits",
            "1001"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "genohype",
            "association",
            "view",
            "results.tsv",
            "--build",
            "GRCh38",
            "--width",
            "16385"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "genohype",
            "association",
            "view",
            "results.tsv",
            "--build",
            "GRCh38",
            "--retention-hours",
            "8761"
        ])
        .is_err());
        assert!(validate_association_raster_dimensions(4096, 4096).is_ok());
        assert!(validate_association_raster_dimensions(4097, 4096).is_err());
        assert!(validate_association_raster_dimensions(MAX_RASTER_DIMENSION, 1).is_ok());
        assert!(validate_association_raster_dimensions(MAX_RASTER_DIMENSION + 1, 1).is_err());
        assert!(association_retention_duration(1).is_ok());
        let maximum = association_retention_duration(MAX_ASSOCIATION_RETENTION_HOURS).unwrap();
        assert_eq!(maximum.as_secs(), MAX_ASSOCIATION_RETENTION_HOURS * 60 * 60);
        assert!(std::time::SystemTime::now().checked_add(maximum).is_some());
        assert!(association_retention_duration(0).is_err());
        assert!(association_retention_duration(MAX_ASSOCIATION_RETENTION_HOURS + 1).is_err());
        assert!(association_retention_duration(u64::MAX).is_err());
    }

    #[test]
    fn help_registers_generalized_surface_without_changing_manhattan() {
        let root = Cli::command();
        assert!(root.find_subcommand("association").is_some());
        assert!(root.find_subcommand("manhattan").is_some());
        let association = root.find_subcommand("association").unwrap();
        assert!(association.find_subcommand("inspect").is_some());
        assert!(association.find_subcommand("view").is_some());
    }
}
