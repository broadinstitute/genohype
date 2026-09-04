//! Generalized association input command arguments.

use clap::{Args, Subcommand, ValueEnum};
use std::fmt;
use std::path::PathBuf;

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
    #[arg(long, default_value_t = 3000, value_parser = clap::value_parser!(u32).range(1..))]
    pub width: u32,

    /// Raster height in pixels
    #[arg(long, default_value_t = 800, value_parser = clap::value_parser!(u32).range(1..))]
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
    #[arg(long, default_value_t = 168)]
    pub retention_hours: u64,

    /// Remove expired and incomplete unlocked association sessions first
    #[arg(long)]
    pub cleanup_expired: bool,
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
