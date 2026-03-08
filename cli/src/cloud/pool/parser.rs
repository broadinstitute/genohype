//! Command parsing for distributed pool jobs.

use super::PoolManager;
use crate::cloud::CloudProvider;
use crate::HailError;
use crate::Result;

impl<P: CloudProvider + Sync> PoolManager<P> {
    /// Parse a command array into a JobSpec and input path.
    ///
    /// Supported formats:
    /// - `export parquet <input> <output> [--where ...] [--interval ...]`
    /// - `export json <input> <output> [--where ...] [--interval ...]`
    ///
    /// Returns (input_path, job_spec, filters, intervals)
    pub(crate) fn parse_command_to_job_spec(
        command: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::JobSpec;

        if command.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Empty command",
            )));
        }

        let cmd = command.get(0).map(|s| s.as_str()).unwrap_or("<empty>");

        // Handle 'summary <input>' command
        if cmd == "summary" {
            if command.len() < 2 {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Summary command requires: summary <input>\n\
                     Example: summary gs://bucket/input.ht",
                )));
            }
            let input_path = command[1].clone();
            return Ok((input_path, JobSpec::Summary, Vec::new(), Vec::new()));
        }

        // Handle 'manhattan' command
        if cmd == "manhattan" {
            return Self::parse_manhattan_command(&command[1..]);
        }

        // Handle 'manhattan-batch' command
        if cmd == "manhattan-batch" {
            return Self::parse_manhattan_batch_command(&command[1..]);
        }

        // Handle 'loci' command
        if cmd == "loci" {
            return Self::parse_loci_command(&command[1..]);
        }

        // Handle 'ingest' command
        if cmd == "ingest" {
            return Self::parse_ingest_command(&command[1..]);
        }

        // Handle 'stress' command
        if cmd == "stress" {
            return Self::parse_stress_command(&command[1..]);
        }

        // Expect: export <type> <input> <output> [args...]
        if cmd != "export" {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Distributed mode supports: export, summary, manhattan, manhattan-batch, loci, ingest. Got: '{}'\n\
                     Examples:\n  \
                     pool submit mypool -- export parquet gs://bucket/input.ht gs://bucket/output/\n  \
                     pool submit mypool -- summary gs://bucket/input.ht\n  \
                     pool submit mypool -- manhattan --exome gs://bucket/exome.ht --output gs://bucket/out/\n  \
                     pool submit mypool -- manhattan-batch --assets-json ./assets.json --output-dir gs://bucket/manhattans/\n  \
                     pool submit mypool -- loci --dir gs://bucket/manhattan_output/ --exome gs://bucket/exome.ht\n  \
                     pool submit mypool -- ingest manhattan --input-dir gs://bucket/manhattans/ --clickhouse-url http://ch:8123",
                    cmd
                ),
            )));
        }

        if command.len() < 4 {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Export command requires: export <type> <input> <output>\n\
                 Example: export parquet gs://bucket/input.ht gs://bucket/output/",
            )));
        }

        let export_type = &command[1];
        let input_path = command[2].clone();
        let output_path = command[3].clone();

        // Parse optional arguments (--where, --interval)
        let mut filters = Vec::new();
        let mut intervals = Vec::new();
        let mut i = 4;
        while i < command.len() {
            match command[i].as_str() {
                "--where" => {
                    if i + 1 < command.len() {
                        filters.push(command[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--interval" => {
                    if i + 1 < command.len() {
                        intervals.push(command[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let job_spec = match export_type.as_str() {
            "parquet" => JobSpec::ExportParquet {
                output_path,
            },
            "json" => JobSpec::ExportJson {
                output_path,
                group_by: None,
            },
            "clickhouse" => {
                // Format: export clickhouse <input> <url> <table>
                // command[2] = input, command[3] = url, command[4] = table
                if command.len() < 5 {
                    return Err(HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Export clickhouse requires: export clickhouse <input> <url> <table>\n\
                         Example: export clickhouse gs://bucket/input.ht http://clickhouse:8123 my_table",
                    )));
                }
                let clickhouse_url = command[3].clone();
                let table_name = command[4].clone();

                JobSpec::ExportClickhouse {
                    clickhouse_url,
                    table_name,
                }
            },
            other => {
                return Err(HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Unsupported export type for distributed mode: '{}'\n\
                         Supported types: parquet, json, clickhouse",
                        other
                    ),
                )));
            }
        };

        Ok((input_path, job_spec, filters, intervals))
    }

    /// Parse a `manhattan` command into a ManhattanSpec job.
    ///
    /// Supports: manhattan --exome <path> --genome <path> --output <path> [--threshold ...] ...
    pub(crate) fn parse_manhattan_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, ManhattanSpec};

        // Parse named arguments
        let mut exome: Option<String> = None;
        let mut exome_annotations: Option<String> = None;
        let mut genome: Option<String> = None;
        let mut genome_annotations: Option<String> = None;
        let mut gene_burden: Option<String> = None;
        let mut genes: Option<String> = None;
        let mut output: Option<String> = None;
        let mut threshold: f64 = 5e-8;
        let mut gene_threshold: f64 = 2.5e-6;
        let mut locus_threshold: f64 = 0.01;
        let mut locus_window: i32 = 1_000_000;
        let mut locus_plots = false;
        let mut min_variants_per_locus: usize = 1;
        let mut skip_composite = false;
        let mut width: u32 = 3000;
        let mut height: u32 = 800;
        let mut y_field = "Pvalue".to_string();
        let mut scan_only = false;
        let mut aggregate_only = false;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--exome" => {
                    if i + 1 < args.len() {
                        exome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome-annotations" => {
                    if i + 1 < args.len() {
                        exome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome" => {
                    if i + 1 < args.len() {
                        genome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome-annotations" => {
                    if i + 1 < args.len() {
                        genome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-burden" => {
                    if i + 1 < args.len() {
                        gene_burden = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genes" => {
                    if i + 1 < args.len() {
                        genes = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--output" => {
                    if i + 1 < args.len() {
                        output = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" | "--variant-threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().unwrap_or(5e-8);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().unwrap_or(2.5e-6);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-threshold" => {
                    if i + 1 < args.len() {
                        locus_threshold = args[i + 1].parse().unwrap_or(0.01);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().unwrap_or(1_000_000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = true;
                    i += 1;
                }
                "--min-variants-per-locus" => {
                    if i + 1 < args.len() {
                        min_variants_per_locus = args[i + 1].parse().unwrap_or(1);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--no-composite" => {
                    skip_composite = true;
                    i += 1;
                }
                "--width" => {
                    if i + 1 < args.len() {
                        width = args[i + 1].parse().unwrap_or(3000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--height" => {
                    if i + 1 < args.len() {
                        height = args[i + 1].parse().unwrap_or(800);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--y-field" => {
                    if i + 1 < args.len() {
                        y_field = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--scan-only" => {
                    scan_only = true;
                    i += 1;
                }
                "--aggregate-only" => {
                    aggregate_only = true;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Validate: need at least one input table and an output
        let output_path = output.ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Manhattan command requires --output <path>",
            ))
        })?;

        // Determine primary input for partition counting
        let input_path = exome
            .as_ref()
            .or(genome.as_ref())
            .or(gene_burden.as_ref())
            .cloned()
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Manhattan command requires at least one input table: --exome, --genome, or --gene-burden",
                ))
            })?;

        let spec = ManhattanSpec {
            // Identity metadata - None for single mode, extracted from output path by coordinator
            phenotype: None,
            ancestry: None,
            exome,
            exome_annotations,
            genome,
            genome_annotations,
            gene_burden,
            genes,
            exome_exp_p: None,  // Not supported in single-job CLI mode
            genome_exp_p: None, // Not supported in single-job CLI mode
            threshold,
            gene_threshold,
            locus_threshold,
            locus_window,
            locus_plots,
            min_variants_per_locus,
            width,
            height,
            y_field,
            output_path,
            layout: None,  // Computed by coordinator before dispatch
            y_scale: None, // Computed by coordinator before dispatch
            contig_lengths: None, // Computed by submit_distributed
            skip_composite,
            exome_partitions: None, // Computed by submit_distributed
            genome_partitions: None, // Computed by submit_distributed
            styling: crate::manhattan::config::ManhattanConfig::default(),
        };

        let mode = if scan_only {
            crate::distributed::message::ExecutionMode::ScanOnly
        } else if aggregate_only {
            crate::distributed::message::ExecutionMode::AggregateOnly
        } else {
            crate::distributed::message::ExecutionMode::Full
        };

        Ok((input_path, JobSpec::Manhattan { spec, mode }, Vec::new(), Vec::new()))
    }

    /// Parse a `manhattan-batch` command into a ManhattanBatch job.
    ///
    /// Supports: manhattan-batch --config <path> or --assets-json <path> --output-dir <path> [--analysis-ids <id,...>] ...
    pub(crate) fn parse_manhattan_batch_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, ManhattanSpec};
        use crate::manhattan::batch::{load_and_group_assets, create_specs, BatchConfig};
        use crate::manhattan::config::ManhattanJobConfig;

        // Parse named arguments
        let mut config_path: Option<String> = None;
        let mut assets_json: Option<String> = None;
        let mut output_dir: Option<String> = None;
        let mut analysis_ids: Option<Vec<String>> = None;
        let mut ancestries: Option<Vec<String>> = None;
        let mut sample: Option<f64> = None;
        let mut limit: Option<usize> = None;
        let mut genes: Option<String> = None;
        let mut exome_annotations: Option<String> = None;
        let mut genome_annotations: Option<String> = None;
        let mut threshold: Option<f64> = None;
        let mut gene_threshold: Option<f64> = None;
        let mut locus_threshold: Option<f64> = None;
        let mut locus_window: Option<i32> = None;
        let mut locus_plots: Option<bool> = None;
        let mut width: Option<u32> = None;
        let mut height: Option<u32> = None;
        let mut y_field: Option<String> = None;
        let mut scan_only = false;
        let mut aggregate_only = false;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--config" => {
                    if i + 1 < args.len() {
                        config_path = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--assets-json" => {
                    if i + 1 < args.len() {
                        assets_json = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--output-dir" => {
                    if i + 1 < args.len() {
                        output_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--analysis-ids" => {
                    if i + 1 < args.len() {
                        let ids: Vec<String> = args[i + 1]
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        if !ids.is_empty() {
                            analysis_ids = Some(ids);
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--ancestries" => {
                    if i + 1 < args.len() {
                        let ancs: Vec<String> = args[i + 1]
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        if !ancs.is_empty() {
                            ancestries = Some(ancs);
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--sample" => {
                    if i + 1 < args.len() {
                        sample = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--limit" => {
                    if i + 1 < args.len() {
                        limit = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genes" => {
                    if i + 1 < args.len() {
                        genes = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome-annotations" => {
                    if i + 1 < args.len() {
                        exome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome-annotations" => {
                    if i + 1 < args.len() {
                        genome_annotations = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" | "--variant-threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-threshold" => {
                    if i + 1 < args.len() {
                        locus_threshold = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = Some(true);
                    i += 1;
                }
                "--width" => {
                    if i + 1 < args.len() {
                        width = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--height" => {
                    if i + 1 < args.len() {
                        height = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--y-field" => {
                    if i + 1 < args.len() {
                        y_field = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--scan-only" => {
                    scan_only = true;
                    i += 1;
                }
                "--aggregate-only" => {
                    aggregate_only = true;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Load config file if provided
        let job_config = if let Some(ref path) = config_path {
            ManhattanJobConfig::load(std::path::Path::new(path))?
        } else {
            ManhattanJobConfig::default()
        };

        // Merge CLI arguments with config (CLI overrides config)
        let assets_json = assets_json
            .or(job_config.job.assets_json.clone())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "manhattan-batch requires --assets-json <path> or job.assets_json in config",
                ))
            })?;

        let output_dir = output_dir
            .or(job_config.job.output_dir.clone())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "manhattan-batch requires --output-dir <path> or job.output_dir in config",
                ))
            })?;

        // Merge other settings (CLI overrides config)
        let analysis_ids = analysis_ids.or_else(|| {
            if job_config.job.analysis_ids.is_empty() {
                None
            } else {
                Some(job_config.job.analysis_ids.clone())
            }
        });
        let ancestries = ancestries.or_else(|| {
            if job_config.job.ancestries.is_empty() {
                None
            } else {
                Some(job_config.job.ancestries.clone())
            }
        });
        let sample = sample.or(job_config.job.sample);
        let limit = limit.or(job_config.job.limit);
        let genes = genes.or(job_config.job.genes.clone());
        let exome_annotations = exome_annotations.or(job_config.job.exome_annotations.clone());
        let genome_annotations = genome_annotations.or(job_config.job.genome_annotations.clone());
        let threshold = threshold.unwrap_or(job_config.job.threshold);
        let gene_threshold = gene_threshold.unwrap_or(job_config.job.gene_threshold);
        let locus_threshold = locus_threshold.unwrap_or(job_config.job.locus_threshold);
        let locus_window = locus_window.unwrap_or(job_config.job.locus_window);
        let locus_plots = locus_plots.unwrap_or(job_config.job.locus_plots);
        let min_variants_per_locus = job_config.job.min_variants_per_locus;
        let width = width.unwrap_or(job_config.job.width);
        let height = height.unwrap_or(job_config.job.height);
        let y_field = y_field.unwrap_or(job_config.job.y_field.clone());
        let styling = job_config.styling();

        // Load and group assets
        let inputs = load_and_group_assets(&assets_json, analysis_ids.as_deref(), ancestries.as_deref(), sample, limit)?;

        if inputs.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No phenotypes found in assets JSON (check filters if specified)",
            )));
        }

        // Create batch config
        let config = BatchConfig {
            output_dir,
            threshold,
            gene_threshold,
            locus_threshold,
            locus_window,
            locus_plots,
            min_variants_per_locus,
            width,
            height,
            y_field,
            genes_path: genes,
            exome_annotations,
            genome_annotations,
            styling,
        };

        // Convert to specs
        let specs: Vec<ManhattanSpec> = create_specs(inputs, &config);

        // For batch jobs, we need a dummy input path for the coordinator
        // The actual tables are specified per-spec. We use the first available
        // table path as the "primary" for any initialization the coordinator needs.
        let primary_input = specs
            .iter()
            .find_map(|s| s.primary_input_path())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "batch".to_string());

        let scan_only = scan_only || job_config.job.scan_only;
        let aggregate_only = aggregate_only || job_config.job.aggregate_only;

        let mode = if scan_only {
            crate::distributed::message::ExecutionMode::ScanOnly
        } else if aggregate_only {
            crate::distributed::message::ExecutionMode::AggregateOnly
        } else {
            crate::distributed::message::ExecutionMode::Full
        };

        Ok((primary_input, JobSpec::ManhattanBatch { specs, mode }, Vec::new(), Vec::new()))
    }

    /// Parse a `loci` command into a LociSpec job.
    pub(crate) fn parse_loci_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, LociSpec};

        let mut output_dir: Option<String> = None;
        let mut exome: Option<String> = None;
        let mut genome: Option<String> = None;
        let mut gene_burden: Option<String> = None;
        let mut threshold: f64 = 5e-8;
        let mut gene_threshold: f64 = 2.5e-6;
        let mut locus_window: i32 = 1_000_000;
        let mut locus_plots: bool = false;
        let mut min_variants_per_locus: usize = 1;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--dir" => {
                    if i + 1 < args.len() {
                        output_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--exome" => {
                    if i + 1 < args.len() {
                        exome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--genome" => {
                    if i + 1 < args.len() {
                        genome = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-burden" => {
                    if i + 1 < args.len() {
                        gene_burden = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threshold" => {
                    if i + 1 < args.len() {
                        threshold = args[i + 1].parse().unwrap_or(5e-8);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--gene-threshold" => {
                    if i + 1 < args.len() {
                        gene_threshold = args[i + 1].parse().unwrap_or(2.5e-6);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-window" => {
                    if i + 1 < args.len() {
                        locus_window = args[i + 1].parse().unwrap_or(1_000_000);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--locus-plots" => {
                    locus_plots = true;
                    i += 1;
                }
                "--min-variants-per-locus" => {
                    if i + 1 < args.len() {
                        min_variants_per_locus = args[i + 1].parse().unwrap_or(1);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        let output_dir = output_dir.ok_or_else(|| {
            HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Loci command requires --dir <manhattan_output_directory>",
            ))
        })?;

        if exome.is_none() && genome.is_none() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Loci command requires at least one of --exome or --genome",
            )));
        }

        let spec = LociSpec {
            output_dir: output_dir.clone(),
            exome_results: exome,
            genome_results: genome,
            gene_burden,
            locus_window,
            threshold,
            gene_threshold,
            locus_plots,
            min_variants_per_locus,
        };

        // Use output_dir as the "input_path" for job tracking
        Ok((output_dir, JobSpec::Loci(spec), Vec::new(), Vec::new()))
    }

    /// Parse a `stress` command into a StressSpec job.
    pub(crate) fn parse_stress_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{JobSpec, StressSpec};

        let mut partitions = 100;
        let mut cpu_secs = 0.0;
        let mut memory_mb = 0;
        let mut read_path = None;
        let mut write_dir = None;
        let mut generate_read_data = false;
        let mut read_data_size_mb = 32;
        let mut leak_memory_mb = None;
        let mut skip_memory_check = false;
        let mut memory_jitter_pct = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--partitions" | "--tasks" => {
                    if i + 1 < args.len() {
                        partitions = args[i + 1].parse().unwrap_or(100);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--cpu-secs" => {
                    if i + 1 < args.len() {
                        cpu_secs = args[i + 1].parse().unwrap_or(0.0);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--memory-mb" => {
                    if i + 1 < args.len() {
                        memory_mb = args[i + 1].parse().unwrap_or(0);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--read-path" => {
                    if i + 1 < args.len() {
                        read_path = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--write-dir" => {
                    if i + 1 < args.len() {
                        write_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--generate-read-data" => {
                    generate_read_data = true;
                    i += 1;
                }
                "--read-data-size-mb" => {
                    if i + 1 < args.len() {
                        read_data_size_mb = args[i + 1].parse().unwrap_or(32);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--leak-memory-mb" => {
                    if i + 1 < args.len() {
                        leak_memory_mb = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--skip-memory-check" => {
                    skip_memory_check = true;
                    i += 1;
                }
                "--memory-jitter-pct" => {
                    if i + 1 < args.len() {
                        memory_jitter_pct = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => i += 1,
            }
        }

        // Validate: --generate-read-data requires --write-dir
        if generate_read_data && write_dir.is_none() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--generate-read-data requires --write-dir to be set",
            )));
        }

        // Safety warning for dangerous memory options
        if skip_memory_check || leak_memory_mb.is_some() {
            use owo_colors::OwoColorize;
            eprintln!("{}", "WARNING: Using unsafe memory options - worker may be killed by OOM".yellow().bold());
        }

        let spec = StressSpec {
            partitions,
            cpu_secs,
            memory_mb,
            read_path,
            write_dir,
            generate_read_data,
            read_data_size_mb,
            leak_memory_mb,
            skip_memory_check,
            memory_jitter_pct,
        };

        // Use a dummy input path and empty filters since stress tests don't read Hail tables
        Ok(("stress_synthetic".to_string(), JobSpec::Stress(spec), Vec::new(), Vec::new()))
    }

    /// Parse an `ingest` command into an IngestManhattan job.
    ///
    /// Supports: ingest manhattan --input-dir <path> --clickhouse-url <url> [--database <db>]
    pub(crate) fn parse_ingest_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        if args.is_empty() {
            return Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Ingest command requires a subcommand: ingest manhattan ...",
            )));
        }

        let subcommand = args[0].as_str();

        match subcommand {
            "manhattan" => Self::parse_ingest_manhattan_command(&args[1..]),
            other => Err(HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Unknown ingest subcommand: '{}'\n\
                     Supported: manhattan",
                    other
                ),
            ))),
        }
    }

    /// Parse `ingest manhattan` command arguments.
    pub(crate) fn parse_ingest_manhattan_command(
        args: &[String],
    ) -> Result<(String, crate::distributed::message::JobSpec, Vec<String>, Vec<String>)> {
        use crate::distributed::message::{InitStrategy, JobSpec};
        use crate::manhattan::config::ManhattanJobConfig;

        let mut config_path: Option<String> = None;
        let mut input_dir: Option<String> = None;
        let mut clickhouse_url: Option<String> = None;
        let mut database: Option<String> = None;
        let mut init_strategy: Option<InitStrategy> = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--config" => {
                    if i + 1 < args.len() {
                        config_path = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--input-dir" => {
                    if i + 1 < args.len() {
                        input_dir = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--clickhouse-url" => {
                    if i + 1 < args.len() {
                        clickhouse_url = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--database" => {
                    if i + 1 < args.len() {
                        database = Some(args[i + 1].clone());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--init-strategy" => {
                    if i + 1 < args.len() {
                        init_strategy = Some(match args[i + 1].to_lowercase().as_str() {
                            "create" => InitStrategy::Create,
                            "replace" => InitStrategy::Replace,
                            "append" => InitStrategy::Append,
                            other => {
                                return Err(HailError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    format!(
                                        "Invalid init-strategy '{}'. Must be: create, replace, or append",
                                        other
                                    ),
                                )));
                            }
                        });
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Load config file if provided
        let job_config = if let Some(ref path) = config_path {
            ManhattanJobConfig::load(std::path::Path::new(path))?
        } else {
            ManhattanJobConfig::default()
        };

        // Merge CLI args with config (CLI overrides)
        let input_dir = input_dir
            .or(job_config.ingest_input_dir())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Ingest manhattan requires --input-dir <gcs_path> or ingest.input_dir/job.output_dir in config\n\
                     Example: ingest manhattan --input-dir gs://bucket/manhattans/ --clickhouse-url http://ch:8123",
                ))
            })?;

        let clickhouse_url = clickhouse_url
            .or(job_config.ingest.clickhouse_url.clone())
            .ok_or_else(|| {
                HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Ingest manhattan requires --clickhouse-url <url> or ingest.clickhouse_url in config\n\
                     Example: ingest manhattan --input-dir gs://bucket/manhattans/ --clickhouse-url http://ch:8123",
            ))
        })?;

        // Merge database and init_strategy with config defaults
        let database = database.unwrap_or(job_config.ingest.database.clone());
        let init_strategy = init_strategy.unwrap_or_else(|| {
            match job_config.ingest.init_strategy.to_lowercase().as_str() {
                "replace" => InitStrategy::Replace,
                "append" => InitStrategy::Append,
                _ => InitStrategy::Create,
            }
        });

        let spec = JobSpec::IngestManhattan {
            input_dir: input_dir.clone(),
            clickhouse_url,
            database,
            init_strategy,
        };

        // Use input_dir as the "input_path" for job tracking
        Ok((input_dir, spec, Vec::new(), Vec::new()))
    }
}
