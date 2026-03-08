//! Manhattan plot generation commands.

use crate::cli::{LociArgs, LocusArgs, ManhattanArgs, ManhattanBatchArgs};
use crate::commands::utils::progress_style_spinner;
use crate::manhattan::batch::{load_and_group_assets, BatchSummary};
use crate::manhattan::config::ManhattanJobConfig;
use crate::manhattan::data::{
    extract_plot_data, ManhattanSidecar, SidecarChromosome, SidecarImage, SidecarThreshold,
    SidecarYAxis, SignificantHit, VariantSource,
};
use crate::manhattan::genes::{process_gene_burden, GeneMap};
use crate::manhattan::layout::{ChromosomeLayout, YScale};
use crate::manhattan::pipeline::{aggregate_shards_and_render, run_integrated_pipeline, PipelineConfig};
use crate::manhattan::reference::get_contig_lengths;
use crate::manhattan::render::ManhattanRenderer;
use crossbeam_channel::bounded;
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::thread;

pub fn run_manhattan(args: ManhattanArgs) -> Result<()> {
    use crate::manhattan::annotate::Annotator;

    // Check if aggregating from distributed shards
    if let Some(shards_path) = &args.from_shards {
        use crate::manhattan::pipeline::composite_partial_pngs;

        println!(
            "{} Aggregating distributed shards",
            "Mode:".cyan().bold()
        );

        let output_prefix = args.output.as_deref().unwrap_or("manhattan");

        // Detect whether shards are PNG or JSON by checking what files exist
        let shards_dir = shards_path.trim_end_matches('/');
        let png_check = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/part-*.png", shards_dir)])
            .output();

        let has_pngs = png_check.map(|o| o.status.success()).unwrap_or(false);

        if has_pngs {
            println!("  Detected PNG shards, compositing images...");
            let final_png = format!("{}.png", output_prefix);
            return composite_partial_pngs(
                shards_path,
                &final_png,
                args.width,
                args.height,
                args.threshold,
            );
        } else {
            println!("  Detected JSON shards, aggregating points...");
            return aggregate_shards_and_render(
                shards_path,
                output_prefix,
                args.width,
                args.height,
                args.threshold,
            );
        }
    }

    // Check if using new multi-table mode (exome/genome inputs)
    let use_pipeline = args.exome.is_some() || args.genome.is_some();

    if use_pipeline {
        println!(
            "{} Running integrated multi-table pipeline",
            "Mode:".cyan().bold()
        );

        // Convert CLI args to PipelineConfig
        let config = PipelineConfig {
            exome: args.exome.clone(),
            exome_annotations: args.exome_annotations.clone(),
            genome: args.genome.clone(),
            genome_annotations: args.genome_annotations.clone(),
            gene_burden: args.gene_burden.clone(),
            genes: args.genes.clone(),
            threshold: args.threshold,
            gene_threshold: args.gene_threshold,
            gene_maf_filter: args.gene_maf_filter,
            locus_threshold: args.locus_threshold,
            locus_window: args.locus_window,
            locus_plots: args.locus_plots,
            min_variants_per_locus: args.min_variants_per_locus,
            output: args.output.clone(),
            width: args.width,
            height: args.height,
            y_field: args.y_field.clone(),
            scan_only: args.scan_only,
            aggregate_only: args.aggregate_only,
        };

        return run_integrated_pipeline(&config);
    }

    // Legacy single-table mode below
    println!(
        "{} Running legacy single-table mode",
        "Mode:".cyan().bold()
    );

    // Load Gene Map if provided
    let gene_map = if let Some(path) = &args.genes {
        println!(
            "{} {}",
            "Loading genes from:".green(),
            path.bright_white()
        );
        Some(GeneMap::load(path)?)
    } else {
        None
    };

    // Output directory setup
    let output_base = args.output.as_deref().unwrap_or("manhattan");

    // Process Gene Burden if provided
    if let Some(burden_path) = &args.gene_burden {
        println!(
            "{} {}",
            "Processing gene burden:".green(),
            burden_path.bright_white()
        );

        let (png, sidecar, regions) = process_gene_burden(
            burden_path,
            args.gene_threshold,
            args.width,
            args.height,
            gene_map.as_ref(),
        )?;

        let png_path = format!("{}.gene_manhattan.png", output_base);
        let json_path = format!("{}.gene_manhattan.json", output_base);

        fs::write(&png_path, png)?;
        fs::write(&json_path, sidecar)?;

        println!(
            "{} {} + {}",
            "Saved gene plot:".green().bold(),
            png_path.bright_white(),
            json_path.bright_white()
        );
        println!(
            "{} {} regions of interest",
            "Found:".cyan(),
            regions.len().to_string().bright_white()
        );
    }

    // Process variant table if provided (existing logic)
    let table_path = match &args.table {
        Some(path) => path,
        None => {
            // If no variant table and no gene burden, error out
            if args.gene_burden.is_none() {
                eprintln!(
                    "{} Either --table, --exome, or --genome must be provided",
                    "Error:".red().bold()
                );
                std::process::exit(1);
            }
            // Gene burden only mode - we're done
            return Ok(());
        }
    };

    println!(
        "{} {}",
        "Generating Manhattan plot for:".green(),
        table_path.bright_white()
    );

    // 1. Open table
    let engine = QueryEngine::open_path(table_path)?;
    let num_partitions = engine.num_partitions();

    // 2. Get contig lengths and filter to requested chromosomes
    let all_contigs = get_contig_lengths(&engine);

    let contigs: Vec<(String, u32)> = if args.chrom == "all" {
        all_contigs
    } else {
        let requested: Vec<&str> = args.chrom.split(',').collect();
        all_contigs
            .into_iter()
            .filter(|(name, _)| requested.contains(&name.as_str()))
            .collect()
    };

    if contigs.is_empty() {
        eprintln!(
            "{} No matching chromosomes found for --chrom {}",
            "Error:".red().bold(),
            args.chrom
        );
        std::process::exit(1);
    }

    // 3. Init components
    let layout = ChromosomeLayout::new(&contigs, args.width, 4);
    let mut renderer = ManhattanRenderer::new(args.width, args.height);
    let _annotator = Annotator::new(args.annotate, args.annotate_fields)?;

    // Use log-log scale: linear 0-10, then log 10+
    // Max of 300 handles extremely significant hits (p ~ 10^-300)
    let y_scale = YScale::new(args.height, 300.0);

    // Draw threshold line
    let threshold_y = y_scale.threshold_y(args.threshold);
    renderer.render_threshold_line(threshold_y, args.width);

    let mut significant_hits: Vec<SignificantHit> = Vec::new();

    // 4. Parallel Scan & Render using producer-consumer pattern
    // Workers extract data and compute layout, main thread only renders
    println!("{}", "Scanning rows (parallel)...".dimmed());
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    /// Batch of render data sent from workers to main thread
    struct RenderBatch {
        /// Points to render: (x, y, color_idx) - color_idx is 0 or 1 for alternating colors
        points: Vec<(f32, f32, u8)>,
        /// Significant hits identified
        hits: Vec<SignificantHit>,
        /// Number of rows processed in this batch
        row_count: usize,
    }

    // Channel for sending batches from workers to main thread
    let (tx, rx) = bounded::<Result<RenderBatch>>(100);

    // Clone data for the background thread
    let table_path_owned = table_path.to_string();
    let y_field = args.y_field.clone();
    let threshold = args.threshold;
    let width = args.width;
    let height = args.height;
    let layout_clone = layout.clone();
    let y_scale_copy = y_scale;
    let limit = args.limit;

    // Spawn producer thread that drives parallel workers
    let producer_handle = thread::spawn(move || {
        let result: Result<()> = (|| {
            // Process partitions in parallel
            let _ = QueryEngine::open_path(&table_path_owned)?; // Verify path is valid
            let partitions_to_process: Vec<usize> = (0..num_partitions).collect();

            partitions_to_process
                .into_par_iter()
                .try_for_each_with(tx.clone(), |sender, partition_idx| -> Result<()> {
                    // Each worker opens its own engine for thread safety
                    let worker_engine = QueryEngine::open_path(&table_path_owned)?;
                    let iter = worker_engine.scan_partition_iter(partition_idx, &[])?;

                    let mut batch = RenderBatch {
                        points: Vec::with_capacity(2000),
                        hits: Vec::new(),
                        row_count: 0,
                    };

                    for row_result in iter {
                        let row = row_result?;
                        batch.row_count += 1;

                        if let Some(point) = extract_plot_data(&row, &y_field) {
                            // Normalize contig name (strip "chr" prefix)
                            let contig_name = if point.contig.starts_with("chr") {
                                &point.contig[3..]
                            } else {
                                &point.contig
                            };

                            if let Some(x) = layout_clone.get_x(contig_name, point.position) {
                                let y = y_scale_copy.get_y(point.neg_log10_p);

                                // Get color index (0 or 1) for alternating colors
                                let color = layout_clone.get_color(contig_name);
                                let color_idx = if color == "#404040" { 0u8 } else { 1u8 };

                                batch.points.push((x, y, color_idx));

                                if point.pvalue < threshold {
                                    let hit = SignificantHit {
                                        variant_id: format!("{}:{}", point.contig, point.position),
                                        pvalue: point.pvalue,
                                        x_px: x,
                                        y_px: y,
                                        x_normalized: x / width as f32,
                                        y_normalized: y / height as f32,
                                        annotations: serde_json::Value::Null,
                                    };
                                    batch.hits.push(hit);
                                }
                            }
                        }

                        // Send batch when full
                        if batch.points.len() >= 2000 {
                            if sender.send(Ok(batch)).is_err() {
                                return Err(genohype_core::HailError::Io(std::io::Error::new(
                                    std::io::ErrorKind::BrokenPipe,
                                    "Receiver dropped",
                                )));
                            }
                            batch = RenderBatch {
                                points: Vec::with_capacity(2000),
                                hits: Vec::new(),
                                row_count: 0,
                            };
                        }
                    }

                    // Send remaining data
                    if batch.row_count > 0 {
                        if sender.send(Ok(batch)).is_err() {
                            return Err(genohype_core::HailError::Io(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "Receiver dropped",
                            )));
                        }
                    }

                    Ok(())
                })
        })();

        if let Err(e) = result {
            let _ = tx.send(Err(e));
        }
        // tx drops here, closing the channel
    });

    // Consumer (Main Thread) - only does lightweight rendering
    let colors = ["#404040", "#4682B4"];
    let mut row_count: usize = 0;

    for batch_result in rx {
        let batch = batch_result?;

        // Render points (serial, but fast pixel operations)
        for (x, y, color_idx) in batch.points {
            renderer.render_point(x, y, colors[color_idx as usize], 0.5);
        }

        // Aggregate significant hits
        significant_hits.extend(batch.hits);

        // Update progress
        row_count += batch.row_count;
        if row_count % 50_000 < batch.row_count {
            pb.set_message(format!("{} rows scanned...", row_count));
        }

        // Check limit (approximate - may process slightly more due to batching)
        if let Some(lim) = limit {
            if row_count >= lim {
                break;
            }
        }
    }

    // Wait for producer to finish (may already be done if limit reached)
    let _ = producer_handle.join();

    pb.finish_and_clear();
    println!(
        "{} {} rows, {} significant hits",
        "Scanned:".green(),
        row_count.to_string().bright_white(),
        significant_hits.len().to_string().bright_white()
    );

    // 5. Annotate significant hits (if annotation table provided)
    // Note: annotation requires key reconstruction which needs the full row key.
    // For now we skip key-based annotation; the sidecar still contains hit positions.
    // TODO: reconstruct EncodedValue keys for annotation lookup

    // 6. Write output
    let output_base = args.output.unwrap_or_else(|| "manhattan".to_string());

    let png_data = renderer.encode_png()?;
    let png_path = format!("{}.png", output_base);
    let mut f = File::create(&png_path)?;
    f.write_all(&png_data)?;

    let sidecar = ManhattanSidecar {
        image: SidecarImage {
            width: args.width,
            height: args.height,
        },
        chromosomes: layout
            .chromosome_info
            .iter()
            .map(|ci| SidecarChromosome {
                name: ci.name.clone(),
                x_start_px: ci.x_start_px,
                x_end_px: ci.x_end_px,
                color: ci.color.clone(),
            })
            .collect(),
        threshold: SidecarThreshold {
            pvalue: args.threshold,
            y_px: threshold_y,
        },
        y_axis: SidecarYAxis {
            log_threshold: 10.0,
            linear_fraction: 0.6,
            max_neg_log_p: 300.0,
        },
        significant_hits,
    };

    let json_path = format!("{}.json", output_base);
    let mut f = File::create(&json_path)?;
    serde_json::to_writer_pretty(&mut f, &sidecar)?;

    println!(
        "{} {} + {}",
        "Saved:".green().bold(),
        png_path.bright_white(),
        json_path.bright_white()
    );

    Ok(())
}

/// Run a batch of Manhattan plots from assets JSON.
///
/// This command parses the assets JSON file, groups entries by phenotype,
/// and submits a batch job to the coordinator for parallel processing.
pub fn run_manhattan_batch(args: ManhattanBatchArgs) -> Result<()> {
    // Load config file if provided
    let job_config = if let Some(ref path) = args.config {
        ManhattanJobConfig::load(Path::new(path))?
    } else {
        ManhattanJobConfig::default()
    };

    // Merge CLI args with config (CLI overrides)
    let assets_json = args
        .assets_json
        .clone()
        .or(job_config.job.assets_json.clone())
        .ok_or_else(|| {
            genohype_core::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "manhattan-batch requires --assets-json or job.assets_json in config",
            ))
        })?;

    let output_dir = args
        .output_dir
        .clone()
        .or(job_config.job.output_dir.clone())
        .ok_or_else(|| {
            genohype_core::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "manhattan-batch requires --output-dir or job.output_dir in config",
            ))
        })?;

    let analysis_ids = args.analysis_ids.clone().or_else(|| {
        if job_config.job.analysis_ids.is_empty() {
            None
        } else {
            Some(job_config.job.analysis_ids.clone())
        }
    });
    let ancestries = args.ancestries.clone().or_else(|| {
        if job_config.job.ancestries.is_empty() {
            None
        } else {
            Some(job_config.job.ancestries.clone())
        }
    });
    let sample = args.sample.or(job_config.job.sample);
    let limit = args.limit.or(job_config.job.limit);

    println!(
        "{} Manhattan batch validation",
        "Running".green().bold()
    );
    println!("  Assets JSON: {}", assets_json.bright_white());
    println!("  Output dir: {}", output_dir.bright_white());

    if let Some(ref ids) = analysis_ids {
        println!("  Analysis IDs filter: {:?}", ids);
    }

    if let Some(ref ancs) = ancestries {
        println!("  Ancestries filter: {:?}", ancs);
    }

    if let Some(sample_val) = sample {
        println!("  Sample: {:.0}%", sample_val * 100.0);
    }

    if let Some(limit_val) = limit {
        println!("  Limit: {}", limit_val);
    }

    println!();

    // Load and group assets
    let inputs = load_and_group_assets(
        &assets_json,
        analysis_ids.as_deref(),
        ancestries.as_deref(),
        sample,
        limit,
    )?;

    if inputs.is_empty() {
        println!(
            "{} No phenotypes found in assets JSON",
            "Warning:".yellow().bold()
        );
        println!("  Check your --analysis-ids filter or assets file content.");
        return Ok(());
    }

    // Build summary
    let summary = BatchSummary::from_inputs(&inputs);

    println!("{}", "Batch Summary".cyan().bold());
    println!(
        "  {} phenotypes found",
        summary.total_phenotypes.to_string().bright_white()
    );
    println!();

    // By input type
    println!("{}", "  By Input Type:".dimmed());
    if summary.combined > 0 {
        println!(
            "    Combined (exome+genome): {}",
            summary.combined.to_string().green()
        );
    }
    if summary.exome_only > 0 {
        println!(
            "    Exome only:              {}",
            summary.exome_only.to_string().cyan()
        );
    }
    if summary.genome_only > 0 {
        println!(
            "    Genome only:             {}",
            summary.genome_only.to_string().cyan()
        );
    }
    if summary.gene_burden_only > 0 {
        println!(
            "    Gene burden only:        {}",
            summary.gene_burden_only.to_string().yellow()
        );
    }
    println!();

    // By ancestry
    println!("{}", "  By Ancestry:".dimmed());
    let mut ancestries_sorted: Vec<_> = summary.by_ancestry.iter().collect();
    ancestries_sorted.sort_by_key(|(k, _)| *k);
    for (ancestry, count) in ancestries_sorted {
        println!("    {}: {}", ancestry.bright_white(), count);
    }
    println!();

    // Configuration summary
    println!("{}", "Configuration".cyan().bold());
    println!(
        "  Threshold: {}",
        format!("{:.0e}", args.threshold).bright_white()
    );
    println!(
        "  Gene threshold: {}",
        format!("{:.1e}", args.gene_threshold).bright_white()
    );
    if args.locus_plots {
        println!("  Locus plots: {}", "enabled".green());
        println!(
            "    Locus threshold: {}",
            format!("{}", args.locus_threshold).dimmed()
        );
        println!(
            "    Locus window: {} bp",
            args.locus_window.to_string().dimmed()
        );
    }
    println!("  Image size: {}x{}", args.width, args.height);

    if args.genes.is_some() {
        println!("  Genes table: {}", "specified".green());
    }
    if args.exome_annotations.is_some() {
        println!("  Exome annotations: {}", "specified".green());
    }
    if args.genome_annotations.is_some() {
        println!("  Genome annotations: {}", "specified".green());
    }

    println!();
    println!("{}", "Ready for Submission".green().bold());
    let mode_flag = if args.scan_only {
        " \\\n      --scan-only"
    } else if args.aggregate_only {
        " \\\n      --aggregate-only"
    } else {
        ""
    };
    if let Some(ref config_path) = args.config {
        println!(
            "  To submit this batch to a pool, run:\n    \
             genohype pool submit <pool> -- manhattan-batch \\\n      \
             --config {}{}",
            config_path, mode_flag
        );
    } else {
        println!(
            "  To submit this batch to a pool, run:\n    \
             genohype pool submit <pool> -- manhattan-batch \\\n      \
             --assets-json {} \\\n      \
             --output-dir {}{}",
            assets_json, output_dir, mode_flag
        );
    }

    Ok(())
}

pub fn run_loci(args: LociArgs) -> Result<()> {
    use crate::manhattan::aggregate::generate_loci_standalone;

    println!(
        "{} locus plots from {}",
        "Generating".green().bold(),
        args.dir.bright_white()
    );

    let loci = generate_loci_standalone(
        &args.dir,
        args.exome.as_deref(),
        args.genome.as_deref(),
        args.gene_burden.as_deref(),
        args.locus_window,
        args.threshold,
        args.gene_threshold,
        args.threads,
        args.locus_plots,
        args.min_variants_per_locus,
    )?;

    println!(
        "{} Generated {} locus plots",
        "Done:".green().bold(),
        loci.len()
    );

    Ok(())
}

pub fn run_locus(args: LocusArgs) -> Result<()> {
    use crate::manhattan::locus::{LocusPlotConfig, LocusRenderer, RenderVariant};
    use genohype_core::query::{KeyRange, KeyValue, QueryBound};

    // 1. Parse region string (chr:start-end)
    let parts: Vec<&str> = args.region.split([':', '-']).collect();
    if parts.len() != 3 {
        eprintln!(
            "{} Invalid region format. Use chr:start-end (e.g. chr1:100000-200000)",
            "Error:".red().bold()
        );
        std::process::exit(1);
    }
    let chrom = parts[0].to_string();
    let start_pos: i32 = parts[1].parse().unwrap_or_else(|_| {
        eprintln!(
            "{} Invalid start position: {}",
            "Error:".red().bold(),
            parts[1]
        );
        std::process::exit(1);
    });
    let end_pos: i32 = parts[2].parse().unwrap_or_else(|_| {
        eprintln!(
            "{} Invalid end position: {}",
            "Error:".red().bold(),
            parts[2]
        );
        std::process::exit(1);
    });

    println!(
        "{} {}:{}-{}",
        "Plotting region:".green(),
        chrom.bright_white(),
        start_pos.to_string().bright_white(),
        end_pos.to_string().bright_white()
    );

    // 2. Initialize Renderer
    let config = LocusPlotConfig {
        width: args.width,
        height: args.height,
        start_pos,
        end_pos,
        y_max: args.y_max,
    };
    let mut renderer = LocusRenderer::new(config);

    // 3. Helper to fetch variants from a table
    let fetch_variants = |path: &str,
                          source: VariantSource,
                          chrom: &str,
                          start: i32,
                          end: i32,
                          y_field: &str,
                          threshold: f64|
     -> Result<Vec<RenderVariant>> {
        println!(
            "{} {} variants from {}...",
            "Fetching".cyan(),
            match source {
                VariantSource::Exome => "exome",
                VariantSource::Genome => "genome",
            },
            path.bright_white()
        );
        let engine = QueryEngine::open_path(path)?;

        // Construct filters for efficient partition pruning and row filtering
        let filters = vec![
            // Filter by chromosome (locus.contig)
            KeyRange {
                field_path: vec!["locus".to_string(), "contig".to_string()],
                start: QueryBound::Included(KeyValue::String(chrom.to_string())),
                end: QueryBound::Included(KeyValue::String(chrom.to_string())),
            },
            // Filter by position range (locus.position)
            KeyRange {
                field_path: vec!["locus".to_string(), "position".to_string()],
                start: QueryBound::Included(KeyValue::Int32(start)),
                end: QueryBound::Included(KeyValue::Int32(end)),
            },
        ];

        let iter = engine.query_iter(&filters)?;
        let mut variants = Vec::new();

        for row_res in iter {
            let row = row_res?;
            if let Some(pt) = extract_plot_data(&row, y_field) {
                // Normalize contig name (strip "chr" prefix if present for comparison)
                let contig_normalized = if pt.contig.starts_with("chr") {
                    &pt.contig[3..]
                } else {
                    &pt.contig
                };
                let chrom_normalized = if chrom.starts_with("chr") {
                    &chrom[3..]
                } else {
                    chrom
                };

                // Ensure exact bounds check
                if pt.position >= start && pt.position <= end && contig_normalized == chrom_normalized
                {
                    variants.push(RenderVariant {
                        position: pt.position,
                        ref_allele: String::new(),
                        alt_allele: String::new(),
                        pvalue: pt.pvalue,
                        beta: None,
                        se: None,
                        af: None,
                        source,
                        is_significant: pt.pvalue < threshold,
                        ac_cases: None,
                        ac_controls: None,
                        af_cases: None,
                        af_controls: None,
                        association_ac: None,
                    });
                }
            }
        }
        println!(
            "  {} {} variants",
            "Found:".green(),
            variants.len().to_string().bright_white()
        );
        Ok(variants)
    };

    // 4. Fetch and Render variants
    let mut all_variants = Vec::new();

    if let Some(ref path) = args.genome {
        let vars = fetch_variants(
            path,
            VariantSource::Genome,
            &chrom,
            start_pos,
            end_pos,
            &args.y_field,
            args.threshold,
        )?;
        all_variants.extend(vars);
    }

    if let Some(ref path) = args.exome {
        let vars = fetch_variants(
            path,
            VariantSource::Exome,
            &chrom,
            start_pos,
            end_pos,
            &args.y_field,
            args.threshold,
        )?;
        all_variants.extend(vars);
    }

    if all_variants.is_empty() {
        println!(
            "{} No variants found in the specified region.",
            "Warning:".yellow()
        );
    }

    renderer.draw_variants(&all_variants);
    renderer.draw_threshold_line(args.threshold);

    // 5. Save Output
    let png_data = renderer.encode_png()?;
    std::fs::write(&args.output, png_data)?;
    println!(
        "{} {}",
        "Saved plot to:".green().bold(),
        args.output.bright_white()
    );

    Ok(())
}
