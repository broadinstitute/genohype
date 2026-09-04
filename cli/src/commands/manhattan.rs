//! Manhattan plot generation commands.

use crate::association_server::{serve_viewer, ViewerArtifacts, ViewerOptions};
use crate::cli::{LociArgs, LocusArgs, ManhattanArgs, ManhattanBatchArgs};
use crate::commands::utils::progress_style_spinner;
use crate::manhattan::batch::{
    load_and_group_assets, validate_gene_associations_only_inputs, BatchSummary,
};
use crate::manhattan::config::ManhattanJobConfig;
use crate::manhattan::data::{
    extract_association_point, extract_plot_data, ManhattanSidecar, SidecarChromosome,
    SidecarImage, SidecarThreshold, SidecarYAxis, SignificantHit, VariantSource,
};
use crate::manhattan::genes::{process_gene_burden, GeneMap};
use crate::manhattan::legacy_manifest::{
    merge_partition_summaries, source_ordinal, write_manifest, LegacyPartitionSummary,
};
use crate::manhattan::pipeline::{
    aggregate_shards_and_render, run_integrated_pipeline, PipelineConfig,
};
use crate::manhattan::render::ManhattanRenderer;
use genohype_association::{canonical_contig, canonical_contig_lengths, Assembly, GenomeLayoutV1};
use genohype_core::query::QueryEngine;
use genohype_core::{HailError, Result};
use indicatif::ProgressBar;
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::fs;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

// Keep local raster memory bounded independently of partition count. At the default
// 3000x800 dimensions, 12 transparent shards consume about 110 MiB.
const MAX_LOCAL_RASTER_SHARDS: usize = 16;
const LOCAL_RASTER_SHARD_BUDGET_BYTES: u64 = 192 * 1024 * 1024;

struct LegacyRasterShard {
    index: usize,
    renderer: ManhattanRenderer,
    significant_hits: Vec<SignificantHit>,
    partition_summaries: Vec<LegacyPartitionSummary>,
}

impl LegacyRasterShard {
    fn new(index: usize, width: u32, height: u32) -> Self {
        Self {
            index,
            renderer: ManhattanRenderer::new_transparent(width, height),
            significant_hits: Vec::new(),
            partition_summaries: Vec::new(),
        }
    }
}

fn local_raster_shard_count(num_partitions: usize, width: u32, height: u32) -> usize {
    if num_partitions == 0 {
        return 0;
    }
    let bytes_per_raster = u64::from(width)
        .saturating_mul(u64::from(height))
        .saturating_mul(4)
        .max(1);
    let memory_limited = (LOCAL_RASTER_SHARD_BUDGET_BYTES / bytes_per_raster).max(1) as usize;
    rayon::current_num_threads()
        .min(MAX_LOCAL_RASTER_SHARDS)
        .min(memory_limited)
        .min(num_partitions)
        .max(1)
}

/// Split partition order into non-empty contiguous ranges. Rendering each range in source
/// order and compositing ranges in this order makes overlapping point colors deterministic.
fn contiguous_partition_ranges(num_partitions: usize, shard_count: usize) -> Vec<Range<usize>> {
    if num_partitions == 0 || shard_count == 0 {
        return Vec::new();
    }
    let shard_count = shard_count.min(num_partitions);
    let base = num_partitions / shard_count;
    let remainder = num_partitions % shard_count;
    let mut start = 0;
    (0..shard_count)
        .map(|index| {
            let length = base + usize::from(index < remainder);
            let range = start..start + length;
            start += length;
            range
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn scan_legacy_partition(
    engine: &QueryEngine,
    partition_index: usize,
    row_limit: Option<usize>,
    y_field: &str,
    threshold: f64,
    width: u32,
    height: u32,
    layout: &GenomeLayoutV1,
    renderer: &mut ManhattanRenderer,
    significant_hits: &mut Vec<SignificantHit>,
) -> Result<LegacyPartitionSummary> {
    let iter = engine.scan_partition_iter(partition_index, &[])?;
    let mut summary = LegacyPartitionSummary::new(partition_index)?;

    for (row_index, row_result) in iter.enumerate() {
        if row_limit.is_some_and(|limit| row_index >= limit) {
            break;
        }
        let row = row_result?;
        summary.observe_source_row(&row)?;
        let ordinal = source_ordinal(partition_index, row_index)?;

        match extract_association_point(&row, y_field, ordinal) {
            Ok((point, warnings)) => {
                let x = layout.x_for(&point.contig, point.position);
                let rendered = x.is_some();
                let significant = point.pvalue < threshold;

                if let Some(x) = x {
                    let y = layout.y_scale.y_for(point.neg_log10_p);
                    let color = layout.color_for(&point.contig).ok_or_else(|| {
                        HailError::InvalidFormat(format!(
                            "canonical Manhattan layout has no color for {:?}",
                            point.contig
                        ))
                    })?;
                    renderer.render_point(x, y, color, 0.5);
                    if significant {
                        significant_hits.push(SignificantHit {
                            variant_id: format!("{}:{}", point.contig, point.position),
                            pvalue: point.pvalue,
                            x_px: x,
                            y_px: y,
                            x_normalized: x / width as f32,
                            y_normalized: y / height as f32,
                            annotations: serde_json::Value::Null,
                        });
                    }
                }

                summary.observe_valid_point(point, warnings, rendered, significant)?;
            }
            Err(_) => summary.observe_skipped_row()?,
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod legacy_raster_tests {
    use super::*;

    #[test]
    fn contiguous_shard_plan_is_balanced_complete_and_ordered() {
        let ranges = contiguous_partition_ranges(10, 3);
        assert_eq!(ranges, [0..4, 4..7, 7..10]);
        assert!(ranges.windows(2).all(|pair| pair[0].end == pair[1].start));
        assert_eq!(
            ranges.into_iter().flatten().collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn shard_plan_never_creates_empty_or_partition_count_sized_excess() {
        assert!(contiguous_partition_ranges(0, 8).is_empty());
        assert_eq!(contiguous_partition_ranges(3, 8), [0..1, 1..2, 2..3]);
        assert_eq!(
            local_raster_shard_count(1_849, 3_000, 800),
            rayon::current_num_threads().min(MAX_LOCAL_RASTER_SHARDS)
        );
        assert_eq!(local_raster_shard_count(1, u32::MAX, u32::MAX), 1);
    }
}

pub fn run_manhattan(args: ManhattanArgs) -> Result<()> {
    use crate::manhattan::annotate::Annotator;

    let viewer_options = args.view.then_some(ViewerOptions {
        port: args.port.unwrap_or(0),
        open: !args.no_open,
    });
    if args.view
        && (args.from_shards.is_some()
            || args.exome.is_some()
            || args.genome.is_some()
            || args.scan_only
            || args.aggregate_only)
    {
        return Err(HailError::InvalidFormat(
            "--view currently supports only legacy local --table Manhattan output".into(),
        ));
    }
    if args.view && args.table.is_none() {
        return Err(HailError::InvalidFormat(
            "--view requires a legacy --table input that produces one canonical raster and manifest"
                .into(),
        ));
    }
    if args.view
        && (args.partitioning.worker_id != 0
            || args.partitioning.total_workers != 1
            || args.progress_json)
    {
        return Err(HailError::InvalidFormat(
            "--view is a local foreground action and cannot run in distributed worker mode".into(),
        ));
    }
    if args.view
        && args
            .output
            .as_deref()
            .is_some_and(genohype_core::io::is_cloud_path)
    {
        return Err(HailError::InvalidFormat(
            "--view requires a local output prefix".into(),
        ));
    }

    // Check if aggregating from distributed shards
    if let Some(shards_path) = &args.from_shards {
        use crate::manhattan::pipeline::composite_partial_pngs;

        println!("{} Aggregating distributed shards", "Mode:".cyan().bold());

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
    println!("{} Running legacy single-table mode", "Mode:".cyan().bold());

    // Load Gene Map if provided
    let gene_map = if let Some(path) = &args.genes {
        println!("{} {}", "Loading genes from:".green(), path.bright_white());
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

    // 2. Build the canonical GRCh38 layout, filtered only when --chrom requests it.
    let all_contigs = canonical_contig_lengths(Assembly::Grch38);
    let contigs: Vec<(String, u64)> = if args.chrom == "all" {
        all_contigs
    } else {
        let requested: Vec<String> = args.chrom.split(',').filter_map(canonical_contig).collect();
        all_contigs
            .into_iter()
            .filter(|(name, _)| requested.contains(name))
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

    // 3. Initialize canonical geometry while retaining the legacy raster/sidecar files.
    let layout = GenomeLayoutV1::new(contigs, args.width, args.height, 4)
        .map_err(|error| HailError::InvalidFormat(error.to_string()))?;
    let mut renderer = ManhattanRenderer::new(args.width, args.height);
    let _annotator = Annotator::new(args.annotate, args.annotate_fields)?;
    let threshold_y = layout.y_scale.threshold_y(args.threshold).ok_or_else(|| {
        HailError::InvalidFormat("invalid Manhattan significance threshold".into())
    })?;
    renderer.render_threshold_line(threshold_y, args.width);

    // 4. Scan into a bounded number of transparent in-memory rasters. Each shard owns a
    // contiguous partition range and renders rows in source order. Rayon parallelizes the
    // expensive tiny-skia circle fills without creating one raster per source partition.
    println!("{}", "Scanning and rendering rows (parallel)...".dimmed());
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());
    let reported_rows = AtomicU64::new(0);

    let mut raster_shards = if let Some(limit) = args.limit {
        // A test limit remains an exact deterministic prefix of partition/row order. Limits
        // are primarily a test/debug facility, so use one shard rather than speculatively
        // scanning later partitions before the prefix boundary is known.
        let mut shards = Vec::new();
        if limit > 0 && num_partitions > 0 {
            let mut shard = LegacyRasterShard::new(0, args.width, args.height);
            let mut remaining = limit;
            for partition_index in 0..num_partitions {
                if remaining == 0 {
                    break;
                }
                let summary = scan_legacy_partition(
                    &engine,
                    partition_index,
                    Some(remaining),
                    &args.y_field,
                    args.threshold,
                    args.width,
                    args.height,
                    &layout,
                    &mut shard.renderer,
                    &mut shard.significant_hits,
                )?;
                remaining = remaining.saturating_sub(summary.row_count as usize);
                let scanned = reported_rows.fetch_add(summary.row_count, Ordering::Relaxed)
                    + summary.row_count;
                pb.set_message(format!("{scanned} rows scanned..."));
                shard.partition_summaries.push(summary);
            }
            shards.push(shard);
        }
        shards
    } else {
        let shard_count = local_raster_shard_count(num_partitions, args.width, args.height);
        let ranges = contiguous_partition_ranges(num_partitions, shard_count);
        pb.set_message(format!("using {} in-memory raster shards...", ranges.len()));

        ranges
            .into_par_iter()
            .enumerate()
            .map(|(index, partitions)| -> Result<LegacyRasterShard> {
                let mut shard = LegacyRasterShard::new(index, args.width, args.height);
                for partition_index in partitions {
                    let summary = scan_legacy_partition(
                        &engine,
                        partition_index,
                        None,
                        &args.y_field,
                        args.threshold,
                        args.width,
                        args.height,
                        &layout,
                        &mut shard.renderer,
                        &mut shard.significant_hits,
                    )?;
                    let scanned = reported_rows.fetch_add(summary.row_count, Ordering::Relaxed)
                        + summary.row_count;
                    pb.set_message(format!("{scanned} rows scanned..."));
                    shard.partition_summaries.push(summary);
                }
                Ok(shard)
            })
            .collect::<Result<Vec<_>>>()?
    };

    // Indexed Rayon collection is ordered, but sort explicitly so compositing order remains a
    // visible invariant if the producer implementation changes. Sidecar hits follow the same
    // deterministic partition/row order.
    raster_shards.sort_by_key(|shard| shard.index);
    let mut significant_hits = Vec::new();
    let mut partition_summaries = Vec::with_capacity(num_partitions);
    for shard in raster_shards {
        renderer.composite(&shard.renderer)?;
        significant_hits.extend(shard.significant_hits);
        partition_summaries.extend(shard.partition_summaries);
    }

    let scan_summary = merge_partition_summaries(partition_summaries)?;
    pb.finish_and_clear();
    println!(
        "{} {} rows, {} significant hits",
        "Scanned:".green(),
        scan_summary.row_count.to_string().bright_white(),
        scan_summary
            .significant_hit_count
            .to_string()
            .bright_white()
    );

    // 5. Annotate significant hits (if annotation table provided)
    // Note: annotation requires key reconstruction which needs the full row key.
    // For now we skip key-based annotation; the sidecar still contains hit positions.
    // TODO: reconstruct EncodedValue keys for annotation lookup

    // 6. Write output
    let output_base = args.output.unwrap_or_else(|| "manhattan".to_string());

    let png_data = renderer.encode_png()?;
    let png_path = format!("{}.png", output_base);
    fs::write(&png_path, &png_data)?;

    let sidecar = ManhattanSidecar {
        image: SidecarImage {
            width: args.width,
            height: args.height,
        },
        chromosomes: layout
            .contigs
            .iter()
            .map(|contig| SidecarChromosome {
                name: contig.name.clone(),
                x_start_px: contig.x_start as f32,
                x_end_px: contig.x_end as f32,
                color: contig.color.clone(),
            })
            .collect(),
        threshold: SidecarThreshold {
            pvalue: args.threshold,
            y_px: threshold_y,
        },
        y_axis: SidecarYAxis {
            log_threshold: layout.y_scale.log_threshold,
            linear_fraction: layout.y_scale.linear_fraction,
            max_neg_log_p: layout.y_scale.max_neg_log_p,
        },
        significant_hits,
    };

    let json_path = format!("{}.json", output_base);
    let mut sidecar_file = fs::File::create(&json_path)?;
    serde_json::to_writer_pretty(&mut sidecar_file, &sidecar)?;

    // Publish the manifest last so it never references a missing or partial raster.
    let manifest_path = write_manifest(
        &output_base,
        layout,
        args.threshold,
        scan_summary,
        &png_data,
    )?;

    println!(
        "{} {} + {} + {}",
        "Saved:".green().bold(),
        png_path.as_str().bright_white(),
        json_path.bright_white(),
        manifest_path.as_str().bright_white()
    );

    if let Some(options) = viewer_options {
        // These are the exact paths returned by this successful generation. The viewer
        // never rescans the source or reconstructs artifact names.
        serve_viewer(ViewerArtifacts::new(manifest_path, png_path), options)?;
    }

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

    println!("{} Manhattan batch validation", "Running".green().bold());
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
    let gene_associations_only =
        args.gene_associations_only || job_config.job.gene_associations_only;
    let mut inputs = load_and_group_assets(
        &assets_json,
        analysis_ids.as_deref(),
        ancestries.as_deref(),
        sample,
        limit,
    )?;
    if gene_associations_only {
        inputs = validate_gene_associations_only_inputs(inputs, analysis_ids.as_deref())?;
    }

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
    if gene_associations_only {
        println!(
            "  Products: {}",
            "gene_associations.parquet only".bright_white()
        );
    } else {
        println!("  Image size: {}x{}", args.width, args.height);
    }

    if !gene_associations_only && args.genes.is_some() {
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
    let mode_flag = if gene_associations_only {
        " \\\n      --gene-associations-only"
    } else if args.scan_only {
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
                if pt.position >= start
                    && pt.position <= end
                    && contig_normalized == chrom_normalized
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
