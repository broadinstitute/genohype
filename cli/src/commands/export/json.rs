//! JSON export command.

use crate::cli::ExportJsonArgs;
use crate::commands::utils::{parse_export_filters, parse_export_intervals};
use genohype_core::export::hail_to_json_sharded_full;
use genohype_core::partitioning::PartitionAllocator;
use genohype_core::query::QueryEngine;
use genohype_core::Result;
use owo_colors::OwoColorize;

pub fn run_export_json(args: ExportJsonArgs) -> Result<()> {
    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    // Validate partitioning args
    if args.common.partitioning.worker_id >= args.common.partitioning.total_workers {
        eprintln!(
            "{} Worker ID ({}) must be less than total workers ({})",
            "Error:".red().bold(),
            args.common.partitioning.worker_id,
            args.common.partitioning.total_workers
        );
        std::process::exit(1);
    }

    println!(
        "{} {} {} {} {}",
        "Converting".green(),
        args.common.input.bright_white(),
        "to".green(),
        args.output.bright_white(),
        "(JSON)".dimmed()
    );

    // Open the query engine to get metadata
    let engine = QueryEngine::open_path(&args.common.input)?;
    let num_partitions = engine.num_partitions();
    println!(
        "{} {}",
        "Partitions:".cyan(),
        num_partitions.to_string().bright_white()
    );
    println!("{} {:?}", "Key fields:".cyan(), engine.key_fields());
    if !where_filters.is_empty() {
        println!(
            "{} {:?}",
            "Filters:".cyan(),
            where_filters
                .iter()
                .map(|r| r.field_path_str())
                .collect::<Vec<_>>()
        );
    }
    if let Some(ref ivl) = intervals {
        println!(
            "{} {} intervals",
            "Interval filter:".cyan(),
            ivl.len().to_string().bright_white()
        );
    }
    if let Some(l) = args.common.limit {
        println!(
            "{} {}",
            "Row limit:".cyan(),
            l.to_string().bright_white()
        );
    }

    // We only support sharded export for JSON currently
    let shard_count = if args.per_partition {
        None
    } else {
        args.shard_count
    };

    if args.per_partition {
        println!(
            "{} {} files",
            "Output shards:".cyan(),
            num_partitions.to_string().bright_white()
        );
    } else if let Some(n) = args.shard_count {
        println!(
            "{} {} files",
            "Output shards:".cyan(),
            n.to_string().bright_white()
        );
    }

    if args.common.partitioning.is_distributed() {
        println!(
            "{} worker {}/{}",
            "Distributed mode:".cyan(),
            args.common.partitioning.worker_id.to_string().bright_white(),
            args.common.partitioning.total_workers.to_string().bright_white()
        );
    }
    println!();

    // Close engine so converter can open its own
    drop(engine);

    // Create allocator for distributed processing
    let allocator = if args.common.partitioning.is_distributed() {
        Some(PartitionAllocator::new(
            args.common.partitioning.worker_id,
            args.common.partitioning.total_workers,
        ))
    } else {
        None
    };

    let total_rows = hail_to_json_sharded_full(
        &args.common.input,
        &args.output,
        true,
        shard_count,
        None,
        None,
        allocator,
        args.common.progress_json,
        where_filters,
        intervals,
    )?;

    println!();
    println!("{}", "Conversion complete!".green().bold());
    println!(
        "  {} {}",
        "Rows written:".cyan(),
        total_rows.to_string().bright_white()
    );
    println!(
        "  {} {}",
        "Output directory:".cyan(),
        args.output.bright_white()
    );

    Ok(())
}
