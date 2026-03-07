//! Hail decoder CLI tool
//!
//! Commands:
//! - info: Show table metadata, keys, partition layout, and schema (fast)
//! - query: Stream rows with optional filtering (lazy)
//! - summary: Scan full dataset to calculate row counts and field statistics (slow)
//! - export: Export data to other formats (Parquet, ClickHouse, BigQuery)

mod benchmark;
mod cli;
mod clickhouse;
mod cloud;
mod cluster;
mod config;
mod distributed;
mod env;
mod export;
mod manhattan;

#[cfg(feature = "clickhouse")]
mod ingest;

#[cfg(feature = "genomic")]
mod genomic;

// Re-export core error types for use in CLI modules
pub use genohype_core::{HailError, Result};

use clap::Parser;
use cli::{Cli, ClickHouseCommands, ClusterCommands, Commands, EnvCommands, ExportCommands, ExportParquetArgs, ExportJsonArgs, ExportVcfArgs, ExportHailArgs, HasCommonExportArgs, LociArgs, LocusArgs, ManhattanArgs, ManhattanBatchArgs, PoolCommands, QueryArgs, ServiceCommands};
#[cfg(feature = "validation")]
use cli::{SchemaSubcommands, ValidateArgs};
#[cfg(feature = "clickhouse")]
use cli::{ExportClickhouseArgs, IngestCommands};
use genohype_core::codec::EncodedValue;
use genohype_core::io::{get_file_size, join_path};
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use genohype_core::summary::{format_schema_clean, StatsAccumulator};
use std::sync::Arc;
#[cfg(feature = "validation")]
use genohype_core::validation::{SchemaGenerator, SchemaValidator};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration from file
    let config = config::Config::load_from_path(cli.config.as_deref());

    match cli.command {
        Commands::Info { path } => show_info(&path)?,
        Commands::Summary { path } => run_summary(&path)?,
        Commands::Query(args) => run_query(args)?,
        Commands::Export { command } => match command {
            ExportCommands::Parquet(args) => run_export_parquet(args)?,
            ExportCommands::Json(args) => run_export_json(args)?,
            ExportCommands::Vcf(args) => run_export_vcf(args)?,
            ExportCommands::Hail(args) => run_export_hail(args)?,
            #[cfg(feature = "clickhouse")]
            ExportCommands::Clickhouse(args) => run_export_clickhouse(args)?,
            #[cfg(feature = "bigquery")]
            ExportCommands::Bigquery(args) => run_export_bigquery(args)?,
        },
        Commands::Manhattan(args) => run_manhattan(args)?,
        Commands::ManhattanBatch(args) => run_manhattan_batch(args)?,
        Commands::Loci(args) => run_loci(args)?,
        Commands::Locus(args) => run_locus(args)?,
        #[cfg(feature = "validation")]
        Commands::Schema { command } => match command {
            SchemaSubcommands::Validate(args) => run_validate(args)?,
            SchemaSubcommands::Generate(args) => run_generate_schema(&args.table, args.output.as_deref())?,
        },
        Commands::Pool { command } => run_pool_command(command, &config)?,
        Commands::Cluster { command } => match command {
            ClusterCommands::List { status } => cluster::list_clusters(&config, status.as_ref())?,
            ClusterCommands::Show { name } => cluster::show_cluster(&config, &name)?,
            ClusterCommands::Verify { name } => cluster::verify_cluster(&config, &name)?,
            ClusterCommands::Deploy { name, tag, backend_only } => {
                cluster::deploy_cluster(&config, &name, &tag, backend_only)?
            }
        },
        Commands::Clickhouse { command } => match command {
            ClickHouseCommands::Create {
                name,
                profile,
                machine_type,
                disk_size_gb,
                zone,
            } => clickhouse::create_instance(
                &config,
                &name,
                profile.as_deref(),
                machine_type.as_deref(),
                disk_size_gb,
                zone.as_deref(),
            )?,
            ClickHouseCommands::List => clickhouse::list_instances(&config)?,
            ClickHouseCommands::Show { name } => clickhouse::show_instance(&config, &name)?,
            ClickHouseCommands::Destroy { name, yes } => {
                clickhouse::destroy_instance(&config, &name, yes)?
            }
            ClickHouseCommands::Ip { name } => clickhouse::get_instance_ip(&config, &name)?,
            ClickHouseCommands::Ssh { name, command } => {
                clickhouse::ssh_instance(&config, &name, &command)?
            }
            ClickHouseCommands::Tunnel { name, port } => {
                clickhouse::tunnel_instance(&config, &name, port)?
            }
        },
        Commands::Env { command } => match command {
            EnvCommands::Init {
                name,
                storage,
                clickhouse,
            } => env::init_env(&config, &name, storage.as_deref(), clickhouse.as_deref())?,
            EnvCommands::Show => env::show_env(&config)?,
            EnvCommands::Verify => env::verify_env(&config)?,
        },
        Commands::Service { command } => run_service_command(command)?,
        #[cfg(feature = "clickhouse")]
        Commands::Ingest { command } => run_ingest_command(command)?,
        Commands::Stress(args) => {
            use owo_colors::OwoColorize;
            println!("{} The stress command is designed to be run via a worker pool:", "Note:".cyan());
            println!("  genohype pool submit <pool_name> -- stress --partitions {} --cpu-secs {} --memory-mb {}",
                args.partitions, args.cpu_secs, args.memory_mb);
        }
    }

    Ok(())
}

/// Create a standard progress bar style (no emojis)
fn progress_style_bar() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-")
}

/// Create a standard spinner style (no emojis)
fn progress_style_spinner() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg}")
        .unwrap()
}

/// Parse where filters from any export args implementing HasCommonExportArgs.
/// This enforces at compile time that all export targets have common args.
fn parse_export_filters(args: &impl HasCommonExportArgs) -> Vec<KeyRange> {
    let mut filters = Vec::new();
    for clause in &args.common().where_clauses {
        if let Some(range) = parse_where_condition(clause) {
            filters.push(range);
        } else {
            eprintln!("{} Invalid --where format: {}", "Error:".red().bold(), clause);
            std::process::exit(1);
        }
    }
    filters
}

/// Parse interval list from CLI arguments (file and/or strings)
///
/// # Arguments
/// * `file` - Optional path to interval file (.bed, .json, or text)
/// * `strings` - Optional list of interval strings (chr:start-end format)
///
/// # Returns
/// * `Ok(None)` if no intervals specified
/// * `Ok(Some(Arc<IntervalList>))` with merged and optimized intervals
/// * `Err` on parse errors
fn parse_interval_list(
    file: Option<&str>,
    strings: &[String],
) -> Result<Option<Arc<IntervalList>>> {
    // Return None if no intervals specified
    if file.is_none() && strings.is_empty() {
        return Ok(None);
    }

    let mut list = IntervalList::new();

    // Load from file if specified
    if let Some(path) = file {
        let file_list = IntervalList::from_file(path)?;
        list.merge(file_list);
    }

    // Parse string intervals if specified
    if !strings.is_empty() {
        let string_list = IntervalList::from_strings(strings)?;
        list.merge(string_list);
    }

    // Optimize the combined list
    list.optimize();

    Ok(Some(Arc::new(list)))
}

/// Parse interval list from export args
fn parse_export_intervals(args: &impl HasCommonExportArgs) -> Result<Option<Arc<IntervalList>>> {
    parse_interval_list(
        args.common().intervals_file.as_deref(),
        &args.common().interval,
    )
}

fn show_info(table_path: &str) -> Result<()> {
    // Check if this is a VCF file
    if table_path.ends_with(".vcf") || table_path.ends_with(".vcf.gz") || table_path.ends_with(".vcf.bgz") {
        println!("{}", "VCF File Information".bold().underline());
        println!();
        println!("{} {}", "Path:".green(), table_path.bright_white());
        println!();

        // Open using query engine to get schema info
        let engine = QueryEngine::open_path(table_path)?;

        println!("{} {}", "Contigs:".green(), engine.num_partitions().to_string().bright_white());
        println!("{}", "Row Schema:".green());
        println!("{:?}", engine.row_type());
        return Ok(());
    }

    // Hail Table - Read basic metadata first
    let metadata_path = genohype_core::io::join_path(table_path, "metadata.json.gz");

    // Try reading metadata (might fail if not a hail table)
    let metadata = match genohype_core::io::get_reader(&metadata_path) {
        Ok(mut reader) => {
            let mut data = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut data)?;
            genohype_core::schema::Metadata::from_gzipped_json(&data)?
        },
        Err(_) => {
            println!("{} Not a valid Hail table or VCF file", "Error:".red());
            return Ok(());
        }
    };

    println!("{}", "Hail Table Information".bold().underline());
    println!();
    println!("{} {}", "Path:".green(), table_path.bright_white());
    println!("{} {}", "Format version:".green(), metadata.file_version.bright_white());
    println!("{} {}", "Hail version:".green(), metadata.hail_version.bright_white());
    println!("{} {}", "References:".green(), metadata.references_rel_path.bright_white());
    println!();

    // Open using query engine for structural inspection
    let engine = QueryEngine::open_path(table_path)?;

    // Key information
    println!("{}", "Key Fields:".green());
    let keys = engine.key_fields();
    if keys.is_empty() {
        println!("  {}", "(none)".dimmed());
    } else {
        for (i, key) in keys.iter().enumerate() {
            println!("  {}. {}", (i + 1).to_string().cyan(), key.bright_white());
        }
    }
    println!();

    // Partition information
    println!("{} {}", "Partitions:".green(), engine.num_partitions().to_string().bright_white());
    if engine.has_index() {
        println!("{} {}", "Index:".green(), "Yes".bright_green());
    } else {
        println!("{} {}", "Index:".green(), "No".yellow());
    }
    println!();

    // Hail-specific structural information
    if let Some(rvd_spec) = engine.rvd_spec() {
        // Partition files
        if rvd_spec.part_files.len() <= 5 {
            println!("{}", "Partition Files:".green());
            for (i, part) in rvd_spec.part_files.iter().enumerate() {
                println!("  {}. {}", i.to_string().cyan(), part.dimmed());
            }
        } else {
            println!("{}", "Partition Files (first 5):".green());
            for (i, part) in rvd_spec.part_files.iter().take(5).enumerate() {
                println!("  {}. {}", i.to_string().cyan(), part.dimmed());
            }
            println!("  {} ({} more)", "...".dimmed(), rvd_spec.part_files.len() - 5);
        }
        println!();

        // Index information
        if let Some(ref index_spec) = rvd_spec.index_spec {
            println!("{}", "Index Details:".green());
            println!("  {} {}", "Path:".cyan(), index_spec.rel_path.dimmed());
            println!("  {} {}", "Key Type:".cyan(), index_spec.key_type.dimmed());
            println!();
        }

        // Partition bounds (sample)
        println!("{}", "Partition Bounds (first 3):".green());
        for (i, interval) in rvd_spec.range_bounds.iter().take(3).enumerate() {
            println!("  {} {}:", "Partition".cyan(), i);
            // Just show start/end JSON cleanly
            let start = serde_json::to_string(&interval.start).unwrap_or_default();
            let end = serde_json::to_string(&interval.end).unwrap_or_default();
            println!("    {} .. {}", start.dimmed(), end.dimmed());
        }
        if rvd_spec.range_bounds.len() > 3 {
            println!("    {}", "...".dimmed());
        }
        println!();

        // Codec information
        println!("{}", "Row Codec:".green());
        // Clean format the VType
        println!("{}", format_schema_clean(&rvd_spec.codec_spec.v_type));
    }

    Ok(())
}

fn run_query(args: QueryArgs) -> Result<()> {
    let table_path = &args.table;
    let mut key_filters: Vec<(String, String)> = Vec::new();
    let mut where_filters: Vec<KeyRange> = Vec::new();

    // Parse --key if provided
    if let Some(ref key_str) = args.key {
        if let Some((field, value)) = parse_equality(key_str) {
            key_filters.push((field, value));
        } else {
            eprintln!("{} Invalid --key format. Use field=value", "Error:".red().bold());
            std::process::exit(1);
        }
    }

    // Parse --where clauses
    for clause in &args.where_clauses {
        if let Some(range) = parse_where_condition(clause) {
            where_filters.push(range);
        } else {
            eprintln!("{} Invalid --where format: {}", "Error:".red().bold(), clause);
            std::process::exit(1);
        }
    }

    // Parse interval list
    let intervals = parse_interval_list(args.intervals_file.as_deref(), &args.interval)?;

    // Open the table (supports both local and cloud paths)
    let mut engine = QueryEngine::open_path(table_path)?;

    println!("{} {}", "Querying table:".green(), table_path.bright_white());
    println!("{} {:?}", "Key fields:".green(), engine.key_fields());
    if let Some(ref ivl) = intervals {
        println!("{} {} intervals", "Interval filter:".green(), ivl.len().to_string().bright_white());
    }
    println!();

    // Execute query
    if !key_filters.is_empty() {
        // Point lookup using --key
        let key = build_key_from_filters(&key_filters, engine.key_fields())?;
        println!("{} {:?}", "Point lookup for key:".cyan(), key_filters);

        match engine.lookup(&key)? {
            Some(row) => {
                // Apply interval filter to lookup result if specified
                if let Some(ref ivl) = intervals {
                    if !row_matches_intervals(&row, ivl) {
                        println!();
                        println!("{}", "Row found but filtered out by interval list.".yellow());
                        return Ok(());
                    }
                }
                println!();
                println!("{}", "Found row:".green().bold());
                print_row(&row, args.json)?;
            }
            None => {
                println!();
                println!("{}", "No matching row found.".yellow());
            }
        }
    } else {
        // Range query using --where (or full scan if no filters)
        if where_filters.is_empty() && intervals.is_none() {
            println!("{}", "Warning: No filters specified. This may scan all partitions.".yellow());
        } else {
            if !where_filters.is_empty() {
                println!(
                    "{} {:?}",
                    "Filter conditions:".cyan(),
                    where_filters
                        .iter()
                        .map(|r| r.field_path_str())
                        .collect::<Vec<_>>()
                );
            }
        }

        println!();
        println!("{}", "Streaming results...".dimmed());

        // Use streaming query with intervals for memory-efficient iteration
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        let mut count = 0;
        for row_result in iterator {
            let row = row_result?;
            count += 1;
            if !args.json {
                println!();
                println!("{} {} {}", "---".dimmed(), format!("Row {}", count).cyan(), "---".dimmed());
            }
            print_row(&row, args.json)?;
        }

        println!();
        println!("{} {}", "Rows returned:".green(), count.to_string().bright_white());
    }

    Ok(())
}

/// Check if a row's locus matches any interval (used for point lookup filtering)
fn row_matches_intervals(row: &EncodedValue, intervals: &IntervalList) -> bool {
    // Extract locus.contig and locus.position from the row
    if let EncodedValue::Struct(fields) = row {
        if let Some((_, locus)) = fields.iter().find(|(name, _)| name == "locus") {
            if let EncodedValue::Struct(locus_fields) = locus {
                let contig = locus_fields.iter().find(|(name, _)| name == "contig").map(|(_, v)| v);
                let position = locus_fields.iter().find(|(name, _)| name == "position").map(|(_, v)| v);

                if let (Some(EncodedValue::Binary(c)), Some(EncodedValue::Int32(p))) = (contig, position) {
                    let contig_str = String::from_utf8_lossy(c);
                    return intervals.contains(&contig_str, *p);
                }
            }
        }
    }
    // If we can't extract locus, pass through
    true
}

fn parse_equality(s: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

// Filter parsing moved to genohype_core::query::filter
use genohype_core::query::filter::parse_where_condition;

fn build_key_from_filters(
    filters: &[(String, String)],
    key_fields: &[String],
) -> Result<EncodedValue> {
    let mut fields = Vec::new();

    for key_field in key_fields {
        if let Some((_, value)) = filters.iter().find(|(f, _)| f == key_field) {
            // Use heuristics to determine if value should be an integer:
            // - If it looks like a pure integer (no letters), parse as Int32
            // - Otherwise treat as string/binary
            let encoded = if value.chars().all(|c| c.is_ascii_digit() || c == '-')
                && !value.is_empty()
                && value.parse::<i32>().is_ok()
                && !looks_like_string_field(key_field)
            {
                // Parse as integer
                EncodedValue::Int32(value.parse().unwrap())
            } else {
                // Keep as string/binary
                EncodedValue::Binary(value.as_bytes().to_vec())
            };
            fields.push((key_field.clone(), encoded));
        }
    }

    Ok(EncodedValue::Struct(fields))
}

fn looks_like_string_field(field_name: &str) -> bool {
    // Fields that are commonly strings even if they contain only digits
    let string_fields = ["chrom", "chromosome", "contig", "gene_id", "transcript_id", "id"];
    string_fields.iter().any(|&s| field_name.to_lowercase().contains(s))
}

fn print_row(row: &EncodedValue, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", encoded_value_to_json(row));
    } else {
        print_encoded_value(row, 0);
    }
    Ok(())
}

fn print_encoded_value(value: &EncodedValue, indent: usize) {
    let prefix = "  ".repeat(indent);
    match value {
        EncodedValue::Null => println!("{}{}", prefix, "null".dimmed()),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            println!("{}\"{}\"", prefix, s.bright_white())
        }
        EncodedValue::Int32(i) => println!("{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Int64(i) => println!("{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Float32(f) => println!("{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Float64(f) => println!("{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Boolean(b) => {
            if *b {
                println!("{}{}", prefix, "true".green());
            } else {
                println!("{}{}", prefix, "false".yellow());
            }
        }
        EncodedValue::Struct(fields) => {
            for (name, val) in fields {
                print!("{}{}: ", prefix, name.green());
                match val {
                    EncodedValue::Struct(_) | EncodedValue::Array(_) => {
                        println!();
                        print_encoded_value(val, indent + 1);
                    }
                    _ => print_encoded_value(val, 0),
                }
            }
        }
        EncodedValue::Array(elements) => {
            println!("{}[", prefix);
            for elem in elements {
                print_encoded_value(elem, indent + 1);
            }
            println!("{}]", prefix);
        }
    }
}

fn encoded_value_to_json(value: &EncodedValue) -> String {
    match value {
        EncodedValue::Null => "null".to_string(),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            format!("\"{}\"", s.replace('\"', "\\\""))
        }
        EncodedValue::Int32(i) => i.to_string(),
        EncodedValue::Int64(i) => i.to_string(),
        EncodedValue::Float32(f) => f.to_string(),
        EncodedValue::Float64(f) => f.to_string(),
        EncodedValue::Boolean(b) => b.to_string(),
        EncodedValue::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|(name, val)| format!("\"{}\":{}", name, encoded_value_to_json(val)))
                .collect();
            format!("{{{}}}", field_strs.join(","))
        }
        EncodedValue::Array(elements) => {
            let elem_strs: Vec<String> = elements.iter().map(encoded_value_to_json).collect();
            format!("[{}]", elem_strs.join(","))
        }
    }
}

fn run_export_parquet(args: ExportParquetArgs) -> Result<()> {
    use genohype_core::parquet::{build_record_batch, ParquetWriter, hail_to_parquet_with_options, hail_to_parquet_sharded_full};
    use genohype_core::partitioning::PartitionAllocator;
    use crate::benchmark::{MetricsCollector, InputMetadata};
    use std::time::Duration;

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

    // Determine if sharded export is requested
    let use_sharded = args.per_partition || args.shard_count.is_some();

    if use_sharded {
        println!("{} {} {} {} {}", "Converting".green(), args.common.input.bright_white(), "to".green(), args.output.bright_white(), "(sharded)".dimmed());
    } else {
        println!("{} {} {} {}", "Converting".green(), args.common.input.bright_white(), "to".green(), args.output.bright_white());
    }

    // Open the query engine to get metadata
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    let num_partitions = engine.num_partitions();
    println!("{} {}", "Partitions:".cyan(), num_partitions.to_string().bright_white());
    println!("{} {:?}", "Key fields:".cyan(), engine.key_fields());
    if !where_filters.is_empty() {
        println!("{} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("{} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    if args.per_partition {
        println!("{} {} files", "Output shards:".cyan(), num_partitions.to_string().bright_white());
    } else if let Some(n) = args.shard_count {
        println!("{} {} files", "Output shards:".cyan(), n.to_string().bright_white());
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

    // Check for incompatible options with sharded export
    if use_sharded && (!where_filters.is_empty() || intervals.is_some() || args.common.limit.is_some()) {
        eprintln!("{} Sharded export (--per-partition or --shard-count) does not support --where, --interval, or --limit", "Error:".red().bold());
        std::process::exit(1);
    }

    // Distributed mode requires sharded export
    if args.common.partitioning.is_distributed() && !use_sharded {
        eprintln!("{} Distributed mode (--worker-id, --total-workers) requires --per-partition or --shard-count", "Error:".red().bold());
        std::process::exit(1);
    }

    // Calculate input table size for benchmarking (before dropping engine)
    let input_size_bytes = if args.benchmark {
        // Try to calculate total size from partition files
        engine.rvd_spec().map(|rvd| {
            let parts_dir = join_path(&join_path(&args.common.input, "rows"), "parts");
            rvd.part_files.iter()
                .filter_map(|part| {
                    let path = join_path(&parts_dir, part);
                    get_file_size(&path).ok()
                })
                .sum::<u64>()
        })
    } else {
        None
    };

    // Count schema fields
    let num_fields = match engine.row_type() {
        genohype_core::codec::EncodedType::EBaseStruct { fields, .. } => fields.len(),
        _ => 0,
    };

    // Start metrics collector if benchmarking
    let metrics_collector = if args.benchmark && use_sharded {
        println!("{}", "Benchmark mode enabled - collecting system metrics...".yellow());
        let mut collector = MetricsCollector::start(Duration::from_secs(2), num_partitions);

        // Set input metadata
        collector.set_input_metadata(InputMetadata {
            path: args.common.input.clone(),
            num_partitions,
            total_size_bytes: input_size_bytes,
            key_fields: engine.key_fields().to_vec(),
            num_fields,
        });
        collector.set_output_path(args.output.clone());

        Some(collector)
    } else {
        None
    };

    let (total_rows, is_directory) = if use_sharded {
        // Use sharded export (one file per shard, true parallelism)
        let shard_count = if args.per_partition { None } else { args.shard_count };
        if args.common.partitioning.is_distributed() {
            println!("{}", format!(
                "Using sharded parallel export (worker {}/{})...",
                args.common.partitioning.worker_id,
                args.common.partitioning.total_workers
            ).dimmed());
        } else {
            println!("{}", "Using sharded parallel export (all CPU cores)...".dimmed());
        }
        drop(engine); // Close engine so converter can open its own

        // Create allocator for distributed processing
        let allocator = if args.common.partitioning.is_distributed() {
            Some(PartitionAllocator::new(
                args.common.partitioning.worker_id,
                args.common.partitioning.total_workers,
            ))
        } else {
            None
        };

        let rows = if let Some(ref collector) = metrics_collector {
            // Pass counters to the sharded export for metrics tracking
            hail_to_parquet_sharded_full(
                &args.common.input,
                &args.output,
                true,
                shard_count,
                Some(collector.rows_counter.clone()),
                Some(collector.partitions_counter.clone()),
                Some(collector.row_size_stats_handle()),
                allocator,
                args.common.progress_json,
            )?
        } else {
            hail_to_parquet_sharded_full(
                &args.common.input,
                &args.output,
                true,
                shard_count,
                None,
                None,
                None,
                allocator,
                args.common.progress_json,
            )?
        };
        (rows, true)
    } else {
        // Use parallel converter for full table exports (no filters/intervals/limit)
        // This uses producer-consumer pattern with all CPU cores
        let can_use_parallel = where_filters.is_empty()
            && intervals.is_none()
            && args.common.limit.is_none();

        let total_rows = if can_use_parallel {
            println!("{}", "Using parallel export (all CPU cores)...".dimmed());
            drop(engine); // Close engine so converter can open its own
            hail_to_parquet_with_options(&args.common.input, &args.output, true, None)?
        } else {
            // Fall back to sequential streaming for filtered exports
            println!("{}", "Using sequential export (filtered)...".dimmed());

            // Create writer and get schema
            let mut writer = ParquetWriter::new(&args.output, &row_type)?;
            let arrow_schema = writer.schema().clone();

            // Use streaming query with filters and intervals
            let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

            // Apply limit if specified
            let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
                Box::new(iterator.take(n))
            } else {
                Box::new(iterator)
            };

            // Collect rows in batches for efficient parquet writing
            let batch_size = 10000;
            let mut batch_rows = Vec::with_capacity(batch_size);
            let mut total_rows = 0;

            // Progress indicator
            let pb = ProgressBar::new_spinner();
            pb.set_style(progress_style_spinner());

            for row_result in iterator {
                let row = row_result?;
                batch_rows.push(row);
                total_rows += 1;

                if batch_rows.len() >= batch_size {
                    pb.set_message(format!("{} rows processed...", total_rows));
                    let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                    writer.write_batch(&batch)?;
                    batch_rows.clear();
                }
            }

            // Write remaining rows
            if !batch_rows.is_empty() {
                let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                writer.write_batch(&batch)?;
            }

            pb.finish_and_clear();
            writer.close()?;
            total_rows
        };
        (total_rows, false)
    };

    // Print summary
    let output_size = if is_directory {
        // Calculate total size of all files in directory
        std::fs::read_dir(&args.output)
            .map(|entries| {
                let total: u64 = entries
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum();
                format_bytes(total)
            })
            .unwrap_or_else(|_| "unknown".to_string())
    } else {
        std::fs::metadata(&args.output)
            .map(|m| format_bytes(m.len()))
            .unwrap_or_else(|_| "unknown".to_string())
    };

    println!();
    println!("{}", "Conversion complete!".green().bold());
    println!("  {} {}", "Rows written:".cyan(), total_rows.to_string().bright_white());
    if is_directory {
        println!("  {} {}", "Output directory:".cyan(), args.output.bright_white());
    } else {
        println!("  {} {}", "Output file:".cyan(), args.output.bright_white());
    }
    println!("  {} {}", "Output size:".cyan(), output_size.bright_white());

    // Finish benchmark and print/write report
    if let Some(collector) = metrics_collector {
        let report = collector.finish(total_rows);

        // Print to console
        report.print();

        // Write to JSON file
        let report_path = if is_directory {
            format!("{}/benchmark-report.json", args.output)
        } else {
            format!("{}.benchmark.json", args.output)
        };

        let report_json = serde_json::json!({
            "input": report.input_metadata.as_ref().map(|m| serde_json::json!({
                "path": m.path,
                "num_partitions": m.num_partitions,
                "total_size_gb": m.total_size_bytes.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
                "key_fields": m.key_fields,
                "num_fields": m.num_fields,
            })),
            "output": {
                "path": report.output_path,
                "size_gb": report.output_size_bytes.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
            },
            "duration_secs": report.duration.as_secs_f64(),
            "total_rows": report.total_rows,
            "total_partitions": report.total_partitions,
            "num_cpus": report.num_cpus,
            "rows_per_sec": report.rows_per_sec(),
            "partitions_per_sec": report.partitions_per_sec(),
            "cpu": {
                "avg_percent": report.avg_cpu_percent(),
                "max_percent": report.max_cpu_percent(),
            },
            "memory": {
                "total_gb": report.total_memory_gb(),
                "avg_used_gb": report.avg_memory_gb(),
                "max_used_gb": report.max_memory_gb(),
            },
            "disk_io": {
                "avg_read_mb_sec": report.avg_disk_read_mb_sec(),
                "max_read_mb_sec": report.max_disk_read_mb_sec(),
                "avg_write_mb_sec": report.avg_disk_write_mb_sec(),
                "max_write_mb_sec": report.max_disk_write_mb_sec(),
            },
            "disk_space": {
                "available_gb": report.disk_space_available.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
                "total_gb": report.disk_space_total.map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0)),
            },
            "decoded_row_size": report.row_size_stats.as_ref().map(|stats| {
                let total_avg = stats.avg_bytes();
                serde_json::json!({
                    "_note": "In-memory sizes after decoding, not on-disk compressed sizes",
                    "sample_count": stats.sample_count,
                    "avg_bytes": stats.avg_bytes(),
                    "min_bytes": stats.min_bytes,
                    "max_bytes": stats.max_bytes,
                    "estimated_memory_footprint_gb": stats.avg_bytes() * report.total_rows as f64 / (1024.0 * 1024.0 * 1024.0),
                    "schema": stats.schema_stats.map(|(fields, depth, arrays)| serde_json::json!({
                        "total_fields": fields,
                        "max_depth": depth,
                        "array_count": arrays,
                    })),
                    "fields": stats.sorted_field_stats().iter().map(|f| serde_json::json!({
                        "name": f.name,
                        "type": f.type_desc,
                        "avg_bytes": f.avg_bytes(),
                        "min_bytes": f.min_bytes,
                        "max_bytes": f.max_bytes,
                        "pct_of_row": if total_avg > 0.0 { f.avg_bytes() / total_avg * 100.0 } else { 0.0 },
                    })).collect::<Vec<_>>(),
                })
            }),
            "bottleneck": format!("{:?}", report.identify_bottleneck()),
            "recommendations": report.scaling_recommendations(),
            "samples": report.samples.iter().map(|s| {
                serde_json::json!({
                    "elapsed_secs": s.elapsed_secs,
                    "cpu_percent": s.cpu_percent,
                    "memory_used_gb": s.memory_used as f64 / (1024.0 * 1024.0 * 1024.0),
                    "disk_read_mb_sec": s.disk_read_bytes_sec as f64 / (1024.0 * 1024.0),
                    "disk_write_mb_sec": s.disk_write_bytes_sec as f64 / (1024.0 * 1024.0),
                    "rows_processed": s.rows_processed,
                    "partitions_completed": s.partitions_completed,
                })
            }).collect::<Vec<_>>(),
        });

        if let Err(e) = std::fs::write(&report_path, serde_json::to_string_pretty(&report_json).unwrap()) {
            eprintln!("{} Failed to write benchmark report: {}", "Warning:".yellow(), e);
        } else {
            println!();
            println!("  {} {}", "Benchmark report:".cyan(), report_path.bright_white());
        }
    }

    Ok(())
}

fn run_export_json(args: ExportJsonArgs) -> Result<()> {
    use crate::export::hail_to_json_sharded_full;
    use genohype_core::partitioning::PartitionAllocator;

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
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
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

fn run_export_vcf(args: ExportVcfArgs) -> Result<()> {
    use genohype_core::vcf::VcfWriter;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {} {} {}", "Converting".green(), args.common.input.bright_white(), "to VCF".green(), args.output.bright_white());

    // Open the query engine
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!("{} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("{} {:?}", "Key fields:".cyan(), engine.key_fields());
    if !where_filters.is_empty() {
        println!("{} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("{} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    if args.bgzip {
        println!("{} {}", "Compression:".cyan(), "BGZF".bright_white());
    }
    println!();

    // Extract sample names from globals if available, or use empty list for sites-only VCF
    let sample_names: Vec<String> = Vec::new(); // TODO: Extract from globals if available

    // Create VCF writer
    let mut total_rows = 0;
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    if args.bgzip {
        let mut writer = VcfWriter::new_bgzf(&args.output, &row_type, sample_names)?;

        // Use streaming query with filters and intervals
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        for row_result in iterator {
            let row = row_result?;
            writer.write_row(&row)?;
            total_rows += 1;
            if total_rows % 10000 == 0 {
                pb.set_message(format!("{} rows written...", total_rows));
            }
        }

        writer.finish()?;
    } else {
        let mut writer = VcfWriter::new(&args.output, &row_type, sample_names)?;

        // Use streaming query with filters and intervals
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        for row_result in iterator {
            let row = row_result?;
            writer.write_row(&row)?;
            total_rows += 1;
            if total_rows % 10000 == 0 {
                pb.set_message(format!("{} rows written...", total_rows));
            }
        }

        writer.finish()?;
    }

    pb.finish_and_clear();

    // Print summary
    let output_size = std::fs::metadata(&args.output)
        .map(|m| format_bytes(m.len()))
        .unwrap_or_else(|_| "unknown".to_string());

    println!();
    println!("{}", "Conversion complete!".green().bold());
    println!("  {} {}", "Rows written:".cyan(), total_rows.to_string().bright_white());
    println!("  {} {}", "Output file:".cyan(), args.output.bright_white());
    println!("  {} {}", "Output size:".cyan(), output_size.bright_white());

    Ok(())
}

fn run_export_hail(args: ExportHailArgs) -> Result<()> {
    use crate::export::hail::HailTableWriter;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {} {} {}", "Converting".green(), args.common.input.bright_white(), "to Hail Table".green(), args.output.bright_white());

    // Open the query engine
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    let key_fields = engine.key_fields().to_vec();
    let rvd_spec = engine.rvd_spec().cloned();

    println!("{} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("{} {:?}", "Key fields:".cyan(), key_fields);
    if !where_filters.is_empty() {
        println!("{} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("{} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // Create Hail table writer
    let mut writer = HailTableWriter::new(&args.output, &row_type, &key_fields, rvd_spec.as_ref())?;

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Progress indicator
    let mut total_rows = 0;
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    // Collect rows for writing (simple MVP: single partition)
    let mut rows = Vec::new();
    for row_result in iterator {
        let row = row_result?;
        rows.push(row);
        total_rows += 1;
        if total_rows % 10000 == 0 {
            pb.set_message(format!("{} rows collected...", total_rows));
        }
    }

    pb.set_message("Writing partition...");
    writer.write_partition(0, rows.into_iter())?;
    writer.finish()?;

    pb.finish_and_clear();

    println!();
    println!("{}", "Export complete!".green().bold());
    println!("  {} {}", "Rows written:".cyan(), total_rows.to_string().bright_white());
    println!("  {} {}", "Output directory:".cyan(), args.output.bright_white());

    Ok(())
}

/// Format bytes into a human-readable string
fn format_bytes(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{} B", bytes);
    }
    if bytes < UNIT.pow(2) {
        return format!("{:.2} KiB", bytes as f64 / UNIT as f64);
    }
    if bytes < UNIT.pow(3) {
        return format!("{:.2} MiB", bytes as f64 / UNIT.pow(2) as f64);
    }
    format!("{:.2} GiB", bytes as f64 / UNIT.pow(3) as f64)
}

/// Run the summary command
fn run_summary(table_path: &str) -> Result<()> {
    let engine = QueryEngine::open_path(table_path)?;
    let part_count = engine.num_partitions();

    // Print header
    println!("{}", "Table Summary".bold().underline());
    println!();

    // Basic info
    let name = std::path::Path::new(table_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    println!("{} {}", "Name:".green(), name.bright_white());
    println!("{} {}", "Path:".green(), table_path.bright_white());
    println!("{} {}", "Partitions:".green(), part_count.to_string().bright_white());
    println!();

    // Key fields
    println!("{}", "Key Fields:".green());
    let keys = engine.key_fields();
    if keys.is_empty() {
        println!("  {}", "(none)".dimmed());
    } else {
        for (i, key) in keys.iter().enumerate() {
            println!("  {}. {}", (i + 1).to_string().cyan(), key.bright_white());
        }
    }
    println!();

    // Hail-specific partition size calculation
    if let Some(rvd) = engine.rvd_spec() {
        // Calculate partition sizes (parallel)
        println!("{}", "Calculating partition sizes...".dimmed());
        let pb = ProgressBar::new(part_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions")
                .unwrap()
                .progress_chars("#>-"),
        );

        let parts_dir = join_path(&join_path(table_path, "rows"), "parts");

        let sizes: Vec<u64> = rvd.part_files
            .par_iter()
            .map(|part| {
                let path = join_path(&parts_dir, part);
                let size = get_file_size(&path).unwrap_or(0);
                pb.inc(1);
                size
            })
            .collect();

        pb.finish_and_clear();

        let total_size: u64 = sizes.iter().sum();
        let mean_size = if part_count > 0 {
            total_size as f64 / part_count as f64
        } else {
            0.0
        };

        // Calculate standard deviation
        let variance = if part_count > 1 {
            let mean = mean_size;
            sizes.iter().map(|&s| {
                let diff = s as f64 - mean;
                diff * diff
            }).sum::<f64>() / (part_count - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();

        println!("{}", "Size Statistics:".green());
        println!("  {} {}", "Total Size:".cyan(), format_bytes(total_size).bright_white());
        println!("  {} {}", "Mean Partition Size:".cyan(), format_bytes(mean_size as u64).bright_white());
        println!("  {} {}", "Std Dev:".cyan(), format_bytes(std_dev as u64).bright_white());
        println!();

        // Schema
        println!("{}", "Schema:".green());
        println!("{}", "-".repeat(40).dimmed());
        println!("{}", format_schema_clean(&rvd.codec_spec.v_type));
        println!("{}", "-".repeat(40).dimmed());
        println!();
    } else {
        // VCF or other non-Hail source
        println!("{}", "(Size statistics not available for this format)".dimmed());
        println!();
    }

    // Data scan for statistics (parallel)
    println!("{}", "Scanning data for field statistics...".dimmed());
    let pb = ProgressBar::new(part_count as u64);
    pb.set_style(progress_style_bar());

    let total_rows = AtomicUsize::new(0);

    // Parallel scan using rayon - each thread gets its own StatsAccumulator
    // Uses streaming to avoid loading entire partitions into memory (prevents OOM)
    let stats = (0..part_count)
        .into_par_iter()
        .fold(
            || StatsAccumulator::new(),
            |mut acc, i| {
                match engine.scan_partition_iter(i, &[]) {
                    Ok(iter) => {
                        for row_result in iter {
                            match row_result {
                                Ok(row) => {
                                    total_rows.fetch_add(1, Ordering::Relaxed);
                                    acc.process_row(&row);
                                }
                                Err(e) => {
                                    eprintln!(
                                        "{} Error reading row in partition {}: {}",
                                        "Warning:".yellow(),
                                        i,
                                        e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "{} Failed to scan partition {}: {}",
                            "Warning:".yellow(),
                            i,
                            e
                        );
                    }
                }
                pb.inc(1);
                acc
            },
        )
        .reduce(
            || StatsAccumulator::new(),
            |mut a, b| {
                a.merge(b);
                a
            },
        );

    pb.finish_and_clear();
    let total_rows = total_rows.load(Ordering::Relaxed);

    println!();
    println!("{} {}", "Row Count:".green(), total_rows.to_string().bright_white().bold());
    println!();

    // Print field statistics
    println!("{}", "Field Statistics:".green().bold());
    println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
        "Field".cyan(), "Count".cyan(), "Nulls".cyan(), "Min".cyan(), "Max".cyan());
    println!("{}", "-".repeat(120).dimmed());

    for key in stats.sorted_fields() {
        let s = &stats.stats[key];

        // Truncate field name if too long
        let field_display = if key.len() > 48 {
            format!("...{}", &key[key.len() - 45..])
        } else {
            key.clone()
        };

        // Truncate min/max if too long
        let min_display = match &s.min {
            Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
            Some(m) => m.clone(),
            None => String::new(),
        };
        let max_display = match &s.max {
            Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
            Some(m) => m.clone(),
            None => String::new(),
        };

        println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
            field_display,
            s.count,
            s.null_count,
            min_display,
            max_display
        );
    }

    Ok(())
}

/// Run the validate command
#[cfg(feature = "validation")]
fn run_validate(args: ValidateArgs) -> Result<()> {
    // Validate that --limit and --sample aren't both specified
    if args.limit.is_some() && args.sample.is_some() {
        eprintln!("{} Cannot use both --limit and --sample. Choose one.", "Error:".red().bold());
        std::process::exit(1);
    }

    println!("{} {}", "Validating table:".green(), args.table.bright_white());
    println!("{} {}", "Using schema:".green(), args.schema.bright_white());
    if let Some(l) = args.limit {
        println!("{} {} {}", "Row limit:".cyan(), l.to_string().bright_white(), "(sequential)".dimmed());
    }
    if let Some(s) = args.sample {
        println!("{} {} {}", "Sample size:".cyan(), s.to_string().bright_white(), "(random)".dimmed());
    }
    if args.fail_fast {
        println!("{} {}", "Mode:".cyan(), "fail-fast".yellow());
    }
    println!();

    // Load the JSON schema
    let validator = SchemaValidator::from_file(&args.schema)?;

    // Open the table
    let engine = QueryEngine::open_path(&args.table)?;

    println!("{} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!();

    // Run validation
    let report = if let Some(sample_size) = args.sample {
        if args.verbose {
            validator.validate_sample_verbose(&engine, sample_size, args.fail_fast)?
        } else {
            validator.validate_sample(&engine, sample_size, args.fail_fast)?
        }
    } else {
        println!("{}", "Validating rows sequentially...".dimmed());
        validator.validate(&engine, args.limit, args.fail_fast)?
    };

    // Print results
    println!();
    println!("{}", report);

    // Exit with error code if validation failed
    if report.invalid_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Run the generate-schema command
#[cfg(feature = "validation")]
fn run_generate_schema(table_path: &str, output_path: Option<&str>) -> Result<()> {
    eprintln!("{} {}", "Generating JSON schema for:".green(), table_path.bright_white());

    // Open the table
    let engine = QueryEngine::open_path(table_path)?;

    // Get the table name from path for title
    let title = std::path::Path::new(table_path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_end_matches(".ht"));

    // Generate the schema
    let schema = SchemaGenerator::from_engine(&engine, title)?;

    if let Some(path) = output_path {
        // Write to file
        SchemaGenerator::write_to_file(&schema, path)?;
        eprintln!("{} {}", "Schema written to:".green(), path.bright_white());
    } else {
        // Print to stdout
        println!("{}", serde_json::to_string_pretty(&schema)?);
    }

    Ok(())
}

/// Export a Hail table to ClickHouse with optional filtering
#[cfg(feature = "clickhouse")]
fn run_export_clickhouse(args: ExportClickhouseArgs) -> Result<()> {
    use crate::export::clickhouse::generate_create_table;
    use crate::export::ClickHouseClient;
    use genohype_core::parquet::{build_record_batch, ParquetWriter};
    use uuid::Uuid;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {}", "Exporting to ClickHouse:".green().bold(), args.common.input.bright_white());
    println!("  {} {}", "ClickHouse URL:".cyan(), args.url.bright_white());
    println!("  {} {}", "Target table:".cyan(), args.table.bright_white());
    if !where_filters.is_empty() {
        println!("  {} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("  {} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("  {} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // Step 1: Open the query engine
    println!("{}", "Reading table metadata...".dimmed());
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!("  {} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("  {} {:?}", "Key fields:".cyan(), engine.key_fields());
    println!();

    // Step 2: Create ClickHouse client and generate DDL
    let client = ClickHouseClient::new(&args.url);

    println!("{}", "Generating CREATE TABLE DDL...".dimmed());
    let ddl = generate_create_table(&args.table, &row_type, engine.key_fields())
        .map_err(|e| genohype_core::HailError::InvalidFormat(e.to_string()))?;
    println!("{}", ddl.dimmed());
    println!();

    // Step 3: Execute CREATE TABLE
    println!("{}", "Creating table in ClickHouse...".dimmed());
    client
        .execute(&ddl)
        .map_err(|e| genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;
    println!("  {}", "Table created (or already exists)".green());
    println!();

    // Step 4: Convert filtered rows to temporary Parquet file
    let temp_path = format!("/tmp/hail_export_{}.parquet", Uuid::new_v4());
    println!("{}", "Converting to temporary Parquet file...".dimmed());
    println!("  {} {}", "Temp file:".cyan(), temp_path.dimmed());

    // Create writer and get schema
    let mut writer = ParquetWriter::new(&temp_path, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Collect rows in batches for efficient parquet writing
    let batch_size = 10000;
    let mut batch_rows = Vec::with_capacity(batch_size);
    let mut total_rows = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        total_rows += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", total_rows));
            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write remaining rows
    if !batch_rows.is_empty() {
        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
        writer.write_batch(&batch)?;
    }

    pb.finish_and_clear();
    writer.close()?;
    println!("  {} {}", "Converted".green(), format!("{} rows", total_rows).bright_white());
    println!();

    // Step 5: Insert Parquet data into ClickHouse
    println!("{}", "Inserting data into ClickHouse...".dimmed());
    client
        .insert_parquet(&args.table, &temp_path)
        .map_err(|e| genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;

    // Step 6: Clean up temp file
    if let Err(e) = std::fs::remove_file(&temp_path) {
        eprintln!("{} Failed to remove temp file {}: {}", "Warning:".yellow(), temp_path, e);
    }

    // Step 7: Verify
    let row_count = client
        .count_rows(&args.table)
        .map_err(|e| genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!("  {} {}", "Rows in ClickHouse table:".cyan(), row_count.to_string().bright_white());

    Ok(())
}

/// Export a Hail table to BigQuery via GCS staging
#[cfg(feature = "bigquery")]
fn run_export_bigquery(args: cli::ExportBigqueryArgs) -> Result<()> {
    use crate::export::BigQueryClient;
    use genohype_core::parquet::{build_record_batch, ParquetWriter};
    use uuid::Uuid;

    // Parse destination: project:dataset.table
    let (project, dataset_table) = args.destination.split_once(':').ok_or_else(|| {
        genohype_core::HailError::InvalidFormat(
            "Destination format must be project:dataset.table".to_string(),
        )
    })?;
    let (dataset, table) = dataset_table.split_once('.').ok_or_else(|| {
        genohype_core::HailError::InvalidFormat(
            "Destination format must be project:dataset.table".to_string(),
        )
    })?;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {} {} {}:{}.{}",
        "Exporting".green().bold(),
        args.common.input.bright_white(),
        "to BigQuery".green(),
        project, dataset, table);
    println!("  {} {}", "Staging bucket:".cyan(), args.bucket.bright_white());
    if !where_filters.is_empty() {
        println!("  {} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("  {} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("  {} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // 1. Get Schema/Metadata
    println!("{}", "Reading table metadata...".dimmed());
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!("  {} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("  {} {:?}", "Key fields:".cyan(), engine.key_fields());
    println!();

    // 2. Convert filtered rows to Parquet locally
    let temp_file_path = std::path::Path::new(&args.temp_dir).join(format!("{}.parquet", Uuid::new_v4()));
    println!("{} {}", "Converting to temporary Parquet file:".dimmed(), temp_file_path.display());

    // Create writer and get schema
    let mut writer = ParquetWriter::new(temp_file_path.to_string_lossy().as_ref(), &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Collect rows in batches for efficient parquet writing
    let batch_size = 10000;
    let mut batch_rows = Vec::with_capacity(batch_size);
    let mut rows_written = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        rows_written += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", rows_written));
            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write remaining rows
    if !batch_rows.is_empty() {
        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
        writer.write_batch(&batch)?;
    }

    pb.finish_and_clear();
    writer.close()?;
    println!("  {} {}", "Converted".green(), format!("{} rows", rows_written).bright_white());
    println!();

    // 3. Upload and Load (Async)
    println!("{}", "Starting BigQuery export...".dimmed());
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let client = BigQueryClient::new(project, &args.bucket)
            .await
            .map_err(|e| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        println!("{}", "Uploading to GCS...".dimmed());
        let gcs_uri = client.upload_parquet(&temp_file_path).await.map_err(|e| {
            genohype_core::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        println!("  {} {}", "Uploaded to:".green(), gcs_uri.bright_white());

        println!("{}", "Triggering BigQuery Load Job...".dimmed());
        client
            .load_parquet(dataset, table, &gcs_uri, &row_type)
            .await
            .map_err(|e| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
        println!("  {}", "Load Job completed successfully.".green());

        println!("{}", "Cleaning up GCS staging object...".dimmed());
        let _ = client.delete_object(&gcs_uri).await;

        Ok::<(), genohype_core::HailError>(())
    });

    // 4. Cleanup Local temp file
    if std::fs::remove_file(&temp_file_path).is_err() {
        eprintln!("{} Failed to remove local temp file", "Warning:".yellow());
    }

    // Propagate any errors from the async block
    result?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!("  {} {}", "Rows exported:".cyan(), rows_written.to_string().bright_white());
    println!("  {} {}:{}.{}", "BigQuery table:".cyan(), project, dataset, table);

    Ok(())
}

/// Run pool management commands
fn run_pool_command(command: PoolCommands, app_config: &config::Config) -> Result<()> {
    use crate::cloud::gcp::GcpClient;
    use crate::cloud::pool::PoolManager;
    use crate::cloud::PoolConfig;

    let client = GcpClient::new();
    let manager = PoolManager::new(client);

    match command {
        PoolCommands::Create {
            name,
            workers,
            machine_type,
            zone,
            spot,
            project,
            network,
            subnet,
            wait,
            skip_build,
            with_coordinator,
        } => {
            // Try to load pool profile from config (if exists)
            let profile = app_config.get_pool(&name);

            // Resolve values: CLI args > profile > defaults
            // starting_workers defaults to 0 (coordinator-only pool)
            let resolved_workers = workers
                .or_else(|| profile.as_ref().map(|p| p.starting_workers))
                .unwrap_or(0);
            let resolved_machine_type = machine_type
                .or_else(|| profile.as_ref().map(|p| p.machine_type.clone()))
                .unwrap_or_else(|| "c3-highcpu-22".to_string());
            let resolved_zone = zone
                .or_else(|| profile.as_ref().map(|p| p.zone.clone()))
                .or_else(|| app_config.defaults.zone.clone())
                .unwrap_or_else(|| "us-central1-a".to_string());
            let resolved_spot = spot
                .or_else(|| profile.as_ref().map(|p| p.spot))
                .unwrap_or(false);
            let resolved_network = network
                .or_else(|| profile.as_ref().and_then(|p| p.network.clone()))
                .or_else(|| app_config.defaults.network.clone());
            let resolved_subnet = subnet
                .or_else(|| profile.as_ref().and_then(|p| p.subnet.clone()))
                .or_else(|| app_config.defaults.subnet.clone());

            // Resolve project ID: CLI > config > gcloud default
            let project_id = project
                .or_else(|| profile.as_ref().and_then(|p| p.project.clone()))
                .or_else(|| app_config.defaults.project.clone())
                .map(Ok)
                .unwrap_or_else(|| GcpClient::new().get_current_project())?;

            // Convert WireGuard config from config module to cloud module
            // Resolve env: prefixes (for USB-sourced secrets) at this point
            let wireguard = match profile.as_ref().and_then(|p| p.wireguard.as_ref()) {
                Some(wg) => {
                    // Resolve env:VAR_NAME references from environment
                    let resolved = wg.resolve_env_vars().map_err(|e| {
                        genohype_core::HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            e,
                        ))
                    })?;
                    Some(crate::cloud::WireGuardConfig {
                        endpoint: resolved.endpoint,
                        client_address: resolved.client_address,
                        allowed_ips: resolved.allowed_ips,
                        peer_public_key: resolved.peer_public_key,
                        client_private_key: resolved.client_private_key,
                    })
                }
                None => None,
            };

            // Resolve with_coordinator: CLI flag > config profile > auto-enable if WireGuard
            let resolved_with_coordinator = with_coordinator
                || profile.as_ref().map(|p| p.with_coordinator).unwrap_or(false)
                || wireguard.is_some();

            if wireguard.is_some() && !with_coordinator && !profile.as_ref().map(|p| p.with_coordinator).unwrap_or(false) {
                println!("{} WireGuard config found, enabling coordinator node", "Note:".cyan());
            }

            let pool_config = PoolConfig {
                name,
                worker_count: resolved_workers,
                machine_type: resolved_machine_type,
                zone: resolved_zone,
                spot: resolved_spot,
                project_id,
                network: resolved_network,
                subnet: resolved_subnet,
                with_coordinator: resolved_with_coordinator,
                wireguard,
                pool_db_path: profile.as_ref().and_then(|p| p.pool_db_path.clone()),
            };

            manager.create(&pool_config, wait, skip_build)?;
        }
        PoolCommands::Submit {
            name,
            zone,
            cluster,
            binary,
            auto_stop,
            redeploy_binary,
            force,
            autoscale,
            skip_build,
            batch_size,
            memory_weight_mb,
            mut command,
        } => {
            // Intercept: Apply cluster config overrides if requested
            if let Some(cluster_name) = cluster {
                let cluster_conf = app_config
                    .get_cluster(Some(&cluster_name))
                    .ok_or_else(|| {
                        genohype_core::HailError::InvalidFormat(format!(
                            "Unknown cluster: {}",
                            cluster_name
                        ))
                    })?
                    .resolve_env_vars()
                    .map_err(genohype_core::HailError::Config)?;

                // We only override if it's a manhattan-batch command with a --config flag
                if command.len() >= 2 && command[0] == "manhattan-batch" {
                    if let Some(config_idx) = command.iter().position(|a| a == "--config") {
                        if let Some(config_path) = command.get(config_idx + 1).cloned() {
                            // Load the local job config
                            let mut job_config =
                                crate::manhattan::config::ManhattanJobConfig::load(
                                    std::path::Path::new(&config_path),
                                )?;

                            // Override specific fields with cluster details
                            job_config.ingest.clickhouse_url = Some(cluster_conf.clickhouse_url.clone());

                            // Generate timestamp (YYYYMMDD-HHMM format)
                            let timestamp = {
                                use std::time::{SystemTime, UNIX_EPOCH};
                                let secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                // Convert to approximate date/time (simplified, not TZ-aware)
                                let days = secs / 86400;
                                let time_of_day = secs % 86400;
                                let hours = time_of_day / 3600;
                                let minutes = (time_of_day % 3600) / 60;
                                // Approximate date calculation (from 1970-01-01)
                                let mut year = 1970u64;
                                let mut remaining_days = days;
                                loop {
                                    let days_in_year = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 366 } else { 365 };
                                    if remaining_days < days_in_year { break; }
                                    remaining_days -= days_in_year;
                                    year += 1;
                                }
                                let month_days = [31, 28 + if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 1 } else { 0 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
                                let mut month = 1u64;
                                for days_in_month in month_days {
                                    if remaining_days < days_in_month { break; }
                                    remaining_days -= days_in_month;
                                    month += 1;
                                }
                                let day = remaining_days + 1;
                                format!("{:04}{:02}{:02}-{:02}{:02}", year, month, day, hours, minutes)
                            };
                            let new_output_dir = cluster_conf.output_dir(&timestamp);
                            job_config.job.output_dir = Some(new_output_dir.clone());

                            // Write modified config to temp file
                            let temp_path =
                                std::env::temp_dir().join(format!("job-config-{}.toml", timestamp));
                            let content = toml::to_string_pretty(&job_config)
                                .map_err(|e| genohype_core::HailError::Config(e.to_string()))?;
                            std::fs::write(&temp_path, content)?;

                            // Replace config path in the command vector to point to the new temp file
                            command[config_idx + 1] = temp_path.to_string_lossy().to_string();

                            println!(
                                "{} Using cluster '{}' ({})",
                                "Cluster:".green().bold(),
                                cluster_name.cyan(),
                                cluster_conf.clickhouse_url
                            );
                            println!(
                                "{} {}",
                                "Output:".green().bold(),
                                new_output_dir.bright_white()
                            );
                        }
                    }
                }
            }

            // Convert ResolvedPoolConfig to ScalingConfig if available
            let scaling_config = app_config.get_pool(&name).map(|p| {
                crate::cloud::ScalingConfig {
                    machine_type: p.machine_type.clone(),
                    workers: p.workers,
                    spot: p.spot,
                    network: p.network.clone(),
                    subnet: p.subnet.clone(),
                    project: p.project.clone(),
                    with_coordinator: p.with_coordinator,
                    pool_db_path: p.pool_db_path.clone(),
                }
            });
            manager.submit(
                &name,
                &zone,
                binary,
                auto_stop,
                redeploy_binary,
                force,
                autoscale,
                skip_build,
                batch_size,
                memory_weight_mb,
                scaling_config.as_ref(),
                &command,
            )?;
        }
        PoolCommands::Scale {
            name,
            workers,
            zone,
            binary,
            skip_build,
        } => {
            let pool_config = app_config.get_pool(&name).ok_or_else(|| {
                genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Pool '{}' not found in config. Required for scaling to know machine type/network.",
                        name
                    ),
                ))
            })?;

            let scaling_config = crate::cloud::ScalingConfig {
                machine_type: pool_config.machine_type.clone(),
                workers: pool_config.workers,
                spot: pool_config.spot,
                network: pool_config.network.clone(),
                subnet: pool_config.subnet.clone(),
                project: pool_config.project.clone(),
                with_coordinator: pool_config.with_coordinator,
                pool_db_path: pool_config.pool_db_path.clone(),
            };

            manager.scale(&name, workers, &zone, binary, skip_build, &scaling_config)?;
        }
        PoolCommands::Destroy { name, zone, metrics_bucket } => {
            manager.destroy(&name, &zone, metrics_bucket.as_deref())?;
        }
        PoolCommands::List { name } => {
            manager.list(&name)?;
        }
        PoolCommands::Status { name, zone } => {
            manager.status(&name, &zone)?;
        }
        PoolCommands::UpdateBinary {
            name,
            zone,
            binary,
            skip_build,
        } => {
            manager.update_binary(&name, &zone, binary, skip_build)?;
        }
        PoolCommands::Cancel { name, zone } => {
            manager.cancel(&name, &zone)?;
        }
        PoolCommands::Workers { name, zone } => {
            manager.workers(&name, &zone)?;
        }
        PoolCommands::Events { name, zone, follow } => {
            manager.events(&name, &zone, follow)?;
        }
        PoolCommands::Failures { name, zone } => {
            manager.failures(&name, &zone)?;
        }
        PoolCommands::Logs { name, zone, worker } => {
            manager.logs(&name, &zone, &worker)?;
        }
    }

    Ok(())
}

/// Run service commands (coordinator or worker)
fn run_service_command(command: ServiceCommands) -> Result<()> {
    use crate::distributed::{coordinator, worker};

    // Create a runtime for the async service components
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(genohype_core::HailError::Io)?;

    match command {
        ServiceCommands::StartCoordinator {
            port,
            db_path,
            backup_path,
            input,
            output,
            total_partitions,
            batch_size,
            timeout,
        } => {
            // If no job parameters provided, start in idle mode (0 partitions)
            // Job can be submitted later via POST /api/job
            rt.block_on(coordinator::run_coordinator(
                port,
                db_path,
                backup_path,
                input.unwrap_or_default(),
                output.unwrap_or_default(),
                total_partitions.unwrap_or(0),
                batch_size,
                timeout,
            ))
        }
        ServiceCommands::StartWorker {
            url,
            worker_id,
            poll_interval,
        } => {
            let config = worker::WorkerConfig {
                coordinator_url: url,
                worker_id,
                poll_interval_ms: poll_interval,
                connect_timeout_secs: 30,
            };
            rt.block_on(worker::run_worker(config))
        }
    }
}

// ---------------------------------------------------------------------------
// Ingest commands
// ---------------------------------------------------------------------------

#[cfg(feature = "clickhouse")]
fn run_ingest_command(command: IngestCommands) -> Result<()> {
    use crate::ingest::manhattan::run_ingest_task;

    match command {
        IngestCommands::Manhattan(args) => {
            // For local/non-distributed execution, we run the ingestion directly
            // by parsing the input_dir to discover phenotypes
            println!("Starting Manhattan ingestion (local mode)");
            println!("  Input dir: {}", args.input_dir);
            println!("  ClickHouse: {}", args.clickhouse_url);
            println!("  Database: {}", args.database);

            // Discover phenotypes using gsutil
            let output = std::process::Command::new("gsutil")
                .args(["-m", "ls", "-r", &format!("{}/**/manifest.json", args.input_dir.trim_end_matches('/'))])
                .output()
                .map_err(|e| genohype_core::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to run gsutil: {}", e),
                )))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !stderr.contains("matched no objects") {
                    return Err(genohype_core::HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("gsutil failed: {}", stderr),
                    )));
                }
                println!("No phenotypes found in {}", args.input_dir);
                return Ok(());
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut total_rows = 0;
            let mut count = 0;

            for line in stdout.lines() {
                let line = line.trim();
                if !line.ends_with("/manifest.json") {
                    continue;
                }

                let base_path = line.trim_end_matches("/manifest.json");
                let segments: Vec<&str> = base_path.split('/').collect();
                if segments.len() < 2 {
                    continue;
                }

                let phenotype_id = segments[segments.len() - 1].to_string();
                let ancestry = segments[segments.len() - 2].to_string();

                if ancestry.is_empty() || phenotype_id.is_empty() {
                    continue;
                }

                count += 1;
                println!("Ingesting {}/{} ({})...", ancestry, phenotype_id, base_path);

                match run_ingest_task(&phenotype_id, &ancestry, base_path, &args.clickhouse_url, &args.database) {
                    Ok(rows) => {
                        total_rows += rows;
                        println!("  Done: {} rows", rows);
                    }
                    Err(e) => {
                        eprintln!("  Error: {}", e);
                    }
                }
            }

            println!("Ingestion complete: {} phenotypes, {} total rows", count, total_rows);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Manhattan plot
// ---------------------------------------------------------------------------

fn run_manhattan(args: ManhattanArgs) -> Result<()> {
    use crossbeam_channel::bounded;
    use crate::manhattan::{
        annotate::Annotator,
        data::{
            extract_plot_data, ManhattanSidecar, SidecarChromosome, SidecarImage,
            SidecarThreshold, SidecarYAxis, SignificantHit,
        },
        genes::{process_gene_burden, GeneMap},
        layout::{ChromosomeLayout, YScale},
        pipeline::{aggregate_shards_and_render, run_integrated_pipeline, PipelineConfig},
        reference::get_contig_lengths,
        render::ManhattanRenderer,
    };
    use std::fs::{self, File};
    use std::io::Write;
    use std::thread;

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

        let has_pngs = png_check
            .map(|o| o.status.success())
            .unwrap_or(false);

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

// ---------------------------------------------------------------------------
// Manhattan batch processing
// ---------------------------------------------------------------------------

/// Run a batch of Manhattan plots from assets JSON.
///
/// This command parses the assets JSON file, groups entries by phenotype,
/// and submits a batch job to the coordinator for parallel processing.
///
/// Phase 1: Stub implementation - actual logic comes in Phase 3.
fn run_manhattan_batch(args: ManhattanBatchArgs) -> Result<()> {
    use crate::manhattan::batch::{load_and_group_assets, BatchSummary};
    use crate::manhattan::config::ManhattanJobConfig;
    use std::path::Path;

    // Load config file if provided
    let job_config = if let Some(ref path) = args.config {
        ManhattanJobConfig::load(Path::new(path))?
    } else {
        ManhattanJobConfig::default()
    };

    // Merge CLI args with config (CLI overrides)
    let assets_json = args.assets_json.clone()
        .or(job_config.job.assets_json.clone())
        .ok_or_else(|| genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "manhattan-batch requires --assets-json or job.assets_json in config",
        )))?;

    let output_dir = args.output_dir.clone()
        .or(job_config.job.output_dir.clone())
        .ok_or_else(|| genohype_core::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "manhattan-batch requires --output-dir or job.output_dir in config",
        )))?;

    let analysis_ids = args.analysis_ids.clone().or_else(|| {
        if job_config.job.analysis_ids.is_empty() { None } else { Some(job_config.job.analysis_ids.clone()) }
    });
    let ancestries = args.ancestries.clone().or_else(|| {
        if job_config.job.ancestries.is_empty() { None } else { Some(job_config.job.ancestries.clone()) }
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
    println!("  {} phenotypes found", summary.total_phenotypes.to_string().bright_white());
    println!();

    // By input type
    println!("{}", "  By Input Type:".dimmed());
    if summary.combined > 0 {
        println!("    Combined (exome+genome): {}", summary.combined.to_string().green());
    }
    if summary.exome_only > 0 {
        println!("    Exome only:              {}", summary.exome_only.to_string().cyan());
    }
    if summary.genome_only > 0 {
        println!("    Genome only:             {}", summary.genome_only.to_string().cyan());
    }
    if summary.gene_burden_only > 0 {
        println!("    Gene burden only:        {}", summary.gene_burden_only.to_string().yellow());
    }
    println!();

    // By ancestry
    println!("{}", "  By Ancestry:".dimmed());
    let mut ancestries: Vec<_> = summary.by_ancestry.iter().collect();
    ancestries.sort_by_key(|(k, _)| *k);
    for (ancestry, count) in ancestries {
        println!("    {}: {}", ancestry.bright_white(), count);
    }
    println!();

    // Configuration summary
    println!("{}", "Configuration".cyan().bold());
    println!("  Threshold: {}", format!("{:.0e}", args.threshold).bright_white());
    println!("  Gene threshold: {}", format!("{:.1e}", args.gene_threshold).bright_white());
    if args.locus_plots {
        println!("  Locus plots: {}", "enabled".green());
        println!("    Locus threshold: {}", format!("{}", args.locus_threshold).dimmed());
        println!("    Locus window: {} bp", args.locus_window.to_string().dimmed());
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
    let mode_flag = if args.scan_only { " \\\n      --scan-only" } else if args.aggregate_only { " \\\n      --aggregate-only" } else { "" };
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

// ---------------------------------------------------------------------------
// Loci generation from existing Manhattan output
// ---------------------------------------------------------------------------

fn run_loci(args: LociArgs) -> Result<()> {
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

// ---------------------------------------------------------------------------
// Locus plot (zoomed-in LocusZoom-style)
// ---------------------------------------------------------------------------

fn run_locus(args: LocusArgs) -> Result<()> {
    use crate::manhattan::data::{extract_plot_data, VariantSource};
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
        eprintln!("{} Invalid start position: {}", "Error:".red().bold(), parts[1]);
        std::process::exit(1);
    });
    let end_pos: i32 = parts[2].parse().unwrap_or_else(|_| {
        eprintln!("{} Invalid end position: {}", "Error:".red().bold(), parts[2]);
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
    let fetch_variants = |path: &str, source: VariantSource, chrom: &str, start: i32, end: i32, y_field: &str, threshold: f64| -> Result<Vec<RenderVariant>> {
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
        let vars = fetch_variants(path, VariantSource::Genome, &chrom, start_pos, end_pos, &args.y_field, args.threshold)?;
        all_variants.extend(vars);
    }

    if let Some(ref path) = args.exome {
        let vars = fetch_variants(path, VariantSource::Exome, &chrom, start_pos, end_pos, &args.y_field, args.threshold)?;
        all_variants.extend(vars);
    }

    if all_variants.is_empty() {
        println!("{} No variants found in the specified region.", "Warning:".yellow());
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
