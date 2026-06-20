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
mod commands;
mod config;
mod distributed;
mod env;
mod manhattan;

#[cfg(feature = "clickhouse")]
mod ingest;

// Re-export core error types for use in CLI modules
pub use genohype_core::{HailError, Result};

use clap::Parser;
use cli::{Cli, CacheCommands, ClickHouseCommands, ClusterCommands, Commands, EnvCommands, ExportCommands, VcfCommands};
#[cfg(feature = "clickhouse")]
use cli::IngestCommands;
#[cfg(feature = "vep")]
use commands::annotate::run_annotate;
use genohype_core::metadata::CacheOptions;
use owo_colors::OwoColorize;

// Import command handlers from the commands module
use commands::export::{
    run_export_cache_build, run_export_hail, run_export_json, run_export_parquet, run_export_vcf,
};
#[cfg(feature = "bigquery")]
use commands::export::run_export_bigquery;
#[cfg(feature = "clickhouse")]
use commands::export::{run_export_clickhouse, run_export_genes_clickhouse};
#[cfg(feature = "elasticsearch")]
use commands::export::{run_export_elasticsearch, run_export_genes_elasticsearch};
#[cfg(feature = "postgres")]
use commands::export::{run_export_genes_postgres, run_export_postgres};
use commands::info::show_info;
#[cfg(feature = "clickhouse")]
use commands::ingest::run_ingest_command;
use commands::manhattan::{run_loci, run_locus, run_manhattan, run_manhattan_batch};
use commands::pool::run_pool_command;
use commands::query::run_query;
#[cfg(feature = "validation")]
use commands::schema::{run_generate_schema, run_validate};
use commands::service::run_service_command;
use commands::summary::run_summary;
use commands::vcf::run_vcf_index;

fn main() -> Result<()> {
    // Suppress gcloud warnings (e.g., the IAP NumPy warning) globally for all child processes
    std::env::set_var("CLOUDSDK_CORE_DISABLE_WARNINGS", "1");

    let cli = Cli::parse();

    // Initialize tracing: --profile writes Chrome trace JSON, otherwise use RUST_LOG env filter
    // The _guard must live until main() returns so the trace file gets flushed.
    let _chrome_guard = init_tracing(&cli);

    // Load configuration from file
    let config = config::Config::load_from_path(cli.config.as_deref());

    // Build cache options from config + CLI flags
    let cache_ttl_secs = std::env::var("GENOHYPE_CACHE_TTL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|h| h * 3600)
        .unwrap_or(config.cache.ttl_hours * 3600);
    let cache_opts = Some(CacheOptions {
        enabled: true,
        ttl_secs: cache_ttl_secs,
        force_refresh: cli.no_cache,
    });

    match cli.command {
        Commands::Info { path, json, count, globals } => show_info(&path, json, count, globals, cache_opts)?,
        Commands::Summary { path } => run_summary(&path, cache_opts)?,
        Commands::Query(args) => run_query(args, cache_opts)?,
        Commands::Export { command } => match command {
            ExportCommands::Parquet(args) => run_export_parquet(args)?,
            ExportCommands::Json(args) => run_export_json(args)?,
            ExportCommands::Vcf(args) => run_export_vcf(args)?,
            ExportCommands::Hail(args) => run_export_hail(args)?,
            #[cfg(feature = "clickhouse")]
            ExportCommands::Clickhouse(args) => run_export_clickhouse(args)?,
            #[cfg(feature = "elasticsearch")]
            ExportCommands::Elasticsearch(args) => run_export_elasticsearch(args)?,
            #[cfg(feature = "postgres")]
            ExportCommands::Postgres(args) => run_export_postgres(args)?,
            #[cfg(feature = "postgres")]
            ExportCommands::GenesPostgres(args) => run_export_genes_postgres(args)?,
            #[cfg(feature = "elasticsearch")]
            ExportCommands::GenesElasticsearch(args) => run_export_genes_elasticsearch(args)?,
            #[cfg(feature = "clickhouse")]
            ExportCommands::GenesClickhouse(args) => run_export_genes_clickhouse(args)?,
            ExportCommands::CacheBuild(args) => run_export_cache_build(args)?,
            #[cfg(feature = "bigquery")]
            ExportCommands::Bigquery(args) => run_export_bigquery(args)?,
        },
        Commands::Manhattan(args) => run_manhattan(args)?,
        Commands::ManhattanBatch(args) => run_manhattan_batch(args)?,
        Commands::Loci(args) => run_loci(args)?,
        Commands::Locus(args) => run_locus(args)?,
        #[cfg(feature = "validation")]
        Commands::Schema { command } => match command {
            cli::SchemaSubcommands::Validate(args) => run_validate(args)?,
            cli::SchemaSubcommands::Generate(args) => {
                run_generate_schema(&args.table, args.output.as_deref())?
            }
        },
        Commands::Pool { command } => run_pool_command(command, &config)?,
        Commands::Cluster { command } => match command {
            ClusterCommands::List { status } => cluster::list_clusters(&config, status.as_ref())?,
            ClusterCommands::Show { name } => cluster::show_cluster(&config, &name)?,
            ClusterCommands::Verify { name } => cluster::verify_cluster(&config, &name)?,
            ClusterCommands::Deploy {
                name,
                tag,
                backend_only,
            } => cluster::deploy_cluster(&config, &name, &tag, backend_only)?,
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
        Commands::Cache { command } => match command {
            CacheCommands::Clear => {
                if let Some(cache) = genohype_core::metadata::MetadataCache::new() {
                    let cache_dir = cache.cache_dir().to_path_buf();
                    cache.clear().map_err(|e| {
                        genohype_core::HailError::Io(e)
                    })?;
                    println!("Cache cleared: {}", cache_dir.display());
                } else {
                    eprintln!("Could not determine cache directory");
                }
            }
        },
        Commands::Service { command } => run_service_command(command)?,
        #[cfg(feature = "clickhouse")]
        Commands::Ingest { command } => run_ingest_command(command)?,
        #[cfg(feature = "vep")]
        Commands::Annotate(args) => run_annotate(args)?,
        Commands::Vcf { command } => match command {
            VcfCommands::Index { path, output } => run_vcf_index(&path, output.as_deref())?,
        },
        Commands::Stress(args) => {
            println!(
                "{} The stress command is designed to be run via a worker pool:",
                "Note:".cyan()
            );
            println!(
                "  genohype pool submit <pool_name> -- stress --partitions {} --cpu-secs {} --memory-mb {}",
                args.partitions, args.cpu_secs, args.memory_mb
            );
        }
    }

    // _chrome_guard is dropped here, flushing the trace file
    Ok(())
}

/// Initialize tracing based on CLI flags.
///
/// - `--profile trace.json` → Chrome trace format (open in https://ui.perfetto.dev)
/// - `RUST_LOG=genohype_core=debug` → stderr text output with timing
/// - Neither → no tracing overhead
fn init_tracing(cli: &Cli) -> Option<tracing_chrome::FlushGuard> {
    use tracing_subscriber::prelude::*;

    if let Some(ref path) = cli.profile {
        // Chrome trace mode: captures all spans as a timeline
        let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .file(path)
            .include_args(true)
            .build();

        tracing_subscriber::registry()
            .with(chrome_layer)
            .init();

        eprintln!("Profiling enabled → {}", path);
        Some(guard)
    } else {
        // Default: RUST_LOG-based text output to stderr
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_target(true)
            .with_timer(tracing_subscriber::fmt::time::uptime())
            .with_writer(std::io::stderr)
            .init();

        None
    }
}
