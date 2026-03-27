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
use cli::{Cli, ClickHouseCommands, ClusterCommands, Commands, EnvCommands, ExportCommands};
#[cfg(feature = "clickhouse")]
use cli::IngestCommands;
use owo_colors::OwoColorize;

// Import command handlers from the commands module
use commands::export::{run_export_hail, run_export_json, run_export_parquet, run_export_vcf};
#[cfg(feature = "bigquery")]
use commands::export::run_export_bigquery;
#[cfg(feature = "clickhouse")]
use commands::export::run_export_clickhouse;
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

fn main() -> Result<()> {
    // Suppress gcloud warnings (e.g., the IAP NumPy warning) globally for all child processes
    std::env::set_var("CLOUDSDK_CORE_DISABLE_WARNINGS", "1");

    let cli = Cli::parse();

    // Load configuration from file
    let config = config::Config::load_from_path(cli.config.as_deref());

    match cli.command {
        Commands::Info { path, json } => show_info(&path, json)?,
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
        Commands::Service { command } => run_service_command(command)?,
        #[cfg(feature = "clickhouse")]
        Commands::Ingest { command } => run_ingest_command(command)?,
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

    Ok(())
}
