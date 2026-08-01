//! Service commands for coordinator and worker processes.

use crate::cli::ServiceCommands;
use crate::distributed::{coordinator, worker};
use genohype_core::Result;

/// Run service commands (coordinator or worker)
pub fn run_service_command(command: ServiceCommands) -> Result<()> {
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
            pool_name,
            gcp_project,
            gcp_zone,
            cluster_machine_type,
            cluster_spot,
            cluster_network,
            cluster_subnet,
            cluster_public_ip,
            cluster_manage_firewall,
            cluster_worker_service_account,
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
                pool_name,
                gcp_project,
                gcp_zone,
                cluster_machine_type,
                cluster_spot,
                cluster_network,
                cluster_subnet,
                cluster_public_ip,
                cluster_manage_firewall,
                cluster_worker_service_account,
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
