//! Cluster management API endpoints.
//!
//! Provides endpoints for viewing cluster configuration, GCP VM state,
//! and scaling the worker pool up/down.

use crate::cloud::CloudProvider;
use crate::distributed::coordinator::SharedState;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};

/// Response for GET /api/cluster/config
#[derive(Serialize)]
pub struct ClusterConfigResponse {
    pub pool_name: Option<String>,
    pub gcp_project: Option<String>,
    pub gcp_zone: Option<String>,
    pub machine_type: Option<String>,
    pub spot: Option<bool>,
    pub network: Option<String>,
    pub subnet: Option<String>,
}

/// A GCP VM instance in the cluster
#[derive(Serialize, Clone)]
pub struct VmInfo {
    pub name: String,
    pub zone: String,
    pub status: String,
    #[serde(rename = "networkInterfaces")]
    pub network_interfaces: Vec<NetworkInterfaceInfo>,
    /// Machine type extracted from zone path (if available)
    pub machine_type: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct NetworkInterfaceInfo {
    #[serde(rename = "networkIP")]
    pub network_ip: String,
}

/// Request body for POST /api/cluster/scale
#[derive(Deserialize)]
pub struct ScaleRequest {
    pub target_workers: usize,
}

/// Response for POST /api/cluster/scale
#[derive(Serialize)]
pub struct ScaleResponse {
    pub success: bool,
    pub message: String,
    pub previous_workers: usize,
    pub target_workers: usize,
}

/// GET /api/cluster/config - Returns the current cluster configuration
pub async fn get_config(
    State(state): State<SharedState>,
) -> Json<ClusterConfigResponse> {
    let data = state.lock().unwrap();
    Json(ClusterConfigResponse {
        pool_name: data.config.pool_name.clone(),
        gcp_project: data.config.gcp_project.clone(),
        gcp_zone: data.config.gcp_zone.clone(),
        machine_type: data.config.machine_type.clone(),
        spot: data.config.spot,
        network: data.config.network.clone(),
        subnet: data.config.subnet.clone(),
    })
}

/// GET /api/cluster/vms - Returns the current GCP VM state
pub async fn get_vms(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    let pool_name = {
        let data = state.lock().unwrap();
        data.config.pool_name.clone()
    };

    let pool_name = match pool_name {
        Some(name) => name,
        None => {
            return Json(serde_json::json!({
                "error": "No pool_name configured",
                "vms": []
            }));
        }
    };

    // Run gcloud list in a blocking task to avoid blocking the async runtime
    let vms = tokio::task::spawn_blocking(move || {
        let client = crate::cloud::gcp::GcpClient::new();
        client.list_instances(&pool_name)
    })
    .await;

    match vms {
        Ok(Ok(instances)) => {
            let vm_infos: Vec<VmInfo> = instances
                .into_iter()
                .map(|inst| VmInfo {
                    name: inst.name,
                    zone: inst.zone,
                    status: inst.status,
                    network_interfaces: inst
                        .network_interfaces
                        .into_iter()
                        .map(|ni| NetworkInterfaceInfo {
                            network_ip: ni.network_ip,
                        })
                        .collect(),
                    machine_type: inst.machine_type.as_ref().map(|mt| {
                        // Extract short name from full URL like
                        // "https://www.googleapis.com/.../machineTypes/c4-highcpu-48"
                        mt.rsplit('/').next().unwrap_or(mt).to_string()
                    }),
                })
                .collect();
            Json(serde_json::json!({ "vms": vm_infos }))
        }
        Ok(Err(e)) => {
            Json(serde_json::json!({
                "error": format!("Failed to list instances: {}", e),
                "vms": []
            }))
        }
        Err(e) => {
            Json(serde_json::json!({
                "error": format!("Task failed: {}", e),
                "vms": []
            }))
        }
    }
}

/// POST /api/cluster/scale - Scale the cluster up or down
pub async fn scale_cluster(
    State(state): State<SharedState>,
    Json(req): Json<ScaleRequest>,
) -> Json<ScaleResponse> {
    // Extract config from state
    let (pool_name, gcp_zone, gcp_project, network, subnet, binary_gcs_url) = {
        let data = state.lock().unwrap();
        (
            data.config.pool_name.clone(),
            data.config.gcp_zone.clone(),
            data.config.gcp_project.clone(),
            data.config.network.clone(),
            data.config.subnet.clone(),
            // We need the binary GCS URL for startup scripts - derive from backup_path pattern
            data.config.backup_path.as_ref().and_then(|bp| {
                // Extract bucket from backup path like gs://bucket/pool-logs/xxx/ops.db
                bp.strip_prefix("gs://").and_then(|rest| {
                    rest.split('/').next().map(|bucket| {
                        format!("gs://{}/binaries/genohype", bucket)
                    })
                })
            }),
        )
    };

    let pool_name = match pool_name {
        Some(name) => name,
        None => {
            return Json(ScaleResponse {
                success: false,
                message: "No pool_name configured".to_string(),
                previous_workers: 0,
                target_workers: req.target_workers,
            });
        }
    };

    let zone = gcp_zone.unwrap_or_else(|| "us-central1-b".to_string());
    let target = req.target_workers;

    // Get current worker VMs
    let pool_name_clone = pool_name.clone();
    let current_vms = tokio::task::spawn_blocking(move || {
        let client = crate::cloud::gcp::GcpClient::new();
        client.list_instances(&pool_name_clone)
    })
    .await;

    let instances = match current_vms {
        Ok(Ok(inst)) => inst,
        Ok(Err(e)) => {
            return Json(ScaleResponse {
                success: false,
                message: format!("Failed to list instances: {}", e),
                previous_workers: 0,
                target_workers: target,
            });
        }
        Err(e) => {
            return Json(ScaleResponse {
                success: false,
                message: format!("Task failed: {}", e),
                previous_workers: 0,
                target_workers: target,
            });
        }
    };

    // Count current workers (exclude coordinator)
    let mut worker_instances: Vec<_> = instances
        .iter()
        .filter(|i| i.name.contains("-worker-"))
        .collect();
    let current_count = worker_instances.len();

    if target == current_count {
        return Json(ScaleResponse {
            success: true,
            message: format!("Already at {} workers", target),
            previous_workers: current_count,
            target_workers: target,
        });
    }

    let machine_type = {
        let data = state.lock().unwrap();
        data.config.machine_type.clone().unwrap_or_else(|| "c4-highcpu-48".to_string())
    };
    let spot = {
        let data = state.lock().unwrap();
        data.config.spot.unwrap_or(true)
    };

    if target > current_count {
        // Scale UP: create new workers
        let to_create = target - current_count;

        // Find existing worker indices
        let existing_indices: std::collections::HashSet<usize> = worker_instances
            .iter()
            .filter_map(|i| {
                i.name
                    .rsplit('-')
                    .next()
                    .and_then(|s| s.parse::<usize>().ok())
            })
            .collect();

        // Find next available indices
        let mut new_indices = Vec::new();
        let mut idx = 0;
        while new_indices.len() < to_create {
            if !existing_indices.contains(&idx) {
                new_indices.push(idx);
            }
            idx += 1;
        }

        let pool_name_for_script = pool_name.clone();
        let startup_script = crate::cloud::startup::generate_worker_startup_script(
            binary_gcs_url.as_deref(),
            &pool_name_for_script,
        );

        let project = gcp_project.unwrap_or_default();
        let net = network;
        let sub = subnet;
        let zone_clone = zone.clone();

        let instance_setups: Vec<crate::cloud::InstanceSetup> = new_indices
            .iter()
            .map(|&i| crate::cloud::InstanceSetup {
                name: format!("{}-worker-{}", pool_name, i),
                machine_type: machine_type.clone(),
                zone: zone_clone.clone(),
                tags: vec![format!(
                    "genohype-worker,pool-{},role-worker",
                    pool_name
                )],
                startup_script: startup_script.clone(),
                spot,
                network: net.clone(),
                subnet: sub.clone(),
                project_id: project.clone(),
            })
            .collect();

        // Spawn creation in background
        tokio::task::spawn_blocking(move || {
            let client = crate::cloud::gcp::GcpClient::new();
            if let Err(e) = client.create_instances(&instance_setups) {
                eprintln!("Failed to create instances: {}", e);
            }
        });

        Json(ScaleResponse {
            success: true,
            message: format!(
                "Scaling up from {} to {} workers (creating {})",
                current_count, target, to_create
            ),
            previous_workers: current_count,
            target_workers: target,
        })
    } else {
        // Scale DOWN: delete highest-indexed workers
        let to_delete = current_count - target;

        // Sort by index descending to remove highest first
        worker_instances.sort_by(|a, b| {
            let idx_a = a.name.rsplit('-').next().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
            let idx_b = b.name.rsplit('-').next().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
            idx_b.cmp(&idx_a)
        });

        let names_to_delete: Vec<String> = worker_instances
            .iter()
            .take(to_delete)
            .map(|i| i.name.clone())
            .collect();

        let project = gcp_project.unwrap_or_default();
        let zone_clone = zone.clone();

        // Spawn deletion in background
        tokio::task::spawn_blocking(move || {
            let client = crate::cloud::gcp::GcpClient::new();
            if let Err(e) = client.delete_instances(&names_to_delete, &zone_clone, &project) {
                eprintln!("Failed to delete instances: {}", e);
            }
        });

        Json(ScaleResponse {
            success: true,
            message: format!(
                "Scaling down from {} to {} workers (deleting {})",
                current_count, target, to_delete
            ),
            previous_workers: current_count,
            target_workers: target,
        })
    }
}
