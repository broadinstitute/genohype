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

/// Minimum seconds between gcloud VM list calls to avoid piling up requests.
const VM_CACHE_TTL_SECS: u64 = 15;

/// GET /api/cluster/vms - Returns the current GCP VM state
pub async fn get_vms(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    let (pool_name, cached) = {
        let data = state.lock().unwrap();
        let cached = data.cached_vms.as_ref().and_then(|(json, ts)| {
            if ts.elapsed().as_secs() < VM_CACHE_TTL_SECS {
                Some(json.clone())
            } else {
                None
            }
        });
        (data.config.pool_name.clone(), cached)
    };

    // Return cached result if fresh enough
    if let Some(cached_json) = cached {
        return Json(cached_json);
    }

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
    let current_vms = tokio::task::spawn_blocking(move || {
        let client = crate::cloud::gcp::GcpClient::new();
        client.list_instances(&pool_name)
    })
    .await;

    match current_vms {
        Ok(Ok(instances)) => {
            // Derive missing config from existing VMs
            {
                let mut data = state.lock().unwrap();
                let worker_instances: Vec<_> = instances.iter().filter(|i| i.name.contains("-worker-")).collect();

                if let Some(template_vm) = worker_instances.first().copied().or(instances.first()) {
                    if data.config.gcp_zone.is_none() {
                        data.config.gcp_zone = Some(template_vm.zone.rsplit('/').next().unwrap_or(&template_vm.zone).to_string());
                    }
                    if data.config.network.is_none() {
                        if let Some(ni) = template_vm.network_interfaces.first() {
                            if let Some(net) = &ni.network {
                                data.config.network = Some(net.rsplit('/').next().unwrap_or(net).to_string());
                            }
                        }
                    }
                    if data.config.subnet.is_none() {
                        if let Some(ni) = template_vm.network_interfaces.first() {
                            if let Some(sub) = &ni.subnetwork {
                                data.config.subnet = Some(sub.rsplit('/').next().unwrap_or(sub).to_string());
                            }
                        }
                    }
                }

                if let Some(worker_vm) = worker_instances.first() {
                    if data.config.machine_type.is_none() {
                        if let Some(mt) = &worker_vm.machine_type {
                            data.config.machine_type = Some(mt.rsplit('/').next().unwrap_or(mt).to_string());
                        }
                    }
                    if data.config.spot.is_none() {
                        if let Some(sched) = &worker_vm.scheduling {
                            if let Some(model) = &sched.provisioning_model {
                                data.config.spot = Some(model == "SPOT");
                            }
                        }
                    }
                }
            }

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
            let result = serde_json::json!({ "vms": vm_infos });

            // Cache the result
            {
                let mut data = state.lock().unwrap();
                data.cached_vms = Some((result.clone(), std::time::Instant::now()));
            }

            Json(result)
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
    let (pool_name, mut gcp_zone, gcp_project, mut network, mut subnet, binary_gcs_url, mut machine_type, mut spot) = {
        let data = state.lock().unwrap();
        (
            data.config.pool_name.clone(),
            data.config.gcp_zone.clone(),
            data.config.gcp_project.clone(),
            data.config.network.clone(),
            data.config.subnet.clone(),
            // Use the actual staged binary URL from the last update-binary call
            data.update_fleet_url.clone(),
            data.config.machine_type.clone(),
            data.config.spot,
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

    // Derive missing config from existing worker VMs
    if let Some(template_vm) = worker_instances.first().copied().or(instances.first()) {
        if gcp_zone.is_none() {
            gcp_zone = Some(template_vm.zone.rsplit('/').next().unwrap_or(&template_vm.zone).to_string());
        }
        if network.is_none() {
            if let Some(ni) = template_vm.network_interfaces.first() {
                if let Some(net) = &ni.network {
                    network = Some(net.rsplit('/').next().unwrap_or(net).to_string());
                }
            }
        }
        if subnet.is_none() {
            if let Some(ni) = template_vm.network_interfaces.first() {
                if let Some(sub) = &ni.subnetwork {
                    subnet = Some(sub.rsplit('/').next().unwrap_or(sub).to_string());
                }
            }
        }
    }

    if let Some(worker_vm) = worker_instances.first() {
        if machine_type.is_none() {
            if let Some(mt) = &worker_vm.machine_type {
                machine_type = Some(mt.rsplit('/').next().unwrap_or(mt).to_string());
            }
        }
        if spot.is_none() {
            if let Some(sched) = &worker_vm.scheduling {
                if let Some(model) = &sched.provisioning_model {
                    spot = Some(model == "SPOT");
                }
            }
        }
    }

    // Persist derived config back to state for UI
    {
        let mut data = state.lock().unwrap();
        if data.config.gcp_zone.is_none() { data.config.gcp_zone = gcp_zone.clone(); }
        if data.config.network.is_none() { data.config.network = network.clone(); }
        if data.config.subnet.is_none() { data.config.subnet = subnet.clone(); }
        if data.config.machine_type.is_none() { data.config.machine_type = machine_type.clone(); }
        if data.config.spot.is_none() { data.config.spot = spot; }
    }

    if target == current_count {
        return Json(ScaleResponse {
            success: true,
            message: format!("Already at {} workers", target),
            previous_workers: current_count,
            target_workers: target,
        });
    }

    let zone = gcp_zone.unwrap_or_else(|| "us-central1-b".to_string());
    let machine_type = machine_type.unwrap_or_else(|| "c4-highcpu-48".to_string());
    let spot = spot.unwrap_or(true);

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
        // Use GCS URL if available, otherwise fall back to downloading from coordinator HTTP
        let effective_binary_url = binary_gcs_url.or_else(|| {
            Some(format!("http://{}-coordinator:3000/api/binary", pool_name_for_script))
        });
        let startup_script = crate::cloud::startup::generate_worker_startup_script(
            effective_binary_url.as_deref(),
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

        // Invalidate VM cache so next poll picks up the new worker
        {
            let mut data = state.lock().unwrap();
            data.cached_vms = None;
        }

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

        // Remove deleted workers from the fleet registry and block future heartbeats
        {
            let mut data = state.lock().unwrap();
            for name in &names_to_delete {
                data.worker_registry.remove(name);
                data.deleted_workers.insert(name.clone());
            }
            // Invalidate VM cache so next poll reflects the change
            data.cached_vms = None;
        }

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
