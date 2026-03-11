//! Generic coordinator server for distributed task management.
//!
//! This module provides an Axum-based HTTP server that coordinates work
//! distribution across workers using a `CoordinatorPlugin` implementation.

use crate::distributed::message::{
    CompleteRequest, CompleteResponse, StatusResponse, WorkRequest, WorkResponse,
};
use crate::traits::{CoordinatorPlugin, TaskResult};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::net::SocketAddr;
use std::sync::Arc;

/// Configuration for the coordinator server.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Port to listen on
    pub port: u16,
    /// Input path (for informational purposes)
    pub input_path: String,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            input_path: String::new(),
        }
    }
}

/// Shared state for the coordinator server.
struct CoordinatorState {
    plugin: Arc<dyn CoordinatorPlugin>,
    #[allow(dead_code)]
    config: CoordinatorConfig,
}

/// Start the coordinator HTTP server.
///
/// This function starts an Axum server that handles worker requests
/// for work assignments and completion reporting.
///
/// # Arguments
/// * `config` - Coordinator configuration
/// * `plugin` - Plugin implementation for work scheduling
///
/// # Example
/// ```ignore
/// let plugin = Arc::new(MyCoordinatorPlugin::new());
/// let config = CoordinatorConfig {
///     port: 3000,
///     input_path: "gs://bucket/data".to_string(),
/// };
/// start_coordinator(config, plugin).await?;
/// ```
pub async fn start_coordinator(
    config: CoordinatorConfig,
    plugin: Arc<dyn CoordinatorPlugin>,
) -> Result<(), anyhow::Error> {
    let state = Arc::new(CoordinatorState {
        plugin,
        config: config.clone(),
    });

    let app = Router::new()
        .route("/work", post(handle_work))
        .route("/complete", post(handle_complete))
        .route("/status", get(handle_status))
        .route("/health", get(handle_health))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    println!("Coordinator listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Handle work request from a worker.
async fn handle_work(
    State(state): State<Arc<CoordinatorState>>,
    Json(request): Json<WorkRequest>,
) -> impl IntoResponse {
    match state.plugin.get_work(&request.worker_id).await {
        Some(assignment) => {
            let total_tasks = assignment.tasks.iter().filter_map(|t| t.total).max().unwrap_or(0);
            let response = WorkResponse::Task {
                tasks: assignment.tasks,
                input_path: assignment.input_path.unwrap_or_default(),
                payload: assignment.payload,
                total_tasks,
                filters: assignment.filters,
                intervals: Vec::new(),
                session_id: None, // Generic pool coordinator doesn't use session tracking
            };
            (StatusCode::OK, Json(response))
        }
        None => {
            // Check if job is complete
            if state.plugin.is_complete().await {
                (StatusCode::OK, Json(WorkResponse::Exit))
            } else {
                (StatusCode::OK, Json(WorkResponse::Wait))
            }
        }
    }
}

/// Handle completion report from a worker.
async fn handle_complete(
    State(state): State<Arc<CoordinatorState>>,
    Json(request): Json<CompleteRequest>,
) -> impl IntoResponse {
    let result = if request.error.is_some() {
        TaskResult::failure(request.error.unwrap_or_default())
    } else {
        TaskResult::success(request.items_processed, request.result_json)
    };

    match state
        .plugin
        .complete_work(&request.worker_id, &request.tasks, result)
        .await
    {
        Ok(()) => (StatusCode::OK, Json(CompleteResponse { acknowledged: true })),
        Err(e) => {
            eprintln!("Error completing work: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CompleteResponse { acknowledged: false }),
            )
        }
    }
}

/// Handle status query.
async fn handle_status(State(state): State<Arc<CoordinatorState>>) -> impl IntoResponse {
    let (pending, processing, completed, total) = state.plugin.get_status().await;
    Json(StatusResponse {
        pending_tasks: pending,
        processing_tasks: processing,
        completed_tasks: completed,
        total_tasks: total,
        total_items: 0,
        failed_tasks: 0,
        is_complete: pending == 0 && processing == 0,
    })
}

/// Health check endpoint.
async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
