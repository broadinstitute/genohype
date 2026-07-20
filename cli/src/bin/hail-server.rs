//! Hail Decoder HTTP Server
//!
//! A REST API server for querying Hail tables with streaming support.
//!
//! ## Endpoints
//!
//! - `GET /health` - Health check
//! - `GET /tables/{path}/info` - Table metadata
//! - `GET /tables/{path}/schema` - Table schema
//! - `GET /tables/{path}/query` - Query with streaming SSE response
//!
//! ## Example
//!
//! ```bash
//! # Start the server
//! cargo run --bin hail-server
//!
//! # Query a table
//! curl "http://localhost:3000/tables/gs%3A%2F%2Fbucket%2Ftable.ht/query?limit=10"
//! ```

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use axum_extra::extract::Query as ExtraQuery;
use genohype_core::codec::EncodedValue;
use genohype_core::query::{KeyRange, KeyValue, QueryBound, QueryEngine};
use genohype_core::summary::format_schema_clean;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use tracing::{debug, error, info};

/// Application state for caching query engines
struct AppState {
    /// Cache of open query engines by path
    engines: RwLock<std::collections::HashMap<String, Arc<QueryEngine>>>,
}

impl AppState {
    fn new() -> Self {
        Self {
            engines: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create a QueryEngine for the given path
    async fn get_engine(&self, path: &str) -> Result<Arc<QueryEngine>, String> {
        // Check cache first
        {
            let engines = self.engines.read().await;
            if let Some(engine) = engines.get(path) {
                return Ok(Arc::clone(engine));
            }
        }

        // Create new engine in a blocking task (since QueryEngine::open_path does sync I/O)
        let path_owned = path.to_string();
        let engine = tokio::task::spawn_blocking(move || QueryEngine::open_path(&path_owned))
            .await
            .map_err(|e| format!("Task join error: {}", e))?
            .map_err(|e| e.to_string())?;

        let engine = Arc::new(engine);

        // Cache it
        {
            let mut engines = self.engines.write().await;
            engines.insert(path.to_string(), Arc::clone(&engine));
        }

        Ok(engine)
    }
}

/// Query parameters for the query endpoint
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// Table path
    table: String,
    /// WHERE clauses (can be repeated)
    #[serde(rename = "where", default)]
    where_clauses: Vec<String>,
    /// Maximum number of results
    limit: Option<usize>,
    /// Response format: "json" or "ndjson" (default: SSE stream)
    format: Option<String>,
}

/// Path parameters for table endpoints
#[derive(Debug, Deserialize)]
struct TableParams {
    /// Table path
    table: String,
}

/// Table info response
#[derive(Serialize)]
struct TableInfo {
    path: String,
    key_fields: Vec<String>,
    num_partitions: usize,
    has_index: bool,
}

/// Error response
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// Convert EncodedValue to JSON
fn encoded_to_json(value: &EncodedValue) -> serde_json::Value {
    match value {
        EncodedValue::Null => serde_json::Value::Null,
        EncodedValue::Binary(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        EncodedValue::Int32(i) => serde_json::json!(*i),
        EncodedValue::Int64(i) => serde_json::json!(*i),
        EncodedValue::Float32(f) => serde_json::json!(*f),
        EncodedValue::Float64(f) => serde_json::json!(*f),
        EncodedValue::Boolean(b) => serde_json::json!(*b),
        EncodedValue::Struct(fields) => {
            let obj: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(name, val)| (name.clone(), encoded_to_json(val)))
                .collect();
            serde_json::Value::Object(obj)
        }
        EncodedValue::Array(elements) => {
            serde_json::Value::Array(elements.iter().map(encoded_to_json).collect())
        }
    }
}

/// Parse a WHERE clause into a KeyRange
fn parse_where_clause(clause: &str) -> Option<KeyRange> {
    // Try different operators
    if let Some(pos) = clause.find(">=") {
        let field_path = parse_field_path(&clause[..pos]);
        let value = parse_key_value(&clause[pos + 2..]);
        return Some(KeyRange {
            field_path,
            start: QueryBound::Included(value),
            end: QueryBound::Unbounded,
        });
    }
    if let Some(pos) = clause.find("<=") {
        let field_path = parse_field_path(&clause[..pos]);
        let value = parse_key_value(&clause[pos + 2..]);
        return Some(KeyRange {
            field_path,
            start: QueryBound::Unbounded,
            end: QueryBound::Included(value),
        });
    }
    if let Some(pos) = clause.find('>') {
        let field_path = parse_field_path(&clause[..pos]);
        let value = parse_key_value(&clause[pos + 1..]);
        return Some(KeyRange {
            field_path,
            start: QueryBound::Excluded(value),
            end: QueryBound::Unbounded,
        });
    }
    if let Some(pos) = clause.find('<') {
        let field_path = parse_field_path(&clause[..pos]);
        let value = parse_key_value(&clause[pos + 1..]);
        return Some(KeyRange {
            field_path,
            start: QueryBound::Unbounded,
            end: QueryBound::Excluded(value),
        });
    }
    if let Some(pos) = clause.find('=') {
        let field_path = parse_field_path(&clause[..pos]);
        let value = parse_key_value(&clause[pos + 1..]);
        return Some(KeyRange {
            field_path,
            start: QueryBound::Included(value.clone()),
            end: QueryBound::Included(value),
        });
    }
    None
}

/// Parse a field path (supports dot notation)
fn parse_field_path(field: &str) -> Vec<String> {
    field.split('.').map(|s| s.to_string()).collect()
}

/// Parse a key value from a string
fn parse_key_value(s: &str) -> KeyValue {
    if let Ok(i) = s.parse::<i32>() {
        return KeyValue::Int32(i);
    }
    if let Ok(i) = s.parse::<i64>() {
        return KeyValue::Int64(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return KeyValue::Float64(f);
    }
    if s == "true" {
        return KeyValue::Boolean(true);
    }
    if s == "false" {
        return KeyValue::Boolean(false);
    }
    KeyValue::String(s.to_string())
}

/// Health check endpoint
async fn health() -> &'static str {
    "ok"
}

/// Get table info
async fn table_info(
    Query(params): Query<TableParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<TableInfo>, (StatusCode, Json<ErrorResponse>)> {
    let engine = state
        .get_engine(&params.table)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })))?;

    Ok(Json(TableInfo {
        path: params.table,
        key_fields: engine.key_fields().to_vec(),
        num_partitions: engine.num_partitions(),
        has_index: engine.has_index(),
    }))
}

/// Get table schema
async fn table_schema(
    Query(params): Query<TableParams>,
    State(state): State<Arc<AppState>>,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let engine = state
        .get_engine(&params.table)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })))?;

    let schema = if let Some(rvd_spec) = engine.rvd_spec() {
        format_schema_clean(&rvd_spec.codec_spec.v_type)
    } else {
        // VCF or other source - show EncodedType debug representation
        format!("{:?}", engine.row_type())
    };
    Ok(schema)
}

/// Query table with streaming response
async fn query_table(
    ExtraQuery(params): ExtraQuery<QueryParams>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let table_path = params.table.clone();

    // Parse WHERE clauses
    let ranges: Vec<KeyRange> = params
        .where_clauses
        .iter()
        .filter_map(|c| parse_where_clause(c))
        .collect();

    let limit = params.limit.unwrap_or(usize::MAX);

    // Get or create engine
    let engine = match state.get_engine(&table_path).await {
        Ok(e) => e,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Check format
    match params.format.as_deref() {
        Some("json") => {
            // Collect all results and return as JSON array (in blocking task)
            let engine_clone = Arc::clone(&engine);
            let ranges_clone = ranges.clone();

            let result = tokio::task::spawn_blocking(move || {
                let iterator = engine_clone.query_iter(&ranges_clone)?;
                let rows: Vec<serde_json::Value> = iterator
                    .take(limit)
                    .filter_map(|r| r.ok())
                    .map(|r| encoded_to_json(&r))
                    .collect();
                Ok::<_, genohype_core::HailError>(rows)
            })
            .await;

            match result {
                Ok(Ok(rows)) => Json(rows).into_response(),
                Ok(Err(e)) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Task error: {}", e),
                    }),
                )
                    .into_response(),
            }
        }
        Some("ndjson") => {
            // Return newline-delimited JSON (in blocking task)
            let engine_clone = Arc::clone(&engine);
            let ranges_clone = ranges.clone();

            let result = tokio::task::spawn_blocking(move || {
                let iterator = engine_clone.query_iter(&ranges_clone)?;
                let body: String = iterator
                    .take(limit)
                    .filter_map(|r| r.ok())
                    .map(|r| {
                        let json = encoded_to_json(&r);
                        format!("{}\n", serde_json::to_string(&json).unwrap_or_default())
                    })
                    .collect();
                Ok::<_, genohype_core::HailError>(body)
            })
            .await;

            match result {
                Ok(Ok(body)) => (
                    [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
                    body,
                )
                    .into_response(),
                Ok(Err(e)) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Task error: {}", e),
                    }),
                )
                    .into_response(),
            }
        }
        _ => {
            // Default: SSE stream
            info!("Starting SSE query stream for table: {}", table_path);

            // Use a tokio channel to bridge sync iterator to async stream
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<serde_json::Value, String>>(100);

            let engine_clone = Arc::clone(&engine);
            let ranges_clone = ranges.clone();

            // Spawn blocking task to iterate and send results
            tokio::task::spawn_blocking(move || {
                info!("Blocking task started");

                let iterator = match engine_clone.query_iter(&ranges_clone) {
                    Ok(iter) => {
                        info!("Got iterator from query_iter");
                        iter
                    }
                    Err(e) => {
                        error!("Failed to get iterator: {}", e);
                        let _ = tx.blocking_send(Err(e.to_string()));
                        return;
                    }
                };

                let mut count = 0;
                let mut sent_count = 0;

                info!("Starting iteration loop");
                for row_result in iterator {
                    if count >= limit {
                        info!("Limit {} reached", limit);
                        break;
                    }
                    match row_result {
                        Ok(row) => {
                            let json = encoded_to_json(&row);
                            // Log every 100 rows to avoid spam
                            if sent_count % 100 == 0 {
                                debug!("Sending row {}", sent_count);
                            }

                            if let Err(e) = tx.blocking_send(Ok(json)) {
                                info!("Receiver dropped, stopping iteration: {}", e);
                                break;
                            }
                            count += 1;
                            sent_count += 1;
                        }
                        Err(e) => {
                            error!("Row error: {}", e);
                            let _ = tx.blocking_send(Err(e.to_string()));
                            break;
                        }
                    }
                }
                info!("Blocking task finished. Sent {} rows.", sent_count);
            });

            // Create async stream from receiver
            let stream = async_stream::stream! {
                debug!("Async stream started polling");
                while let Some(result) = rx.recv().await {
                    match result {
                        Ok(json) => {
                            yield Ok::<_, Infallible>(Event::default().json_data(json).unwrap());
                        }
                        Err(e) => {
                            yield Ok(Event::default().data(format!("error: {}", e)));
                            break;
                        }
                    }
                }
                debug!("Async stream completed");
                // Send done event
                yield Ok(Event::default().event("done").data("complete"));
            };

            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
    }
}

/// Serve the frontend HTML (reads from disk for hot reload)
async fn serve_frontend() -> Response {
    match tokio::fs::read_to_string("static/index.html").await {
        Ok(content) => Html(content).into_response(),
        Err(_) => {
            // Fallback to embedded version if file not found
            Html(include_str!("../../static/index.html")).into_response()
        }
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let state = Arc::new(AppState::new());

    // Build the router
    let app = Router::new()
        .route("/", get(serve_frontend))
        .route("/health", get(health))
        .route("/api/info", get(table_info))
        .route("/api/schema", get(table_schema))
        .route("/api/query", get(query_table))
        .nest_service("/static", ServeDir::new("static"))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    let addr = "0.0.0.0:3000";
    println!("Hail Decoder HTTP Server starting on http://{}", addr);
    println!();
    println!("Endpoints:");
    println!("  GET /                        - Frontend UI");
    println!("  GET /health                  - Health check");
    println!("  GET /api/info?table=<path>   - Table metadata");
    println!("  GET /api/schema?table=<path> - Table schema");
    println!("  GET /api/query?table=<path>  - Query (SSE stream)");
    println!();
    println!("Query parameters:");
    println!("  table=gs://bucket/table.ht   - Table path (required)");
    println!("  where=field=value            - Filter condition");
    println!("  limit=N                      - Limit results");
    println!("  format=json|ndjson           - Response format (default: SSE)");
    println!();

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
