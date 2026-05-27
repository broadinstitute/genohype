use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::traits::{GenomicDataProvider, McpTool};

/// MCP server that hosts a registry of tools backed by a genomic data provider.
///
/// # Usage
///
/// ```rust,no_run
/// let server = McpServer::new(provider)
///     .with_tool(GetVariantDetails)
///     .with_tool(GetGeneSummary);
///
/// // Mount as Axum routes
/// let router = server.into_router();
///
/// // Or run as stdio transport
/// server.run_stdio().await;
/// ```
pub struct McpServer {
    tools: HashMap<String, Box<dyn McpTool>>,
    provider: Arc<dyn GenomicDataProvider>,
    server_name: String,
    server_version: String,
}

impl McpServer {
    /// Create a new MCP server with the given data provider.
    pub fn new(provider: Arc<dyn GenomicDataProvider>) -> Self {
        Self {
            tools: HashMap::new(),
            provider,
            server_name: "genohype-mcp".to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Set the server name reported in MCP initialize response.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = name.into();
        self
    }

    /// Register a tool with the server.
    pub fn with_tool(mut self, tool: impl McpTool + 'static) -> Self {
        self.tools.insert(tool.name().to_string(), Box::new(tool));
        self
    }

    /// List all registered tool definitions (for MCP tools/list).
    pub fn list_tools(&self) -> Vec<ToolDefinition> {
        self.tools
            .values()
            .map(|t| ToolDefinition {
                name: t.name().to_string(),
                description: t.description().to_string(),
                input_schema: t.input_schema(),
            })
            .collect()
    }

    /// Execute a tool by name with the given arguments.
    pub async fn call_tool(&self, name: &str, args: Value) -> anyhow::Result<Value> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("unknown tool: {name}"))?;
        tool.execute(args, self.provider.clone()).await
    }

    /// Convert this server into an Axum router.
    ///
    /// Exposes:
    /// - `POST /tools/list` — list available tools
    /// - `POST /tools/call` — invoke a tool
    /// - `GET /sse` — SSE transport (stub)
    #[cfg(feature = "server")]
    pub fn into_router(self) -> axum::Router {
        use axum::{extract::State, routing::{get, post}, Json, Router};

        let state = Arc::new(McpServerState {
            tools: self.tools,
            provider: self.provider,
            server_name: self.server_name,
            server_version: self.server_version,
        });

        Router::new()
            .route("/tools/list", post(handle_tools_list))
            .route("/tools/call", post(handle_tools_call))
            .route("/sse", get(handle_sse))
            .with_state(state)
    }

    /// Run the server using stdio JSON-RPC transport.
    ///
    /// Reads JSON-RPC requests from stdin, writes responses to stdout.
    /// This is the standard MCP transport for CLI-launched servers.
    pub async fn run_stdio(self) -> anyhow::Result<()> {
        // TODO: Implement stdio JSON-RPC transport
        // Read line-delimited JSON from stdin, dispatch to call_tool/list_tools,
        // write JSON responses to stdout.
        tracing::info!(
            server = %self.server_name,
            tools = self.tools.len(),
            "MCP stdio transport not yet implemented"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Axum handler types (behind "server" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "server")]
struct McpServerState {
    tools: HashMap<String, Box<dyn McpTool>>,
    provider: Arc<dyn GenomicDataProvider>,
    server_name: String,
    server_version: String,
}

#[cfg(feature = "server")]
async fn handle_tools_list(
    State(state): State<Arc<McpServerState>>,
) -> axum::Json<Vec<ToolDefinition>> {
    let tools: Vec<ToolDefinition> = state
        .tools
        .values()
        .map(|t| ToolDefinition {
            name: t.name().to_string(),
            description: t.description().to_string(),
            input_schema: t.input_schema(),
        })
        .collect();
    axum::Json(tools)
}

#[derive(Deserialize)]
#[cfg(feature = "server")]
struct ToolCallRequest {
    name: String,
    arguments: Value,
}

#[derive(Serialize)]
#[cfg(feature = "server")]
struct ToolCallResponse {
    content: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[cfg(feature = "server")]
async fn handle_tools_call(
    State(state): State<Arc<McpServerState>>,
    axum::Json(req): axum::Json<ToolCallRequest>,
) -> axum::Json<ToolCallResponse> {
    let tool = match state.tools.get(&req.name) {
        Some(t) => t,
        None => {
            return axum::Json(ToolCallResponse {
                content: Value::Null,
                error: Some(format!("unknown tool: {}", req.name)),
            });
        }
    };

    match tool.execute(req.arguments, state.provider.clone()).await {
        Ok(result) => axum::Json(ToolCallResponse {
            content: result,
            error: None,
        }),
        Err(e) => axum::Json(ToolCallResponse {
            content: Value::Null,
            error: Some(e.to_string()),
        }),
    }
}

#[cfg(feature = "server")]
async fn handle_sse() -> &'static str {
    // TODO: Implement SSE transport for streaming MCP responses
    "SSE transport not yet implemented"
}

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// MCP tool definition returned by tools/list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}
