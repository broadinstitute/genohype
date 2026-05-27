//! MCP (Model Context Protocol) server primitives for genomic AI tools.
//!
//! This module provides the building blocks for exposing genomic data
//! through MCP-compatible tool interfaces. Downstream applications implement
//! [`GenomicDataProvider`] to bridge their data backends, then register
//! generic or custom [`McpTool`] implementations with an [`McpServer`].
//!
//! # Architecture
//!
//! ```text
//! McpServer
//! ├── Tool registry (HashMap<String, Box<dyn McpTool>>)
//! ├── GenomicDataProvider (Arc<dyn GenomicDataProvider>)
//! └── into_router() → Axum Router with /tools/list, /tools/call, /sse
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use genohype_core::mcp::server::McpServer;
//! use genohype_core::mcp::tools::variant::GetVariantDetails;
//!
//! let provider = Arc::new(my_provider);
//! let server = McpServer::new(provider)
//!     .with_tool(GetVariantDetails);
//! let router = server.into_router();
//! ```

pub mod server;
pub mod tools;
pub mod traits;
pub mod types;

pub use server::McpServer;
pub use traits::{GenomicDataProvider, McpTool};
