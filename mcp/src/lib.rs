//! # genohype-mcp
//!
//! MCP (Model Context Protocol) server primitives for genomic AI tools.
//!
//! This crate provides:
//! - [`GenomicDataProvider`] trait for pluggable data backends
//! - Shared domain types ([`VariantDetails`], [`GeneSummary`], etc.)
//! - [`GenomicToolServer`] with generic genomic tools (variant, gene, region)
//! - Stdio transport via rmcp for CLI-launched servers
//!
//! Downstream applications implement [`GenomicDataProvider`] to bridge their
//! specific data backends, then launch a [`GenomicToolServer`] to expose
//! standardized genomic MCP tools.
//!
//! # Architecture
//!
//! ```text
//! Consumer binary (gnomad-browser-lite, axaou-rust)
//! │
//! ├── Implements GenomicDataProvider (wraps VariantBackend / ClickHouse / etc.)
//! ├── Creates GenomicToolServer with the provider
//! ├── Optionally adds custom domain-specific tools
//! └── Runs via stdio or mounts on existing Axum router
//! ```

pub mod server;
pub mod tools;
pub mod traits;
pub mod types;

pub use server::GenomicToolServer;
pub use traits::GenomicDataProvider;
pub use types::*;
