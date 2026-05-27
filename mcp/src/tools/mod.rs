//! MCP tool parameter types organized by domain.
//!
//! The actual tool implementations are `#[tool]` methods on
//! [`GenomicToolServer`](crate::server::GenomicToolServer),
//! with parameter types defined here for organization.

pub mod gene;
pub mod region;
pub mod variant;
