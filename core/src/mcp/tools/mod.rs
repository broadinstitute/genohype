//! Generic MCP tool implementations backed by [`GenomicDataProvider`].
//!
//! These tools work with any data backend that implements the provider trait.
//! Domain-specific tools (clinical interpretation, phenotype analysis, etc.)
//! should be implemented in the downstream consumer crates.

pub mod gene;
pub mod region;
pub mod variant;
