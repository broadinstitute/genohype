//! Integrated pipeline for multi-table Manhattan plot and locus zoom generation.
//!
//! This module orchestrates the full workflow for both local and distributed execution:
//! - `integrated`: Local pipeline for multi-table Manhattan plots
//! - `shards`: Distributed shard aggregation from worker results
//! - `composite`: PNG compositing for distributed rendering

pub mod composite;
pub mod integrated;
pub mod shards;

// Re-export the main public interfaces
pub use composite::{
    composite_partial_pngs, composite_partial_pngs_with_style, draw_threshold_line_on_pixmap,
};
pub use integrated::{run_integrated_pipeline, PipelineConfig};
pub use shards::{
    aggregate_shards_and_render, build_layout_from_points, read_shard_file, DistributedScanResult,
};
