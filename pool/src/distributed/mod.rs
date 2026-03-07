//! Distributed processing infrastructure.
//!
//! This module provides generic types and functionality for distributed
//! task execution across worker pools.

pub mod coordinator;
pub mod message;
pub mod worker;

// Re-export message types
pub use message::{
    CompleteRequest, CompleteResponse, CoreTaskInfo, DashboardBottleneck, DashboardMetrics,
    DashboardSummary, DashboardWorker, HardwareSpec, HeartbeatRequest, HeartbeatResponse,
    StatusResponse, TaskDescriptor, TelemetrySnapshot, UpdateFleetRequest, WorkRequest, WorkResponse,
};

// Re-export coordinator and worker
pub use coordinator::{start_coordinator, CoordinatorConfig};
pub use worker::{run_worker, WorkerConfig};
