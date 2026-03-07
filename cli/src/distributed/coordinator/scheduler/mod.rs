//! Scheduler module for work distribution and completion handling.
//!
//! This module contains the core work allocation and completion logic for
//! different job types: standard partition jobs, Manhattan pipelines,
//! Manhattan batch jobs, and ingestion jobs.
//!
//! The scheduler functions are kept together due to their tight coupling
//! with CoordinatorData state management.

// Re-export scheduler functions from the main module
// These are implemented in coordinator/mod.rs due to tight coupling
// with state management, but logically belong to the scheduler.
