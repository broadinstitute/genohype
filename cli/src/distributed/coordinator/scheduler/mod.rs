//! Scheduler module for work distribution and completion handling.
//!
//! This module contains the core work allocation and completion logic for
//! different job types: standard partition jobs, Manhattan pipelines,
//! Manhattan batch jobs, and ingestion jobs.

pub mod assignment;
pub mod capacity;
pub mod completion;

pub(crate) use assignment::{get_batch_work, get_ingestion_work, get_manhattan_work};
pub(crate) use capacity::{determine_batch_size, extract_capacity_from_error};
pub(crate) use completion::{
    complete_batch_work, complete_ingestion_work, complete_manhattan_work,
};
