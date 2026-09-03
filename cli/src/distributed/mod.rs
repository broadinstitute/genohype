//! Distributed processing coordination.
//!
//! This module implements a Coordinator/Worker pattern for distributed
//! Hail table processing across multiple VMs.
//!
//! ## Architecture
//!
//! - **Coordinator**: HTTP server that maintains a queue of partitions to process.
//!   Workers request work, process it, and report completion. Handles retries for
//!   failed/timed-out workers (e.g., Spot instance preemption).
//!
//! - **Worker**: HTTP client that polls the coordinator for work, processes
//!   assigned partitions, and reports completion. Runs on each GCP VM.
//!
//! ## Benefits
//!
//! 1. **Spot Instance Resilience**: If a worker dies, its assigned work is
//!    automatically reassigned after a timeout.
//!
//! 2. **Scalability**: No SSH connection limits - workers pull work over HTTP.
//!
//! 3. **Dynamic Load Balancing**: Fast workers naturally get more work.
//!
//! ## Usage
//!
//! Start a coordinator:
//! ```bash
//! genohype service start-coordinator \
//!     --port 3000 \
//!     --input gs://bucket/table.ht \
//!     --output gs://bucket/output/ \
//!     --total-partitions 1000
//! ```
//!
//! Start workers (on each VM):
//! ```bash
//! genohype service start-worker \
//!     --url http://10.0.0.5:3000 \
//!     --worker-id worker-0
//! ```

pub mod coordinator;
pub mod manhattan;
pub mod message;
pub mod metrics_db;
pub mod pool_wrappers;
pub mod worker;
