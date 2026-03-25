//! BED Data Source
//!
//! Implements the `DataSource` trait for Tabix-indexed BED files (.bed.gz).
//! Designed for reading methylation BED files from the HPRC dataset.
//!
//! Supports:
//! - BGZF-compressed BED files (.bed.gz)
//! - Tabix-indexed BED files (.bed.gz.tbi) for efficient region queries
//! - Local and cloud (GCS, S3) paths

mod reader;

pub use reader::BedDataSource;
