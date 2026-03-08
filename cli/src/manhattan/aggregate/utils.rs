//! General string and date utilities for Manhattan aggregation.

use std::time::{SystemTime, UNIX_EPOCH};

/// Extract phenotype name from output path.
pub(crate) fn extract_phenotype_name(output_path: &str) -> String {
    output_path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("unknown")
        .to_string()
}

/// Get current timestamp in ISO format.
pub(crate) fn chrono_now_iso() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Simple ISO format without chrono dependency
    format!("{}", secs) // TODO: Proper ISO format
}
