-- Pipeline status tracking table for Manhattan phenotype pipeline.
-- Tracks which phenotypes have been processed, their status, and resource usage.
-- Uses ReplacingMergeTree to automatically deduplicate and keep latest status.

CREATE TABLE IF NOT EXISTS pipeline_status (
    -- Unique identifiers
    phenotype String,
    ancestry String,

    -- Pipeline status: MANHATTAN_FAILED, INGESTING, INGEST_FAILED, INGESTED
    status String,

    -- Result counts
    loci_count UInt32,
    significant_variants UInt32,

    -- Storage metrics (in bytes)
    original_gcs_bytes UInt64,
    derived_gcs_bytes UInt64,

    -- Error tracking (NULL if no error)
    error_message Nullable(String),

    -- Timestamp for deduplication
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (phenotype, ancestry);
