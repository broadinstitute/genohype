//! Manhattan pipeline output ingestion into ClickHouse.
//!
//! This module handles ingesting the output of the Manhattan pipeline into ClickHouse:
//! - Significant variants parquet files
//! - Loci definitions from manifest.json
//! - Plot metadata (GCS URIs)
//!
//! The ingestion transforms the data as follows:
//! - Adds `phenotype`, `ancestry`, and `sequencing_type` columns
//! - Computes `xpos` from contig and position
//! - Adds "chr" prefix to contig for annotation joining

use crate::Result;
use arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use genohype_core::export::ClickHouseClient;
use genohype_core::io::{get_file_size, is_cloud_path, range_read};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use std::sync::Arc;

/// Manifest JSON structure from Manhattan pipeline output.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    /// Timestamp when the manifest was generated
    #[serde(default)]
    pub generated_at: Option<String>,
    /// Thresholds used
    #[serde(default)]
    pub thresholds: Option<ManifestThresholds>,
    /// Loci definitions
    #[serde(default)]
    pub loci: Vec<LocusDefinition>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestThresholds {
    pub significance: Option<f64>,
    pub gene_significance: Option<f64>,
}

/// Locus definition from manifest.json.
#[derive(Debug, Clone, Deserialize)]
pub struct LocusDefinition {
    /// Unique locus ID
    pub id: String,
    /// Region definition
    pub region: LocusRegion,
    /// Source (exome, genome, or both)
    #[serde(default)]
    pub source: Option<String>,
    /// Lead variant as string (e.g., "1:8147519")
    #[serde(default)]
    pub lead_variant: Option<String>,
    /// Lead variant p-value (separate field)
    #[serde(default)]
    pub lead_pvalue: Option<f64>,
    /// Exome variants info
    #[serde(default)]
    pub exome_variants: Option<VariantCounts>,
    /// Genome variants info
    #[serde(default)]
    pub genome_variants: Option<VariantCounts>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LocusRegion {
    pub contig: String,
    pub start: i32,
    #[serde(alias = "stop", alias = "end")]
    pub end: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct VariantCounts {
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub path: Option<String>,
}

/// Pipeline status values for tracking in ClickHouse.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    ManhattanFailed,
    Ingesting,
    IngestFailed,
    Ingested,
}

impl PipelineStatus {
    fn as_str(&self) -> &'static str {
        match self {
            PipelineStatus::ManhattanFailed => "MANHATTAN_FAILED",
            PipelineStatus::Ingesting => "INGESTING",
            PipelineStatus::IngestFailed => "INGEST_FAILED",
            PipelineStatus::Ingested => "INGESTED",
        }
    }
}

/// Error manifest JSON structure for failed pipeline runs.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ErrorManifest {
    pub phenotype: String,
    pub status: String,
    pub error: String,
    #[serde(default)]
    pub timestamp_ms: Option<u64>,
}

/// Extended manifest JSON structure with stats.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestWithStats {
    /// Basic manifest fields
    #[serde(flatten)]
    pub manifest: Manifest,
    /// Pipeline stats including storage sizes
    #[serde(default)]
    pub stats: Option<ManifestStatsInfo>,
}

/// Stats information from the manifest.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManifestStatsInfo {
    #[serde(default)]
    pub total_loci: Option<usize>,
    #[serde(default)]
    pub input_ht_size_bytes: Option<u64>,
    #[serde(default)]
    pub output_dir_size_bytes: Option<u64>,
}

/// Run the ingestion task for a single phenotype.
///
/// # Arguments
/// * `phenotype_id` - The phenotype identifier
/// * `ancestry` - The ancestry group (e.g., "meta", "eur")
/// * `base_path` - GCS path to the phenotype directory
/// * `clickhouse_url` - ClickHouse HTTP endpoint
/// * `database` - Target ClickHouse database
///
/// # Returns
/// The total number of rows inserted.
pub fn run_ingest_task(
    phenotype_id: &str,
    ancestry: &str,
    base_path: &str,
    clickhouse_url: &str,
    _database: &str, // Currently unused - tables use default database
) -> Result<usize> {
    println!(
        "Ingesting phenotype {} ({}) from {}",
        phenotype_id, ancestry, base_path
    );

    let client = ClickHouseClient::new(clickhouse_url);
    let base = base_path.trim_end_matches('/');
    let mut total_rows = 0;

    // 0. Check for error.json first (indicates Manhattan pipeline failure)
    let error_path = format!("{}/error.json", base);
    if file_exists(&error_path)? {
        let error_manifest = read_error_manifest(&error_path)?;
        println!(
            "  Found error.json: {} - {}",
            error_manifest.status, error_manifest.error
        );

        // Log the failure status to ClickHouse
        let _ = log_pipeline_status(
            &client,
            phenotype_id,
            ancestry,
            PipelineStatus::ManhattanFailed,
            0,
            0,
            0,
            0,
            Some(&error_manifest.error),
        );

        // Return early - nothing to ingest
        return Ok(0);
    }

    // 1. Read manifest.json with stats
    let manifest_path = format!("{}/manifest.json", base);
    let manifest_with_stats = read_manifest_with_stats(&manifest_path)?;
    let manifest = manifest_with_stats.manifest;
    let stats = manifest_with_stats.stats.unwrap_or_default();

    // Extract storage metrics
    let original_gcs_bytes = stats.input_ht_size_bytes.unwrap_or(0);
    let derived_gcs_bytes = stats.output_dir_size_bytes.unwrap_or(0);
    let loci_count = stats.total_loci.unwrap_or(manifest.loci.len()) as u32;

    // Log INGESTING status
    let _ = log_pipeline_status(
        &client,
        phenotype_id,
        ancestry,
        PipelineStatus::Ingesting,
        loci_count,
        0,
        original_gcs_bytes,
        derived_gcs_bytes,
        None,
    );

    // 2. Ingest loci
    let loci_rows = ingest_loci(phenotype_id, ancestry, base, &manifest.loci, &client)?;
    total_rows += loci_rows;

    // 3. Ingest plot metadata
    let plot_rows = ingest_plots(phenotype_id, ancestry, base, &client)?;
    total_rows += plot_rows;

    // 4. Ingest significant variants (exome and genome)
    let mut significant_variant_count: u32 = 0;
    for seq_type in &["exome", "genome"] {
        let parquet_path = format!("{}/{}_significant.parquet", base, seq_type);
        if file_exists(&parquet_path)? {
            let rows = ingest_variants(phenotype_id, ancestry, seq_type, &parquet_path, &client)?;
            total_rows += rows;
            significant_variant_count += rows as u32;
            println!("  Ingested {} {} significant variants", rows, seq_type);
        }
    }

    // 5. Ingest loci variants (all variants in locus regions with ref/alt)
    let loci_variants_path = format!("{}/loci_variants.parquet", base);
    if file_exists(&loci_variants_path)? {
        match ingest_loci_variants(&loci_variants_path, &client) {
            Ok(rows) => {
                total_rows += rows;
                println!("  Ingested {} loci variants", rows);
            }
            Err(e) => {
                eprintln!("  Warning: Failed to ingest loci variants: {}", e);
            }
        }
    }

    // 6. Ingest gene associations (gene burden results)
    let gene_assoc_path = format!("{}/gene_associations.parquet", base);
    if file_exists(&gene_assoc_path)? {
        match ingest_gene_associations(&gene_assoc_path, &client) {
            Ok(rows) => {
                total_rows += rows;
                println!("  Ingested {} gene associations", rows);
            }
            Err(e) => {
                eprintln!("  Warning: Failed to ingest gene associations: {}", e);
            }
        }
    }

    // 7. Ingest QQ plot points (exome and genome)
    for seq_type in &["exome", "genome"] {
        let qq_path = format!("{}/qq_{}.parquet", base, seq_type);
        if file_exists(&qq_path)? {
            match ingest_qq_points(&qq_path, &client) {
                Ok(rows) => {
                    total_rows += rows;
                    println!("  Ingested {} {} QQ points", rows, seq_type);
                }
                Err(e) => {
                    eprintln!("  Warning: Failed to ingest {} QQ points: {}", seq_type, e);
                }
            }
        }
    }

    // 8. Log INGESTED status to pipeline_status table
    let _ = log_pipeline_status(
        &client,
        phenotype_id,
        ancestry,
        PipelineStatus::Ingested,
        loci_count,
        significant_variant_count,
        original_gcs_bytes,
        derived_gcs_bytes,
        None,
    );

    println!(
        "Completed ingestion for {} ({}): {} total rows",
        phenotype_id, ancestry, total_rows
    );

    Ok(total_rows)
}

/// Read entire file contents as bytes.
fn read_file_bytes(path: &str) -> Result<Vec<u8>> {
    if is_cloud_path(path) {
        let size = get_file_size(path)?;
        range_read(path, 0, size as usize)
    } else {
        std::fs::read(path).map_err(|e| crate::HailError::Io(e))
    }
}

/// Check if a file exists.
fn file_exists(path: &str) -> Result<bool> {
    if is_cloud_path(path) {
        match get_file_size(path) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    } else {
        Ok(std::path::Path::new(path).exists())
    }
}

/// Log pipeline status to ClickHouse.
///
/// Uses ReplacingMergeTree so the latest status will be kept.
fn log_pipeline_status(
    client: &ClickHouseClient,
    phenotype: &str,
    ancestry: &str,
    status: PipelineStatus,
    loci_count: u32,
    significant_variants: u32,
    original_gcs_bytes: u64,
    derived_gcs_bytes: u64,
    error_message: Option<&str>,
) -> Result<()> {
    // Build the INSERT query
    // For error_message, we need to handle NULL properly
    let err_val = match error_message {
        Some(e) => format!("'{}'", e.replace('\'', "''")),
        None => "NULL".to_string(),
    };

    let query = format!(
        "INSERT INTO pipeline_status (phenotype, ancestry, status, loci_count, significant_variants, original_gcs_bytes, derived_gcs_bytes, error_message, updated_at) VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {}, now())",
        phenotype.replace('\'', "''"),
        ancestry.replace('\'', "''"),
        status.as_str(),
        loci_count,
        significant_variants,
        original_gcs_bytes,
        derived_gcs_bytes,
        err_val
    );

    client.execute(&query).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to log pipeline status: {}", e),
        ))
    })?;
    Ok(())
}

/// Read and parse error.json from a path (local or GCS).
fn read_error_manifest(path: &str) -> Result<ErrorManifest> {
    let data = read_file_bytes(path)?;
    let manifest: ErrorManifest = serde_json::from_slice(&data).map_err(|e| {
        crate::HailError::InvalidFormat(format!("Failed to parse error.json: {}", e))
    })?;
    Ok(manifest)
}

/// Read manifest with stats.
fn read_manifest_with_stats(path: &str) -> Result<ManifestWithStats> {
    let data = read_file_bytes(path)?;
    let manifest: ManifestWithStats = serde_json::from_slice(&data).map_err(|e| {
        crate::HailError::InvalidFormat(format!("Failed to parse manifest.json: {}", e))
    })?;
    Ok(manifest)
}

/// List PNG files in a directory (local or cloud).
fn list_png_files(dir_path: &str) -> Result<Vec<String>> {
    if is_cloud_path(dir_path) {
        let dir = dir_path.trim_end_matches('/');
        let output = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/*.png", dir)])
            .output()
            .map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to run gsutil: {}", e),
                ))
            })?;

        if !output.status.success() {
            // No PNG files found
            return Ok(vec![]);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let files: Vec<String> = stdout
            .lines()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Ok(files)
    } else {
        if !std::path::Path::new(dir_path).exists() {
            return Ok(vec![]);
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".png") {
                    files.push(entry.path().to_string_lossy().to_string());
                }
            }
        }
        Ok(files)
    }
}

/// List subdirectories in a directory (local or cloud).
fn list_subdirectories(dir_path: &str) -> Result<Vec<String>> {
    if is_cloud_path(dir_path) {
        let dir = dir_path.trim_end_matches('/');
        // gsutil ls returns directories with trailing /
        let output = std::process::Command::new("gsutil")
            .args(["ls", dir])
            .output()
            .map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to run gsutil: {}", e),
                ))
            })?;

        if !output.status.success() {
            // Directory doesn't exist
            return Ok(vec![]);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let subdirs: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                // Lines are like gs://bucket/path/chroms/chr1/
                let trimmed = line.trim().trim_end_matches('/');
                trimmed.rsplit('/').next().map(|s| s.to_string())
            })
            .filter(|s| !s.is_empty())
            .collect();
        Ok(subdirs)
    } else {
        if !std::path::Path::new(dir_path).exists() {
            return Ok(vec![]);
        }

        let mut subdirs = Vec::new();
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    subdirs.push(name.to_string());
                }
            }
        }
        Ok(subdirs)
    }
}

/// Compute xpos from contig and position.
///
/// xpos = contig_index * 1e9 + position
/// where contig_index is 1-22 for autosomes, 23 for X, 24 for Y
fn compute_xpos(contig: &str, position: i32) -> i64 {
    let contig_idx = match contig.trim_start_matches("chr") {
        "1" => 1,
        "2" => 2,
        "3" => 3,
        "4" => 4,
        "5" => 5,
        "6" => 6,
        "7" => 7,
        "8" => 8,
        "9" => 9,
        "10" => 10,
        "11" => 11,
        "12" => 12,
        "13" => 13,
        "14" => 14,
        "15" => 15,
        "16" => 16,
        "17" => 17,
        "18" => 18,
        "19" => 19,
        "20" => 20,
        "21" => 21,
        "22" => 22,
        "X" => 23,
        "Y" => 24,
        _ => 0,
    };
    (contig_idx as i64) * 1_000_000_000 + (position as i64)
}

/// Ensure chr prefix on contig.
fn ensure_chr_prefix(contig: &str) -> String {
    if contig.starts_with("chr") {
        contig.to_string()
    } else {
        format!("chr{}", contig)
    }
}

/// Ingest loci definitions into ClickHouse.
fn ingest_loci(
    phenotype_id: &str,
    ancestry: &str,
    base_path: &str,
    loci: &[LocusDefinition],
    client: &ClickHouseClient,
) -> Result<usize> {
    if loci.is_empty() {
        return Ok(0);
    }

    // Build Arrow arrays for loci table
    let num_rows = loci.len();

    let mut phenotypes = StringBuilder::new();
    let mut ancestries = StringBuilder::new();
    let mut locus_ids = StringBuilder::new();
    let mut contigs = StringBuilder::new();
    let mut starts: Vec<i32> = Vec::with_capacity(num_rows);
    let mut stops: Vec<i32> = Vec::with_capacity(num_rows);
    let mut xstarts: Vec<i64> = Vec::with_capacity(num_rows);
    let mut xstops: Vec<i64> = Vec::with_capacity(num_rows);
    let mut sources = StringBuilder::new();
    let mut lead_variants = StringBuilder::new();
    let mut lead_pvalues: Vec<f64> = Vec::with_capacity(num_rows);
    let mut exome_counts: Vec<u32> = Vec::with_capacity(num_rows);
    let mut genome_counts: Vec<u32> = Vec::with_capacity(num_rows);
    let mut plot_uris = StringBuilder::new();

    for locus in loci {
        phenotypes.append_value(phenotype_id);
        ancestries.append_value(ancestry);
        locus_ids.append_value(&locus.id);

        let contig = ensure_chr_prefix(&locus.region.contig);
        contigs.append_value(&contig);

        starts.push(locus.region.start);
        stops.push(locus.region.end);
        xstarts.push(compute_xpos(&contig, locus.region.start));
        xstops.push(compute_xpos(&contig, locus.region.end));

        // Determine source from locus field or counts
        let exome_count = locus
            .exome_variants
            .as_ref()
            .and_then(|v| v.count)
            .unwrap_or(0);
        let genome_count = locus
            .genome_variants
            .as_ref()
            .and_then(|v| v.count)
            .unwrap_or(0);

        let source = locus.source.clone().unwrap_or_else(|| {
            if exome_count > 0 && genome_count > 0 {
                "both".to_string()
            } else if genome_count > 0 {
                "genome".to_string()
            } else {
                "exome".to_string()
            }
        });
        sources.append_value(&source);

        // Lead variant string (already a string in the manifest)
        let lead_str = locus.lead_variant.clone().unwrap_or_default();
        lead_variants.append_value(&lead_str);

        let lead_pval = locus.lead_pvalue.unwrap_or(1.0);
        lead_pvalues.push(lead_pval);

        exome_counts.push(exome_count as u32);
        genome_counts.push(genome_count as u32);

        // Plot URI
        let plot_uri = format!("{}/loci/{}/plot.png", base_path, locus.id);
        plot_uris.append_value(&plot_uri);
    }

    // Create schema for loci table
    let schema = Arc::new(Schema::new(vec![
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        Field::new("locus_id", DataType::Utf8, false),
        Field::new("contig", DataType::Utf8, false),
        Field::new("start", DataType::Int32, false),
        Field::new("stop", DataType::Int32, false),
        Field::new("xstart", DataType::Int64, false),
        Field::new("xstop", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("lead_variant", DataType::Utf8, false),
        Field::new("lead_pvalue", DataType::Float64, false),
        Field::new("exome_count", DataType::UInt32, false),
        Field::new("genome_count", DataType::UInt32, false),
        Field::new("plot_gcs_uri", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(phenotypes.finish()) as ArrayRef,
            Arc::new(ancestries.finish()) as ArrayRef,
            Arc::new(locus_ids.finish()) as ArrayRef,
            Arc::new(contigs.finish()) as ArrayRef,
            Arc::new(Int32Array::from(starts)) as ArrayRef,
            Arc::new(Int32Array::from(stops)) as ArrayRef,
            Arc::new(Int64Array::from(xstarts)) as ArrayRef,
            Arc::new(Int64Array::from(xstops)) as ArrayRef,
            Arc::new(sources.finish()) as ArrayRef,
            Arc::new(lead_variants.finish()) as ArrayRef,
            Arc::new(Float64Array::from(lead_pvalues)) as ArrayRef,
            Arc::new(arrow::array::UInt32Array::from(exome_counts)) as ArrayRef,
            Arc::new(arrow::array::UInt32Array::from(genome_counts)) as ArrayRef,
            Arc::new(plot_uris.finish()) as ArrayRef,
        ],
    )?;

    // Write to in-memory parquet
    let parquet_bytes = write_batch_to_parquet(&batch)?;

    // Insert into ClickHouse (use just table name, database is set via URL or default)
    client
        .insert_parquet_bytes("loci", parquet_bytes)
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

    println!("  Ingested {} loci", num_rows);
    Ok(num_rows)
}

/// Ingest plot metadata into ClickHouse.
fn ingest_plots(
    phenotype_id: &str,
    ancestry: &str,
    base_path: &str,
    client: &ClickHouseClient,
) -> Result<usize> {
    let mut phenotypes = StringBuilder::new();
    let mut ancestries = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut uris = StringBuilder::new();
    let mut count = 0;

    // Dynamically discover PNG files from the plots/ directory
    let plots_dir = format!("{}/plots", base_path);
    if let Ok(png_files) = list_png_files(&plots_dir) {
        for uri in png_files {
            // Extract just the filename without extension for the plot_type
            if let Some(filename) = uri.rsplit('/').next() {
                let plot_type = filename.trim_end_matches(".png");
                phenotypes.append_value(phenotype_id);
                ancestries.append_value(ancestry);
                types.append_value(plot_type);
                uris.append_value(&uri);
                count += 1;
            }
        }
    }

    // Fallback: Also check for legacy locations (direct in base_path) for backwards compatibility
    let legacy_plot_types = vec![
        ("exome_manhattan", "exome_manhattan.png"),
        ("genome_manhattan", "genome_manhattan.png"),
        ("gene_manhattan", "gene_manhattan.png"),
    ];
    for (plot_type, filename) in legacy_plot_types {
        let uri = format!("{}/{}", base_path, filename);
        // Only include if file exists and wasn't already found in plots/
        if file_exists(&uri).unwrap_or(false) {
            // Check if we already have this plot_type
            // (simple check - if plots/ dir had files, skip legacy)
            if count == 0 {
                phenotypes.append_value(phenotype_id);
                ancestries.append_value(ancestry);
                types.append_value(plot_type);
                uris.append_value(&uri);
                count += 1;
            }
        }
    }

    // Add per-chromosome plots
    let chroms_dir = format!("{}/chroms", base_path);
    if let Ok(chrom_names) = list_subdirectories(&chroms_dir) {
        for chrom in chrom_names {
            // Check for exome manhattan
            let exome_uri = format!("{}/{}/exome_manhattan.png", chroms_dir, chrom);
            if file_exists(&exome_uri).unwrap_or(false) {
                phenotypes.append_value(phenotype_id);
                ancestries.append_value(ancestry);
                types.append_value(&format!("{}_exome_manhattan", chrom));
                uris.append_value(&exome_uri);
                count += 1;
            }

            // Check for genome manhattan
            let genome_uri = format!("{}/{}/genome_manhattan.png", chroms_dir, chrom);
            if file_exists(&genome_uri).unwrap_or(false) {
                phenotypes.append_value(phenotype_id);
                ancestries.append_value(ancestry);
                types.append_value(&format!("{}_genome_manhattan", chrom));
                uris.append_value(&genome_uri);
                count += 1;
            }
        }
    }

    if count == 0 {
        return Ok(0);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("phenotype", DataType::Utf8, false),
        Field::new("ancestry", DataType::Utf8, false),
        Field::new("plot_type", DataType::Utf8, false),
        Field::new("gcs_uri", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(phenotypes.finish()) as ArrayRef,
            Arc::new(ancestries.finish()) as ArrayRef,
            Arc::new(types.finish()) as ArrayRef,
            Arc::new(uris.finish()) as ArrayRef,
        ],
    )?;

    let parquet_bytes = write_batch_to_parquet(&batch)?;

    client
        .insert_parquet_bytes("phenotype_plots", parquet_bytes)
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

    println!("  Ingested {} plot metadata records", count);
    Ok(count)
}

/// Ingest significant variants from a parquet file into ClickHouse.
///
/// Transforms the data by:
/// - Adding phenotype, ancestry, sequencing_type columns
/// - Computing xpos
/// - Adding chr prefix to contig
fn ingest_variants(
    phenotype_id: &str,
    ancestry: &str,
    seq_type: &str,
    parquet_path: &str,
    client: &ClickHouseClient,
) -> Result<usize> {
    // Read parquet file
    let batches = read_parquet_file(parquet_path)?;
    if batches.is_empty() {
        return Ok(0);
    }

    let mut total_rows = 0;

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        // Get source columns
        let contig_col = batch
            .column_by_name("contig")
            .ok_or_else(|| crate::HailError::InvalidFormat("Missing contig column".to_string()))?;
        let position_col = batch.column_by_name("position").ok_or_else(|| {
            crate::HailError::InvalidFormat("Missing position column".to_string())
        })?;

        let contig_arr = contig_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| crate::HailError::InvalidFormat("contig is not string".to_string()))?;
        let position_arr = position_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| crate::HailError::InvalidFormat("position is not int32".to_string()))?;

        // Build new columns
        let mut phenotypes = StringBuilder::with_capacity(num_rows, num_rows * phenotype_id.len());
        let mut ancestries = StringBuilder::with_capacity(num_rows, num_rows * ancestry.len());
        let mut seq_types = StringBuilder::with_capacity(num_rows, num_rows * seq_type.len());
        let mut xpos_values: Vec<i64> = Vec::with_capacity(num_rows);
        let mut chr_contigs = StringBuilder::with_capacity(num_rows, num_rows * 5);

        for i in 0..num_rows {
            phenotypes.append_value(phenotype_id);
            ancestries.append_value(ancestry);
            seq_types.append_value(seq_type);

            let contig = contig_arr.value(i);
            let position = position_arr.value(i);

            xpos_values.push(compute_xpos(contig, position));
            chr_contigs.append_value(&ensure_chr_prefix(contig));
        }

        // Build output schema with new columns first, then existing
        let mut fields = vec![
            Field::new("phenotype", DataType::Utf8, false),
            Field::new("ancestry", DataType::Utf8, false),
            Field::new("sequencing_type", DataType::Utf8, false),
            Field::new("xpos", DataType::Int64, false),
            Field::new("contig", DataType::Utf8, false),
        ];

        // Add remaining columns from source (except contig which we replaced)
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(phenotypes.finish()),
            Arc::new(ancestries.finish()),
            Arc::new(seq_types.finish()),
            Arc::new(Int64Array::from(xpos_values)),
            Arc::new(chr_contigs.finish()),
        ];

        // Skip columns we're adding/replacing to avoid duplicates
        let skip_columns = ["contig", "phenotype", "ancestry", "sequencing_type", "xpos"];
        for (i, field) in batch.schema().fields().iter().enumerate() {
            if !skip_columns.contains(&field.name().as_str()) {
                fields.push(field.as_ref().clone());
                columns.push(batch.column(i).clone());
            }
        }

        let output_schema = Arc::new(Schema::new(fields));
        let output_batch = RecordBatch::try_new(output_schema, columns)?;

        // Write to parquet and insert
        let parquet_bytes = write_batch_to_parquet(&output_batch)?;

        client
            .insert_parquet_bytes("significant_variants", parquet_bytes)
            .map_err(|e| {
                crate::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        total_rows += num_rows;
    }

    Ok(total_rows)
}

/// Ingest loci variants from parquet file into ClickHouse.
///
/// The loci_variants.parquet file is already fully prepared with all columns
/// (locus_id, phenotype, ancestry, sequencing_type, contig, xpos, position,
/// ref, alt, pvalue, neg_log10_p, is_significant), so we can upload it directly.
fn ingest_loci_variants(parquet_path: &str, client: &ClickHouseClient) -> Result<usize> {
    ingest_parquet_direct(parquet_path, "loci_variants", client)
}

/// Ingest gene associations from parquet file into ClickHouse.
///
/// The gene_associations.parquet file is already fully prepared with all columns
/// (gene_id, gene_symbol, annotation, max_maf, phenotype, ancestry, pvalue,
/// pvalue_burden, pvalue_skat, beta_burden, mac, contig, gene_start_position, xpos),
/// so we can upload it directly.
fn ingest_gene_associations(parquet_path: &str, client: &ClickHouseClient) -> Result<usize> {
    ingest_parquet_direct(parquet_path, "gene_associations", client)
}

/// Ingest QQ plot points from parquet file into ClickHouse.
///
/// The qq_*.parquet files are already fully prepared with all columns
/// (phenotype, ancestry, sequencing_type, contig, position, ref_allele,
/// alt_allele, pvalue_log10, pvalue_expected_log10), so we can upload directly.
fn ingest_qq_points(parquet_path: &str, client: &ClickHouseClient) -> Result<usize> {
    ingest_parquet_direct(parquet_path, "qq_points", client)
}

/// Generic helper to ingest a parquet file directly into a ClickHouse table.
///
/// Use this when the parquet file already has the correct schema matching
/// the ClickHouse table - no transformation needed.
fn ingest_parquet_direct(
    parquet_path: &str,
    table_name: &str,
    client: &ClickHouseClient,
) -> Result<usize> {
    // Read raw bytes to upload directly (file already has correct schema)
    let data = read_file_bytes(parquet_path)?;
    if data.is_empty() {
        return Ok(0);
    }

    // Get row count from bytes
    let bytes = bytes::Bytes::from(data.clone());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.build()?;
    let mut row_count = 0;
    for batch in reader {
        row_count += batch?.num_rows();
    }

    // Upload to ClickHouse
    client.insert_parquet_bytes(table_name, data).map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;

    Ok(row_count)
}

/// Read parquet file (local or cloud) into record batches.
fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    if is_cloud_path(path) {
        read_cloud_parquet_file(path)
    } else {
        read_local_parquet_file(path)
    }
}

/// Read parquet from local filesystem.
fn read_local_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Read parquet from cloud storage.
fn read_cloud_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    // Download entire file to memory (sig.parquet files are small)
    let file_size = get_file_size(path)?;
    let data = range_read(path, 0, file_size as usize)?;

    // bytes::Bytes implements ChunkReader
    let bytes = bytes::Bytes::from(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Write a RecordBatch to an in-memory parquet buffer.
fn write_batch_to_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_xpos() {
        assert_eq!(compute_xpos("1", 1000), 1_000_001_000);
        assert_eq!(compute_xpos("chr1", 1000), 1_000_001_000);
        assert_eq!(compute_xpos("22", 500), 22_000_000_500);
        assert_eq!(compute_xpos("X", 100), 23_000_000_100);
        assert_eq!(compute_xpos("chrY", 200), 24_000_000_200);
    }

    #[test]
    fn test_ensure_chr_prefix() {
        assert_eq!(ensure_chr_prefix("1"), "chr1");
        assert_eq!(ensure_chr_prefix("chr1"), "chr1");
        assert_eq!(ensure_chr_prefix("X"), "chrX");
        assert_eq!(ensure_chr_prefix("chrX"), "chrX");
    }
}
