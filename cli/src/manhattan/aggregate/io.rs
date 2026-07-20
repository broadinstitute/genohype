//! File I/O utilities for Manhattan aggregation.
//!
//! This module handles local and cloud file operations for parquet files,
//! directory discovery, cleanup, and size calculations.

use arrow::record_batch::RecordBatch;
use genohype_core::error::Result;
use genohype_core::io::is_cloud_path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

// =============================================================================
// Parquet File Listing
// =============================================================================

/// List parquet files matching a suffix in a local directory.
pub(crate) fn list_local_parquet_files(dir: &str, suffix: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(suffix) {
                files.push(path.to_string_lossy().to_string());
            }
        }
    }
    files.sort();
    Ok(files)
}

/// List parquet files matching a suffix in a cloud directory.
pub(crate) fn list_cloud_parquet_files(dir: &str, suffix: &str) -> Result<Vec<String>> {
    use crate::HailError;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use url::Url;

    let url =
        Url::parse(dir).map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    let (store, prefix, base_url): (Arc<dyn object_store::ObjectStore>, ObjPath, String) =
        match url.scheme() {
            #[cfg(feature = "gcp")]
            "gs" => {
                let bucket = url.host_str().ok_or_else(|| {
                    HailError::InvalidFormat("Missing bucket in GCS URL".to_string())
                })?;
                let path = url.path().trim_start_matches('/');
                (
                    genohype_core::io::get_gcs_client(bucket)?,
                    ObjPath::from(path),
                    format!("gs://{}/", bucket),
                )
            }
            #[cfg(feature = "aws")]
            "s3" => {
                let bucket = url.host_str().ok_or_else(|| {
                    HailError::InvalidFormat("Missing bucket in S3 URL".to_string())
                })?;
                let path = url.path().trim_start_matches('/');
                let s3 = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| {
                        HailError::InvalidFormat(format!("Failed to create S3 client: {}", e))
                    })?;
                (
                    Arc::new(s3),
                    ObjPath::from(path),
                    format!("s3://{}/", bucket),
                )
            }
            scheme => {
                return Err(HailError::InvalidFormat(format!(
                    "Unsupported URL scheme: {}",
                    scheme
                )));
            }
        };

    // Use blocking runtime for object_store async operations
    let rt = tokio::runtime::Runtime::new()?;
    let list_result = rt.block_on(async {
        let mut files = Vec::new();
        let stream = store.list(Some(&prefix));
        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;
        for result in results {
            if let Ok(meta) = result {
                let path = meta.location.to_string();
                if path.ends_with(suffix) {
                    // Reconstruct full URL
                    let full_path = format!("{}{}", base_url, path);
                    files.push(full_path);
                }
            }
        }
        files
    });

    let mut files = list_result;
    files.sort();
    Ok(files)
}

// =============================================================================
// Parquet Reading
// =============================================================================

/// Read all record batches from a parquet file.
pub(crate) fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    if is_cloud_path(path) {
        read_cloud_parquet_file(path)
    } else {
        read_local_parquet_file(path)
    }
}

/// Read parquet from local filesystem.
pub(crate) fn read_local_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok(batches)
}

/// Read parquet from cloud storage.
pub(crate) fn read_cloud_parquet_file(path: &str) -> Result<Vec<RecordBatch>> {
    use genohype_core::io::{get_file_size, range_read};

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

// =============================================================================
// Parquet Writing
// =============================================================================

/// Write record batches to a parquet file.
pub(crate) fn write_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    if is_cloud_path(path) {
        write_cloud_parquet_batches(path, schema, batches)
    } else {
        write_local_parquet_batches(path, schema, batches)
    }
}

/// Write parquet to local filesystem.
fn write_local_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

/// Write parquet to cloud storage.
fn write_cloud_parquet_batches(
    path: &str,
    schema: &Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    use genohype_core::io::CloudWriter;

    let cloud_writer = CloudWriter::new(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(cloud_writer, schema.clone(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    let cloud_writer = writer.into_inner()?;
    cloud_writer.finish()?;
    Ok(())
}

// =============================================================================
// Directory Discovery and Cleanup
// =============================================================================

/// Discover chromosomes by listing directories in the chroms/ folder.
pub(crate) fn discover_chromosomes(chroms_dir: &str) -> Result<Vec<String>> {
    if is_cloud_path(chroms_dir) {
        let dir = chroms_dir.trim_end_matches('/');
        // gsutil ls gs://bucket/path/chroms/ returns directories
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
            // It's okay if dir doesn't exist (no chroms found)
            return Ok(vec![]);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let chroms: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                // line is like gs://bucket/path/chroms/chr1/
                let trimmed = line.trim().trim_end_matches('/');
                trimmed.rsplit('/').next().map(|s| s.to_string())
            })
            .collect();
        Ok(chroms)
    } else {
        if !std::path::Path::new(chroms_dir).exists() {
            return Ok(vec![]);
        }

        let mut chroms = Vec::new();
        for entry in std::fs::read_dir(chroms_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    chroms.push(name.to_string());
                }
            }
        }
        Ok(chroms)
    }
}

/// Check if a directory contains any partial PNGs.
pub(crate) fn has_partial_pngs(dir_path: &str) -> Result<bool> {
    if is_cloud_path(dir_path) {
        let dir = dir_path.trim_end_matches('/');
        // Check for at least one file
        let output = std::process::Command::new("gsutil")
            .args(["ls", &format!("{}/part-*.png", dir)])
            .output();

        Ok(output.map(|o| o.status.success()).unwrap_or(false))
    } else {
        if !std::path::Path::new(dir_path).exists() {
            return Ok(false);
        }

        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("part-") && name.ends_with(".png") {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Clean up intermediate partition files.
pub(crate) fn cleanup_intermediates(output_base: &str) -> Result<()> {
    // Delete exome/part-*.png and exome/part-*-sig.parquet
    // Delete genome/part-*.png and genome/part-*-sig.parquet

    if is_cloud_path(output_base) {
        // For cloud, we'd need to list and delete
        // TODO: Implement cloud cleanup
        println!("    Cloud cleanup not yet implemented");
    } else {
        for source in &["exome", "genome"] {
            let dir = format!("{}/{}", output_base, source);
            if std::path::Path::new(&dir).exists() {
                std::fs::remove_dir_all(&dir)?;
            }
        }
    }

    Ok(())
}

/// Cleanup intermediate files in chrom directories.
pub(crate) fn cleanup_chrom_intermediates(output_base: &str) -> Result<()> {
    if is_cloud_path(output_base) {
        println!("    Cloud cleanup of chroms not yet implemented");
    } else {
        let chroms_dir = format!("{}/chroms", output_base);
        if std::path::Path::new(&chroms_dir).exists() {
            // Walk through chroms dir and delete part-*.png files
            for chrom_entry in std::fs::read_dir(&chroms_dir)? {
                let chrom_entry = chrom_entry?;
                if chrom_entry.file_type()?.is_dir() {
                    // Each chrom has exome/ and genome/ subdirs
                    for source in &["exome", "genome"] {
                        let source_dir = chrom_entry.path().join(source);
                        if source_dir.exists() {
                            std::fs::remove_dir_all(&source_dir)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// =============================================================================
// Size Calculation
// =============================================================================

/// Get the total size of a GCS directory in bytes using gsutil du.
/// Returns None for local paths or if the command fails.
pub fn get_gcs_dir_size(path: &str) -> Option<u64> {
    if !is_cloud_path(path) {
        // For local paths, use std::fs to calculate directory size
        return get_local_dir_size(path);
    }

    let output = std::process::Command::new("gsutil")
        .args(["du", "-s", path])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Output format: "12345  gs://bucket/path"
    stdout
        .lines()
        .next()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Get the total size of a local directory in bytes.
pub(crate) fn get_local_dir_size(path: &str) -> Option<u64> {
    use std::path::Path;

    let path = Path::new(path);
    if !path.exists() {
        return None;
    }

    fn dir_size(path: &std::path::Path) -> u64 {
        let mut size = 0;
        if path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        size += dir_size(&entry_path);
                    } else if let Ok(metadata) = entry.metadata() {
                        size += metadata.len();
                    }
                }
            }
        } else if let Ok(metadata) = std::fs::metadata(path) {
            size = metadata.len();
        }
        size
    }

    Some(dir_size(path))
}

/// Write a file (handles both local and cloud paths).
pub(crate) fn write_locus_file(path: &str, data: &[u8]) -> Result<()> {
    if is_cloud_path(path) {
        use genohype_core::io::CloudWriter;
        use std::io::Write;

        // Ensure parent directory structure is implied in the path
        let mut writer = CloudWriter::new(path)?;
        writer.write_all(data)?;
        writer.finish()?;
    } else {
        // Create parent directory
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, data)?;
    }

    Ok(())
}
