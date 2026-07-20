//! Significant hits merging for Manhattan aggregation.
//!
//! This module handles merging significant hits from scan phase parquet files,
//! finding top hits, and combining exome/genome results.

use arrow::array::{Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use genohype_core::error::Result;
use genohype_core::io::is_cloud_path;
use std::path::Path;
use std::time::Instant;

use super::io::{
    list_cloud_parquet_files, list_local_parquet_files, read_parquet_file, write_parquet_batches,
};
use crate::manhattan::data::ManifestTopHit;

/// Candidate for top hit found during parallel scan.
#[derive(Clone)]
pub(crate) struct TopHitCandidate {
    pub contig: String,
    pub position: i32,
    pub ref_allele: String,
    pub alt_allele: String,
    pub pvalue: f64,
}

/// Merge significant hits from scan phase parquet files.
///
/// Optimized approach:
/// - Parallel file reads with rayon
/// - Find top hit by scanning for min pvalue (no full sort needed)
/// - Concatenate batches without sorting for output (partitions are already sorted)
pub(crate) fn merge_significant_hits(
    output_base: &str,
    source: &str,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use rayon::prelude::*;

    let sig_dir = format!("{}/{}", output_base, source);
    let output_file = format!("{}/{}_significant.parquet", output_base, source);

    // Collect all sig parquet files
    let sig_files = if is_cloud_path(&sig_dir) {
        list_cloud_parquet_files(&sig_dir, "-sig.parquet")?
    } else {
        let path = Path::new(&sig_dir);
        if !path.exists() {
            return Ok((0, None));
        }
        list_local_parquet_files(&sig_dir, "-sig.parquet")?
    };

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    let start = Instant::now();
    println!(
        "    Reading {} sig.parquet files in parallel...",
        sig_files.len()
    );

    // Read all parquet files in parallel
    let results: Vec<Result<(Vec<RecordBatch>, Option<TopHitCandidate>)>> = sig_files
        .par_iter()
        .map(|file_path| {
            let batches = read_parquet_file(file_path)?;
            // Find top hit candidate in this file while we have it in memory
            let top_candidate = find_top_hit_in_batches(&batches);
            Ok((batches, top_candidate))
        })
        .collect();

    // Collect batches and find global top hit
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;
    let mut global_top: Option<TopHitCandidate> = None;

    for result in results {
        let (batches, top_candidate) = result?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
        // Update global top hit if this file has a better one
        if let Some(candidate) = top_candidate {
            global_top = Some(match global_top {
                None => candidate,
                Some(current) if candidate.pvalue < current.pvalue => candidate,
                Some(current) => current,
            });
        }
    }

    let read_time = start.elapsed();
    println!(
        "    Read {} batches in {:.1}s",
        all_batches.len(),
        read_time.as_secs_f64()
    );

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Write concatenated output (no sorting - files are already partition-sorted)
    let write_start = Instant::now();
    write_parquet_batches(&output_file, &schema, &all_batches)?;
    println!(
        "    Wrote {} rows in {:.1}s",
        total_count,
        write_start.elapsed().as_secs_f64()
    );

    // Convert top candidate to ManifestTopHit
    let top_hit = global_top.map(|c| ManifestTopHit {
        id: format!(
            "{}:{}:{}:{}",
            c.contig, c.position, c.ref_allele, c.alt_allele
        ),
        pvalue: c.pvalue,
        gene: None,
        consequence: None,
    });

    Ok((total_count, top_hit))
}

/// Merge and combine significant hits from both exome and genome into a single file.
///
/// This function reads all `*-sig.parquet` files from both the `exome/` and `genome/`
/// directories and writes them to a single `significant.parquet` at the output root.
/// Since the scan phase now includes `sequencing_type` in the output, we can safely
/// merge them into one file.
pub(crate) fn merge_and_combine_hits(
    output_base: &str,
    has_exome: bool,
    has_genome: bool,
) -> Result<(u64, Option<ManifestTopHit>)> {
    use rayon::prelude::*;

    let output_file = format!("{}/significant.parquet", output_base);

    // Collect all sig parquet files from both sources
    let mut sig_files: Vec<String> = Vec::new();

    if has_exome {
        let exome_dir = format!("{}/exome", output_base);
        if is_cloud_path(&exome_dir) {
            if let Ok(files) = list_cloud_parquet_files(&exome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        } else if Path::new(&exome_dir).exists() {
            if let Ok(files) = list_local_parquet_files(&exome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        }
    }

    if has_genome {
        let genome_dir = format!("{}/genome", output_base);
        if is_cloud_path(&genome_dir) {
            if let Ok(files) = list_cloud_parquet_files(&genome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        } else if Path::new(&genome_dir).exists() {
            if let Ok(files) = list_local_parquet_files(&genome_dir, "-sig.parquet") {
                sig_files.extend(files);
            }
        }
    }

    if sig_files.is_empty() {
        return Ok((0, None));
    }

    let start = Instant::now();
    println!(
        "    Reading {} sig.parquet files from exome+genome in parallel...",
        sig_files.len()
    );

    // Read all parquet files in parallel
    let results: Vec<Result<(Vec<RecordBatch>, Option<TopHitCandidate>)>> = sig_files
        .par_iter()
        .map(|file_path| {
            let batches = read_parquet_file(file_path)?;
            // Find top hit candidate in this file while we have it in memory
            let top_candidate = find_top_hit_in_batches(&batches);
            Ok((batches, top_candidate))
        })
        .collect();

    // Collect batches and find global top hit
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;
    let mut global_top: Option<TopHitCandidate> = None;

    for result in results {
        let (batches, top_candidate) = result?;
        for batch in batches {
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            all_batches.push(batch);
        }
        // Update global top hit if this file has a better one
        if let Some(candidate) = top_candidate {
            global_top = Some(match global_top {
                None => candidate,
                Some(current) if candidate.pvalue < current.pvalue => candidate,
                Some(current) => current,
            });
        }
    }

    let read_time = start.elapsed();
    println!(
        "    Read {} batches in {:.1}s",
        all_batches.len(),
        read_time.as_secs_f64()
    );

    if all_batches.is_empty() {
        return Ok((0, None));
    }

    let schema = schema.unwrap();
    let total_count: u64 = all_batches.iter().map(|b| b.num_rows() as u64).sum();

    if total_count == 0 {
        return Ok((0, None));
    }

    // Write concatenated output (no sorting - files are already partition-sorted)
    let write_start = Instant::now();
    write_parquet_batches(&output_file, &schema, &all_batches)?;
    println!(
        "    Wrote {} rows to significant.parquet in {:.1}s",
        total_count,
        write_start.elapsed().as_secs_f64()
    );

    // Convert top candidate to ManifestTopHit
    let top_hit = global_top.map(|c| ManifestTopHit {
        id: format!(
            "{}:{}:{}:{}",
            c.contig, c.position, c.ref_allele, c.alt_allele
        ),
        pvalue: c.pvalue,
        gene: None,
        consequence: None,
    });

    Ok((total_count, top_hit))
}

/// Find the top hit (lowest pvalue) in a set of batches.
pub(crate) fn find_top_hit_in_batches(batches: &[RecordBatch]) -> Option<TopHitCandidate> {
    let mut best: Option<TopHitCandidate> = None;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let schema = batch.schema();

        // Get column indices
        let pvalue_idx = schema.fields().iter().position(|f| f.name() == "pvalue")?;
        let contig_idx = schema.fields().iter().position(|f| f.name() == "contig")?;
        let position_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "position")?;
        let ref_idx = schema.fields().iter().position(|f| f.name() == "ref")?;
        let alt_idx = schema.fields().iter().position(|f| f.name() == "alt")?;

        let pvalue_col = batch
            .column(pvalue_idx)
            .as_any()
            .downcast_ref::<Float64Array>()?;
        let contig_col = batch
            .column(contig_idx)
            .as_any()
            .downcast_ref::<StringArray>()?;
        let position_col = batch
            .column(position_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()?;
        let ref_col = batch
            .column(ref_idx)
            .as_any()
            .downcast_ref::<StringArray>()?;
        let alt_col = batch
            .column(alt_idx)
            .as_any()
            .downcast_ref::<StringArray>()?;

        for i in 0..batch.num_rows() {
            if pvalue_col.is_null(i) {
                continue;
            }
            let pvalue = pvalue_col.value(i);

            let dominated = best.as_ref().map(|b| pvalue >= b.pvalue).unwrap_or(false);
            if dominated {
                continue;
            }

            best = Some(TopHitCandidate {
                contig: contig_col.value(i).to_string(),
                position: position_col.value(i),
                ref_allele: ref_col.value(i).to_string(),
                alt_allele: alt_col.value(i).to_string(),
                pvalue,
            });
        }
    }

    best
}
