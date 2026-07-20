//! Schema validation for Hail tables
//!
//! This module provides JSON schema validation for Hail table data,
//! allowing verification that table contents conform to expected schemas
//! before export to databases or other systems.

use crate::codec::EncodedValue;
use crate::query::QueryEngine;
use crate::HailError;
use crate::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use jsonschema::Validator;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use serde_json::{json, Map, Value};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Report of validation results
#[derive(Debug, Default)]
pub struct ValidationReport {
    /// Total number of rows scanned
    pub scanned_count: usize,
    /// Number of rows that passed validation
    pub valid_count: usize,
    /// Number of rows that failed validation
    pub invalid_count: usize,
    /// Sample of validation errors (up to 10)
    pub errors: Vec<String>,
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Validation Report")?;
        writeln!(f, "=================")?;
        writeln!(f, "Rows scanned: {}", self.scanned_count)?;
        writeln!(f, "Valid rows:   {}", self.valid_count)?;
        writeln!(f, "Invalid rows: {}", self.invalid_count)?;

        if !self.errors.is_empty() {
            writeln!(f)?;
            writeln!(f, "Sample errors (first {}):", self.errors.len())?;
            for err in &self.errors {
                writeln!(f, "  - {}", err)?;
            }
        }

        Ok(())
    }
}

/// JSON schema validator for Hail tables
pub struct SchemaValidator {
    validator: Validator,
}

impl SchemaValidator {
    /// Create a validator from a JSON schema file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let schema_json: serde_json::Value = serde_json::from_reader(file)?;
        Self::from_value(&schema_json)
    }

    /// Create a validator from a JSON schema value
    pub fn from_value(schema: &serde_json::Value) -> Result<Self> {
        let validator = Validator::new(schema)
            .map_err(|e| HailError::InvalidFormat(format!("Invalid JSON schema: {}", e)))?;

        Ok(Self { validator })
    }

    /// Validate rows from a query engine
    ///
    /// # Arguments
    /// * `engine` - The query engine to read rows from
    /// * `limit` - Optional limit on number of rows to validate
    /// * `fail_fast` - If true, stop on first error
    pub fn validate(
        &self,
        engine: &QueryEngine,
        limit: Option<usize>,
        fail_fast: bool,
    ) -> Result<ValidationReport> {
        let mut report = ValidationReport::default();
        let max_errors = 10;

        'outer: for i in 0..engine.num_partitions() {
            // Check if we've hit the limit
            if let Some(l) = limit {
                if report.scanned_count >= l {
                    break;
                }
            }

            let iter = engine.scan_partition_iter(i, &[])?;

            for row_result in iter {
                let row = row_result?;

                // Check limit
                if let Some(l) = limit {
                    if report.scanned_count >= l {
                        break 'outer;
                    }
                }

                report.scanned_count += 1;

                // Serialize row to JSON value for validation
                let json_val = serde_json::to_value(&row)?;

                // Validate against schema
                let validation_result = self.validator.validate(&json_val);

                if validation_result.is_ok() {
                    report.valid_count += 1;
                } else {
                    report.invalid_count += 1;

                    // Collect errors (up to max_errors)
                    if report.errors.len() < max_errors {
                        for err in self.validator.iter_errors(&json_val) {
                            report.errors.push(format!(
                                "Row {}: {} at path '{}'",
                                report.scanned_count, err, err.instance_path
                            ));
                            if report.errors.len() >= max_errors {
                                break;
                            }
                        }
                    }

                    if fail_fast {
                        break 'outer;
                    }
                }
            }
        }

        Ok(report)
    }

    /// Validate a collection of pre-sampled rows
    ///
    /// Used by data sources that provide optimized random sampling (like indexed VCFs).
    fn validate_rows(&self, rows: Vec<EncodedValue>, fail_fast: bool) -> Result<ValidationReport> {
        let mut report = ValidationReport::default();
        let max_errors = 10;

        let pb = ProgressBar::new(rows.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        for row in rows {
            report.scanned_count += 1;

            // Serialize row to JSON value for validation
            let json_val = serde_json::to_value(&row)?;

            // Validate against schema
            if self.validator.validate(&json_val).is_ok() {
                report.valid_count += 1;
            } else {
                report.invalid_count += 1;

                if report.errors.len() < max_errors {
                    for err in self.validator.iter_errors(&json_val) {
                        report.errors.push(format!(
                            "Row {}: {} at path '{}'",
                            report.scanned_count, err, err.instance_path
                        ));
                        if report.errors.len() >= max_errors {
                            break;
                        }
                    }
                }

                if fail_fast {
                    break;
                }
            }

            pb.inc(1);
        }

        pb.finish_and_clear();
        Ok(report)
    }

    /// Validate a collection of pre-sampled rows with verbose output
    ///
    /// Shows each row's validation result in real-time.
    fn validate_rows_verbose(
        &self,
        rows: Vec<EncodedValue>,
        fail_fast: bool,
        key_fields: &[String],
    ) -> Result<ValidationReport> {
        let mut report = ValidationReport::default();
        let max_errors = 10;

        let pb = ProgressBar::new(rows.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} Validating: [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        for (idx, row) in rows.iter().enumerate() {
            report.scanned_count += 1;

            // Extract key for display - try locus for VCFs, fall back to key fields
            let key_display = if let EncodedValue::Struct(fields) = &row {
                // Try to get locus info (common for VCF/genomic data)
                let locus = fields.iter().find(|(n, _)| n == "locus");
                if let Some((_, EncodedValue::Struct(locus_fields))) = locus {
                    let contig = locus_fields
                        .iter()
                        .find(|(n, _)| n == "contig")
                        .map(|(_, v)| match v {
                            EncodedValue::Binary(b) => String::from_utf8_lossy(b).to_string(),
                            _ => "?".to_string(),
                        })
                        .unwrap_or_default();
                    let pos = locus_fields
                        .iter()
                        .find(|(n, _)| n == "position")
                        .map(|(_, v)| match v {
                            EncodedValue::Int32(i) => i.to_string(),
                            _ => "?".to_string(),
                        })
                        .unwrap_or_default();
                    format!("{}:{}", contig, pos)
                } else if !key_fields.is_empty() {
                    // Fall back to key fields for Hail tables
                    key_fields
                        .iter()
                        .filter_map(|k| fields.iter().find(|(n, _)| n == k))
                        .map(|(_, v)| match v {
                            EncodedValue::Binary(b) => String::from_utf8_lossy(b).to_string(),
                            EncodedValue::Int32(i) => i.to_string(),
                            _ => format!("{:?}", v),
                        })
                        .collect::<Vec<_>>()
                        .join(":")
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

            // Serialize row to JSON value for validation
            let json_val = serde_json::to_value(&row)?;

            // Validate against schema
            if self.validator.validate(&json_val).is_ok() {
                report.valid_count += 1;
                println!("  row {:>6} {} ok", idx, key_display);
            } else {
                report.invalid_count += 1;
                println!("  row {:>6} {} INVALID", idx, key_display);

                if report.errors.len() < max_errors {
                    for err in self.validator.iter_errors(&json_val) {
                        report.errors.push(format!(
                            "Row {}: {} at path '{}'",
                            idx, err, err.instance_path
                        ));
                        if report.errors.len() >= max_errors {
                            break;
                        }
                    }
                }

                if fail_fast {
                    break;
                }
            }

            pb.inc(1);
        }

        pb.finish_and_clear();
        Ok(report)
    }

    /// Validate rows using streaming iteration (memory efficient)
    ///
    /// For single-partition sources (like VCFs), validates first N rows
    /// sequentially with early termination.
    fn validate_streaming(
        &self,
        engine: &QueryEngine,
        limit: Option<usize>,
        fail_fast: bool,
    ) -> Result<ValidationReport> {
        let mut report = ValidationReport::default();
        let max_errors = 10;

        if limit.is_some() {
            println!(
                "Note: Single-partition source - validating first {} rows sequentially",
                limit.unwrap()
            );
        } else {
            println!("Streaming validation (single partition)...");
        }

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        // Use streaming iterator with early termination
        let iterator = engine.query_iter(&[])?;

        for row_result in iterator {
            // Check limit
            if let Some(l) = limit {
                if report.scanned_count >= l {
                    break;
                }
            }

            let row = row_result?;
            report.scanned_count += 1;

            if report.scanned_count % 100 == 0 {
                pb.set_message(format!(
                    "{} rows ({} ok, {} invalid)",
                    report.scanned_count, report.valid_count, report.invalid_count
                ));
            }

            // Serialize row to JSON value for validation
            let json_val = serde_json::to_value(&row)?;

            // Validate against schema
            if self.validator.validate(&json_val).is_ok() {
                report.valid_count += 1;
            } else {
                report.invalid_count += 1;

                if report.errors.len() < max_errors {
                    for err in self.validator.iter_errors(&json_val) {
                        report.errors.push(format!(
                            "Row {}: {} at path '{}'",
                            report.scanned_count, err, err.instance_path
                        ));
                        if report.errors.len() >= max_errors {
                            break;
                        }
                    }
                }

                if fail_fast {
                    break;
                }
            }
        }

        pb.finish_and_clear();
        Ok(report)
    }

    /// Validate a random sample of rows from a query engine (parallel)
    ///
    /// # Arguments
    /// * `engine` - The query engine to read rows from
    /// * `sample_size` - Number of rows to randomly sample
    /// * `fail_fast` - If true, stop on first error
    pub fn validate_sample(
        &self,
        engine: &QueryEngine,
        sample_size: usize,
        fail_fast: bool,
    ) -> Result<ValidationReport> {
        let num_partitions = engine.num_partitions();
        if num_partitions == 0 {
            return Ok(ValidationReport::default());
        }

        // Try optimized random sampling first (e.g., VCFs with tabix index)
        match engine.sample_random(sample_size) {
            Ok(samples) => {
                println!(
                    "Using indexed random sampling ({} samples)...",
                    samples.len()
                );
                return self.validate_rows(samples, fail_fast);
            }
            Err(_) => {
                // Fall through to partition-based or streaming sampling
            }
        }

        // For single-partition sources without optimized sampling (like unindexed VCFs),
        // use streaming validation to avoid loading the entire file into memory
        if num_partitions == 1 {
            return self.validate_streaming(engine, Some(sample_size), fail_fast);
        }

        // Sample from a reasonable number of partitions to balance coverage vs speed
        // Use sqrt(sample_size) partitions, capped at available partitions and sample_size
        let partitions_to_sample = std::cmp::min(
            sample_size,
            std::cmp::min(
                num_partitions,
                std::cmp::max(1, (sample_size as f64).sqrt().ceil() as usize),
            ),
        );

        let mut partition_indices: Vec<usize> = (0..num_partitions).collect();
        {
            let mut rng = rand::thread_rng();
            partition_indices.shuffle(&mut rng);
        }
        partition_indices.truncate(partitions_to_sample);

        // Calculate roughly how many rows to sample per partition
        let rows_per_partition = (sample_size / partitions_to_sample) + 1;

        // Get thread count for display
        let num_threads = rayon::current_num_threads();

        // Progress bar with informative prefix
        println!(
            "Sampling {} rows across {} partitions using {} threads...",
            sample_size, partitions_to_sample, num_threads
        );
        let pb = ProgressBar::new(partitions_to_sample as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        // Atomic counters for coordination
        let total_scanned = AtomicUsize::new(0);
        let should_stop = AtomicBool::new(false);

        // Parallel validation across partitions
        let results: Vec<ValidationReport> = partition_indices
            .par_iter()
            .map(|&partition_idx| {
                // Check if we should stop early
                if should_stop.load(Ordering::Relaxed) {
                    pb.inc(1);
                    return ValidationReport::default();
                }

                // Check if we've already scanned enough
                if total_scanned.load(Ordering::Relaxed) >= sample_size {
                    pb.inc(1);
                    return ValidationReport::default();
                }

                let mut local_report = ValidationReport::default();
                let max_errors = 10;

                // Use reservoir sampling to select rows without loading entire partition
                let mut reservoir: Vec<(usize, EncodedValue)> =
                    Vec::with_capacity(rows_per_partition);
                let mut rng = rand::thread_rng();
                use rand::Rng;

                let iter = match engine.scan_partition_iter(partition_idx, &[]) {
                    Ok(it) => it,
                    Err(_) => {
                        pb.inc(1);
                        return local_report;
                    }
                };

                // Reservoir sampling loop
                for (i, row_res) in iter.enumerate() {
                    let row = match row_res {
                        Ok(r) => r,
                        Err(_) => continue,
                    };

                    if reservoir.len() < rows_per_partition {
                        reservoir.push((i, row));
                    } else {
                        let j = rng.gen_range(0..=i);
                        if j < rows_per_partition {
                            reservoir[j] = (i, row);
                        }
                    }
                }

                if reservoir.is_empty() {
                    pb.inc(1);
                    return local_report;
                }

                // Validate sampled rows
                for (row_idx, row) in reservoir {
                    // Check global limits
                    let current_total = total_scanned.fetch_add(1, Ordering::Relaxed);
                    if current_total >= sample_size {
                        total_scanned.fetch_sub(1, Ordering::Relaxed);
                        break;
                    }

                    if should_stop.load(Ordering::Relaxed) {
                        break;
                    }

                    local_report.scanned_count += 1;

                    // Serialize row to JSON value for validation
                    let json_val = match serde_json::to_value(&row) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // Validate against schema
                    if self.validator.validate(&json_val).is_ok() {
                        local_report.valid_count += 1;
                    } else {
                        local_report.invalid_count += 1;

                        // Collect errors (up to max_errors per partition)
                        if local_report.errors.len() < max_errors {
                            for err in self.validator.iter_errors(&json_val) {
                                local_report.errors.push(format!(
                                    "Partition {}, Row {}: {} at path '{}'",
                                    partition_idx, row_idx, err, err.instance_path
                                ));
                                if local_report.errors.len() >= max_errors {
                                    break;
                                }
                            }
                        }

                        if fail_fast {
                            should_stop.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }

                pb.inc(1);
                local_report
            })
            .collect();

        pb.finish_and_clear();

        // Merge all results
        let mut final_report = ValidationReport::default();
        for r in results {
            final_report.scanned_count += r.scanned_count;
            final_report.valid_count += r.valid_count;
            final_report.invalid_count += r.invalid_count;
            // Keep only first 10 errors total
            for err in r.errors {
                if final_report.errors.len() < 10 {
                    final_report.errors.push(err);
                }
            }
        }

        Ok(final_report)
    }

    /// Validate a random sample of rows with verbose output (parallel)
    ///
    /// Shows progress in a fixed-height box with worker status lines.
    pub fn validate_sample_verbose(
        &self,
        engine: &QueryEngine,
        sample_size: usize,
        fail_fast: bool,
    ) -> Result<ValidationReport> {
        use crate::codec::EncodedValue;
        use std::sync::{Arc, Mutex};

        let num_partitions = engine.num_partitions();
        if num_partitions == 0 {
            return Ok(ValidationReport::default());
        }

        // Try optimized random sampling first (e.g., VCFs with tabix index)
        // This is the same logic as validate_sample - both methods should use
        // the optimized path when available
        match engine.sample_random(sample_size) {
            Ok(samples) => {
                println!(
                    "Using indexed random sampling ({} samples)...",
                    samples.len()
                );
                return self.validate_rows_verbose(samples, fail_fast, engine.key_fields());
            }
            Err(_) => {
                // Fall through to partition-based sampling
            }
        }

        // For single-partition sources without optimized sampling,
        // fall back to non-verbose streaming (verbose doesn't help for sequential)
        if num_partitions == 1 {
            println!("Note: Single-partition source - using sequential validation");
            return self.validate_streaming(engine, Some(sample_size), fail_fast);
        }

        // Sample from partitions
        let partitions_to_sample = std::cmp::min(sample_size, num_partitions);

        let mut partition_indices: Vec<usize> = (0..num_partitions).collect();
        {
            let mut rng = rand::thread_rng();
            partition_indices.shuffle(&mut rng);
        }
        partition_indices.truncate(partitions_to_sample);

        let num_threads = rayon::current_num_threads();
        let key_fields = engine.key_fields().to_vec();

        println!(
            "Sampling {} rows across {} partitions using {} threads",
            sample_size, partitions_to_sample, num_threads
        );
        if !key_fields.is_empty() {
            println!("Key fields: {:?}", key_fields);
        }
        println!();

        // Create multi-progress with fixed worker bars + summary bar
        let mp = MultiProgress::new();

        // Create fixed worker bars (one per thread)
        let idle_style = ProgressStyle::with_template("  {msg}").unwrap();
        let spinner_style = ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏");

        let worker_bars: Vec<ProgressBar> = (0..num_threads)
            .map(|i| {
                let pb = mp.add(ProgressBar::new_spinner());
                pb.set_style(idle_style.clone());
                pb.set_message(format!("worker {} idle", i));
                pb
            })
            .collect();
        let worker_bars = Arc::new(worker_bars);

        // Summary bar at the bottom
        let summary_bar = mp.add(ProgressBar::new(partitions_to_sample as u64));
        summary_bar.set_style(
            ProgressStyle::with_template("{bar:40.green/dim} {pos}/{len} done | {msg}")
                .unwrap()
                .progress_chars("##-"),
        );
        summary_bar.set_message("0 ok, 0 invalid");

        // Shared counters
        let valid_count = Arc::new(AtomicUsize::new(0));
        let invalid_count = Arc::new(AtomicUsize::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));
        let total_scanned = Arc::new(AtomicUsize::new(0));

        // Results collector
        let results: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        // Helper to extract key values from a row
        fn extract_keys(row: &EncodedValue, key_fields: &[String]) -> String {
            if key_fields.is_empty() {
                return String::new();
            }
            if let EncodedValue::Struct(fields) = row {
                let key_vals: Vec<String> = key_fields
                    .iter()
                    .filter_map(|k| {
                        fields
                            .iter()
                            .find(|(name, _)| name == k)
                            .map(|(name, val)| {
                                let val_str = match val {
                                    EncodedValue::Binary(b) => {
                                        String::from_utf8_lossy(b).to_string()
                                    }
                                    EncodedValue::Int32(i) => i.to_string(),
                                    EncodedValue::Int64(i) => i.to_string(),
                                    EncodedValue::Float32(f) => f.to_string(),
                                    EncodedValue::Float64(f) => f.to_string(),
                                    EncodedValue::Boolean(b) => b.to_string(),
                                    EncodedValue::Null => "null".to_string(),
                                    EncodedValue::Struct(inner) => {
                                        let parts: Vec<String> = inner
                                            .iter()
                                            .map(|(n, v)| {
                                                let v_str = match v {
                                                    EncodedValue::Binary(b) => {
                                                        String::from_utf8_lossy(b).to_string()
                                                    }
                                                    EncodedValue::Int32(i) => i.to_string(),
                                                    EncodedValue::Int64(i) => i.to_string(),
                                                    _ => format!("{:?}", v),
                                                };
                                                format!("{}={}", n, v_str)
                                            })
                                            .collect();
                                        format!("{{{}}}", parts.join(","))
                                    }
                                    _ => format!("{:?}", val),
                                };
                                format!("{}={}", name, val_str)
                            })
                    })
                    .collect();
                key_vals.join(" ")
            } else {
                String::new()
            }
        }

        // Process partitions in parallel
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        partition_indices.par_iter().for_each(|&partition_idx| {
            if should_stop.load(Ordering::Relaxed) {
                return;
            }

            if total_scanned.load(Ordering::Relaxed) >= sample_size {
                return;
            }

            // Get the actual thread's worker bar
            let worker_id = rayon::current_thread_index().unwrap_or(0);
            let pb = &worker_bars[worker_id];

            pb.set_style(spinner_style.clone());
            pb.enable_steady_tick(std::time::Duration::from_millis(80));
            pb.set_message(format!("partition {:>4} reading...", partition_idx));

            // Check limit before processing
            if total_scanned.fetch_add(1, Ordering::Relaxed) >= sample_size {
                total_scanned.fetch_sub(1, Ordering::Relaxed);
                pb.set_style(idle_style.clone());
                pb.set_message(format!("worker {} idle", worker_id));
                return;
            }

            // Use reservoir sampling to select one row from stream
            let mut selected_row: Option<(usize, EncodedValue)> = None;
            let mut count = 0;
            let mut rng = rand::thread_rng();
            use rand::Rng;

            let iter = match engine.scan_partition_iter(partition_idx, &[]) {
                Ok(it) => it,
                Err(e) => {
                    pb.set_style(idle_style.clone());
                    pb.set_message(format!("partition {:>4} ERROR: {}", partition_idx, e));
                    summary_bar.inc(1);
                    // Give back the count since we didn't process one
                    total_scanned.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };

            for (i, row_res) in iter.enumerate() {
                if let Ok(r) = row_res {
                    count += 1;
                    if selected_row.is_none() {
                        selected_row = Some((i, r));
                    } else {
                        // 1/count probability to replace
                        if rng.gen_range(0..count) == 0 {
                            selected_row = Some((i, r));
                        }
                    }
                }
            }

            if selected_row.is_none() {
                pb.set_style(idle_style.clone());
                pb.set_message(format!("partition {:>4} (empty)", partition_idx));
                summary_bar.inc(1);
                // Give back the count since we didn't process one
                total_scanned.fetch_sub(1, Ordering::Relaxed);
                return;
            }

            pb.set_message(format!("partition {:>4} validating...", partition_idx));

            let (row_idx, row) = selected_row.unwrap();
            let key_str = extract_keys(&row, &key_fields);
            let key_display = if key_str.is_empty() {
                format!("row {}", row_idx)
            } else {
                key_str
            };

            // Serialize to JSON
            let json_val = match serde_json::to_value(&row) {
                Ok(v) => v,
                Err(e) => {
                    invalid_count.fetch_add(1, Ordering::Relaxed);
                    let inv = invalid_count.load(Ordering::Relaxed);
                    let val = valid_count.load(Ordering::Relaxed);
                    summary_bar.set_message(format!("{} ok, {} invalid", val, inv));
                    summary_bar.inc(1);
                    pb.set_style(idle_style.clone());
                    pb.set_message(format!("partition {:>4} ERROR: {}", partition_idx, e));
                    return;
                }
            };

            // Validate
            if self.validator.validate(&json_val).is_ok() {
                valid_count.fetch_add(1, Ordering::Relaxed);
                let inv = invalid_count.load(Ordering::Relaxed);
                let val = valid_count.load(Ordering::Relaxed);
                summary_bar.set_message(format!("{} ok, {} invalid", val, inv));

                results.lock().unwrap().push(format!(
                    "  OK  partition {:>4}  {}",
                    partition_idx, key_display
                ));

                // Show last validated key
                pb.set_style(idle_style.clone());
                pb.set_message(format!("OK  {}", key_display));
            } else {
                invalid_count.fetch_add(1, Ordering::Relaxed);
                let inv = invalid_count.load(Ordering::Relaxed);
                let val = valid_count.load(Ordering::Relaxed);
                summary_bar.set_message(format!("{} ok, {} invalid", val, inv));

                let errs: Vec<String> = self
                    .validator
                    .iter_errors(&json_val)
                    .take(2)
                    .map(|e| format!("{} at '{}'", e, e.instance_path))
                    .collect();

                let error_summary = if errs.len() == 1 {
                    errs[0].clone()
                } else if !errs.is_empty() {
                    format!("{} (+{} more)", errs[0], errs.len() - 1)
                } else {
                    "unknown error".to_string()
                };

                results.lock().unwrap().push(format!(
                    "  FAIL partition {:>4}  {}  [{}]",
                    partition_idx, key_display, error_summary
                ));

                let mut err_lock = errors.lock().unwrap();
                for err in errs {
                    if err_lock.len() < 10 {
                        err_lock.push(format!(
                            "Partition {}, Row {}: {}",
                            partition_idx, row_idx, err
                        ));
                    }
                }

                // Show last validated key with error
                pb.set_style(idle_style.clone());
                pb.set_message(format!("FAIL {}  [{}]", key_display, error_summary));

                if fail_fast {
                    should_stop.store(true, Ordering::Relaxed);
                }
            }

            summary_bar.inc(1);
        });

        // Clear worker bars and summary
        for pb in worker_bars.iter() {
            pb.finish_and_clear();
        }
        summary_bar.finish_and_clear();

        // Print collected results
        println!("Results:");
        for line in results.lock().unwrap().iter() {
            println!("{}", line);
        }
        println!();

        // Build final report
        let final_report = ValidationReport {
            scanned_count: total_scanned.load(Ordering::Relaxed),
            valid_count: valid_count.load(Ordering::Relaxed),
            invalid_count: invalid_count.load(Ordering::Relaxed),
            errors: errors.lock().unwrap().clone(),
        };

        Ok(final_report)
    }
}

/// Generates JSON schemas from Hail table VType definitions
pub struct SchemaGenerator;

impl SchemaGenerator {
    /// Generate a JSON schema from a Hail table's VType string
    pub fn from_vtype(vtype: &str, title: Option<&str>) -> Result<Value> {
        let parsed = Self::parse_type(vtype)?;
        let mut schema = Self::type_to_schema(&parsed);

        // Add schema metadata
        if let Value::Object(ref mut map) = schema {
            map.insert(
                "$schema".to_string(),
                json!("http://json-schema.org/draft-07/schema#"),
            );
            if let Some(t) = title {
                map.insert("title".to_string(), json!(t));
            }
        }

        Ok(schema)
    }

    /// Generate a JSON schema from a QueryEngine
    ///
    /// For Hail Tables, uses the VType from the codec spec.
    /// For VCF files, generates schema from the EncodedType.
    pub fn from_engine(engine: &QueryEngine, title: Option<&str>) -> Result<Value> {
        if let Some(rvd_spec) = engine.rvd_spec() {
            // Hail Table - use VType string
            let vtype = &rvd_spec.codec_spec.v_type;
            Self::from_vtype(vtype, title)
        } else {
            // VCF or other source - generate from EncodedType
            Self::from_encoded_type(engine.row_type(), title)
        }
    }

    /// Generate a JSON schema from an EncodedType
    pub fn from_encoded_type(
        row_type: &crate::codec::EncodedType,
        title: Option<&str>,
    ) -> Result<Value> {
        use crate::codec::EncodedType;

        fn type_to_schema(t: &EncodedType) -> Value {
            match t {
                EncodedType::EBoolean { required } => {
                    if *required {
                        json!({"type": "boolean"})
                    } else {
                        json!({"type": ["boolean", "null"]})
                    }
                }
                EncodedType::EInt32 { required } | EncodedType::EInt64 { required } => {
                    if *required {
                        json!({"type": "integer"})
                    } else {
                        json!({"type": ["integer", "null"]})
                    }
                }
                EncodedType::EFloat32 { required } | EncodedType::EFloat64 { required } => {
                    if *required {
                        json!({"type": "number"})
                    } else {
                        json!({"type": ["number", "null"]})
                    }
                }
                EncodedType::EBinary { required } => {
                    if *required {
                        json!({"type": "string"})
                    } else {
                        json!({"type": ["string", "null"]})
                    }
                }
                EncodedType::EArray { required, element } => {
                    let items = type_to_schema(element);
                    if *required {
                        json!({"type": "array", "items": items})
                    } else {
                        json!({"type": ["array", "null"], "items": items})
                    }
                }
                EncodedType::EBaseStruct { required, fields } => {
                    let mut properties = Map::new();
                    let mut required_fields = Vec::new();
                    for field in fields {
                        properties.insert(field.name.clone(), type_to_schema(&field.encoded_type));
                        // Check if field type is required
                        if field.encoded_type.is_required() {
                            required_fields.push(Value::String(field.name.clone()));
                        }
                    }
                    let type_val = if *required {
                        json!("object")
                    } else {
                        json!(["object", "null"])
                    };
                    let mut obj = json!({
                        "type": type_val,
                        "properties": properties,
                        "additionalProperties": false
                    });
                    if !required_fields.is_empty() {
                        obj["required"] = Value::Array(required_fields);
                    }
                    obj
                }
            }
        }

        let mut schema = type_to_schema(row_type);
        schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        if let Some(t) = title {
            schema["title"] = json!(t);
        }
        Ok(schema)
    }

    /// Write schema to a file
    pub fn write_to_file<P: AsRef<Path>>(schema: &Value, path: P) -> Result<()> {
        let mut file = File::create(path)?;
        let json_str = serde_json::to_string_pretty(schema)?;
        file.write_all(json_str.as_bytes())?;
        Ok(())
    }

    /// Parse a Hail VType string into a ParsedType
    fn parse_type(s: &str) -> Result<ParsedType> {
        let s = s.trim();

        // Handle nullable types (no + prefix means nullable in the EType,
        // but VType doesn't have this - all VType fields can be null unless required)

        if s.starts_with("Struct{") && s.ends_with('}') {
            let inner = &s[7..s.len() - 1];
            let fields = Self::parse_struct_fields(inner)?;
            Ok(ParsedType::Struct(fields))
        } else if s.starts_with("Array[") && s.ends_with(']') {
            let inner = &s[6..s.len() - 1];
            let elem_type = Self::parse_type(inner)?;
            Ok(ParsedType::Array(Box::new(elem_type)))
        } else if s.starts_with("Set[") && s.ends_with(']') {
            let inner = &s[4..s.len() - 1];
            let elem_type = Self::parse_type(inner)?;
            Ok(ParsedType::Array(Box::new(elem_type))) // Sets are arrays in JSON
        } else if s.starts_with("Dict[") && s.ends_with(']') {
            // Dict[K,V] - in JSON this becomes an object or array of key-value pairs
            // For simplicity, treat as object with additionalProperties
            let inner = &s[5..s.len() - 1];
            let parts: Vec<&str> = Self::split_type_args(inner);
            if parts.len() == 2 {
                let value_type = Self::parse_type(parts[1])?;
                Ok(ParsedType::Dict(Box::new(value_type)))
            } else {
                Ok(ParsedType::Dict(Box::new(ParsedType::Any)))
            }
        } else if s.starts_with("Locus(") && s.ends_with(')') {
            let rg = &s[6..s.len() - 1];
            Ok(ParsedType::Locus(rg.to_string()))
        } else if s.starts_with("Interval[") && s.ends_with(']') {
            let inner = &s[9..s.len() - 1];
            let point_type = Self::parse_type(inner)?;
            Ok(ParsedType::Interval(Box::new(point_type)))
        } else if s.starts_with("Tuple[") && s.ends_with(']') {
            let inner = &s[6..s.len() - 1];
            let parts = Self::split_type_args(inner);
            let types: Result<Vec<_>> = parts.iter().map(|p| Self::parse_type(p)).collect();
            Ok(ParsedType::Tuple(types?))
        } else {
            // Primitive types
            match s {
                "Int32" | "int32" => Ok(ParsedType::Int32),
                "Int64" | "int64" => Ok(ParsedType::Int64),
                "Float32" | "float32" => Ok(ParsedType::Float32),
                "Float64" | "float64" => Ok(ParsedType::Float64),
                "Boolean" | "bool" => Ok(ParsedType::Boolean),
                "String" | "str" => Ok(ParsedType::String),
                "Binary" => Ok(ParsedType::Binary),
                "Call" => Ok(ParsedType::Call),
                _ => {
                    // Unknown type - treat as any
                    Ok(ParsedType::Any)
                }
            }
        }
    }

    /// Parse struct fields from the inner part of Struct{...}
    fn parse_struct_fields(s: &str) -> Result<Vec<(String, ParsedType)>> {
        let mut fields = Vec::new();
        let mut depth = 0;
        let mut current_start = 0;
        let chars: Vec<char> = s.chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '{' | '[' | '(' => depth += 1,
                '}' | ']' | ')' => depth -= 1,
                ',' if depth == 0 => {
                    let field_str: String = chars[current_start..i].iter().collect();
                    if let Some(field) = Self::parse_field(&field_str)? {
                        fields.push(field);
                    }
                    current_start = i + 1;
                }
                _ => {}
            }
        }

        // Don't forget the last field
        if current_start < chars.len() {
            let field_str: String = chars[current_start..].iter().collect();
            if let Some(field) = Self::parse_field(&field_str)? {
                fields.push(field);
            }
        }

        Ok(fields)
    }

    /// Parse a single field "name:Type"
    fn parse_field(s: &str) -> Result<Option<(String, ParsedType)>> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(None);
        }

        if let Some(colon_pos) = s.find(':') {
            let name = s[..colon_pos].trim().to_string();
            let type_str = s[colon_pos + 1..].trim();
            let parsed_type = Self::parse_type(type_str)?;
            Ok(Some((name, parsed_type)))
        } else {
            Err(HailError::InvalidFormat(format!(
                "Invalid field format: {}",
                s
            )))
        }
    }

    /// Split type arguments respecting nesting
    fn split_type_args(s: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;
        let bytes = s.as_bytes();

        for (i, &b) in bytes.iter().enumerate() {
            match b {
                b'{' | b'[' | b'(' => depth += 1,
                b'}' | b']' | b')' => depth -= 1,
                b',' if depth == 0 => {
                    result.push(s[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }

        if start < s.len() {
            result.push(s[start..].trim());
        }

        result
    }

    /// Convert a ParsedType to a JSON schema Value
    fn type_to_schema(t: &ParsedType) -> Value {
        match t {
            ParsedType::Int32 | ParsedType::Int64 => json!({"type": ["integer", "null"]}),
            ParsedType::Float32 | ParsedType::Float64 => json!({"type": ["number", "null"]}),
            ParsedType::Boolean => json!({"type": ["boolean", "null"]}),
            ParsedType::String | ParsedType::Binary => json!({"type": ["string", "null"]}),
            ParsedType::Call => json!({
                "type": ["integer", "null"],
                "description": "Genotype call (encoded as integer)"
            }),
            ParsedType::Array(elem) => {
                let elem_schema = Self::type_to_schema(elem);
                json!({
                    "type": ["array", "null"],
                    "items": elem_schema
                })
            }
            ParsedType::Dict(value_type) => {
                // Hail serializes Dict as array of {key, value} objects
                let value_schema = Self::type_to_schema(value_type);
                json!({
                    "type": ["array", "null"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {},
                            "value": value_schema
                        },
                        "required": ["key", "value"]
                    }
                })
            }
            ParsedType::Struct(fields) => {
                let mut properties = Map::new();
                let mut required_fields = Vec::new();

                for (name, field_type) in fields {
                    properties.insert(name.clone(), Self::type_to_schema(field_type));
                    required_fields.push(Value::String(name.clone()));
                }

                json!({
                    "type": ["object", "null"],
                    "properties": properties,
                    "required": required_fields,
                    "additionalProperties": false
                })
            }
            ParsedType::Locus(rg) => {
                // Locus is a struct with contig and position
                let contigs = Self::get_contigs_for_reference(rg);
                json!({
                    "type": ["object", "null"],
                    "description": format!("Genomic locus ({})", rg),
                    "properties": {
                        "contig": {
                            "type": "string",
                            "enum": contigs
                        },
                        "position": {
                            "type": "integer",
                            "minimum": 1
                        }
                    },
                    "required": ["contig", "position"]
                })
            }
            ParsedType::Interval(point_type) => {
                let point_schema = Self::type_to_schema(point_type);
                json!({
                    "type": ["object", "null"],
                    "description": "Genomic interval",
                    "properties": {
                        "start": point_schema.clone(),
                        "end": point_schema,
                        "includesStart": {"type": "boolean"},
                        "includesEnd": {"type": "boolean"}
                    }
                })
            }
            ParsedType::Tuple(types) => {
                let items: Vec<Value> = types.iter().map(|t| Self::type_to_schema(t)).collect();
                json!({
                    "type": ["array", "null"],
                    "items": items,
                    "minItems": types.len(),
                    "maxItems": types.len()
                })
            }
            ParsedType::Any => json!({}),
        }
    }

    /// Get chromosome names for a reference genome
    fn get_contigs_for_reference(rg: &str) -> Vec<&'static str> {
        match rg {
            "GRCh38" | "hg38" => vec![
                "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
                "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",
                "chr20", "chr21", "chr22", "chrX", "chrY", "chrM",
            ],
            "GRCh37" | "hg19" => vec![
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "X", "Y", "MT",
            ],
            _ => vec![], // Unknown reference genome - no enum constraint
        }
    }
}

/// Intermediate representation of a parsed Hail type
#[derive(Debug, Clone)]
enum ParsedType {
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    String,
    Binary,
    Call,
    Array(Box<ParsedType>),
    Dict(Box<ParsedType>),
    Struct(Vec<(String, ParsedType)>),
    Locus(String),
    Interval(Box<ParsedType>),
    Tuple(Vec<ParsedType>),
    Any,
}
