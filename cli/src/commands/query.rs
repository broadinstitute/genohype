//! Query command for streaming data from tables.

use crate::cli::QueryArgs;
use crate::commands::utils::{parse_interval_list, parse_where_condition, progress_style_spinner};
use genohype_core::codec::EncodedValue;
use genohype_core::metadata::CacheOptions;
use genohype_core::projection::Projection;
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use genohype_core::summary::StatsAccumulator;
use genohype_core::Result;
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use std::io::Write;
use std::sync::Arc;

pub fn run_query(args: QueryArgs, cache_opts: Option<CacheOptions>) -> Result<()> {
    let table_path = &args.table;
    let mut key_filters: Vec<(String, String)> = Vec::new();
    let mut where_filters: Vec<KeyRange> = Vec::new();

    // Parse --key if provided
    if let Some(ref key_str) = args.key {
        if let Some((field, value)) = parse_equality(key_str) {
            key_filters.push((field, value));
        } else {
            eprintln!(
                "{} Invalid --key format. Use field=value",
                "Error:".red().bold()
            );
            std::process::exit(1);
        }
    }

    // Parse --where clauses
    for clause in &args.where_clauses {
        if let Some(range) = parse_where_condition(clause) {
            where_filters.push(range);
        } else {
            eprintln!(
                "{} Invalid --where format: {}",
                "Error:".red().bold(),
                clause
            );
            std::process::exit(1);
        }
    }

    // Parse interval list
    let intervals = parse_interval_list(args.intervals_file.as_deref(), &args.interval)?;

    // Open output writer early to fail fast on permission errors
    let mut writer: Box<dyn Write> = match args.output.as_deref() {
        Some("-") | None => Box::new(std::io::BufWriter::new(std::io::stdout().lock())),
        Some(path) => Box::new(std::io::BufWriter::new(std::fs::File::create(path)?)),
    };

    // Open the table (supports both local and cloud paths)
    let show_spinner = !args.json && !args.stats_json;
    let spinner = if show_spinner {
        let s = ProgressBar::new_spinner();
        s.set_style(progress_style_spinner());
        s.set_message("Loading table metadata...");
        s.enable_steady_tick(std::time::Duration::from_millis(100));
        Some(s)
    } else {
        None
    };

    let load_start = std::time::Instant::now();
    let mut engine = QueryEngine::open_path_cached(table_path, cache_opts)?;
    let load_elapsed = load_start.elapsed();

    let num_partitions = engine.num_partitions();

    if let Some(s) = spinner {
        s.finish_and_clear();
    }
    if !args.stats_json {
        eprintln!(
            "{} Loaded {} partitions in {:.1}s",
            "✓".green(),
            num_partitions.to_string().bright_white(),
            load_elapsed.as_secs_f64()
        );
    }

    // Parse and validate projection
    let projection = if let Some(ref fields_str) = args.fields {
        let proj = Projection::from_fields_str(fields_str).unwrap_or_else(|e| {
            eprintln!("{} Invalid --fields: {}", "Error:".red().bold(), e);
            std::process::exit(1);
        });
        proj.validate(engine.row_type()).unwrap_or_else(|e| {
            eprintln!("{} {}", "Error:".red().bold(), e);
            std::process::exit(1);
        });
        Some(proj)
    } else if let Some(ref exclude_str) = args.exclude {
        let proj = Projection::from_exclude_str(exclude_str).unwrap_or_else(|e| {
            eprintln!("{} Invalid --exclude: {}", "Error:".red().bold(), e);
            std::process::exit(1);
        });
        proj.validate(engine.row_type()).unwrap_or_else(|e| {
            eprintln!("{} {}", "Error:".red().bold(), e);
            std::process::exit(1);
        });
        Some(proj)
    } else {
        None
    };

    // Show filter info
    if !args.stats_json {
        if let Some(ref ivl) = intervals {
            eprintln!(
                "{} {} interval(s)",
                "✓".green(),
                ivl.len().to_string().bright_white()
            );
        }
        if !where_filters.is_empty() {
            eprintln!(
                "{} {} filter(s): {:?}",
                "✓".green(),
                where_filters.len().to_string().bright_white(),
                where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>()
            );
        }
    }

    let stats_mode = args.stats_json;
    let summary_mode = args.summary;

    // Execute query
    if !key_filters.is_empty() {
        // Point lookup using --key
        let key = build_key_from_filters(&key_filters, engine.key_fields())?;
        let _ = writeln!(writer, "{} {:?}", "Point lookup for key:".cyan(), key_filters);

        match engine.lookup(&key)? {
            Some(row) => {
                // Apply interval filter to lookup result if specified
                if let Some(ref ivl) = intervals {
                    if !row_matches_intervals(&row, ivl) {
                        let _ = writeln!(writer);
                        let _ = writeln!(
                            writer,
                            "{}",
                            "Row found but filtered out by interval list.".yellow()
                        );
                        return Ok(());
                    }
                }
                let row = if let Some(ref proj) = projection {
                    proj.apply(&row)
                } else {
                    row
                };
                let _ = writeln!(writer);
                let _ = writeln!(writer, "{}", "Found row:".green().bold());
                write_row(&mut writer, &row, args.json)?;
            }
            None => {
                let _ = writeln!(writer);
                let _ = writeln!(writer, "{}", "No matching row found.".yellow());
            }
        }
    } else {
        // Range query using --where (or full scan if no filters)
        if !args.json && !stats_mode && !summary_mode && where_filters.is_empty() && intervals.is_none() {
            eprintln!(
                "{}",
                "Warning: No filters specified. This may scan all partitions.".yellow()
            );
        }

        // Build Level 2 decode-time projection for --fields mode.
        // For --exclude, we skip Level 2 and rely on Level 1 only.
        // The decode projection must include filter-dependent fields (locus for intervals).
        let decode_projection = match &projection {
            Some(Projection::Fields(tree)) => {
                let mut decode_tree = tree.clone();
                // Ensure locus is decoded when interval filtering is active
                if intervals.is_some() {
                    decode_tree.ensure_field("locus");
                }
                Some(Arc::new(decode_tree))
            }
            _ => None, // --exclude or no projection: full decode
        };

        // Use streaming query with intervals and optional decode projection
        let iterator = engine.query_iter_with_projection(&where_filters, intervals, decode_projection)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        // Progress bar (stderr only, hidden in --json, --stats-json, and --summary modes)
        let pb = if !args.json && !stats_mode && !summary_mode {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} Scanning partitions... {msg}")
                    .unwrap(),
            );
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            Some(pb)
        } else {
            None
        };

        let mut accumulator = if summary_mode {
            Some(StatsAccumulator::new())
        } else {
            None
        };

        let consume_start = std::time::Instant::now();
        let mut count = 0;
        let mut _serialize_ns: u64 = 0;
        let mut broken_pipe = false;
        for row_result in iterator {
            let row = row_result?;
            count += 1;

            // Update progress bar periodically
            if let Some(ref pb) = pb {
                if count % 100 == 0 {
                    pb.set_message(format!("{} rows", count));
                }
            }

            if stats_mode {
                // In stats mode, consume rows but don't print them
                continue;
            }

            if summary_mode {
                let row = if let Some(ref proj) = projection {
                    proj.apply(&row)
                } else {
                    row
                };
                accumulator.as_mut().unwrap().process_row(&row);
                continue;
            }

            if !args.json {
                let _ = writeln!(writer);
                let _ = writeln!(
                    writer,
                    "{} {} {}",
                    "---".dimmed(),
                    format!("Row {}", count).cyan(),
                    "---".dimmed()
                );
            }
            let row = if let Some(ref proj) = projection {
                proj.apply(&row)
            } else {
                row
            };
            let ser_start = std::time::Instant::now();
            if let Err(e) = write_row(&mut writer, &row, args.json) {
                if e.kind() == std::io::ErrorKind::BrokenPipe {
                    broken_pipe = true;
                    break;
                }
                return Err(e.into());
            }
            _serialize_ns += ser_start.elapsed().as_nanos() as u64;
        }
        let consume_elapsed = consume_start.elapsed();

        if let Some(pb) = pb {
            pb.finish_and_clear();
        }

        // Flush writer (ignore BrokenPipe on flush too)
        if !broken_pipe {
            if let Err(e) = writer.flush() {
                if e.kind() == std::io::ErrorKind::BrokenPipe {
                    broken_pipe = true;
                } else {
                    return Err(e.into());
                }
            }
        }

        if stats_mode {
            let total_time_ms = (load_elapsed + consume_elapsed).as_secs_f64() * 1000.0;
            let stats = serde_json::json!({
                "rows": count,
                "partitions": num_partitions,
                "metadata_load_time_ms": load_elapsed.as_secs_f64() * 1000.0,
                "query_execution_time_ms": consume_elapsed.as_secs_f64() * 1000.0,
                "total_time_ms": total_time_ms,
            });
            println!("{}", stats);
        } else if summary_mode {
            let acc = accumulator.unwrap();
            if args.json {
                // Machine-readable JSON summary
                let mut fields_json = serde_json::Map::new();
                for key in acc.sorted_fields() {
                    let s = &acc.stats[key];
                    fields_json.insert(
                        key.clone(),
                        serde_json::json!({
                            "count": s.count,
                            "null_count": s.null_count,
                            "min": s.min,
                            "max": s.max,
                            "distinct_sample": s.distinct_sample,
                        }),
                    );
                }
                let output = serde_json::json!({
                    "rows": count,
                    "time_secs": consume_elapsed.as_secs_f64(),
                    "fields": fields_json,
                });
                println!("{}", serde_json::to_string_pretty(&output).unwrap());
            } else {
                // Pretty-printed summary
                eprintln!(
                    "{} {} rows in {:.1}s",
                    "✓".green(),
                    count.to_string().bright_white(),
                    consume_elapsed.as_secs_f64()
                );
                println!();
                println!(
                    "{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
                    "Field".cyan(),
                    "Count".cyan(),
                    "Nulls".cyan(),
                    "Min".cyan(),
                    "Max".cyan()
                );
                println!("{}", "-".repeat(120).dimmed());

                for key in acc.sorted_fields() {
                    let s = &acc.stats[key];
                    let field_display = if key.len() > 48 {
                        format!("...{}", &key[key.len() - 45..])
                    } else {
                        key.clone()
                    };
                    let min_display = match &s.min {
                        Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
                        Some(m) => m.clone(),
                        None => String::new(),
                    };
                    let max_display = match &s.max {
                        Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
                        Some(m) => m.clone(),
                        None => String::new(),
                    };
                    println!(
                        "{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
                        field_display, s.count, s.null_count, min_display, max_display
                    );
                }
            }
        } else {
            // Standard row output mode — print summary to stderr
            let output_suffix = match args.output.as_deref() {
                Some("-") | None => String::new(),
                Some(path) => format!(" → {}", path),
            };
            eprintln!(
                "{} {} rows in {:.1}s{}",
                "✓".green(),
                count.to_string().bright_white(),
                consume_elapsed.as_secs_f64(),
                output_suffix
            );
        }
    }

    Ok(())
}

/// Check if a row's locus matches any interval (used for point lookup filtering)
fn row_matches_intervals(row: &EncodedValue, intervals: &IntervalList) -> bool {
    // Extract locus.contig and locus.position from the row
    if let EncodedValue::Struct(fields) = row {
        if let Some((_, locus)) = fields.iter().find(|(name, _)| name == "locus") {
            if let EncodedValue::Struct(locus_fields) = locus {
                let contig = locus_fields
                    .iter()
                    .find(|(name, _)| name == "contig")
                    .map(|(_, v)| v);
                let position = locus_fields
                    .iter()
                    .find(|(name, _)| name == "position")
                    .map(|(_, v)| v);

                if let (Some(EncodedValue::Binary(c)), Some(EncodedValue::Int32(p))) =
                    (contig, position)
                {
                    let contig_str = String::from_utf8_lossy(c);
                    return intervals.contains(&contig_str, *p);
                }
            }
        }
    }
    // If we can't extract locus, pass through
    true
}

fn parse_equality(s: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

fn build_key_from_filters(
    filters: &[(String, String)],
    key_fields: &[String],
) -> Result<EncodedValue> {
    let mut fields = Vec::new();

    for key_field in key_fields {
        if let Some((_, value)) = filters.iter().find(|(f, _)| f == key_field) {
            // Use heuristics to determine if value should be an integer:
            // - If it looks like a pure integer (no letters), parse as Int32
            // - Otherwise treat as string/binary
            let encoded = if value.chars().all(|c| c.is_ascii_digit() || c == '-')
                && !value.is_empty()
                && value.parse::<i32>().is_ok()
                && !looks_like_string_field(key_field)
            {
                // Parse as integer
                EncodedValue::Int32(value.parse().unwrap())
            } else {
                // Keep as string/binary
                EncodedValue::Binary(value.as_bytes().to_vec())
            };
            fields.push((key_field.clone(), encoded));
        }
    }

    Ok(EncodedValue::Struct(fields))
}

fn looks_like_string_field(field_name: &str) -> bool {
    // Fields that are commonly strings even if they contain only digits
    let string_fields = [
        "chrom",
        "chromosome",
        "contig",
        "gene_id",
        "transcript_id",
        "id",
    ];
    string_fields
        .iter()
        .any(|&s| field_name.to_lowercase().contains(s))
}

/// Write a row to the writer, returning io::Result to allow BrokenPipe detection.
fn write_row(writer: &mut dyn Write, row: &EncodedValue, json_output: bool) -> std::io::Result<()> {
    if json_output {
        writeln!(writer, "{}", encoded_value_to_json(row))
    } else {
        write_encoded_value(writer, row, 0)
    }
}

fn write_encoded_value(writer: &mut dyn Write, value: &EncodedValue, indent: usize) -> std::io::Result<()> {
    let prefix = "  ".repeat(indent);
    match value {
        EncodedValue::Null => writeln!(writer, "{}{}", prefix, "null".dimmed()),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            writeln!(writer, "{}\"{}\"", prefix, s.bright_white())
        }
        EncodedValue::Int32(i) => writeln!(writer, "{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Int64(i) => writeln!(writer, "{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Float32(f) => writeln!(writer, "{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Float64(f) => writeln!(writer, "{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Boolean(b) => {
            if *b {
                writeln!(writer, "{}{}", prefix, "true".green())
            } else {
                writeln!(writer, "{}{}", prefix, "false".yellow())
            }
        }
        EncodedValue::Struct(fields) => {
            for (name, val) in fields {
                write!(writer, "{}{}: ", prefix, name.green())?;
                match val {
                    EncodedValue::Struct(_) | EncodedValue::Array(_) => {
                        writeln!(writer)?;
                        write_encoded_value(writer, val, indent + 1)?;
                    }
                    _ => write_encoded_value(writer, val, 0)?,
                }
            }
            Ok(())
        }
        EncodedValue::Array(elements) => {
            writeln!(writer, "{}[", prefix)?;
            for elem in elements {
                write_encoded_value(writer, elem, indent + 1)?;
            }
            writeln!(writer, "{}]", prefix)
        }
    }
}

fn encoded_value_to_json(value: &EncodedValue) -> String {
    match value {
        EncodedValue::Null => "null".to_string(),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            serde_json::to_string(s.as_ref()).unwrap_or_else(|_| format!("\"{}\"", s))
        }
        EncodedValue::Int32(i) => i.to_string(),
        EncodedValue::Int64(i) => i.to_string(),
        EncodedValue::Float32(f) => f.to_string(),
        EncodedValue::Float64(f) => f.to_string(),
        EncodedValue::Boolean(b) => b.to_string(),
        EncodedValue::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|(name, val)| format!("\"{}\":{}", name, encoded_value_to_json(val)))
                .collect();
            format!("{{{}}}", field_strs.join(","))
        }
        EncodedValue::Array(elements) => {
            let elem_strs: Vec<String> = elements.iter().map(encoded_value_to_json).collect();
            format!("[{}]", elem_strs.join(","))
        }
    }
}
