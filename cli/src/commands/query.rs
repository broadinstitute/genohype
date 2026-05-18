//! Query command for streaming data from tables.

use crate::cli::QueryArgs;
use crate::commands::utils::{parse_interval_list, parse_where_condition, progress_style_spinner};
use genohype_core::codec::EncodedValue;
use genohype_core::query::{IntervalList, KeyRange, QueryEngine};
use genohype_core::Result;
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;

pub fn run_query(args: QueryArgs) -> Result<()> {
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

    // Open the table (supports both local and cloud paths)
    let show_spinner = !args.json;
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
    let mut engine = QueryEngine::open_path(table_path)?;
    let load_elapsed = load_start.elapsed();

    if let Some(s) = spinner {
        s.finish_and_clear();
    }
    eprintln!(
        "{} Loaded {} partitions in {:.1}s",
        "✓".green(),
        engine.num_partitions().to_string().bright_white(),
        load_elapsed.as_secs_f64()
    );

    // Show filter info
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

    // Execute query
    if !key_filters.is_empty() {
        // Point lookup using --key
        let key = build_key_from_filters(&key_filters, engine.key_fields())?;
        println!("{} {:?}", "Point lookup for key:".cyan(), key_filters);

        match engine.lookup(&key)? {
            Some(row) => {
                // Apply interval filter to lookup result if specified
                if let Some(ref ivl) = intervals {
                    if !row_matches_intervals(&row, ivl) {
                        println!();
                        println!(
                            "{}",
                            "Row found but filtered out by interval list.".yellow()
                        );
                        return Ok(());
                    }
                }
                println!();
                println!("{}", "Found row:".green().bold());
                print_row(&row, args.json)?;
            }
            None => {
                println!();
                println!("{}", "No matching row found.".yellow());
            }
        }
    } else {
        // Range query using --where (or full scan if no filters)
        if !args.json && where_filters.is_empty() && intervals.is_none() {
            eprintln!(
                "{}",
                "Warning: No filters specified. This may scan all partitions.".yellow()
            );
        }

        // Use streaming query with intervals for memory-efficient iteration
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        // Progress bar (stderr only, hidden in --json mode)
        let pb = if !args.json {
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

        let consume_start = std::time::Instant::now();
        let mut count = 0;
        let mut serialize_ns: u64 = 0;
        for row_result in iterator {
            let row = row_result?;
            count += 1;

            // Update progress bar periodically
            if let Some(ref pb) = pb {
                if count % 100 == 0 {
                    pb.set_message(format!("{} rows", count));
                }
            }

            if !args.json {
                println!();
                println!(
                    "{} {} {}",
                    "---".dimmed(),
                    format!("Row {}", count).cyan(),
                    "---".dimmed()
                );
            }
            let ser_start = std::time::Instant::now();
            print_row(&row, args.json)?;
            serialize_ns += ser_start.elapsed().as_nanos() as u64;
        }
        let consume_elapsed = consume_start.elapsed();

        if let Some(pb) = pb {
            pb.finish_and_clear();
        }

        eprintln!(
            "{} {} rows in {:.1}s",
            "✓".green(),
            count.to_string().bright_white(),
            consume_elapsed.as_secs_f64()
        );
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

fn print_row(row: &EncodedValue, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", encoded_value_to_json(row));
    } else {
        print_encoded_value(row, 0);
    }
    Ok(())
}

fn print_encoded_value(value: &EncodedValue, indent: usize) {
    let prefix = "  ".repeat(indent);
    match value {
        EncodedValue::Null => println!("{}{}", prefix, "null".dimmed()),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            println!("{}\"{}\"", prefix, s.bright_white())
        }
        EncodedValue::Int32(i) => println!("{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Int64(i) => println!("{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Float32(f) => println!("{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Float64(f) => println!("{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Boolean(b) => {
            if *b {
                println!("{}{}", prefix, "true".green());
            } else {
                println!("{}{}", prefix, "false".yellow());
            }
        }
        EncodedValue::Struct(fields) => {
            for (name, val) in fields {
                print!("{}{}: ", prefix, name.green());
                match val {
                    EncodedValue::Struct(_) | EncodedValue::Array(_) => {
                        println!();
                        print_encoded_value(val, indent + 1);
                    }
                    _ => print_encoded_value(val, 0),
                }
            }
        }
        EncodedValue::Array(elements) => {
            println!("{}[", prefix);
            for elem in elements {
                print_encoded_value(elem, indent + 1);
            }
            println!("{}]", prefix);
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
