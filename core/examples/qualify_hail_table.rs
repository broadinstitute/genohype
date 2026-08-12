//! Bounded compatibility probe for an exact Hail Table.
//!
//! This helper reads metadata, at most two rows from one explicitly selected
//! partition, and the table's single globals value. It never scans subsequent
//! partitions and only writes when `--parquet` names a local output file.
//!
//! ```text
//! cargo run -p genohype-core --example qualify_hail_table -- \
//!   gs://bucket/exact-table.ht --partition 0 --limit 2 \
//!   [--parquet /tmp/two-rows.parquet]
//! ```

use genohype_core::codec::EncodedValue;
use genohype_core::parquet::ParquetWriter;
use genohype_core::query::QueryEngine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::env;
use std::path::PathBuf;

struct Args {
    table: String,
    partition: usize,
    limit: usize,
    parquet: Option<PathBuf>,
}

fn usage() -> &'static str {
    "qualify_hail_table TABLE [--partition N] [--limit 1|2] [--parquet LOCAL_PATH]"
}

fn parse_args() -> Result<Args, String> {
    let mut args = env::args().skip(1);
    let table = args.next().ok_or_else(|| usage().to_string())?;
    if table.starts_with('-') {
        return Err(usage().to_string());
    }

    let mut partition = 0;
    let mut limit = 2;
    let mut parquet = None;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {flag}; {}", usage()))?;
        match flag.as_str() {
            "--partition" => {
                partition = value
                    .parse()
                    .map_err(|_| format!("invalid partition {value:?}"))?;
            }
            "--limit" => {
                limit = value
                    .parse()
                    .map_err(|_| format!("invalid limit {value:?}"))?;
            }
            "--parquet" => {
                if value.contains("://") {
                    return Err("--parquet must be a local path".to_string());
                }
                parquet = Some(PathBuf::from(value));
            }
            _ => return Err(format!("unknown option {flag:?}; {}", usage())),
        }
    }
    if !(1..=2).contains(&limit) {
        return Err("--limit must be 1 or 2 (the helper is intentionally bounded)".to_string());
    }
    Ok(Args {
        table,
        partition,
        limit,
        parquet,
    })
}

// Type tags, fixed-width big-endian numbers, explicit lengths, original struct
// order, and raw binary bytes make this encoding injective over EncodedValue.
fn canonical_bytes(value: &EncodedValue, out: &mut Vec<u8>) {
    match value {
        EncodedValue::Null => out.push(0),
        EncodedValue::Boolean(value) => {
            out.push(1);
            out.push(u8::from(*value));
        }
        EncodedValue::Int32(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_be_bytes());
        }
        EncodedValue::Int64(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_be_bytes());
        }
        EncodedValue::Float32(value) => {
            out.push(4);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        EncodedValue::Float64(value) => {
            out.push(5);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        EncodedValue::Binary(value) => {
            out.push(6);
            put_len(value.len(), out);
            out.extend_from_slice(value);
        }
        EncodedValue::Array(values) => {
            out.push(7);
            put_len(values.len(), out);
            for value in values {
                canonical_bytes(value, out);
            }
        }
        EncodedValue::Struct(fields) => {
            out.push(8);
            put_len(fields.len(), out);
            for (name, value) in fields {
                put_len(name.len(), out);
                out.extend_from_slice(name.as_bytes());
                canonical_bytes(value, out);
            }
        }
    }
}

fn put_len(len: usize, out: &mut Vec<u8>) {
    out.extend_from_slice(&(len as u64).to_be_bytes());
}

fn digest(value: &EncodedValue) -> Value {
    let mut canonical = Vec::new();
    canonical_bytes(value, &mut canonical);
    json!({
        "canonical_encoding": "genohype-encoded-value-v1",
        "canonical_bytes": canonical.len(),
        "sha256": format!("{:x}", Sha256::digest(&canonical)),
    })
}

fn digest_sequence(values: &[EncodedValue]) -> Value {
    digest(&EncodedValue::Array(values.to_vec()))
}

fn key_for(row: &EncodedValue, key_fields: &[String]) -> Result<EncodedValue, String> {
    let fields = match row {
        EncodedValue::Struct(fields) => fields,
        _ => return Err("decoded row root is not a struct".to_string()),
    };
    let mut key = Vec::with_capacity(key_fields.len());
    for name in key_fields {
        let value = fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.clone())
            .ok_or_else(|| format!("key field {name:?} is absent from decoded row"))?;
        key.push((name.clone(), value));
    }
    Ok(EncodedValue::Struct(key))
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };

    let engine = match QueryEngine::open_path(&args.table) {
        Ok(engine) => engine,
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "ok": false,
                    "table": args.table,
                    "open_error": error.to_string(),
                }))
                .unwrap()
            );
            std::process::exit(1);
        }
    };

    let row_spec = engine.rvd_spec().expect("Hail source has RVD metadata");
    let globals_spec = engine.globals_rvd_spec();
    let key_fields = engine.key_fields().to_vec();
    let mut rows = Vec::new();
    let mut row_error = None;
    if args.partition >= engine.num_partitions() {
        row_error = Some(format!(
            "partition {} out of bounds for {} partitions",
            args.partition,
            engine.num_partitions()
        ));
    } else {
        match engine.scan_partition_iter(args.partition, &[]) {
            Ok(iter) => {
                for result in iter.take(args.limit) {
                    match result {
                        Ok(row) => rows.push(row),
                        Err(error) => {
                            row_error = Some(error.to_string());
                            break;
                        }
                    }
                }
            }
            Err(error) => row_error = Some(error.to_string()),
        }
    }

    let mut row_results = Vec::new();
    if row_error.is_none() {
        for (index, row) in rows.iter().enumerate() {
            match key_for(row, &key_fields) {
                Ok(key) => row_results.push(json!({
                    "index": index,
                    "row": digest(row),
                    "key": digest(&key),
                })),
                Err(error) => row_error = Some(error),
            }
        }
    }

    let (globals_result, globals_ok) = match engine.globals() {
        Ok(globals) => (json!({"ok": true, "digest": digest(&globals)}), true),
        Err(error) => (json!({"ok": false, "error": error.to_string()}), false),
    };

    let mut parquet_result = Value::Null;
    let mut parquet_ok = true;
    if let Some(path) = &args.parquet {
        let result = (|| -> genohype_core::Result<usize> {
            let path = path.to_string_lossy();
            let mut writer = ParquetWriter::new(&path, engine.row_type())?;
            for row in &rows {
                writer.write_row(row)?;
            }
            writer.close()
        })();
        match result {
            Ok(count) => {
                parquet_result = json!({"ok": true, "path": path, "rows": count});
            }
            Err(error) => {
                parquet_ok = false;
                parquet_result = json!({"ok": false, "path": path, "error": error.to_string()});
            }
        }
    }

    let table_metadata = engine.table_metadata();
    let globals_metadata = match globals_spec {
        Ok(Some(spec)) => json!({
            "e_type": spec.codec_spec.e_type,
            "v_type": spec.codec_spec.v_type,
        }),
        Ok(None) => Value::Null,
        Err(error) => json!({"metadata_error": error.to_string()}),
    };
    let rows_ok = row_error.is_none();
    let ok = rows_ok && globals_ok && parquet_ok;
    let output = json!({
        "ok": ok,
        "table": args.table,
        "hail_version": table_metadata.map(|m| m.hail_version.as_str()),
        "file_version": table_metadata.map(|m| m.file_version),
        "partition": args.partition,
        "limit": args.limit,
        "partition_count": engine.num_partitions(),
        "key_fields": key_fields,
        "rows_e_type": row_spec.codec_spec.e_type,
        "rows_v_type": row_spec.codec_spec.v_type,
        "globals_schema": globals_metadata,
        "rows": {
            "ok": rows_ok,
            "decoded": rows.len(),
            "error": row_error,
            "sequence_digest": digest_sequence(&rows),
            "items": row_results,
        },
        "globals": globals_result,
        "parquet": parquet_result,
    });
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
    if !ok {
        std::process::exit(1);
    }
}
