//! ClickHouse export functionality
//!
//! This module provides functionality to export Hail tables to ClickHouse
//! using Parquet as an intermediate format.

use crate::codec::{EncodedField, EncodedType};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use thiserror::Error;

/// Errors that can occur during ClickHouse export
#[derive(Error, Debug)]
pub enum ClickHouseError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("ClickHouse error: {0}")]
    Query(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
}

pub type Result<T> = std::result::Result<T, ClickHouseError>;

/// Convert a Hail EncodedType to a ClickHouse type string
pub fn hail_type_to_clickhouse(etype: &EncodedType) -> Result<String> {
    // ClickHouse does not support Nullable for complex types (Tuple, Array)
    // Only primitive types can be wrapped in Nullable
    let (base_type, can_be_nullable) = match etype {
        EncodedType::EInt32 { .. } => ("Int32".to_string(), true),
        EncodedType::EInt64 { .. } => ("Int64".to_string(), true),
        EncodedType::EFloat32 { .. } => ("Float32".to_string(), true),
        EncodedType::EFloat64 { .. } => ("Float64".to_string(), true),
        EncodedType::EBoolean { .. } => ("Bool".to_string(), true),
        EncodedType::EBinary { .. } => ("String".to_string(), true),
        EncodedType::EArray { element, .. } => {
            let inner = hail_type_to_clickhouse(element)?;
            // Arrays cannot be Nullable in ClickHouse
            (format!("Array({})", inner), false)
        }
        EncodedType::EBaseStruct { fields, .. } => {
            let field_defs: Vec<String> = fields
                .iter()
                .map(|f| {
                    let ch_type = hail_type_to_clickhouse(&f.encoded_type)?;
                    Ok(format!("{} {}", f.name, ch_type))
                })
                .collect::<Result<Vec<_>>>()?;
            // Tuples cannot be Nullable in ClickHouse
            (format!("Tuple({})", field_defs.join(", ")), false)
        }
    };

    // Wrap in Nullable only if the type supports it and is not required
    if can_be_nullable && !etype.is_required() {
        Ok(format!("Nullable({})", base_type))
    } else {
        Ok(base_type)
    }
}

/// Generate a ClickHouse DDL field definition
fn field_to_ddl(field: &EncodedField) -> Result<String> {
    let ch_type = hail_type_to_clickhouse(&field.encoded_type)?;
    Ok(format!("    `{}` {}", field.name, ch_type))
}

/// Generate a CREATE TABLE DDL statement for ClickHouse
///
/// # Arguments
///
/// * `table_name` - Name of the table to create
/// * `schema` - The root EncodedType (must be a struct)
/// * `order_by` - Fields to use in ORDER BY clause (typically key fields)
///
/// # Returns
///
/// A CREATE TABLE IF NOT EXISTS statement
pub fn generate_create_table(
    table_name: &str,
    schema: &EncodedType,
    order_by: &[String],
) -> Result<String> {
    let fields = match schema {
        EncodedType::EBaseStruct { fields, .. } => fields,
        _ => {
            return Err(ClickHouseError::InvalidSchema(
                "Root schema must be a struct".to_string(),
            ))
        }
    };

    let field_defs: Vec<String> = fields
        .iter()
        .map(|f| field_to_ddl(f))
        .collect::<Result<Vec<_>>>()?;

    // Build ORDER BY clause
    let order_by_clause = if order_by.is_empty() {
        "tuple()".to_string()
    } else {
        format!(
            "({})",
            order_by
                .iter()
                .map(|f| format!("`{}`", f))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };

    // Use allow_nullable_key since Hail tables often have nullable key fields
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS `{}` (\n{}\n) ENGINE = MergeTree()\nORDER BY {}\nSETTINGS allow_nullable_key = 1",
        table_name,
        field_defs.join(",\n"),
        order_by_clause
    ))
}

/// ClickHouse HTTP client for executing queries and inserting data
#[derive(Clone)]
pub struct ClickHouseClient {
    client: reqwest::blocking::Client,
    base_url: String,
    auth: Option<(String, String)>,
}

impl ClickHouseClient {
    /// Create a new ClickHouse client
    ///
    /// # Arguments
    ///
    /// * `url` - ClickHouse HTTP interface URL. Supports credentials in URL:
    ///   - `http://localhost:8123` (no auth)
    ///   - `http://user:password@localhost:8123` (basic auth)
    pub fn new(url: &str) -> Self {
        // Parse URL to extract credentials if present
        let (base_url, auth) = if let Ok(parsed) = url::Url::parse(url) {
            let username = parsed.username();
            let password = parsed.password().unwrap_or("");

            if !username.is_empty() {
                // Rebuild URL without credentials
                let mut clean_url = parsed.clone();
                clean_url.set_username("").ok();
                clean_url.set_password(None).ok();
                let clean = clean_url.as_str().trim_end_matches('/').to_string();
                (clean, Some((username.to_string(), password.to_string())))
            } else {
                (url.trim_end_matches('/').to_string(), None)
            }
        } else {
            (url.trim_end_matches('/').to_string(), None)
        };

        Self {
            client: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_else(|_| reqwest::blocking::Client::new()),
            base_url,
            auth,
        }
    }

    /// Build a request with optional authentication
    fn request(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        let mut req = self.client.post(url);
        if let Some((user, pass)) = &self.auth {
            req = req.basic_auth(user, Some(pass));
        }
        req
    }

    /// Execute a DDL or query statement
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query to execute
    pub fn execute(&self, query: &str) -> Result<String> {
        let response = self
            .request(&self.base_url)
            .body(query.to_string())
            .send()?;

        let status = response.status();
        let text = response.text()?;

        if !status.is_success() {
            return Err(ClickHouseError::Query(text));
        }

        Ok(text)
    }

    /// Insert data from a Parquet file into a table
    ///
    /// # Arguments
    ///
    /// * `table_name` - Target table name
    /// * `parquet_path` - Path to the Parquet file to insert
    pub fn insert_parquet<P: AsRef<Path>>(&self, table_name: &str, parquet_path: P) -> Result<()> {
        // Read the Parquet file
        let mut file = File::open(parquet_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        self.insert_parquet_bytes(table_name, data)
    }

    /// Insert Parquet data from bytes into a table
    ///
    /// This is useful for streaming uploads where the Parquet data is already in memory,
    /// avoiding the need to write temporary files to disk.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Target table name
    /// * `data` - Parquet file contents as bytes
    pub fn insert_parquet_bytes(&self, table_name: &str, data: Vec<u8>) -> Result<()> {
        // Build the insert URL
        let url = format!(
            "{}/?input_format_parquet_max_block_size=8192&max_insert_block_size=8192&max_memory_usage=8000000000&query=INSERT%20INTO%20`{}`%20FORMAT%20Parquet",
            self.base_url, table_name
        );

        let response = self
            .request(&url)
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()?;

        let status = response.status();
        let text = response.text()?;

        if !status.is_success() {
            return Err(ClickHouseError::Query(text));
        }

        Ok(())
    }

    /// Clean up orphaned temporary tables created by failed/crashed workers.
    ///
    /// Finds tables matching `{target_table}_part_%` in the current database
    /// and drops them. This prevents storage leaks from workers that crashed
    /// before their `TempTableGuard` could run.
    pub fn cleanup_orphaned_temp_tables(&self, target_table: &str) -> Result<()> {
        let query = format!(
            "SELECT name FROM system.tables WHERE name LIKE '{}\\_part\\_%' AND database = currentDatabase() FORMAT JSON",
            target_table
        );

        let response = match self.execute(&query) {
            Ok(res) => res,
            Err(e) => {
                return Err(ClickHouseError::Query(format!(
                    "Failed to query orphaned tables: {}",
                    e
                )))
            }
        };

        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(data) = parsed.get("data").and_then(|d| d.as_array()) {
                for row in data {
                    if let Some(name) = row.get("name").and_then(|n| n.as_str()) {
                        let drop_sql = format!("DROP TABLE IF EXISTS `{}`", name);
                        let _ = self.execute(&drop_sql); // Best effort
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the row count of a table
    ///
    /// # Arguments
    ///
    /// * `table_name` - Table name to count
    pub fn count_rows(&self, table_name: &str) -> Result<u64> {
        let query = format!("SELECT count() FROM `{}`", table_name);
        let result = self.execute(&query)?;
        result
            .trim()
            .parse()
            .map_err(|e| ClickHouseError::Query(format!("Failed to parse count: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_mapping() {
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EInt32 { required: true }).unwrap(),
            "Int32"
        );
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EInt32 { required: false }).unwrap(),
            "Nullable(Int32)"
        );
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EInt64 { required: true }).unwrap(),
            "Int64"
        );
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EFloat32 { required: true }).unwrap(),
            "Float32"
        );
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EFloat64 { required: true }).unwrap(),
            "Float64"
        );
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EBoolean { required: true }).unwrap(),
            "Bool"
        );
        assert_eq!(
            hail_type_to_clickhouse(&EncodedType::EBinary { required: true }).unwrap(),
            "String"
        );
    }

    #[test]
    fn test_array_type_mapping() {
        let array_type = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EInt32 { required: true }),
        };
        assert_eq!(
            hail_type_to_clickhouse(&array_type).unwrap(),
            "Array(Int32)"
        );

        // Arrays cannot be Nullable in ClickHouse, even if not required
        let nullable_array = EncodedType::EArray {
            required: false,
            element: Box::new(EncodedType::EBinary { required: true }),
        };
        assert_eq!(
            hail_type_to_clickhouse(&nullable_array).unwrap(),
            "Array(String)"
        );
    }

    #[test]
    fn test_struct_type_mapping() {
        let struct_type = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "x".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "y".to_string(),
                    encoded_type: EncodedType::EBinary { required: false },
                    index: 1,
                },
            ],
        };
        assert_eq!(
            hail_type_to_clickhouse(&struct_type).unwrap(),
            "Tuple(x Int32, y Nullable(String))"
        );
    }

    #[test]
    fn test_generate_create_table() {
        let schema = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "id".to_string(),
                    encoded_type: EncodedType::EInt64 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "name".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 1,
                },
                EncodedField {
                    name: "score".to_string(),
                    encoded_type: EncodedType::EFloat64 { required: false },
                    index: 2,
                },
            ],
        };

        let ddl = generate_create_table("test_table", &schema, &["id".to_string()]).unwrap();

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(ddl.contains("`id` Int64"));
        assert!(ddl.contains("`name` String"));
        assert!(ddl.contains("`score` Nullable(Float64)"));
        assert!(ddl.contains("ORDER BY (`id`)"));
        assert!(ddl.contains("SETTINGS allow_nullable_key = 1"));
    }

    #[test]
    fn test_generate_create_table_no_keys() {
        let schema = EncodedType::EBaseStruct {
            required: true,
            fields: vec![EncodedField {
                name: "value".to_string(),
                encoded_type: EncodedType::EInt32 { required: true },
                index: 0,
            }],
        };

        let ddl = generate_create_table("no_key_table", &schema, &[]).unwrap();
        assert!(ddl.contains("ORDER BY tuple()"));
        assert!(ddl.contains("SETTINGS allow_nullable_key = 1"));
    }
}
