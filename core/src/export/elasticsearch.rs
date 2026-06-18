//! Elasticsearch export functionality for Hail tables.
//!
//! This is the genohype port of the gnomAD-browser data pipeline's Elasticsearch
//! loader (`data-pipeline/src/data_pipeline/helpers/elasticsearch_export.py`,
//! `_elasticsearch_mapping_for_hail_type` + `export_table_to_elasticsearch`). It
//! reproduces the **prod document shape and index mapping** so a benchmark ES arm
//! behaves like production, then streams documents to the `_bulk` endpoint over
//! HTTP (the same "buffer NDJSON, POST in batches" pattern as
//! [`crate::export::clickhouse`] and `gnomad-lr/src/clickhouse.rs`).
//!
//! # Prod fidelity (what we faithfully reproduce)
//!
//! Each ES document is `{ <index_fields…>, value: <whole row> }`:
//! - The configured **index fields** (e.g. `variant_id`, `locus`,
//!   `transcript_consequences.gene_id`) are *hoisted* to the top level and
//!   indexed. These are what the prod query DSL filters/sorts on
//!   (`graphql-api/.../gnomad-v4-variant-queries.ts`): `term locus.contig`,
//!   `range locus.position`, `term gene_id`, `term variant_id`,
//!   `sort locus.position`.
//! - The full row is nested under `value` with `enabled: false` — stored in
//!   `_source` (the browser reads `_source.value.*`) but **not** indexed, exactly
//!   like prod's `disable_fields=("value",)`.
//!
//! **Critical mapping detail:** `locus.position` must map to `integer` (Lucene BKD
//! trees) or region/gene range queries silently fall back to a full scan. We both
//! map `EInt32 → integer` generically *and* hard-map a locus-shaped struct to
//! `{contig: keyword, position: integer}` to match prod's `hl.tlocus` handling and
//! defend against an unexpected `int64` position encoding.
//!
//! # Schema width / projection
//!
//! The index *mapping* is always built from the full source row type (matching
//! prod, which maps `table.row_value.dtype`). The schema-width dimension
//! (`full` vs `browser-minimal`, see [`crate::projection::SchemaWidth`]) is
//! expressed in the **documents**: callers pass already-projected rows, so the
//! `value` `_source` carries only the projected fields. Because `value` is
//! `enabled: false`, the wider declared mapping is inert for the narrowed docs.

use crate::codec::{EncodedField, EncodedType, EncodedValue};
use crate::export::json::to_json_value;
use serde_json::{json, Map, Value};
use std::time::Duration;
use thiserror::Error;

/// Errors that can occur during Elasticsearch export.
#[derive(Error, Debug)]
pub enum ElasticsearchError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Elasticsearch API error: {0}")]
    Api(String),

    #[error("Bulk indexing error: {0}")]
    Bulk(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
}

pub type Result<T> = std::result::Result<T, ElasticsearchError>;

// ---------------------------------------------------------------------------
// Hail → Elasticsearch type mapping
// ---------------------------------------------------------------------------

/// Convert a Hail [`EncodedType`] to an Elasticsearch field mapping fragment.
///
/// Mirrors `_elasticsearch_mapping_for_hail_type` in the prod Python loader:
/// - `EInt32` → `integer`, `EInt64` → `long`
/// - `EFloat32` → `float`, `EFloat64` → `double`
/// - `EBinary` (Hail `tstr`) → `keyword`
/// - `EBoolean` → `boolean`
/// - `EArray<T>` → mapping of `T`; if `T` is a struct, the array becomes `nested`
///   (prod: a `tarray`/`tset` of `tstruct` gets `"type": "nested"`)
/// - `EBaseStruct` → `{ "properties": { … } }` (an `object`); a locus-shaped
///   struct is hard-mapped so `position` is always `integer`.
pub fn hail_type_to_es_mapping(etype: &EncodedType) -> Value {
    match etype {
        EncodedType::EInt32 { .. } => json!({ "type": "integer" }),
        EncodedType::EInt64 { .. } => json!({ "type": "long" }),
        EncodedType::EFloat32 { .. } => json!({ "type": "float" }),
        EncodedType::EFloat64 { .. } => json!({ "type": "double" }),
        EncodedType::EBoolean { .. } => json!({ "type": "boolean" }),
        EncodedType::EBinary { .. } => json!({ "type": "keyword" }),
        EncodedType::EArray { element, .. } => {
            let mut mapping = hail_type_to_es_mapping(element);
            // An array of structs is a `nested` type in Elasticsearch so each
            // element is queryable independently (prod does the same).
            if matches!(**element, EncodedType::EBaseStruct { .. }) {
                if let Value::Object(ref mut obj) = mapping {
                    obj.insert("type".to_string(), Value::String("nested".to_string()));
                }
            }
            mapping
        }
        EncodedType::EBaseStruct { fields, .. } => {
            // Hail `tlocus` is decoded as a `{contig, position}` struct. Prod maps
            // tlocus to `{type: object, properties: {contig: keyword, position:
            // integer}}`. Reproduce that exactly so range queries on
            // `locus.position` use BKD trees regardless of the position encoding.
            if is_locus_struct(fields) {
                return json!({
                    "type": "object",
                    "properties": {
                        "contig": { "type": "keyword" },
                        "position": { "type": "integer" },
                    }
                });
            }

            let mut properties = Map::new();
            for field in fields {
                properties.insert(
                    field.name.clone(),
                    hail_type_to_es_mapping(&field.encoded_type),
                );
            }
            json!({ "properties": properties })
        }
    }
}

/// True if a struct has exactly the locus shape (`contig` + `position`).
fn is_locus_struct(fields: &[EncodedField]) -> bool {
    fields.len() == 2
        && fields.iter().any(|f| f.name == "contig")
        && fields.iter().any(|f| f.name == "position")
}

// ---------------------------------------------------------------------------
// Index fields (hoisted/indexed top-level fields)
// ---------------------------------------------------------------------------

/// A field hoisted to the top level of the document and indexed.
///
/// Built from a dotted path (e.g. `transcript_consequences.gene_id`). The
/// top-level key is the last path segment (`gene_id`), matching the prod helper
/// `get_index_fields` which keys by `field.split(".")[-1]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexField {
    /// Path segments from the row root to the value (e.g. `["transcript_consequences", "gene_id"]`).
    pub path: Vec<String>,
    /// Top-level document key (the last path segment).
    pub key: String,
}

impl IndexField {
    /// Parse a dotted path into an [`IndexField`].
    pub fn parse(spec: &str) -> Self {
        let path: Vec<String> = spec
            .split('.')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let key = path.last().cloned().unwrap_or_default();
        IndexField { path, key }
    }
}

/// Parse a comma-separated list of dotted index-field paths.
pub fn parse_index_fields(spec: &str) -> Vec<IndexField> {
    spec.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(IndexField::parse)
        .collect()
}

/// Resolve the [`EncodedType`] of a dotted path within a schema.
///
/// Arrays are *transparent*: descending `transcript_consequences.gene_id` walks
/// into the array's struct element to find `gene_id`. Returns `None` if any
/// segment is absent (the loader is schema-tolerant and simply skips it).
fn resolve_field_type<'a>(schema: &'a EncodedType, path: &[String]) -> Option<&'a EncodedType> {
    if path.is_empty() {
        return Some(schema);
    }
    match schema {
        EncodedType::EBaseStruct { fields, .. } => {
            let field = fields.iter().find(|f| f.name == path[0])?;
            resolve_field_type(&field.encoded_type, &path[1..])
        }
        // Array is transparent for path resolution (a set/array of the leaf).
        EncodedType::EArray { element, .. } => resolve_field_type(element, path),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Index mapping + request body
// ---------------------------------------------------------------------------

/// Build the index `mappings` object: hoisted/indexed fields plus the disabled
/// `value` field carrying the full row in `_source`.
///
/// `row_schema` is the **full** source row type (matching prod, which always maps
/// the whole `row_value.dtype`). Index fields absent from the schema are skipped.
pub fn build_index_mapping(row_schema: &EncodedType, index_fields: &[IndexField]) -> Value {
    let mut properties = Map::new();

    for field in index_fields {
        if let Some(ty) = resolve_field_type(row_schema, &field.path) {
            properties.insert(field.key.clone(), hail_type_to_es_mapping(ty));
        }
        // Absent field: schema-tolerant skip (e.g. exomes vs genomes differences).
    }

    // `value` = the whole row, stored but not indexed (prod: disable_fields=("value",)).
    let mut value_mapping = hail_type_to_es_mapping(row_schema);
    if let Value::Object(ref mut obj) = value_mapping {
        obj.insert("enabled".to_string(), Value::Bool(false));
    }
    properties.insert("value".to_string(), value_mapping);

    json!({ "properties": properties })
}

/// Build the full index-creation request body (mappings + settings).
///
/// Settings mirror prod (`export_table_to_elasticsearch`): `best_compression`,
/// `number_of_replicas: 0`, `refresh_interval: -1` (disabled during bulk load),
/// and `number_of_shards = num_shards`. For a single benchmark VM `num_shards`
/// should equal the VM's vCPU count (one Lucene shard per core), not prod's 48
/// (which assumes a multi-node cluster).
pub fn build_request_body(
    row_schema: &EncodedType,
    index_fields: &[IndexField],
    num_shards: usize,
) -> Value {
    let mapping = build_index_mapping(row_schema, index_fields);
    json!({
        "mappings": mapping,
        "settings": {
            "index.codec": "best_compression",
            "index.mapping.total_fields.limit": 10000,
            "index.number_of_replicas": 0,
            "index.number_of_shards": num_shards,
            "index.refresh_interval": -1,
        }
    })
}

// ---------------------------------------------------------------------------
// Document construction
// ---------------------------------------------------------------------------

/// Build a single ES document `{ <hoisted index fields…>, value: <row> }` from a
/// decoded row.
///
/// Hoisted values are extracted with array-transparent path navigation: a path
/// that crosses an array (e.g. `transcript_consequences.gene_id`) yields the
/// deduplicated set of leaf values across all elements, matching prod's
/// `hl.set(...)` over collection fields.
pub fn build_document(row: &EncodedValue, index_fields: &[IndexField]) -> Value {
    let mut doc = Map::new();
    for field in index_fields {
        doc.insert(field.key.clone(), extract_path_value(row, &field.path));
    }
    doc.insert("value".to_string(), to_json_value(row));
    Value::Object(doc)
}

/// Navigate a dotted path within an [`EncodedValue`], treating arrays as
/// transparent and collecting a deduplicated set when a path crosses an array.
fn extract_path_value(value: &EncodedValue, path: &[String]) -> Value {
    if path.is_empty() {
        return to_json_value(value);
    }
    match value {
        EncodedValue::Struct(fields) => match fields.iter().find(|(k, _)| k == &path[0]) {
            Some((_, v)) => extract_path_value(v, &path[1..]),
            None => Value::Null,
        },
        EncodedValue::Array(items) => {
            // Array is transparent: apply the remaining path to each element and
            // flatten into a deduplicated array (set semantics, like hl.set).
            let mut out: Vec<Value> = Vec::new();
            for item in items {
                push_flat_unique(&mut out, extract_path_value(item, path));
            }
            Value::Array(out)
        }
        EncodedValue::Null => Value::Null,
        // Path continues but value is scalar: nothing to descend into.
        other => to_json_value(other),
    }
}

/// Push `v` into `out`, flattening one level of array and skipping duplicates and
/// nulls (set semantics for hoisted collection fields).
fn push_flat_unique(out: &mut Vec<Value>, v: Value) {
    match v {
        Value::Null => {}
        Value::Array(inner) => {
            for item in inner {
                if !item.is_null() && !out.contains(&item) {
                    out.push(item);
                }
            }
        }
        scalar => {
            if !out.contains(&scalar) {
                out.push(scalar);
            }
        }
    }
}

/// Truncate a string to at most `max_bytes`, respecting UTF-8 char boundaries
/// (a naive byte slice would panic on a multi-byte boundary in an error body).
fn truncate_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Extract the `_id` for a document from a configured id field.
///
/// Returns `None` (→ auto-generated id) if the field is missing or not a scalar.
/// A stable id makes re-loads **idempotent**: re-indexing the same document `_id`
/// overwrites rather than appends, so the final doc count is unchanged.
fn document_id(doc: &Value, id_field: &str) -> Option<String> {
    match doc.get(id_field) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

/// Minimal Elasticsearch HTTP client (blocking) for index management and bulk
/// indexing. Mirrors the auth/URL handling of [`crate::export::clickhouse::ClickHouseClient`].
#[derive(Clone)]
pub struct ElasticsearchClient {
    client: reqwest::blocking::Client,
    base_url: String,
    auth: Option<(String, String)>,
}

impl ElasticsearchClient {
    /// Create a client. Supports credentials embedded in the URL
    /// (`http://user:pass@host:9200`).
    pub fn new(url: &str) -> Self {
        let (base_url, auth) = if let Ok(parsed) = url::Url::parse(url) {
            let username = parsed.username();
            if !username.is_empty() {
                let password = parsed.password().unwrap_or("").to_string();
                let mut clean = parsed.clone();
                clean.set_username("").ok();
                clean.set_password(None).ok();
                (
                    clean.as_str().trim_end_matches('/').to_string(),
                    Some((username.to_string(), password)),
                )
            } else {
                (url.trim_end_matches('/').to_string(), None)
            }
        } else {
            (url.trim_end_matches('/').to_string(), None)
        };

        Self {
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(300))
                .build()
                .unwrap_or_else(|_| reqwest::blocking::Client::new()),
            base_url,
            auth,
        }
    }

    fn auth_apply(
        &self,
        req: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        if let Some((user, pass)) = &self.auth {
            req.basic_auth(user, Some(pass))
        } else {
            req
        }
    }

    /// Whether an index exists (`HEAD /{index}`).
    pub fn index_exists(&self, index: &str) -> Result<bool> {
        let url = format!("{}/{}", self.base_url, index);
        let resp = self.auth_apply(self.client.head(&url)).send()?;
        Ok(resp.status().is_success())
    }

    /// Delete an index (ignores "not found").
    pub fn delete_index(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}", self.base_url, index);
        let resp = self.auth_apply(self.client.delete(&url)).send()?;
        let status = resp.status();
        if status.is_success() || status.as_u16() == 404 {
            Ok(())
        } else {
            Err(ElasticsearchError::Api(resp.text().unwrap_or_default()))
        }
    }

    /// Create the index with the given request body (mappings + settings).
    ///
    /// When `recreate` is true an existing index is deleted first. Otherwise, if
    /// the index already exists it is left untouched (re-loads remain idempotent
    /// via stable document `_id`s).
    pub fn create_index(&self, index: &str, body: &Value, recreate: bool) -> Result<bool> {
        if self.index_exists(index)? {
            if recreate {
                self.delete_index(index)?;
            } else {
                return Ok(false);
            }
        }

        let url = format!("{}/{}", self.base_url, index);
        let resp = self
            .auth_apply(self.client.put(&url))
            .header("Content-Type", "application/json")
            .json(body)
            .send()?;

        let status = resp.status();
        let text = resp.text()?;
        if !status.is_success() {
            return Err(ElasticsearchError::Api(format!(
                "create index '{}' failed ({}): {}",
                index, status, text
            )));
        }
        Ok(true)
    }

    /// Send a pre-built NDJSON `_bulk` body to `{index}/_bulk`.
    ///
    /// Returns an error if the request fails *or* if any item reports an error
    /// (`"errors": true`) — partial failures must not be silently dropped.
    pub fn bulk(&self, index: &str, ndjson_body: String) -> Result<()> {
        let url = format!("{}/{}/_bulk", self.base_url, index);
        let resp = self
            .auth_apply(self.client.post(&url))
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson_body)
            .send()?;

        let status = resp.status();
        let text = resp.text()?;
        if !status.is_success() {
            return Err(ElasticsearchError::Bulk(format!(
                "bulk request failed ({}): {}",
                status,
                truncate_str(&text, 500)
            )));
        }

        // Inspect the per-item response for `errors: true`.
        if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
            if parsed.get("errors").and_then(|e| e.as_bool()) == Some(true) {
                let first_error = parsed
                    .get("items")
                    .and_then(|items| items.as_array())
                    .and_then(|items| {
                        items.iter().find_map(|item| {
                            item.as_object()
                                .and_then(|o| o.values().next())
                                .and_then(|op| op.get("error"))
                        })
                    })
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown bulk error".to_string());
                return Err(ElasticsearchError::Bulk(first_error));
            }
        }
        Ok(())
    }

    /// Refresh an index so freshly-indexed docs become searchable (needed before
    /// counting, because the loader sets `refresh_interval: -1`).
    pub fn refresh(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}/_refresh", self.base_url, index);
        let resp = self.auth_apply(self.client.post(&url)).send()?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(ElasticsearchError::Api(resp.text().unwrap_or_default()))
        }
    }

    /// Force-merge an index (prod calls this after load to compact segments).
    pub fn forcemerge(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}/_forcemerge", self.base_url, index);
        let resp = self.auth_apply(self.client.post(&url)).send()?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(ElasticsearchError::Api(resp.text().unwrap_or_default()))
        }
    }

    /// Count documents in an index (`GET /{index}/_count`). Refreshes first.
    pub fn count(&self, index: &str) -> Result<u64> {
        self.refresh(index)?;
        let url = format!("{}/{}/_count", self.base_url, index);
        let resp = self.auth_apply(self.client.get(&url)).send()?;
        let status = resp.status();
        let text = resp.text()?;
        if !status.is_success() {
            return Err(ElasticsearchError::Api(text));
        }
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|e| ElasticsearchError::Api(format!("failed to parse count: {}", e)))?;
        parsed
            .get("count")
            .and_then(|c| c.as_u64())
            .ok_or_else(|| ElasticsearchError::Api(format!("no count in response: {}", text)))
    }
}

// ---------------------------------------------------------------------------
// Bulk inserter (buffer NDJSON, POST in batches)
// ---------------------------------------------------------------------------

/// Buffers documents as `_bulk` NDJSON and flushes in batches, mirroring
/// `ClickHouseInserter` / the gnomad-lr inserter.
pub struct BulkInserter<'a> {
    client: &'a ElasticsearchClient,
    index: String,
    id_field: Option<String>,
    batch_size: usize,
    buffer: String,
    buffered_docs: usize,
    /// Total documents successfully sent.
    pub total_docs: usize,
    /// Number of `_bulk` requests sent.
    pub flush_count: usize,
    /// Accumulated time spent in `_bulk` POSTs (ms).
    pub insert_time_ms: u64,
}

impl<'a> BulkInserter<'a> {
    pub fn new(
        client: &'a ElasticsearchClient,
        index: &str,
        id_field: Option<String>,
        batch_size: usize,
    ) -> Self {
        Self {
            client,
            index: index.to_string(),
            id_field,
            batch_size: batch_size.max(1),
            buffer: String::new(),
            buffered_docs: 0,
            total_docs: 0,
            flush_count: 0,
            insert_time_ms: 0,
        }
    }

    /// Buffer a document. Auto-flushes when the batch size is reached.
    ///
    /// When an `id_field` is configured, every document must carry a scalar value
    /// at that field: a stable `_id` is what makes re-loads idempotent (the
    /// `index` op overwrites). A missing id field is a hard error rather than a
    /// silent fall-back to an auto-generated `_id` (which would make re-loads
    /// append duplicates). Pass `id_field = None` to deliberately opt out.
    pub fn add(&mut self, doc: &Value) -> Result<()> {
        let action = match &self.id_field {
            Some(field) => {
                let id = document_id(doc, field).ok_or_else(|| {
                    ElasticsearchError::Bulk(format!(
                        "document is missing a scalar id field '{}' (required for idempotent \
                         _id assignment); pass --id-field \"\" to use auto-generated ids",
                        field
                    ))
                })?;
                json!({ "index": { "_id": id } })
            }
            None => json!({ "index": {} }),
        };

        self.buffer.push_str(&action.to_string());
        self.buffer.push('\n');
        self.buffer.push_str(&doc.to_string());
        self.buffer.push('\n');
        self.buffered_docs += 1;

        if self.buffered_docs >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush buffered documents to `_bulk`.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffered_docs == 0 {
            return Ok(());
        }
        let body = std::mem::take(&mut self.buffer);
        let docs_in_batch = self.buffered_docs;

        let start = std::time::Instant::now();
        self.client.bulk(&self.index, body)?;
        self.insert_time_ms += start.elapsed().as_millis() as u64;

        self.total_docs += docs_in_batch;
        self.flush_count += 1;
        self.buffered_docs = 0;
        Ok(())
    }

    /// Flush any remaining buffered documents.
    pub fn finish(&mut self) -> Result<()> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locus_struct_type() -> EncodedType {
        EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "contig".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "position".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 1,
                },
            ],
        }
    }

    #[test]
    fn test_primitive_type_mapping() {
        assert_eq!(
            hail_type_to_es_mapping(&EncodedType::EInt32 { required: true }),
            json!({ "type": "integer" })
        );
        assert_eq!(
            hail_type_to_es_mapping(&EncodedType::EInt64 { required: false }),
            json!({ "type": "long" })
        );
        assert_eq!(
            hail_type_to_es_mapping(&EncodedType::EFloat32 { required: true }),
            json!({ "type": "float" })
        );
        assert_eq!(
            hail_type_to_es_mapping(&EncodedType::EFloat64 { required: true }),
            json!({ "type": "double" })
        );
        assert_eq!(
            hail_type_to_es_mapping(&EncodedType::EBoolean { required: true }),
            json!({ "type": "boolean" })
        );
        // Hail tstr → keyword (term queries, not analyzed text).
        assert_eq!(
            hail_type_to_es_mapping(&EncodedType::EBinary { required: true }),
            json!({ "type": "keyword" })
        );
    }

    #[test]
    fn test_locus_position_is_integer() {
        // The headline requirement: locus.position MUST be `integer` or range
        // queries on it silently full-scan.
        let mapping = hail_type_to_es_mapping(&locus_struct_type());
        assert_eq!(mapping["type"], "object");
        assert_eq!(mapping["properties"]["contig"]["type"], "keyword");
        assert_eq!(mapping["properties"]["position"]["type"], "integer");
    }

    #[test]
    fn test_array_of_struct_is_nested() {
        let arr = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EBaseStruct {
                required: true,
                fields: vec![EncodedField {
                    name: "gene_id".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 0,
                }],
            }),
        };
        let mapping = hail_type_to_es_mapping(&arr);
        assert_eq!(mapping["type"], "nested");
        assert_eq!(mapping["properties"]["gene_id"]["type"], "keyword");

        // An array of scalars is NOT nested (just the element mapping).
        let arr_scalar = EncodedType::EArray {
            required: true,
            element: Box::new(EncodedType::EBinary { required: true }),
        };
        assert_eq!(
            hail_type_to_es_mapping(&arr_scalar),
            json!({ "type": "keyword" })
        );
    }

    fn variant_row_type() -> EncodedType {
        EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "locus".to_string(),
                    encoded_type: locus_struct_type(),
                    index: 0,
                },
                EncodedField {
                    name: "variant_id".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 1,
                },
                EncodedField {
                    name: "transcript_consequences".to_string(),
                    encoded_type: EncodedType::EArray {
                        required: false,
                        element: Box::new(EncodedType::EBaseStruct {
                            required: true,
                            fields: vec![EncodedField {
                                name: "gene_id".to_string(),
                                encoded_type: EncodedType::EBinary { required: true },
                                index: 0,
                            }],
                        }),
                    },
                    index: 2,
                },
            ],
        }
    }

    #[test]
    fn test_index_mapping_hoists_fields_and_disables_value() {
        let index_fields = parse_index_fields("variant_id,locus,transcript_consequences.gene_id");
        let mapping = build_index_mapping(&variant_row_type(), &index_fields);
        let props = &mapping["properties"];

        // Hoisted/indexed fields with correct types.
        assert_eq!(props["variant_id"]["type"], "keyword");
        assert_eq!(props["locus"]["properties"]["position"]["type"], "integer");
        // `transcript_consequences.gene_id` hoisted to top-level key `gene_id`.
        assert_eq!(props["gene_id"]["type"], "keyword");

        // The whole row is nested under `value` and disabled (stored, not indexed).
        assert_eq!(props["value"]["enabled"], false);
        assert!(props["value"]["properties"].is_object());
    }

    #[test]
    fn test_build_request_body_settings() {
        let index_fields = parse_index_fields("variant_id");
        let body = build_request_body(&variant_row_type(), &index_fields, 16);
        assert_eq!(body["settings"]["index.number_of_shards"], 16);
        assert_eq!(body["settings"]["index.codec"], "best_compression");
        assert_eq!(body["settings"]["index.refresh_interval"], -1);
        assert_eq!(body["settings"]["index.number_of_replicas"], 0);
    }

    fn sample_row() -> EncodedValue {
        EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    (
                        "contig".to_string(),
                        EncodedValue::Binary(b"chr22".to_vec()),
                    ),
                    ("position".to_string(), EncodedValue::Int32(16050075)),
                ]),
            ),
            (
                "variant_id".to_string(),
                EncodedValue::Binary(b"22-16050075-A-G".to_vec()),
            ),
            (
                "transcript_consequences".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Struct(vec![(
                        "gene_id".to_string(),
                        EncodedValue::Binary(b"ENSG00000100053".to_vec()),
                    )]),
                    // Duplicate gene_id to verify set/dedup semantics.
                    EncodedValue::Struct(vec![(
                        "gene_id".to_string(),
                        EncodedValue::Binary(b"ENSG00000100053".to_vec()),
                    )]),
                    EncodedValue::Struct(vec![(
                        "gene_id".to_string(),
                        EncodedValue::Binary(b"ENSG00000999999".to_vec()),
                    )]),
                ]),
            ),
        ])
    }

    #[test]
    fn test_build_document_shape() {
        let index_fields = parse_index_fields("variant_id,locus,transcript_consequences.gene_id");
        let doc = build_document(&sample_row(), &index_fields);

        // Hoisted scalar.
        assert_eq!(doc["variant_id"], "22-16050075-A-G");
        // Hoisted locus object (position numeric so range queries work).
        assert_eq!(doc["locus"]["contig"], "chr22");
        assert_eq!(doc["locus"]["position"], 16050075);
        // Hoisted collection field → deduplicated set, keyed by last segment.
        assert_eq!(
            doc["gene_id"],
            json!(["ENSG00000100053", "ENSG00000999999"])
        );
        // The full row nested under `value` (what the browser reads as _source.value).
        assert_eq!(doc["value"]["variant_id"], "22-16050075-A-G");
        assert_eq!(doc["value"]["locus"]["position"], 16050075);
    }

    #[test]
    fn test_projection_respected_in_value() {
        // Simulate a browser-minimal projected row: `vep` dropped before doc build.
        let row = EncodedValue::Struct(vec![
            (
                "variant_id".to_string(),
                EncodedValue::Binary(b"22-1-A-G".to_vec()),
            ),
            // No `vep` field — projection already removed it.
        ]);
        let index_fields = parse_index_fields("variant_id");
        let doc = build_document(&row, &index_fields);
        assert!(doc["value"].get("vep").is_none());
        assert_eq!(doc["value"]["variant_id"], "22-1-A-G");
    }

    #[test]
    fn test_truncate_str_respects_char_boundaries() {
        // Pure ASCII: byte-exact.
        assert_eq!(truncate_str("hello world", 5), "hello");
        // Shorter than limit: unchanged.
        assert_eq!(truncate_str("hi", 500), "hi");
        // Multi-byte: a naive `&s[..max]` would panic mid-codepoint. "é" is 2 bytes.
        let s = "aé"; // bytes: 'a'(1) + 'é'(2) = 3 bytes
        assert_eq!(truncate_str(s, 2), "a"); // backs off the partial 'é'
                                             // Never panics regardless of where the limit lands.
        for n in 0..s.len() + 2 {
            let _ = truncate_str(s, n);
        }
    }

    #[test]
    fn test_document_id_extraction() {
        let doc = json!({ "variant_id": "22-1-A-G", "n": 5 });
        assert_eq!(
            document_id(&doc, "variant_id"),
            Some("22-1-A-G".to_string())
        );
        assert_eq!(document_id(&doc, "n"), Some("5".to_string()));
        assert_eq!(document_id(&doc, "missing"), None);
    }
}
