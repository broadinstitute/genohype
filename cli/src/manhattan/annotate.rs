//! Annotation logic for significant hits using a secondary Hail table.

use crate::manhattan::data::SignificantHit;
use crate::Result;
use genohype_core::codec::EncodedValue;
use genohype_core::query::QueryEngine;

/// Annotates significant hits by looking up their keys in an annotation table.
#[allow(dead_code)]
pub struct Annotator {
    engine: Option<QueryEngine>,
    fields: Vec<String>,
}

#[allow(dead_code)]
impl Annotator {
    /// Create an annotator. If `path` is `None`, annotation is a no-op.
    pub fn new(path: Option<String>, fields: Vec<String>) -> Result<Self> {
        let engine = if let Some(p) = path {
            Some(QueryEngine::open_path(&p)?)
        } else {
            None
        };

        Ok(Self { engine, fields })
    }

    /// Look up the key in the annotation table and attach fields to the hit.
    pub fn annotate(&mut self, hit: &mut SignificantHit, key: &EncodedValue) -> Result<()> {
        let engine = match &mut self.engine {
            Some(e) => e,
            None => return Ok(()),
        };

        if let Some(row) = engine.lookup(key)? {
            if self.fields.is_empty() {
                // If no specific fields requested, serialize the whole row
                hit.annotations =
                    serde_json::to_value(&format!("{:?}", row)).unwrap_or(serde_json::Value::Null);
            } else {
                // Extract only requested fields
                let mut map = serde_json::Map::new();
                if let EncodedValue::Struct(fields) = &row {
                    for (name, value) in fields {
                        if self.fields.iter().any(|f| f == name) {
                            let json_val = encoded_value_to_json(value);
                            map.insert(name.clone(), json_val);
                        }
                    }
                }
                hit.annotations = serde_json::Value::Object(map);
            }
        }
        Ok(())
    }
}

/// Convert an `EncodedValue` to a `serde_json::Value` for the sidecar.
#[allow(dead_code)]
fn encoded_value_to_json(val: &EncodedValue) -> serde_json::Value {
    match val {
        EncodedValue::Null => serde_json::Value::Null,
        EncodedValue::Boolean(b) => serde_json::Value::Bool(*b),
        EncodedValue::Int32(n) => serde_json::json!(*n),
        EncodedValue::Int64(n) => serde_json::json!(*n),
        EncodedValue::Float32(n) => serde_json::json!(*n),
        EncodedValue::Float64(n) => serde_json::json!(*n),
        EncodedValue::Binary(bytes) => {
            serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned())
        }
        EncodedValue::Array(items) => {
            serde_json::Value::Array(items.iter().map(encoded_value_to_json).collect())
        }
        EncodedValue::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (name, value) in fields {
                map.insert(name.clone(), encoded_value_to_json(value));
            }
            serde_json::Value::Object(map)
        }
    }
}
