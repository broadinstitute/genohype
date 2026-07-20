//! Query types for Hail table queries

use serde_json::Value;

/// A value that can be used in a key query
#[derive(Debug, Clone)]
pub enum KeyValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
}

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        use KeyValue::*;
        match (self, other) {
            (String(a), String(b)) => a == b,
            (Int32(a), Int32(b)) => a == b,
            (Int64(a), Int64(b)) => a == b,
            (Int32(a), Int64(b)) => *a as i64 == *b,
            (Int64(a), Int32(b)) => *a == *b as i64,
            (Float32(a), Float32(b)) => a == b,
            (Float64(a), Float64(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl KeyValue {
    /// Create a KeyValue from a JSON value
    pub fn from_json(value: &Value) -> Option<Self> {
        match value {
            Value::String(s) => Some(KeyValue::String(s.clone())),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    // Decide between Int32 and Int64 based on range
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Some(KeyValue::Int32(i as i32))
                    } else {
                        Some(KeyValue::Int64(i))
                    }
                } else if let Some(f) = n.as_f64() {
                    Some(KeyValue::Float64(f))
                } else {
                    None
                }
            }
            Value::Bool(b) => Some(KeyValue::Boolean(*b)),
            _ => None,
        }
    }

    /// Get the value as an i64 if possible
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            KeyValue::Int32(i) => Some(*i as i64),
            KeyValue::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// Get the value as a string if possible
    pub fn as_str(&self) -> Option<&str> {
        match self {
            KeyValue::String(s) => Some(s),
            _ => None,
        }
    }
}

impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use KeyValue::*;
        match (self, other) {
            (String(a), String(b)) => a.partial_cmp(b),
            (Int32(a), Int32(b)) => a.partial_cmp(b),
            (Int64(a), Int64(b)) => a.partial_cmp(b),
            (Int32(a), Int64(b)) => (*a as i64).partial_cmp(b),
            (Int64(a), Int32(b)) => a.partial_cmp(&(*b as i64)),
            (Float32(a), Float32(b)) => a.partial_cmp(b),
            (Float64(a), Float64(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Eq for KeyValue {}

impl Ord for KeyValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Query bound for range queries
#[derive(Debug, Clone)]
pub enum QueryBound {
    /// Include this value in the range
    Included(KeyValue),
    /// Exclude this value from the range
    Excluded(KeyValue),
    /// No bound (unbounded on this side)
    Unbounded,
}

/// A key range query for a single field (supports nested struct access)
#[derive(Debug, Clone)]
pub struct KeyRange {
    /// Field path (e.g., ["locus", "contig"] for locus.contig)
    pub field_path: Vec<String>,
    pub start: QueryBound,
    pub end: QueryBound,
}

impl KeyRange {
    /// Create a point query (exact match) for a simple field
    pub fn point(field: String, value: KeyValue) -> Self {
        KeyRange {
            field_path: vec![field],
            start: QueryBound::Included(value.clone()),
            end: QueryBound::Included(value),
        }
    }

    /// Create a point query (exact match) for a nested field path
    pub fn point_nested(field_path: Vec<String>, value: KeyValue) -> Self {
        KeyRange {
            field_path,
            start: QueryBound::Included(value.clone()),
            end: QueryBound::Included(value),
        }
    }

    /// Create a range query with inclusive bounds
    pub fn inclusive(field: String, start: KeyValue, end: KeyValue) -> Self {
        KeyRange {
            field_path: vec![field],
            start: QueryBound::Included(start),
            end: QueryBound::Included(end),
        }
    }

    /// Create a range query with exclusive bounds
    pub fn exclusive(field: String, start: KeyValue, end: KeyValue) -> Self {
        KeyRange {
            field_path: vec![field],
            start: QueryBound::Excluded(start),
            end: QueryBound::Excluded(end),
        }
    }

    /// Create a greater-than-or-equal query for a simple field
    pub fn gte(field: String, value: KeyValue) -> Self {
        KeyRange {
            field_path: vec![field],
            start: QueryBound::Included(value),
            end: QueryBound::Unbounded,
        }
    }

    /// Create a greater-than-or-equal query for a nested field path
    pub fn gte_nested(field_path: Vec<String>, value: KeyValue) -> Self {
        KeyRange {
            field_path,
            start: QueryBound::Included(value),
            end: QueryBound::Unbounded,
        }
    }

    /// Create a less-than-or-equal query for a simple field
    pub fn lte(field: String, value: KeyValue) -> Self {
        KeyRange {
            field_path: vec![field],
            start: QueryBound::Unbounded,
            end: QueryBound::Included(value),
        }
    }

    /// Create a less-than-or-equal query for a nested field path
    pub fn lte_nested(field_path: Vec<String>, value: KeyValue) -> Self {
        KeyRange {
            field_path,
            start: QueryBound::Unbounded,
            end: QueryBound::Included(value),
        }
    }

    /// Create greater-than query for a nested field path
    pub fn gt_nested(field_path: Vec<String>, value: KeyValue) -> Self {
        KeyRange {
            field_path,
            start: QueryBound::Excluded(value),
            end: QueryBound::Unbounded,
        }
    }

    /// Create less-than query for a nested field path
    pub fn lt_nested(field_path: Vec<String>, value: KeyValue) -> Self {
        KeyRange {
            field_path,
            start: QueryBound::Unbounded,
            end: QueryBound::Excluded(value),
        }
    }

    /// Get the root (top-level) field name
    pub fn root_field(&self) -> &str {
        &self.field_path[0]
    }

    /// Check if this range applies to a nested field
    pub fn is_nested(&self) -> bool {
        self.field_path.len() > 1
    }

    /// Get the field path as a dot-separated string (for display)
    pub fn field_path_str(&self) -> String {
        self.field_path.join(".")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_value_from_json() {
        let json_str = serde_json::json!("test");
        assert_eq!(
            KeyValue::from_json(&json_str),
            Some(KeyValue::String("test".to_string()))
        );

        let json_int = serde_json::json!(42);
        assert_eq!(KeyValue::from_json(&json_int), Some(KeyValue::Int32(42)));
    }

    #[test]
    fn test_key_value_ordering() {
        let a = KeyValue::String("a".to_string());
        let b = KeyValue::String("b".to_string());
        assert!(a < b);

        let x = KeyValue::Int32(10);
        let y = KeyValue::Int32(20);
        assert!(x < y);

        // Cross-type comparison for integers
        let i32_val = KeyValue::Int32(10);
        let i64_val = KeyValue::Int64(10);
        assert!(i32_val == i64_val);
    }

    #[test]
    fn test_key_range_constructors() {
        let point = KeyRange::point("field".to_string(), KeyValue::Int32(42));
        assert_eq!(point.root_field(), "field");
        assert!(!point.is_nested());

        let range = KeyRange::inclusive(
            "field".to_string(),
            KeyValue::Int32(10),
            KeyValue::Int32(20),
        );
        assert_eq!(range.root_field(), "field");
    }

    #[test]
    fn test_nested_key_range() {
        let nested = KeyRange::point_nested(
            vec!["locus".to_string(), "contig".to_string()],
            KeyValue::String("chr1".to_string()),
        );
        assert_eq!(nested.root_field(), "locus");
        assert!(nested.is_nested());
        assert_eq!(nested.field_path_str(), "locus.contig");
    }
}
