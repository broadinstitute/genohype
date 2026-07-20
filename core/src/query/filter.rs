//! Filter parsing for where clauses
//!
//! This module provides shared filter parsing logic used by both
//! the CLI and distributed workers.

use super::{KeyRange, KeyValue};

/// Parse a where clause into a KeyRange
///
/// Supports operators: `>=`, `<=`, `>`, `<`, `=`, `gte`, `lte`
/// Supports dot notation for nested fields (e.g., `locus.position`)
///
/// # Examples
/// ```ignore
/// parse_where_condition("Pvalue < 1e-4")      // Less than
/// parse_where_condition("count >= 100")       // Greater than or equal
/// parse_where_condition("gene.name = BRCA1")  // Nested field equality
/// ```
pub fn parse_where_condition(s: &str) -> Option<KeyRange> {
    // Try different operators (order matters - check multi-char first)
    if let Some(pos) = s.find(">=") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 2..]);
        return Some(KeyRange::gte_nested(field_path, value));
    }
    if let Some(pos) = s.find("<=") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 2..]);
        return Some(KeyRange::lte_nested(field_path, value));
    }
    if let Some(pos) = s.find("gte") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 3..]);
        return Some(KeyRange::gte_nested(field_path, value));
    }
    if let Some(pos) = s.find("lte") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 3..]);
        return Some(KeyRange::lte_nested(field_path, value));
    }
    if let Some(pos) = s.find('>') {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::gt_nested(field_path, value));
    }
    if let Some(pos) = s.find('<') {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::lt_nested(field_path, value));
    }
    if let Some(pos) = s.find('=') {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::point_nested(field_path, value));
    }

    None
}

/// Parse a field string into a field path (supports dot notation)
///
/// # Examples
/// ```ignore
/// parse_field_path("Pvalue")           // vec!["Pvalue"]
/// parse_field_path("locus.position")   // vec!["locus", "position"]
/// ```
pub fn parse_field_path(field: &str) -> Vec<String> {
    field.split('.').map(|s| s.trim().to_string()).collect()
}

/// Parse a value string into a KeyValue
///
/// Tries to parse as: i32 -> i64 -> f64 (handles scientific notation) -> bool -> string
pub fn parse_key_value(s: &str) -> KeyValue {
    let s = s.trim();
    // Try to parse as integer first
    if let Ok(i) = s.parse::<i32>() {
        return KeyValue::Int32(i);
    }
    if let Ok(i) = s.parse::<i64>() {
        return KeyValue::Int64(i);
    }
    // Float supports scientific notation (e.g., 1e-4)
    if let Ok(f) = s.parse::<f64>() {
        return KeyValue::Float64(f);
    }
    if s == "true" {
        return KeyValue::Boolean(true);
    }
    if s == "false" {
        return KeyValue::Boolean(false);
    }
    // Default to string
    KeyValue::String(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_key_value_int() {
        assert!(matches!(parse_key_value("42"), KeyValue::Int32(42)));
        assert!(matches!(parse_key_value("-10"), KeyValue::Int32(-10)));
    }

    #[test]
    fn test_parse_key_value_float() {
        if let KeyValue::Float64(f) = parse_key_value("3.14") {
            assert!((f - 3.14).abs() < 1e-10);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_parse_key_value_scientific() {
        if let KeyValue::Float64(f) = parse_key_value("1e-4") {
            assert!((f - 0.0001).abs() < 1e-10);
        } else {
            panic!("Expected Float64 for scientific notation");
        }
    }

    #[test]
    fn test_parse_key_value_bool() {
        assert!(matches!(parse_key_value("true"), KeyValue::Boolean(true)));
        assert!(matches!(parse_key_value("false"), KeyValue::Boolean(false)));
    }

    #[test]
    fn test_parse_field_path() {
        assert_eq!(parse_field_path("Pvalue"), vec!["Pvalue"]);
        assert_eq!(
            parse_field_path("locus.position"),
            vec!["locus", "position"]
        );
        assert_eq!(parse_field_path(" gene . name "), vec!["gene", "name"]);
    }

    #[test]
    fn test_parse_where_condition() {
        use super::super::QueryBound;

        // Less than
        let range = parse_where_condition("Pvalue < 1e-4").unwrap();
        assert_eq!(range.field_path, vec!["Pvalue"]);
        assert!(matches!(range.start, QueryBound::Unbounded));
        assert!(matches!(range.end, QueryBound::Excluded(_)));

        // Greater than or equal
        let range = parse_where_condition("count >= 100").unwrap();
        assert_eq!(range.field_path, vec!["count"]);
        assert!(matches!(range.start, QueryBound::Included(_)));
        assert!(matches!(range.end, QueryBound::Unbounded));

        // Equality
        let range = parse_where_condition("gene = BRCA1").unwrap();
        assert_eq!(range.field_path, vec!["gene"]);
        assert!(matches!(range.start, QueryBound::Included(_)));
        assert!(matches!(range.end, QueryBound::Included(_)));
    }
}
