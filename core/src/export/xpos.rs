//! Global genomic coordinate (`xpos`) helper shared by the export loaders.
//!
//! `xpos` is a single integer encoding both chromosome and position so that a
//! genome-wide range query becomes a **scalar** range scan that a store's primary
//! index / row-group statistics can prune:
//!
//! ```text
//! xpos = contig_number * 1_000_000_000 + position
//! ```
//!
//! where autosomes map to their number (1–22), `X` → 23, `Y` → 24, `M`/`MT` → 25,
//! and a leading `chr` prefix is stripped. This is the **same formula** used by the
//! Manhattan pipeline (`cli/src/manhattan/reference.rs::calculate_xpos`,
//! `cli/src/ingest/manhattan.rs::compute_xpos`) and the axaou-rust ClickHouse
//! loader (`axaou-server/src/clickhouse/xpos.rs`). The `core` crate cannot depend
//! on `cli`, so the canonical implementation lives here and the loaders reuse it.
//!
//! # Why a materialized column
//!
//! The gnomAD sites tables are keyed `(locus, alleles)` where `locus` is a
//! `{contig, position}` struct/tuple. A store sorted/indexed on that tuple cannot
//! prune a `position` range predicate (ClickHouse cannot use `locus.2`, parquet
//! row-group stats on a struct are useless, Postgres needs an explicit hoisted
//! column). Materializing a scalar `xpos` and ordering/indexing on it gives every
//! arm the same fair range-pruning the BKD-indexed ES arm already has.

use crate::codec::{EncodedField, EncodedType, EncodedValue};

/// The materialized column name used by every store.
pub const XPOS_FIELD: &str = "xpos";

/// Append a non-nullable `xpos` `Int64` field to the end of a struct schema.
///
/// Used by the schema-materializing loaders (ClickHouse `CREATE TABLE`, parquet
/// Arrow schema) so the stored schema carries the derived column. Appending at the
/// end keeps the positional row→schema alignment that `build_record_batch` relies
/// on — [`augment_row_with_xpos`] appends the value in the same position. A no-op
/// for a non-struct schema.
pub fn augment_type_with_xpos(schema: &EncodedType) -> EncodedType {
    match schema {
        EncodedType::EBaseStruct { required, fields } => {
            let mut new_fields = fields.clone();
            new_fields.push(EncodedField {
                name: XPOS_FIELD.to_string(),
                // Required so it becomes a non-nullable Int64/BIGINT primary-index
                // column; rows with no coordinates get 0 (see `augment_row_with_xpos`).
                encoded_type: EncodedType::EInt64 { required: true },
                index: new_fields.len(),
            });
            EncodedType::EBaseStruct {
                required: *required,
                fields: new_fields,
            }
        }
        other => other.clone(),
    }
}

/// Append the computed `xpos` value to the end of a struct row, aligned with the
/// field [`augment_type_with_xpos`] appends. A row whose coordinates can't be
/// determined gets `xpos = 0` (kept non-null so it remains a valid sort key); this
/// never drops or reorders rows, so query *results* are unchanged. A no-op for a
/// non-struct row.
pub fn augment_row_with_xpos(row: EncodedValue) -> EncodedValue {
    match row {
        EncodedValue::Struct(mut fields) => {
            let xpos = compute_xpos_for_fields(&fields).unwrap_or(0);
            fields.push((XPOS_FIELD.to_string(), EncodedValue::Int64(xpos)));
            EncodedValue::Struct(fields)
        }
        other => other,
    }
}

/// Convert a contig name to its numeric index for the `xpos` calculation.
///
/// Strips a leading `chr` prefix, maps `X` → 23, `Y` → 24, `M`/`MT` → 25, parses
/// numeric strings (1–22) directly, and returns 0 for anything unrecognized
/// (matching the Manhattan-pipeline impls so xpos values are identical).
pub fn contig_to_int(contig: &str) -> i64 {
    let name = contig.strip_prefix("chr").unwrap_or(contig);
    match name {
        "X" => 23,
        "Y" => 24,
        "M" | "MT" => 25,
        _ => name.parse::<i64>().unwrap_or(0),
    }
}

/// Compute `xpos = contig_number * 1_000_000_000 + position`.
///
/// `position` is a 1-based genomic position. The canonical formula; see the module
/// docs for the cross-references it must match.
pub fn compute_xpos(contig: &str, position: i64) -> i64 {
    contig_to_int(contig) * 1_000_000_000 + position
}

/// Look up a field by name in a struct value (positional scan).
fn struct_field<'a>(row: &'a EncodedValue, name: &str) -> Option<&'a EncodedValue> {
    match row {
        EncodedValue::Struct(fields) => fields.iter().find(|(k, _)| k == name).map(|(_, v)| v),
        _ => None,
    }
}

/// Compute `xpos` for a decoded gnomAD variant row.
///
/// Reads `contig`/`position` from the `locus` struct (`{contig, position}`) and,
/// for a `browser-minimal` / locus-less projection, falls back to parsing the
/// `variant_id` string (`contig-pos-ref-alt`). Returns `None` only if neither the
/// structured `locus` nor a parseable `variant_id` is present — callers treat that
/// as "no xpos" (NULL/0) rather than dropping the row, since `xpos` is a derived
/// secondary column and the result set must be unchanged.
pub fn compute_xpos_for_row(row: &EncodedValue) -> Option<i64> {
    match row {
        EncodedValue::Struct(fields) => compute_xpos_for_fields(fields),
        _ => None,
    }
}

/// `compute_xpos_for_row` over an already-destructured struct's fields, avoiding a
/// clone when the caller (e.g. [`augment_row_with_xpos`]) already owns the vec.
fn compute_xpos_for_fields(fields: &[(String, EncodedValue)]) -> Option<i64> {
    let field = |name: &str| fields.iter().find(|(k, _)| k == name).map(|(_, v)| v);

    // Preferred: the structured locus.
    if let Some(locus) = field("locus") {
        let contig = struct_field(locus, "contig").and_then(|c| c.as_string());
        let position = struct_field(locus, "position").and_then(|p| match p {
            EncodedValue::Int32(v) => Some(*v as i64),
            EncodedValue::Int64(v) => Some(*v),
            _ => None,
        });
        if let (Some(contig), Some(position)) = (contig, position) {
            return Some(compute_xpos(&contig, position));
        }
    }

    // Fallback: parse the `variant_id` string `contig-pos-ref-alt`.
    let variant_id = field("variant_id").and_then(|v| v.as_string())?;
    let mut parts = variant_id.splitn(4, '-');
    let contig = parts.next()?;
    let position: i64 = parts.next()?.parse().ok()?;
    Some(compute_xpos(contig, position))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_xpos_matches_canonical_formula() {
        // Must match cli/src/ingest/manhattan.rs::compute_xpos and
        // cli/src/manhattan/reference.rs::calculate_xpos.
        assert_eq!(compute_xpos("1", 1000), 1_000_001_000);
        assert_eq!(compute_xpos("chr1", 1000), 1_000_001_000);
        assert_eq!(compute_xpos("22", 500), 22_000_000_500);
        assert_eq!(compute_xpos("X", 100), 23_000_000_100);
        assert_eq!(compute_xpos("chrX", 100), 23_000_000_100);
        assert_eq!(compute_xpos("chrY", 200), 24_000_000_200);
        // M/MT -> 25 (matches reference.rs::contig_to_int; manhattan ingest omits
        // M but the canonical reference impl includes it).
        assert_eq!(compute_xpos("M", 1), 25_000_000_001);
        assert_eq!(compute_xpos("MT", 1), 25_000_000_001);
    }

    #[test]
    fn test_contig_to_int_unknown_is_zero() {
        assert_eq!(contig_to_int("chrUn_foo"), 0);
        assert_eq!(contig_to_int(""), 0);
    }

    fn locus(contig: &str, pos: i32) -> EncodedValue {
        EncodedValue::Struct(vec![
            ("contig".to_string(), EncodedValue::Binary(contig.as_bytes().to_vec())),
            ("position".to_string(), EncodedValue::Int32(pos)),
        ])
    }

    #[test]
    fn test_compute_xpos_for_row_from_locus() {
        let row = EncodedValue::Struct(vec![
            ("locus".to_string(), locus("chr22", 16050075)),
            ("variant_id".to_string(), EncodedValue::Binary(b"22-16050075-A-G".to_vec())),
        ]);
        assert_eq!(compute_xpos_for_row(&row), Some(22_000_000_000 + 16050075));
    }

    #[test]
    fn test_compute_xpos_for_row_falls_back_to_variant_id() {
        // browser-minimal-ish: no locus, only variant_id.
        let row = EncodedValue::Struct(vec![(
            "variant_id".to_string(),
            EncodedValue::Binary(b"1-12345-AC-T".to_vec()),
        )]);
        assert_eq!(compute_xpos_for_row(&row), Some(1_000_012_345));
    }

    #[test]
    fn test_compute_xpos_for_row_none_when_no_coords() {
        let row = EncodedValue::Struct(vec![(
            "rsids".to_string(),
            EncodedValue::Array(vec![]),
        )]);
        assert_eq!(compute_xpos_for_row(&row), None);
    }
}
