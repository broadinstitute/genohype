//! Field projection for selecting/excluding fields from EncodedValue rows.
//!
//! Supports dot-separated paths (`freq.AF`), array indexing (`[0]`),
//! and head slicing (`[:3]`).

use crate::codec::encoded_type::{EncodedType, EncodedValue};
use crate::error::{HailError, Result};
use std::collections::HashMap;

/// A single segment of a field path.
#[derive(Debug, Clone, PartialEq)]
pub enum PathSegment {
    /// Named struct field
    Field(String),
    /// Array index (e.g., `[0]`, `[-1]`)
    Index(i64),
    /// Array slice (e.g., `[:3]`, `[1:5]`)
    Slice(Option<usize>, Option<usize>),
}

/// A parsed field path like `vep.transcript_consequences[0].gene_symbol`.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldPath {
    pub segments: Vec<PathSegment>,
}

impl FieldPath {
    /// Parse a field path string.
    ///
    /// Examples: `locus`, `freq.AF`, `vep.transcript_consequences[0]`,
    /// `vep.transcript_consequences[:3]`
    pub fn parse(input: &str) -> Result<Self> {
        let mut segments = Vec::new();
        let mut remaining = input;

        while !remaining.is_empty() {
            // Strip leading dot (except at start)
            if remaining.starts_with('.') {
                remaining = &remaining[1..];
                if remaining.is_empty() {
                    return Err(HailError::InvalidFormat(format!(
                        "Field path cannot end with '.': {input}"
                    )));
                }
            }

            // Check for bracket notation
            if remaining.starts_with('[') {
                let close = remaining.find(']').ok_or_else(|| {
                    HailError::InvalidFormat(format!("Unclosed bracket in field path: {input}"))
                })?;
                let inner = &remaining[1..close];
                remaining = &remaining[close + 1..];

                if let Some(colon_pos) = inner.find(':') {
                    // Slice notation
                    let start_str = &inner[..colon_pos];
                    let end_str = &inner[colon_pos + 1..];
                    let start = if start_str.is_empty() {
                        None
                    } else {
                        Some(start_str.parse::<usize>().map_err(|_| {
                            HailError::InvalidFormat(format!(
                                "Invalid slice start in field path: {input}"
                            ))
                        })?)
                    };
                    let end = if end_str.is_empty() {
                        None
                    } else {
                        Some(end_str.parse::<usize>().map_err(|_| {
                            HailError::InvalidFormat(format!(
                                "Invalid slice end in field path: {input}"
                            ))
                        })?)
                    };
                    segments.push(PathSegment::Slice(start, end));
                } else {
                    // Index notation
                    let idx = inner.parse::<i64>().map_err(|_| {
                        HailError::InvalidFormat(format!(
                            "Invalid array index in field path: {input}"
                        ))
                    })?;
                    segments.push(PathSegment::Index(idx));
                }
            } else {
                // Field name: consume until '.', '[', or end
                let end = remaining
                    .find(|c: char| c == '.' || c == '[')
                    .unwrap_or(remaining.len());
                let name = &remaining[..end];
                if name.is_empty() {
                    return Err(HailError::InvalidFormat(format!(
                        "Empty field name in path: {input}"
                    )));
                }
                segments.push(PathSegment::Field(name.to_string()));
                remaining = &remaining[end..];
            }
        }

        if segments.is_empty() {
            return Err(HailError::InvalidFormat(format!(
                "Empty field path: {input}"
            )));
        }

        Ok(FieldPath { segments })
    }
}

/// A tree structure for efficiently applying multiple field projections.
///
/// When multiple paths share prefixes (e.g., `freq.AF` and `freq.AN`),
/// the tree collapses them to avoid redundant traversals.
#[derive(Debug, Clone)]
pub struct ProjectionTree {
    /// If true, include this entire subtree (all children).
    select_all: bool,
    /// Child nodes keyed by field name.
    children: HashMap<String, ProjectionTree>,
    /// Array operations to apply at this level.
    array_op: Option<ArrayOp>,
}

#[derive(Debug, Clone)]
pub enum ArrayOp {
    Index(i64),
    Slice(Option<usize>, Option<usize>),
}

impl ProjectionTree {
    /// Check if this node selects the entire subtree.
    pub fn is_select_all(&self) -> bool {
        self.select_all
    }

    /// Get a child node by field name (for struct field navigation during decode).
    pub fn get_child(&self, name: &str) -> Option<&ProjectionTree> {
        self.children.get(name)
    }

    /// Check if this node has any children (i.e., is a partial projection).
    pub fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    /// Get the array operation (index or slice) at this level, if any.
    pub fn array_op(&self) -> Option<&ArrayOp> {
        self.array_op.as_ref()
    }

    /// Ensure a top-level field is included in the projection.
    ///
    /// If the tree already selects all or already has this field, this is a no-op.
    /// Used to ensure filter-dependent fields (like `locus`) are decoded even
    /// if the user didn't request them.
    pub fn ensure_field(&mut self, name: &str) {
        if self.select_all {
            return;
        }
        self.children
            .entry(name.to_string())
            .or_insert_with(|| ProjectionTree {
                select_all: true,
                children: HashMap::new(),
                array_op: None,
            });
    }

    /// Build a projection tree from a list of field paths.
    pub fn from_fields(paths: &[FieldPath]) -> Self {
        let mut root = ProjectionTree {
            select_all: false,
            children: HashMap::new(),
            array_op: None,
        };

        for path in paths {
            root.insert(&path.segments);
        }

        root
    }

    fn insert(&mut self, segments: &[PathSegment]) {
        if segments.is_empty() || self.select_all {
            // No more segments = select entire subtree
            self.select_all = true;
            self.children.clear();
            return;
        }

        match &segments[0] {
            PathSegment::Field(name) => {
                let child = self
                    .children
                    .entry(name.clone())
                    .or_insert_with(|| ProjectionTree {
                        select_all: false,
                        children: HashMap::new(),
                        array_op: None,
                    });
                child.insert(&segments[1..]);
            }
            PathSegment::Index(idx) => {
                self.array_op = Some(ArrayOp::Index(*idx));
                // If there are more segments after the index, they apply to array elements
                if segments.len() > 1 {
                    // Create a synthetic child that represents "after indexing"
                    // Continue inserting remaining segments
                    self.insert(&segments[1..]);
                } else {
                    self.select_all = true;
                }
            }
            PathSegment::Slice(start, end) => {
                self.array_op = Some(ArrayOp::Slice(*start, *end));
                if segments.len() > 1 {
                    self.insert(&segments[1..]);
                } else {
                    self.select_all = true;
                }
            }
        }
    }

    /// Validate that all field paths exist in the given schema.
    pub fn validate_against_schema(&self, schema: &EncodedType) -> Result<()> {
        self.validate_recursive(schema, &mut Vec::new())
    }

    fn validate_recursive(
        &self,
        schema: &EncodedType,
        path_so_far: &mut Vec<String>,
    ) -> Result<()> {
        if self.select_all && self.children.is_empty() {
            return Ok(());
        }

        match schema {
            EncodedType::EBaseStruct { fields, .. } => {
                for (child_name, child_tree) in &self.children {
                    let field = fields.iter().find(|f| &f.name == child_name).ok_or_else(|| {
                        path_so_far.push(child_name.clone());
                        let full_path = path_so_far.join(".");
                        path_so_far.pop();
                        let available: Vec<&str> =
                            fields.iter().map(|f| f.name.as_str()).collect();
                        HailError::InvalidFormat(format!(
                            "Field '{}' not found in schema. Available fields: {}",
                            full_path,
                            available.join(", ")
                        ))
                    })?;
                    path_so_far.push(child_name.clone());
                    child_tree.validate_recursive(&field.encoded_type, path_so_far)?;
                    path_so_far.pop();
                }
            }
            EncodedType::EArray { element, .. } => {
                // Validate children against the element type
                if !self.children.is_empty() {
                    let sub = ProjectionTree {
                        select_all: self.select_all,
                        children: self.children.clone(),
                        array_op: None,
                    };
                    sub.validate_recursive(element, path_so_far)?;
                }
            }
            _ => {
                if !self.children.is_empty() {
                    let full_path = path_so_far.join(".");
                    return Err(HailError::InvalidFormat(format!(
                        "Cannot descend into non-struct/array field '{full_path}'"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Project an EncodedValue, keeping only the fields in the tree.
    pub fn project(&self, value: &EncodedValue) -> EncodedValue {
        if self.select_all && self.children.is_empty() && self.array_op.is_none() {
            return value.clone();
        }

        match value {
            EncodedValue::Struct(fields) => {
                if self.select_all {
                    return value.clone();
                }
                let projected: Vec<(String, EncodedValue)> = fields
                    .iter()
                    .filter_map(|(name, val)| {
                        self.children.get(name).map(|child| {
                            (name.clone(), child.project(val))
                        })
                    })
                    .collect();
                EncodedValue::Struct(projected)
            }
            EncodedValue::Array(elements) => {
                // Apply array operation if present
                let sliced = match &self.array_op {
                    Some(ArrayOp::Index(idx)) => {
                        let resolved = if *idx < 0 {
                            elements.len().checked_sub((-*idx) as usize)
                        } else {
                            Some(*idx as usize)
                        };
                        match resolved {
                            Some(i) if i < elements.len() => {
                                // If we have children to project into the element, do so
                                if !self.children.is_empty() {
                                    let sub = ProjectionTree {
                                        select_all: self.select_all,
                                        children: self.children.clone(),
                                        array_op: None,
                                    };
                                    return sub.project(&elements[i]);
                                }
                                return elements[i].clone();
                            }
                            _ => return EncodedValue::Null,
                        }
                    }
                    Some(ArrayOp::Slice(start, end)) => {
                        let s = start.unwrap_or(0);
                        let e = end.unwrap_or(elements.len()).min(elements.len());
                        if s < e {
                            &elements[s..e]
                        } else {
                            &[]
                        }
                    }
                    None => elements.as_slice(),
                };

                // If we have child projections, apply to each element
                if !self.children.is_empty() {
                    let sub = ProjectionTree {
                        select_all: self.select_all,
                        children: self.children.clone(),
                        array_op: None,
                    };
                    EncodedValue::Array(sliced.iter().map(|e| sub.project(e)).collect())
                } else {
                    EncodedValue::Array(sliced.to_vec())
                }
            }
            // Leaf values just return as-is
            _ => value.clone(),
        }
    }
}

/// Exclude top-level fields from an EncodedValue.
pub fn exclude_fields(value: &EncodedValue, excluded: &[String]) -> EncodedValue {
    match value {
        EncodedValue::Struct(fields) => {
            let filtered: Vec<(String, EncodedValue)> = fields
                .iter()
                .filter(|(name, _)| !excluded.iter().any(|e| e == name))
                .cloned()
                .collect();
            EncodedValue::Struct(filtered)
        }
        _ => value.clone(),
    }
}

/// Validate that excluded field names exist in the schema.
pub fn validate_exclude_fields(excluded: &[String], schema: &EncodedType) -> Result<()> {
    if let EncodedType::EBaseStruct { fields, .. } = schema {
        let available: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        for name in excluded {
            if !available.contains(&name.as_str()) {
                return Err(HailError::InvalidFormat(format!(
                    "Field '{}' not found in schema. Available fields: {}",
                    name,
                    available.join(", ")
                )));
            }
        }
    }
    Ok(())
}

/// Represents the user's projection choice.
#[derive(Debug, Clone)]
pub enum Projection {
    /// Include only these fields
    Fields(ProjectionTree),
    /// Exclude these top-level fields
    Exclude(Vec<String>),
}

impl Projection {
    /// Parse --fields argument into a Projection.
    pub fn from_fields_str(fields_str: &str) -> Result<Self> {
        let paths: Vec<FieldPath> = fields_str
            .split(',')
            .map(|s| FieldPath::parse(s.trim()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Projection::Fields(ProjectionTree::from_fields(
            &paths,
        )))
    }

    /// Build the canonical `browser-minimal` strict inclusion-list projection.
    ///
    /// Keeps only [`BROWSER_MINIMAL_FIELDS`] (each in full) and drops everything
    /// else. This is the single source of truth for the `browser-minimal` schema
    /// width used by the sizing probe and the per-backend loaders, so they don't
    /// each re-specify the field list. Strict: [`Projection::validate`] against a
    /// table that lacks one of these fields will error — see
    /// [`Projection::browser_minimal_present_in`] for the schema-tolerant variant.
    pub fn browser_minimal() -> Self {
        let paths: Vec<FieldPath> = BROWSER_MINIMAL_FIELDS
            .iter()
            .map(|f| FieldPath::parse(f).expect("static browser-minimal field path must parse"))
            .collect();
        Projection::Fields(ProjectionTree::from_fields(&paths))
    }

    /// Build the `browser-minimal` projection restricted to the fields actually
    /// present as top-level fields in `schema`.
    ///
    /// This stays strict (it never includes a field outside [`BROWSER_MINIMAL_FIELDS`])
    /// but tolerates the schema differences between the gnomAD v4 exomes and genomes
    /// sites tables, so the same width definition can be applied to either without a
    /// validation failure on an absent field (e.g. a missing `coverage` or `joint`).
    pub fn browser_minimal_present_in(schema: &EncodedType) -> Self {
        let available: Vec<&str> = match schema {
            EncodedType::EBaseStruct { fields, .. } => {
                fields.iter().map(|f| f.name.as_str()).collect()
            }
            _ => Vec::new(),
        };
        let paths: Vec<FieldPath> = BROWSER_MINIMAL_FIELDS
            .iter()
            .filter(|f| available.contains(*f))
            .map(|f| FieldPath::parse(f).expect("static browser-minimal field path must parse"))
            .collect();
        Projection::Fields(ProjectionTree::from_fields(&paths))
    }

    /// Parse --exclude argument into a Projection.
    pub fn from_exclude_str(exclude_str: &str) -> Result<Self> {
        let names: Vec<String> = exclude_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        Ok(Projection::Exclude(names))
    }

    /// Validate the projection against a schema.
    pub fn validate(&self, schema: &EncodedType) -> Result<()> {
        match self {
            Projection::Fields(tree) => tree.validate_against_schema(schema),
            Projection::Exclude(names) => validate_exclude_fields(names, schema),
        }
    }

    /// Apply the projection to a row.
    pub fn apply(&self, value: &EncodedValue) -> EncodedValue {
        match self {
            Projection::Fields(tree) => tree.project(value),
            Projection::Exclude(names) => exclude_fields(value, names),
        }
    }
}

/// A named schema-width preset for the gnomAD datastore benchmark (Phase 0).
///
/// The benchmark sweeps a `{full, browser-minimal}` width dimension to test the
/// hypothesis that storing the whole Hail document (prod's ES practice) costs far
/// more disk/ingest than storing only what the browser API returns, and hurts
/// row/document stores (ES `_source`, Postgres-JSONB de-TOAST) much more than
/// columnar stores.
///
/// - [`SchemaWidth::Full`] decodes every field in the source table (no projection).
/// - [`SchemaWidth::BrowserMinimal`] applies a *strict inclusion-list* over
///   [`BROWSER_MINIMAL_FIELDS`] — keep only those fields, drop everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaWidth {
    /// Decode the entire row (all ~238 leaves of the gnomAD v4 sites table).
    Full,
    /// Strict allowlist of only the fields the gnomAD browser API returns.
    BrowserMinimal,
}

/// The canonical `browser-minimal` top-level inclusion list.
///
/// These are the *top-level* fields the gnomAD browser variant API actually
/// returns, derived from the columns the browser backend reads in
/// `gnomad-browser-lite/backend/src/backend/duckdb.rs`:
///
/// - region/list view (`get_variants`): `locus`, `alleles`, `rsids`, `variant_id`,
///   `exome.freq`, `genome.freq`, `transcript_consequences`.
/// - variant-detail view (`get_variant_detail`): additionally `caid`, the full
///   `exome` / `genome` / `joint` structs (all population-stratified frequencies
///   shown in the detail page), `in_silico_predictors`, and `coverage`.
///
/// Each entry is kept *in full*: the browser serializes whole structs (e.g. every
/// ancestry-group frequency under `exome`/`genome`), so projecting to whole
/// top-level fields — rather than individual leaves — is intentional and matches
/// the API payload. The giant unused leaves dropped here are the raw `vep` array,
/// the VCF `info` struct, and the quality `histograms` struct, which together
/// account for the bulk of the ~238 leaves in the full schema.
///
/// **Strictness / schema differences:** the gnomAD v4 *exomes* and *genomes* sites
/// tables do not carry identical top-level fields (e.g. coverage may live in a
/// separate joined table). Building the projection with [`Projection::browser_minimal`]
/// is strict and will fail validation if a listed field is absent; use
/// [`Projection::browser_minimal_present_in`] to intersect this allowlist with a
/// concrete schema when extracting from a table that lacks some fields.
pub const BROWSER_MINIMAL_FIELDS: &[&str] = &[
    "locus",
    "alleles",
    "variant_id",
    "rsids",
    "caid",
    "exome",
    "genome",
    "joint",
    "transcript_consequences",
    "in_silico_predictors",
    "coverage",
];

impl SchemaWidth {
    /// Parse a width name (`full` or `browser-minimal`, underscores accepted).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim() {
            "full" => Ok(SchemaWidth::Full),
            "browser-minimal" | "browser_minimal" => Ok(SchemaWidth::BrowserMinimal),
            other => Err(HailError::InvalidFormat(format!(
                "Unknown schema width '{other}'. Expected 'full' or 'browser-minimal'."
            ))),
        }
    }

    /// The string name for this width (inverse of [`SchemaWidth::parse`]).
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaWidth::Full => "full",
            SchemaWidth::BrowserMinimal => "browser-minimal",
        }
    }

    /// Build the projection for this width.
    ///
    /// `Full` => `None` (decode everything, no projection applied).
    /// `BrowserMinimal` => the strict [`Projection::browser_minimal`] inclusion list.
    pub fn projection(&self) -> Option<Projection> {
        match self {
            SchemaWidth::Full => None,
            SchemaWidth::BrowserMinimal => Some(Projection::browser_minimal()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encoded_type::{EncodedField, EncodedType, EncodedValue};

    fn sample_schema() -> EncodedType {
        EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "locus".to_string(),
                    encoded_type: EncodedType::EBaseStruct {
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
                    },
                    index: 0,
                },
                EncodedField {
                    name: "alleles".to_string(),
                    encoded_type: EncodedType::EArray {
                        required: true,
                        element: Box::new(EncodedType::EBinary { required: true }),
                    },
                    index: 1,
                },
                EncodedField {
                    name: "freq".to_string(),
                    encoded_type: EncodedType::EArray {
                        required: true,
                        element: Box::new(EncodedType::EBaseStruct {
                            required: true,
                            fields: vec![
                                EncodedField {
                                    name: "AF".to_string(),
                                    encoded_type: EncodedType::EFloat64 { required: true },
                                    index: 0,
                                },
                                EncodedField {
                                    name: "AC".to_string(),
                                    encoded_type: EncodedType::EInt32 { required: true },
                                    index: 1,
                                },
                            ],
                        }),
                    },
                    index: 2,
                },
                EncodedField {
                    name: "vep".to_string(),
                    encoded_type: EncodedType::EBaseStruct {
                        required: false,
                        fields: vec![],
                    },
                    index: 3,
                },
            ],
        }
    }

    fn sample_row() -> EncodedValue {
        EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    (
                        "contig".to_string(),
                        EncodedValue::Binary(b"chr17".to_vec()),
                    ),
                    ("position".to_string(), EncodedValue::Int32(43044295)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary(b"A".to_vec()),
                    EncodedValue::Binary(b"G".to_vec()),
                ]),
            ),
            (
                "freq".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Struct(vec![
                        ("AF".to_string(), EncodedValue::Float64(0.01)),
                        ("AC".to_string(), EncodedValue::Int32(100)),
                    ]),
                    EncodedValue::Struct(vec![
                        ("AF".to_string(), EncodedValue::Float64(0.02)),
                        ("AC".to_string(), EncodedValue::Int32(200)),
                    ]),
                ]),
            ),
            ("vep".to_string(), EncodedValue::Null),
        ])
    }

    #[test]
    fn test_parse_simple_field() {
        let path = FieldPath::parse("locus").unwrap();
        assert_eq!(path.segments, vec![PathSegment::Field("locus".to_string())]);
    }

    #[test]
    fn test_parse_dotted_path() {
        let path = FieldPath::parse("freq.AF").unwrap();
        assert_eq!(
            path.segments,
            vec![
                PathSegment::Field("freq".to_string()),
                PathSegment::Field("AF".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_array_index() {
        let path = FieldPath::parse("vep.transcript_consequences[0]").unwrap();
        assert_eq!(
            path.segments,
            vec![
                PathSegment::Field("vep".to_string()),
                PathSegment::Field("transcript_consequences".to_string()),
                PathSegment::Index(0),
            ]
        );
    }

    #[test]
    fn test_parse_array_slice() {
        let path = FieldPath::parse("vep.transcript_consequences[:3]").unwrap();
        assert_eq!(
            path.segments,
            vec![
                PathSegment::Field("vep".to_string()),
                PathSegment::Field("transcript_consequences".to_string()),
                PathSegment::Slice(None, Some(3)),
            ]
        );
    }

    #[test]
    fn test_parse_negative_index() {
        let path = FieldPath::parse("arr[-1]").unwrap();
        assert_eq!(
            path.segments,
            vec![
                PathSegment::Field("arr".to_string()),
                PathSegment::Index(-1),
            ]
        );
    }

    #[test]
    fn test_parse_empty_fails() {
        assert!(FieldPath::parse("").is_err());
    }

    #[test]
    fn test_parse_trailing_dot_fails() {
        assert!(FieldPath::parse("foo.").is_err());
    }

    #[test]
    fn test_validate_valid_paths() {
        let schema = sample_schema();
        let paths = vec![
            FieldPath::parse("locus").unwrap(),
            FieldPath::parse("alleles").unwrap(),
            FieldPath::parse("freq.AF").unwrap(),
        ];
        let tree = ProjectionTree::from_fields(&paths);
        assert!(tree.validate_against_schema(&schema).is_ok());
    }

    #[test]
    fn test_validate_invalid_field() {
        let schema = sample_schema();
        let paths = vec![FieldPath::parse("nonexistent").unwrap()];
        let tree = ProjectionTree::from_fields(&paths);
        assert!(tree.validate_against_schema(&schema).is_err());
    }

    #[test]
    fn test_validate_invalid_nested_field() {
        let schema = sample_schema();
        let paths = vec![FieldPath::parse("locus.nonexistent").unwrap()];
        let tree = ProjectionTree::from_fields(&paths);
        assert!(tree.validate_against_schema(&schema).is_err());
    }

    #[test]
    fn test_project_top_level_fields() {
        let row = sample_row();
        let paths = vec![
            FieldPath::parse("locus").unwrap(),
            FieldPath::parse("alleles").unwrap(),
        ];
        let tree = ProjectionTree::from_fields(&paths);
        let projected = tree.project(&row);

        if let EncodedValue::Struct(fields) = &projected {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "locus");
            assert_eq!(fields[1].0, "alleles");
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_project_nested_field() {
        let row = sample_row();
        let paths = vec![
            FieldPath::parse("locus").unwrap(),
            FieldPath::parse("freq.AF").unwrap(),
        ];
        let tree = ProjectionTree::from_fields(&paths);
        let projected = tree.project(&row);

        if let EncodedValue::Struct(fields) = &projected {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "locus");
            assert_eq!(fields[1].0, "freq");
            // freq should be an array of structs with only AF
            if let EncodedValue::Array(elems) = &fields[1].1 {
                if let EncodedValue::Struct(freq_fields) = &elems[0] {
                    assert_eq!(freq_fields.len(), 1);
                    assert_eq!(freq_fields[0].0, "AF");
                } else {
                    panic!("Expected struct inside freq array");
                }
            } else {
                panic!("Expected array for freq");
            }
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_project_array_index() {
        let row = sample_row();
        let paths = vec![FieldPath::parse("freq[0]").unwrap()];
        let tree = ProjectionTree::from_fields(&paths);
        let projected = tree.project(&row);

        if let EncodedValue::Struct(fields) = &projected {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, "freq");
            // freq[0] should return the first element directly
            if let EncodedValue::Struct(freq_fields) = &fields[0].1 {
                assert_eq!(freq_fields.len(), 2); // AF and AC
            } else {
                panic!("Expected struct from freq[0], got {:?}", fields[0].1);
            }
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_project_array_slice() {
        let row = sample_row();
        let paths = vec![FieldPath::parse("freq[:1]").unwrap()];
        let tree = ProjectionTree::from_fields(&paths);
        let projected = tree.project(&row);

        if let EncodedValue::Struct(fields) = &projected {
            assert_eq!(fields.len(), 1);
            if let EncodedValue::Array(elems) = &fields[0].1 {
                assert_eq!(elems.len(), 1); // Only first element
            } else {
                panic!("Expected array for freq[:1]");
            }
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_exclude_fields() {
        let row = sample_row();
        let excluded = vec!["vep".to_string(), "freq".to_string()];
        let result = exclude_fields(&row, &excluded);

        if let EncodedValue::Struct(fields) = &result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "locus");
            assert_eq!(fields[1].0, "alleles");
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_validate_exclude_valid() {
        let schema = sample_schema();
        let excluded = vec!["vep".to_string()];
        assert!(validate_exclude_fields(&excluded, &schema).is_ok());
    }

    #[test]
    fn test_validate_exclude_invalid() {
        let schema = sample_schema();
        let excluded = vec!["nonexistent".to_string()];
        assert!(validate_exclude_fields(&excluded, &schema).is_err());
    }

    #[test]
    fn test_projection_from_fields_str() {
        let proj = Projection::from_fields_str("locus,alleles,freq.AF").unwrap();
        let row = sample_row();
        let result = proj.apply(&row);
        if let EncodedValue::Struct(fields) = &result {
            assert_eq!(fields.len(), 3);
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_projection_from_exclude_str() {
        let proj = Projection::from_exclude_str("vep,freq").unwrap();
        let row = sample_row();
        let result = proj.apply(&row);
        if let EncodedValue::Struct(fields) = &result {
            assert_eq!(fields.len(), 2);
        } else {
            panic!("Expected struct");
        }
    }

    #[test]
    fn test_schema_width_parse_roundtrip() {
        assert_eq!(SchemaWidth::parse("full").unwrap(), SchemaWidth::Full);
        assert_eq!(
            SchemaWidth::parse("browser-minimal").unwrap(),
            SchemaWidth::BrowserMinimal
        );
        // underscore alias + surrounding whitespace
        assert_eq!(
            SchemaWidth::parse(" browser_minimal ").unwrap(),
            SchemaWidth::BrowserMinimal
        );
        assert!(SchemaWidth::parse("minimal").is_err());
        assert_eq!(SchemaWidth::Full.as_str(), "full");
        assert_eq!(SchemaWidth::BrowserMinimal.as_str(), "browser-minimal");
    }

    #[test]
    fn test_full_width_means_no_projection() {
        assert!(SchemaWidth::Full.projection().is_none());
        assert!(SchemaWidth::BrowserMinimal.projection().is_some());
    }

    #[test]
    fn test_browser_minimal_keeps_kept_drops_rest() {
        // A row with browser fields plus a giant `vep`/`info`-like field that must be dropped.
        let row = EncodedValue::Struct(vec![
            (
                "locus".to_string(),
                EncodedValue::Struct(vec![
                    ("contig".to_string(), EncodedValue::Binary(b"chr22".to_vec())),
                    ("position".to_string(), EncodedValue::Int32(16050075)),
                ]),
            ),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![
                    EncodedValue::Binary(b"A".to_vec()),
                    EncodedValue::Binary(b"G".to_vec()),
                ]),
            ),
            ("variant_id".to_string(), EncodedValue::Binary(b"22-16050075-A-G".to_vec())),
            (
                "exome".to_string(),
                EncodedValue::Struct(vec![(
                    "freq".to_string(),
                    EncodedValue::Struct(vec![(
                        "all".to_string(),
                        EncodedValue::Struct(vec![
                            ("ac".to_string(), EncodedValue::Int32(3)),
                            ("an".to_string(), EncodedValue::Int32(100)),
                        ]),
                    )]),
                )]),
            ),
            // Fields that browser-minimal must drop:
            ("vep".to_string(), EncodedValue::Array(vec![EncodedValue::Int32(0)])),
            ("info".to_string(), EncodedValue::Struct(vec![])),
            ("histograms".to_string(), EncodedValue::Struct(vec![])),
        ]);

        let projected = Projection::browser_minimal().apply(&row);
        if let EncodedValue::Struct(fields) = &projected {
            let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
            // Kept (present-and-in-allowlist):
            assert!(names.contains(&"locus"));
            assert!(names.contains(&"alleles"));
            assert!(names.contains(&"variant_id"));
            assert!(names.contains(&"exome"));
            // Dropped:
            assert!(!names.contains(&"vep"));
            assert!(!names.contains(&"info"));
            assert!(!names.contains(&"histograms"));
            // `exome` is kept whole (the API returns all population frequencies).
            let exome = &fields.iter().find(|(n, _)| n == "exome").unwrap().1;
            if let EncodedValue::Struct(exome_fields) = exome {
                assert_eq!(exome_fields[0].0, "freq");
            } else {
                panic!("expected exome struct kept whole");
            }
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_browser_minimal_present_in_is_schema_tolerant() {
        // Schema with only locus + alleles (no coverage/joint/etc.).
        let schema = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "locus".to_string(),
                    encoded_type: EncodedType::EBinary { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "alleles".to_string(),
                    encoded_type: EncodedType::EArray {
                        required: true,
                        element: Box::new(EncodedType::EBinary { required: true }),
                    },
                    index: 1,
                },
            ],
        };
        // Strict variant references absent fields -> validation fails.
        assert!(Projection::browser_minimal().validate(&schema).is_err());
        // Schema-tolerant variant only references present fields -> validates.
        let proj = Projection::browser_minimal_present_in(&schema);
        assert!(proj.validate(&schema).is_ok());
    }

    #[test]
    fn test_parent_path_subsumes_child() {
        // If user requests both "locus" and "locus.contig", just "locus" should win
        let paths = vec![
            FieldPath::parse("locus").unwrap(),
            FieldPath::parse("locus.contig").unwrap(),
        ];
        let tree = ProjectionTree::from_fields(&paths);
        let row = sample_row();
        let projected = tree.project(&row);

        if let EncodedValue::Struct(fields) = &projected {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, "locus");
            // Should have both contig and position since "locus" selects all
            if let EncodedValue::Struct(locus_fields) = &fields[0].1 {
                assert_eq!(locus_fields.len(), 2);
            } else {
                panic!("Expected struct for locus");
            }
        } else {
            panic!("Expected struct");
        }
    }
}
