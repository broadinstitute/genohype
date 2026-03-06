//! ClickHouse schema definitions for Manhattan ingestion tables.
//!
//! This module provides the CREATE TABLE DDL statements for all tables
//! used in the Manhattan ingestion pipeline. These schemas are the
//! source of truth for table creation and should be kept in sync with
//! the Arrow schemas used in `manhattan.rs`.

/// Returns all Manhattan ingestion table schemas.
///
/// Each tuple contains (table_name, create_sql).
/// These are ordered by dependency (tables should be created in this order).
pub fn get_manhattan_schemas() -> Vec<(&'static str, &'static str)> {
    vec![
        ("loci", include_str!("sql/loci.sql")),
        ("loci_variants", include_str!("sql/loci_variants.sql")),
        (
            "significant_variants",
            include_str!("sql/significant_variants.sql"),
        ),
        ("phenotype_plots", include_str!("sql/phenotype_plots.sql")),
        (
            "gene_associations",
            include_str!("sql/gene_associations.sql"),
        ),
        ("qq_points", include_str!("sql/qq_points.sql")),
        ("pipeline_status", include_str!("sql/pipeline_status.sql")),
    ]
}

/// Returns just the table names for Manhattan ingestion.
pub fn get_manhattan_table_names() -> Vec<&'static str> {
    get_manhattan_schemas()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_manhattan_schemas() {
        let schemas = get_manhattan_schemas();
        assert_eq!(schemas.len(), 7);

        // Check all expected tables are present
        let table_names: Vec<_> = schemas.iter().map(|(name, _)| *name).collect();
        assert!(table_names.contains(&"loci"));
        assert!(table_names.contains(&"loci_variants"));
        assert!(table_names.contains(&"significant_variants"));
        assert!(table_names.contains(&"phenotype_plots"));
        assert!(table_names.contains(&"gene_associations"));
        assert!(table_names.contains(&"qq_points"));
        assert!(table_names.contains(&"pipeline_status"));
    }

    #[test]
    fn test_schemas_are_valid_sql() {
        for (name, sql) in get_manhattan_schemas() {
            // Basic sanity checks
            assert!(
                sql.contains("CREATE TABLE IF NOT EXISTS"),
                "Table {} missing CREATE TABLE",
                name
            );
            assert!(sql.contains("ENGINE ="), "Table {} missing ENGINE", name);
            assert!(sql.contains("ORDER BY"), "Table {} missing ORDER BY", name);
        }
    }
}
