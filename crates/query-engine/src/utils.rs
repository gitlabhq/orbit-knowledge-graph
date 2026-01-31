//! Column naming conventions for query results.
//!
//! Centralizes the naming patterns for mandatory columns used in redaction.

/// Column name for a node's ID (single value).
pub fn id_column(alias: &str) -> String {
    format!("_gkg_{alias}_id")
}

/// Column name for a node's type (single value).
pub fn type_column(alias: &str) -> String {
    format!("_gkg_{alias}_type")
}

/// Column name for aggregated entity IDs (Array of Int64).
pub fn ids_array_column(alias: &str) -> String {
    format!("_gkg_{alias}_ids")
}
