//! ETL configuration types for the Knowledge Graph indexer.
//!
//! These types define how data is extracted, transformed, and loaded from
//! source tables into the Knowledge Graph.

use std::collections::BTreeMap;

/// Scope of ETL processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtlScope {
    /// Global scope - processes all records with a single global watermark.
    Global,
    /// Namespaced scope - processes records per-namespace with per-namespace watermarks.
    Namespaced,
}

/// ETL configuration for a node entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtlConfig {
    /// Simple table-based extraction.
    Table {
        scope: EtlScope,
        source: String,
        watermark: String,
        deleted: Option<String>,
    },
    /// Complex query-based extraction (for JOINs, etc.).
    Query {
        scope: EtlScope,
        source: String,
        watermark: String,
        deleted: Option<String>,
        query: String,
    },
}

impl EtlConfig {
    /// Get the scope of this ETL configuration.
    #[must_use]
    pub fn scope(&self) -> EtlScope {
        match self {
            EtlConfig::Table { scope, .. } | EtlConfig::Query { scope, .. } => *scope,
        }
    }

    /// Get the source table name.
    #[must_use]
    pub fn source(&self) -> &str {
        match self {
            EtlConfig::Table { source, .. } | EtlConfig::Query { source, .. } => source,
        }
    }

    /// Get the watermark column name.
    #[must_use]
    pub fn watermark(&self) -> &str {
        match self {
            EtlConfig::Table { watermark, .. } | EtlConfig::Query { watermark, .. } => watermark,
        }
    }

    /// Get the deleted column name, if any.
    #[must_use]
    pub fn deleted(&self) -> Option<&str> {
        match self {
            EtlConfig::Table { deleted, .. } | EtlConfig::Query { deleted, .. } => {
                deleted.as_deref()
            }
        }
    }

    /// Get the custom query, if this is a Query type.
    #[must_use]
    pub fn query(&self) -> Option<&str> {
        match self {
            EtlConfig::Table { .. } => None,
            EtlConfig::Query { query, .. } => Some(query),
        }
    }

    /// Validate that the query contains all required parameters for its scope.
    ///
    /// For Query types:
    /// - All scopes require `{last_watermark:String}` and `{watermark:String}`
    /// - Namespaced scope also requires `{traversal_path:String}`
    ///
    /// Returns a list of missing parameters, or an empty vec if valid.
    #[must_use]
    pub fn validate_query_parameters(&self) -> Vec<&'static str> {
        let EtlConfig::Query { scope, query, .. } = self else {
            return Vec::new();
        };

        let mut missing = Vec::new();

        if !query.contains("{last_watermark:String}") {
            missing.push("{last_watermark:String}");
        }
        if !query.contains("{watermark:String}") {
            missing.push("{watermark:String}");
        }
        if *scope == EtlScope::Namespaced && !query.contains("{traversal_path:String}") {
            missing.push("{traversal_path:String}");
        }

        missing
    }
}

/// Configuration for generating edges from source data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeGenerationConfig {
    /// Type of relationship (e.g., "owner", "contains", "creator").
    pub relationship_type: String,
    /// Column in source data containing the related entity ID.
    pub source_column: String,
    /// Node type of the source entity (e.g., "User", "Group").
    pub source_kind: String,
}

/// Configuration for a property, including source mapping and enum values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyConfig {
    /// The data type of the property (e.g., "string", "int64", "enum").
    pub property_type: String,
    /// The source column name in the database.
    pub source: String,
    /// Whether the property can be null.
    pub nullable: bool,
    /// For enum types, mapping of integer values to string labels.
    pub values: Option<BTreeMap<i64, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_query_parameters_returns_empty_for_table_type() {
        let config = EtlConfig::Table {
            scope: EtlScope::Global,
            source: "test_table".to_string(),
            watermark: "_replicated_at".to_string(),
            deleted: None,
        };

        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_returns_empty_for_valid_global_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Global,
            source: "test_table".to_string(),
            watermark: "_replicated_at".to_string(),
            deleted: None,
            query: "SELECT * FROM t WHERE x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
        };

        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_returns_empty_for_valid_namespaced_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Namespaced,
            source: "test_table".to_string(),
            watermark: "_replicated_at".to_string(),
            deleted: None,
            query: "SELECT * FROM t WHERE path LIKE {traversal_path:String} AND x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
        };

        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_returns_missing_for_global_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Global,
            source: "test_table".to_string(),
            watermark: "_replicated_at".to_string(),
            deleted: None,
            query: "SELECT * FROM t".to_string(),
        };

        let missing = config.validate_query_parameters();
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&"{last_watermark:String}"));
        assert!(missing.contains(&"{watermark:String}"));
    }

    #[test]
    fn validate_query_parameters_returns_missing_traversal_path_for_namespaced_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Namespaced,
            source: "test_table".to_string(),
            watermark: "_replicated_at".to_string(),
            deleted: None,
            query: "SELECT * FROM t WHERE x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
        };

        let missing = config.validate_query_parameters();
        assert_eq!(missing.len(), 1);
        assert!(missing.contains(&"{traversal_path:String}"));
    }
}
