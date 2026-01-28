//! ETL configuration types for the Knowledge Graph indexer.
//!
//! These types define how data is extracted, transformed, and loaded from
//! source tables into the Knowledge Graph.

use std::collections::BTreeMap;

pub const VERSION_COLUMN: &str = "_version";
pub const DELETED_COLUMN: &str = "_deleted";
pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtlScope {
    Global,
    Namespaced,
}

/// Mapping from a source column to an edge in the knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeMapping {
    /// The target node type (e.g., "User", "Project")
    pub target_kind: String,
    /// The relationship name for the edge (e.g., "AUTHORED_BY", "BELONGS_TO")
    pub relationship_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtlConfig {
    Table {
        scope: EtlScope,
        source: String,
        watermark: String,
        deleted: String,
        /// Edges to generate from columns. Key is the source column name.
        edges: BTreeMap<String, EdgeMapping>,
    },
    Query {
        scope: EtlScope,
        query: String,
        /// Edges to generate from columns. Key is the source column name.
        edges: BTreeMap<String, EdgeMapping>,
    },
}

impl EtlConfig {
    pub fn scope(&self) -> EtlScope {
        match self {
            EtlConfig::Table { scope, .. } => *scope,
            EtlConfig::Query { scope, .. } => *scope,
        }
    }

    pub fn deleted(&self) -> Option<&str> {
        match self {
            EtlConfig::Table { deleted, .. } => Some(deleted.as_str()),
            EtlConfig::Query { .. } => None,
        }
    }

    pub fn edges(&self) -> &BTreeMap<String, EdgeMapping> {
        match self {
            EtlConfig::Table { edges, .. } => edges,
            EtlConfig::Query { edges, .. } => edges,
        }
    }

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

        if !query.contains(DELETED_COLUMN) {
            missing.push(DELETED_COLUMN);
        }

        if !query.contains(VERSION_COLUMN) {
            missing.push(VERSION_COLUMN);
        }

        missing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_query_parameters_returns_empty_for_valid_global_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Global,
            query: "SELECT _deleted, _version FROM t WHERE x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
            edges: BTreeMap::new(),
        };

        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_returns_empty_for_valid_namespaced_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Namespaced,
            query: "SELECT _deleted, _version FROM t WHERE path LIKE {traversal_path:String} AND x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
            edges: BTreeMap::new(),
        };

        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_returns_missing_for_global_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Global,
            query: "SELECT * FROM t".to_string(),
            edges: BTreeMap::new(),
        };

        let missing = config.validate_query_parameters();
        assert_eq!(missing.len(), 4);
        assert!(missing.contains(&"{last_watermark:String}"));
        assert!(missing.contains(&"{watermark:String}"));
        assert!(missing.contains(&"_deleted"));
        assert!(missing.contains(&"_version"));
    }

    #[test]
    fn validate_query_parameters_returns_missing_traversal_path_for_namespaced_query() {
        let config = EtlConfig::Query {
            scope: EtlScope::Namespaced,
            query: "SELECT _deleted, _version FROM t WHERE x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
            edges: BTreeMap::new(),
        };

        let missing = config.validate_query_parameters();
        assert_eq!(missing.len(), 1);
        assert!(missing.contains(&"{traversal_path:String}"));
    }

    #[test]
    fn validate_query_parameters_returns_missing_deleted_and_version() {
        let config = EtlConfig::Query {
            scope: EtlScope::Global,
            query: "SELECT * FROM t WHERE x > {last_watermark:String} AND x <= {watermark:String}"
                .to_string(),
            edges: BTreeMap::new(),
        };

        let missing = config.validate_query_parameters();
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&"_deleted"));
        assert!(missing.contains(&"_version"));
    }
}
