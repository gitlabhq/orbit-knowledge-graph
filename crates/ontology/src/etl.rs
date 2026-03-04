//! ETL configuration types for the Knowledge Graph indexer.
//!
//! These types define how data is extracted, transformed, and loaded from
//! source tables into the Knowledge Graph.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtlScope {
    Global,
    Namespaced,
}

/// Direction of an edge relative to the node defining the FK column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeDirection {
    /// Node with FK is edge source: (this_node) -[edge]-> (fk_target)
    #[default]
    Outgoing,
    /// Node with FK is edge target: (fk_target) -[edge]-> (this_node)
    Incoming,
}

/// How the edge target type is determined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeTarget {
    /// A fixed node type (e.g., "User").
    Literal(String),
    /// Type read from a column at runtime (e.g., "noteable_type").
    Column(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeMapping {
    pub target: EdgeTarget,
    pub relationship_kind: String,
    pub direction: EdgeDirection,
    pub delimiter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtlConfig {
    Table {
        scope: EtlScope,
        source: String,
        watermark: String,
        deleted: String,
        /// Columns for ORDER BY in extract queries and cursor-based pagination.
        order_by: Vec<String>,
        /// Edges to generate from columns. Key is the source column name.
        edges: BTreeMap<String, EdgeMapping>,
    },
    Query {
        scope: EtlScope,
        /// Column expressions for the SELECT clause.
        select: String,
        /// Table or JOIN expression for the FROM clause.
        from: String,
        /// Extra WHERE conditions beyond watermark filtering.
        /// May contain ClickHouse parameter placeholders like {traversal_path:String}.
        where_clause: Option<String>,
        /// Column used for incremental processing watermark.
        watermark: String,
        /// Column indicating soft-deleted records.
        deleted: String,
        /// Columns for ORDER BY in extract queries and cursor-based pagination.
        order_by: Vec<String>,
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

    pub fn deleted(&self) -> &str {
        match self {
            EtlConfig::Table { deleted, .. } => deleted.as_str(),
            EtlConfig::Query { deleted, .. } => deleted.as_str(),
        }
    }

    pub fn watermark(&self) -> &str {
        match self {
            EtlConfig::Table { watermark, .. } => watermark.as_str(),
            EtlConfig::Query { watermark, .. } => watermark.as_str(),
        }
    }

    pub fn order_by(&self) -> &[String] {
        match self {
            EtlConfig::Table { order_by, .. } => order_by,
            EtlConfig::Query { order_by, .. } => order_by,
        }
    }

    pub fn edges(&self) -> &BTreeMap<String, EdgeMapping> {
        match self {
            EtlConfig::Table { edges, .. } => edges,
            EtlConfig::Query { edges, .. } => edges,
        }
    }

    pub fn validate_query_parameters(&self) -> Vec<&'static str> {
        let EtlConfig::Query {
            scope,
            where_clause,
            ..
        } = self
        else {
            return Vec::new();
        };

        if *scope == EtlScope::Namespaced {
            let has_traversal_path = where_clause
                .as_deref()
                .is_some_and(|w| w.contains("{traversal_path:String}"));

            if !has_traversal_path {
                return vec!["{traversal_path:String}"];
            }
        }

        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_config(scope: EtlScope, where_clause: Option<&str>) -> EtlConfig {
        EtlConfig::Query {
            scope,
            select: "id, name".to_string(),
            from: "source_table".to_string(),
            where_clause: where_clause.map(String::from),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        }
    }

    #[test]
    fn validate_query_parameters_passes_for_global_query() {
        let config = query_config(EtlScope::Global, None);
        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_passes_for_namespaced_query_with_traversal_path() {
        let config = query_config(
            EtlScope::Namespaced,
            Some("startsWith(traversal_path, {traversal_path:String})"),
        );
        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_query_parameters_fails_for_namespaced_query_without_traversal_path() {
        let config = query_config(EtlScope::Namespaced, Some("status = 'active'"));
        let missing = config.validate_query_parameters();
        assert_eq!(missing, vec!["{traversal_path:String}"]);
    }

    #[test]
    fn validate_query_parameters_fails_for_namespaced_query_with_no_where_clause() {
        let config = query_config(EtlScope::Namespaced, None);
        let missing = config.validate_query_parameters();
        assert_eq!(missing, vec!["{traversal_path:String}"]);
    }

    #[test]
    fn validate_query_parameters_skips_table_etl() {
        let config = EtlConfig::Table {
            scope: EtlScope::Namespaced,
            source: "t".to_string(),
            watermark: "w".to_string(),
            deleted: "d".to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        };
        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn deleted_returns_column_for_both_etl_types() {
        let table = EtlConfig::Table {
            scope: EtlScope::Global,
            source: "t".to_string(),
            watermark: "w".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        };
        assert_eq!(table.deleted(), "_siphon_deleted");

        let query = query_config(EtlScope::Global, None);
        assert_eq!(query.deleted(), "_siphon_deleted");
    }

    #[test]
    fn watermark_returns_column_for_both_etl_types() {
        let table = EtlConfig::Table {
            scope: EtlScope::Global,
            source: "t".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "d".to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        };
        assert_eq!(table.watermark(), "_siphon_replicated_at");

        let query = query_config(EtlScope::Global, None);
        assert_eq!(query.watermark(), "_siphon_replicated_at");
    }
}
