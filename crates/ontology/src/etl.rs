//! ETL configuration types for the Knowledge Graph indexer.
//!
//! These types define how data is extracted, transformed, and loaded from
//! source tables into the Knowledge Graph.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EtlScope {
    Global,
    Namespaced,
}

/// Direction of an edge relative to the node defining the FK column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Deserialize)]
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
    Column {
        column: String,
        type_mapping: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeMapping {
    pub target: EdgeTarget,
    pub relationship_kind: String,
    pub direction: EdgeDirection,
    pub delimiter: Option<String>,
    pub array_field: Option<String>,
    pub array: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtlConfig {
    Table {
        scope: EtlScope,
        source: String,
        watermark: String,
        deleted: String,
        order_by: Vec<String>,
        /// Edges keyed by source column name. Each column may declare one or
        /// more mappings.
        edges: BTreeMap<String, Vec<EdgeMapping>>,
    },
    Query {
        scope: EtlScope,
        /// Complete SQL template with `{CURSOR}` and `{BATCH_SIZE}` markers.
        /// ClickHouse params like `{last_watermark:String}` stay for runtime binding.
        extract: String,
        /// Columns used for cursor-based keyset pagination (ORDER BY).
        sort_keys: Vec<String>,
        /// Base source table name, used by enrichment CTEs that look up
        /// properties from this node on behalf of standalone edge ETLs.
        source: String,
        /// Watermark column for incremental processing. Kept for enrichment
        /// CTEs that other ETLs build against this node.
        watermark: String,
        /// Deleted-flag expression. Kept for enrichment CTEs.
        deleted: String,
        /// Alias of the main table in the extraction query.
        /// Used by enrichment CTEs to qualify bare column references.
        table_alias: Option<String>,
        /// Edges keyed by source column name. Each column may declare one or
        /// more mappings.
        edges: BTreeMap<String, Vec<EdgeMapping>>,
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

    /// Returns the main table alias for Query-type ETLs, if set.
    /// Table-type ETLs always return `None` (single table, no ambiguity).
    pub fn table_alias(&self) -> Option<&str> {
        match self {
            EtlConfig::Table { .. } => None,
            EtlConfig::Query { table_alias, .. } => table_alias.as_deref(),
        }
    }

    pub fn order_by(&self) -> &[String] {
        match self {
            EtlConfig::Table { order_by, .. } => order_by,
            EtlConfig::Query { sort_keys, .. } => sort_keys,
        }
    }

    pub fn edges(&self) -> &BTreeMap<String, Vec<EdgeMapping>> {
        match self {
            EtlConfig::Table { edges, .. } => edges,
            EtlConfig::Query { edges, .. } => edges,
        }
    }

    pub fn edge_mappings(&self) -> impl Iterator<Item = (&String, &EdgeMapping)> + '_ {
        self.edges()
            .iter()
            .flat_map(|(col, mappings)| mappings.iter().map(move |m| (col, m)))
    }

    pub fn has_edges(&self) -> bool {
        self.edges().values().any(|v| !v.is_empty())
    }

    pub fn validate_query_parameters(&self) -> Vec<&'static str> {
        let EtlConfig::Query { scope, extract, .. } = self else {
            return Vec::new();
        };

        let mut missing = Vec::new();

        if !extract.contains("{CURSOR}") {
            missing.push("{CURSOR}");
        }
        if !extract.contains("{BATCH_SIZE}") {
            missing.push("{BATCH_SIZE}");
        }
        if !extract.contains("{last_watermark:String}") {
            missing.push("{last_watermark:String}");
        }
        if *scope == EtlScope::Namespaced && !extract.contains("{traversal_path:String}") {
            missing.push("{traversal_path:String}");
        }

        missing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn global_extract_template() -> String {
        "SELECT id, name, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
         FROM source_table \
         WHERE _siphon_replicated_at > {last_watermark:String} \
         AND _siphon_replicated_at <= {watermark:String}\
         {CURSOR} ORDER BY id LIMIT {BATCH_SIZE}"
            .to_string()
    }

    fn namespaced_extract_template() -> String {
        "SELECT id, name, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
         FROM source_table \
         WHERE _siphon_replicated_at > {last_watermark:String} \
         AND _siphon_replicated_at <= {watermark:String} \
         AND startsWith(traversal_path, {traversal_path:String})\
         {CURSOR} ORDER BY id LIMIT {BATCH_SIZE}"
            .to_string()
    }

    fn query_config(scope: EtlScope, extract: &str) -> EtlConfig {
        EtlConfig::Query {
            scope,
            extract: extract.to_string(),
            sort_keys: vec!["id".to_string()],
            source: "source_table".to_string(),
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            table_alias: None,
            edges: BTreeMap::new(),
        }
    }

    #[test]
    fn validate_query_parameters_passes_for_global_query() {
        let config = query_config(EtlScope::Global, &global_extract_template());
        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_passes_for_namespaced_query_with_all_markers() {
        let config = query_config(EtlScope::Namespaced, &namespaced_extract_template());
        assert!(config.validate_query_parameters().is_empty());
    }

    #[test]
    fn validate_fails_for_missing_cursor_marker() {
        let extract = "SELECT id FROM t WHERE _siphon_replicated_at > {last_watermark:String} \
                        AND _siphon_replicated_at <= {watermark:String} \
                        ORDER BY id LIMIT {BATCH_SIZE}";
        let config = query_config(EtlScope::Global, extract);
        let missing = config.validate_query_parameters();
        assert!(missing.contains(&"{CURSOR}"));
    }

    #[test]
    fn validate_fails_for_missing_batch_size_marker() {
        let extract = "SELECT id FROM t WHERE _siphon_replicated_at > {last_watermark:String} \
                        AND _siphon_replicated_at <= {watermark:String}\
                        {CURSOR} ORDER BY id";
        let config = query_config(EtlScope::Global, extract);
        let missing = config.validate_query_parameters();
        assert!(missing.contains(&"{BATCH_SIZE}"));
    }

    #[test]
    fn validate_fails_for_missing_watermark_marker() {
        let extract = "SELECT id FROM t {CURSOR} ORDER BY id LIMIT {BATCH_SIZE}";
        let config = query_config(EtlScope::Global, extract);
        let missing = config.validate_query_parameters();
        assert!(missing.contains(&"{last_watermark:String}"));
    }

    #[test]
    fn validate_fails_for_namespaced_without_traversal_path() {
        let extract = "SELECT id FROM t \
                        WHERE _siphon_replicated_at > {last_watermark:String} \
                        AND _siphon_replicated_at <= {watermark:String}\
                        {CURSOR} ORDER BY id LIMIT {BATCH_SIZE}";
        let config = query_config(EtlScope::Namespaced, extract);
        let missing = config.validate_query_parameters();
        assert!(missing.contains(&"{traversal_path:String}"));
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

        let query = query_config(EtlScope::Global, &global_extract_template());
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

        let query = query_config(EtlScope::Global, &global_extract_template());
        assert_eq!(query.watermark(), "_siphon_replicated_at");
    }

    #[test]
    fn order_by_returns_sort_keys_for_query_type() {
        let config = query_config(EtlScope::Global, &global_extract_template());
        assert_eq!(config.order_by(), &["id".to_string()]);
    }
}
