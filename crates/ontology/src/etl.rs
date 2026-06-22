//! ETL configuration types for the Knowledge Graph indexer.
//!
//! These types define how data is extracted, transformed, and loaded from
//! source tables into the Knowledge Graph.

use std::collections::BTreeMap;

/// Default transform: the built-in SQL projection. Nodes and standalone edges
/// use it implicitly; derived entities must name a different one.
pub const DEFAULT_TRANSFORM: &str = "data_fusion";

/// A `query:` file is the complete extract, run verbatim — it drives its own
/// paging via these runtime markers (substituted per batch by the indexer)
/// rather than being wrapped. The loader requires both; the indexer keys the
/// verbatim-vs-table decision off the `EtlConfig` variant.
pub fn is_full_query(sql: &str) -> bool {
    sql.contains("{{filters}}") && sql.contains("{{batch_size}}")
}

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
    pub mutable: bool,
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
    /// A complete extract from a sibling `.sql` file, used verbatim. The file
    /// owns its own paging via the `{{filters}}`/`{{batch_size}}` markers and
    /// emits the `_version`/`_deleted` output columns itself.
    Verbatim {
        scope: EtlScope,
        source: String,
        sql: String,
        watermark: String,
        deleted: String,
        order_by: Vec<String>,
        /// Edges keyed by source column name. Each column may declare one or
        /// more mappings.
        edges: BTreeMap<String, Vec<EdgeMapping>>,
    },
}

impl EtlConfig {
    pub fn scope(&self) -> EtlScope {
        match self {
            EtlConfig::Table { scope, .. } => *scope,
            EtlConfig::Verbatim { scope, .. } => *scope,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            EtlConfig::Table { source, .. } => source,
            EtlConfig::Verbatim { source, .. } => source,
        }
    }

    pub fn deleted(&self) -> &str {
        match self {
            EtlConfig::Table { deleted, .. } => deleted.as_str(),
            EtlConfig::Verbatim { deleted, .. } => deleted.as_str(),
        }
    }

    pub fn watermark(&self) -> &str {
        match self {
            EtlConfig::Table { watermark, .. } => watermark.as_str(),
            EtlConfig::Verbatim { watermark, .. } => watermark.as_str(),
        }
    }

    pub fn order_by(&self) -> &[String] {
        match self {
            EtlConfig::Table { order_by, .. } => order_by,
            EtlConfig::Verbatim { order_by, .. } => order_by,
        }
    }

    pub fn edges(&self) -> &BTreeMap<String, Vec<EdgeMapping>> {
        match self {
            EtlConfig::Table { edges, .. } => edges,
            EtlConfig::Verbatim { edges, .. } => edges,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verbatim_config() -> EtlConfig {
        EtlConfig::Verbatim {
            scope: EtlScope::Global,
            source: "source_table".to_string(),
            sql: "SELECT id FROM source_table WHERE 1=1 {{filters}} LIMIT {{batch_size}}"
                .to_string(),
            watermark: crate::constants::siphon_watermark_column().to_string(),
            deleted: crate::constants::siphon_deleted_column().to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        }
    }

    #[test]
    fn deleted_returns_column_for_both_etl_types() {
        let table = EtlConfig::Table {
            scope: EtlScope::Global,
            source: "t".to_string(),
            watermark: "w".to_string(),
            deleted: crate::constants::siphon_deleted_column().to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        };
        assert_eq!(table.deleted(), crate::constants::siphon_deleted_column());
        assert_eq!(
            verbatim_config().deleted(),
            crate::constants::siphon_deleted_column()
        );
    }

    #[test]
    fn watermark_returns_column_for_both_etl_types() {
        let table = EtlConfig::Table {
            scope: EtlScope::Global,
            source: "t".to_string(),
            watermark: crate::constants::siphon_watermark_column().to_string(),
            deleted: "d".to_string(),
            order_by: vec!["id".to_string()],
            edges: BTreeMap::new(),
        };
        assert_eq!(
            table.watermark(),
            crate::constants::siphon_watermark_column()
        );
        assert_eq!(
            verbatim_config().watermark(),
            crate::constants::siphon_watermark_column()
        );
    }
}
