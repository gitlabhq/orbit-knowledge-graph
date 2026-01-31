//! Result context for query execution validation.

use std::collections::HashMap;

use crate::utils::{id_column, ids_array_column, type_column};
use crate::input::QueryType;

/// Column name for the typed path array in path finding queries.
/// Contains Array(Tuple(Int64, String)) with (node_id, entity_type) for each step.
pub const PATH_COLUMN: &str = "_gkg_path";

/// Column names for neighbor queries. The neighbor's ID and type are dynamic
/// (could be any entity type), similar to path finding nodes.
pub const NEIGHBOR_ID_COLUMN: &str = "_gkg_neighbor_id";
pub const NEIGHBOR_TYPE_COLUMN: &str = "_gkg_neighbor_type";

pub const RELATIONSHIP_TYPE_COLUMN: &str = "_gkg_relationship_type";

/// A node whose individual rows are returned (group_by nodes, traversal nodes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionNode {
    pub alias: String,
    pub entity_type: String,
    pub id_column: String,
    pub type_column: String,
}

/// A node whose IDs are collected into an array (aggregation targets).
/// All IDs in the array must be authorized for the row to pass redaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatedNode {
    pub alias: String,
    pub entity_type: String,
    pub ids_column: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
    aggregated_nodes: HashMap<String, AggregatedNode>,
}

impl ResultContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query_type = Some(query_type);
        self
    }

    pub fn add_node(&mut self, alias: &str, entity_type: &str) {
        self.nodes.insert(
            alias.to_string(),
            RedactionNode {
                alias: alias.to_string(),
                entity_type: entity_type.to_string(),
                id_column: id_column(alias),
                type_column: type_column(alias),
            },
        );
    }

    /// Add an aggregated node whose IDs are collected in an array.
    pub fn add_aggregated_node(&mut self, alias: &str, entity_type: &str) {
        self.aggregated_nodes.insert(
            alias.to_string(),
            AggregatedNode {
                alias: alias.to_string(),
                entity_type: entity_type.to_string(),
                ids_column: ids_array_column(alias),
            },
        );
    }

    pub fn nodes(&self) -> impl Iterator<Item = &RedactionNode> {
        self.nodes.values()
    }

    pub fn aggregated_nodes(&self) -> impl Iterator<Item = &AggregatedNode> {
        self.aggregated_nodes.values()
    }

    pub fn get(&self, alias: &str) -> Option<&RedactionNode> {
        self.nodes.get(alias)
    }

    pub fn get_aggregated(&self, alias: &str) -> Option<&AggregatedNode> {
        self.aggregated_nodes.get(alias)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.aggregated_nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn aggregated_len(&self) -> usize {
        self.aggregated_nodes.len()
    }
}
