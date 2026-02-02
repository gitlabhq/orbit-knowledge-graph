//! Result context for query execution validation.

use std::collections::HashMap;

use crate::input::QueryType;

/// Column name for the typed path array in path finding queries.
/// Contains Array(Tuple(Int64, String)) with (node_id, entity_type) for each step.
pub const PATH_COLUMN: &str = "_gkg_path";

/// Column names for neighbor queries. The neighbor's ID and type are dynamic
/// (could be any entity type), similar to path finding nodes.
pub const NEIGHBOR_ID_COLUMN: &str = "_gkg_neighbor_id";
pub const NEIGHBOR_TYPE_COLUMN: &str = "_gkg_neighbor_type";

pub const RELATIONSHIP_TYPE_COLUMN: &str = "_gkg_relationship_type";

/// Column names for batch_search queries. Each row has a single entity type and ID.
pub const BATCH_ENTITY_TYPE_COLUMN: &str = "_gkg_entity_type";
pub const BATCH_ID_COLUMN: &str = "_gkg_id";

pub fn id_column(alias: &str) -> String {
    format!("_gkg_{alias}_id")
}

pub fn type_column(alias: &str) -> String {
    format!("_gkg_{alias}_type")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionNode {
    pub alias: String,
    pub entity_type: String,
    pub id_column: String,
    pub type_column: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
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

    pub fn nodes(&self) -> impl Iterator<Item = &RedactionNode> {
        self.nodes.values()
    }

    pub fn get(&self, alias: &str) -> Option<&RedactionNode> {
        self.nodes.get(alias)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}
