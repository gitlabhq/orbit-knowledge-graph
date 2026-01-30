//! Result context for query execution validation.

use std::collections::HashMap;

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
    nodes: HashMap<String, RedactionNode>,
}

impl ResultContext {
    pub fn new() -> Self {
        Self::default()
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
