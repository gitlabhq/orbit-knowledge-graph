use super::requirements::{Column, OutputValue, Predicate, Projection, node_outputs, node_predicates};
use crate::error::{QueryError, Result};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, EDGE_RESERVED_COLUMNS, SOURCE_TAGS_COLUMN,
    TARGET_TAGS_COLUMN,
};

use super::{Hop, NodePlan};

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
}

#[derive(Clone)]
pub struct BindingSource {
    pub node: String,
    pub alias: String,
    pub column: String,
    pub joined: bool,
}

impl BindingSource {
    pub(super) fn column(&self, property: &str) -> Option<Column> {
        if property == DEFAULT_PRIMARY_KEY {
            Some(Column::new(&self.alias, &self.column))
        } else {
            self.joined.then(|| Column::new(&self.node, property))
        }
    }

    pub(super) fn table(alias: &str) -> Self {
        Self {
            node: alias.into(),
            alias: alias.into(),
            column: DEFAULT_PRIMARY_KEY.into(),
            joined: true,
        }
    }
}

#[derive(Clone)]
pub struct PhysicalPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<Projection>,
}

#[derive(Clone)]
pub enum PhysicalSource {
    KeyFilter {
        value: Column,
        keys: Box<PhysicalPlan>,
        input: Box<Self>,
    },
    Union {
        alias: String,
        arms: Vec<PhysicalPlan>,
        relationship: usize,
    },
    Scan {
        table: String,
        alias: String,
        final_: bool,
        relationship: Option<usize>,
    },
    Filter {
        predicates: Vec<Predicate>,
        input: Box<Self>,
    },
    Scope {
        alias: String,
        input: Box<Self>,
    },
    Join {
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
        left: Box<Self>,
        right: Box<Self>,
    },
    Latest {
        sort_key: Vec<String>,
        alias: String,
        aggregate_condition: Vec<Predicate>,
        input: Box<Self>,
    },
}

impl PhysicalSource {
    pub(super) fn edge(hop: &Hop, alias: &str, final_: bool) -> Self {
        if let Some(table) = hop.single_edge_table() {
            return Self::Scan {
                relationship: Some(hop.input_index),
                table: table.into(),
                alias: alias.into(),
                final_,
            };
        }
        Self::Union {
            alias: alias.into(),
            arms: hop
                .edge_tables
                .iter()
                .map(|table| PhysicalPlan {
                    source: Self::Scan {
                        relationship: Some(hop.input_index),
                        table: table.clone(),
                        alias: format!("_{alias}"),
                        final_,
                    },
                    outputs: EDGE_RESERVED_COLUMNS
                        .iter()
                        .chain([&SOURCE_TAGS_COLUMN, &TARGET_TAGS_COLUMN])
                        .chain([&DELETED_COLUMN])
                        .map(|column| Projection::col(&format!("_{alias}"), column))
                        .collect(),
                })
                .collect(),
            relationship: hop.input_index,
        }
    }

    pub(super) fn inner_join(self, right: Self, endpoints: (Column, Column)) -> Self {
        Self::Join {
            endpoints,
            predicates: vec![],
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    fn node(node: &NodePlan, final_: bool) -> Result<Self> {
        Ok(Self::Scan {
            table: node.table.clone().ok_or_else(|| {
                QueryError::Lowering(format!("node '{}' has no table", node.alias))
            })?,
            alias: node.alias.clone(),
            final_,
            relationship: None,
        })
    }

    pub(crate) fn filter(self, predicates: Vec<Predicate>) -> Self {
        if predicates.is_empty() {
            self
        } else {
            Self::Filter {
                predicates,
                input: Box::new(self),
            }
        }
    }
}

impl PhysicalPlan {
    pub fn exact_keys(node: &NodePlan, column: &str) -> Result<Self> {
        Ok(Self {
            source: PhysicalSource::node(node, true)?.filter(node_predicates(node)),
            outputs: vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, column)),
                DEFAULT_PRIMARY_KEY,
            )],
        })
    }

    pub fn node_scan(
        node: &NodePlan,
    ) -> Result<Self> {
        Ok(Self {
            source: PhysicalSource::Scope {
                alias: node.alias.clone(),
                input: Box::new(PhysicalSource::node(node, true)?.filter(node_predicates(node))),
            },
            outputs: node_outputs(node),
        })
    }

    pub fn single_node(node: &NodePlan) -> Result<Self> {
        Ok(Self {
            outputs: node_outputs(node),
            source: PhysicalSource::node(node, true)?.filter(node_predicates(node)),
        })
    }
}
