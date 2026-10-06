use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, node_outputs, node_predicates,
    property_filter,
};
use crate::error::{QueryError, Result};
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::{Hop, NodePlan};

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub definitions: Vec<(String, PhysicalPlan)>,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
}

pub struct BindingSource {
    pub node: String,
    pub alias: String,
    pub column: String,
    pub joined: bool,
}

impl BindingSource {
    pub(super) fn table(alias: &str) -> Self {
        Self {
            node: alias.into(),
            alias: alias.into(),
            column: DEFAULT_PRIMARY_KEY.into(),
            joined: true,
        }
    }
}

pub(super) fn key_membership(alias: &str, column: &str, name: String) -> Predicate {
    Predicate::Membership {
        column: Column::new(alias, column),
        definition: name,
        key: DEFAULT_PRIMARY_KEY.into(),
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

    pub(super) fn cascade(self, hop: &Hop, alias: &str, upstream: Option<&PhysicalPlan>) -> Self {
        match upstream {
            Some(keys) => Self::KeyFilter {
                value: Column::new(
                    alias,
                    &hop.join_prev.as_ref().expect("cascade join").curr_col,
                ),
                keys: Box::new(keys.clone()),
                input: Box::new(self),
            },
            None => self,
        }
    }

    pub(super) fn edge_keys(
        hop: &Hop,
        alias: &str,
        predicates: Vec<Predicate>,
        upstream: Option<&PhysicalPlan>,
    ) -> Self {
        let source = Self::Filter {
            predicates,
            input: Box::new(Self::Scan {
                table: hop.edge_table.clone(),
                alias: alias.into(),
                final_: false,
                relationship: Some(hop.input_index),
            }),
        };
        source.cascade(hop, alias, upstream)
    }
}

impl PhysicalPlan {
    pub fn candidate_keys(node: &NodePlan, column: &str, extra: Vec<Predicate>) -> Result<Self> {
        Self::keys(node, column, false, extra)
    }

    pub fn filtered_keys(node: &NodePlan, column: &str) -> Result<Self> {
        Self::keys(node, column, true, vec![])
    }

    fn keys(node: &NodePlan, column: &str, final_: bool, extra: Vec<Predicate>) -> Result<Self> {
        let mut predicates = node_predicates(node);
        predicates.extend(extra);
        Ok(Self {
            source: PhysicalSource::node(node, final_)?.filter(predicates),
            outputs: vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, column)),
                DEFAULT_PRIMARY_KEY,
            )],
        })
    }

    pub fn node_scan(
        node: &NodePlan,
        narrowing: Option<Predicate>,
        sort_key: &[String],
    ) -> Result<Self> {
        let mut source = PhysicalSource::node(node, narrowing.is_none())?;
        if let Some(narrowing) = narrowing {
            if sort_key.is_empty() {
                return Err(QueryError::Lowering(format!(
                    "node '{}' has no latest-row key",
                    node.alias
                )));
            }
            let mut predicates = vec![narrowing];
            predicates.extend(
                node.filters
                    .iter()
                    .filter(|(_, filter)| filter.in_sort_key && filter.filter.rhs_column.is_none())
                    .map(|(column, filter)| property_filter(&node.alias, column, filter)),
            );
            if sort_key.iter().any(|column| column == DEFAULT_PRIMARY_KEY) {
                if !node.node_ids.is_empty() {
                    predicates.push(id_list(&node.alias, DEFAULT_PRIMARY_KEY, &node.node_ids));
                }
                if let Some(range) = &node.id_range {
                    predicates.push(Predicate::IdRange {
                        column: Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                        start: range.start,
                        end: range.end,
                    });
                }
            }
            source = PhysicalSource::Latest {
                aggregate_condition: vec![],
                sort_key: sort_key.to_vec(),
                alias: node.alias.clone(),
                input: Box::new(source.filter(predicates)),
            };
        }
        Ok(Self {
            source: PhysicalSource::Scope {
                alias: node.alias.clone(),
                input: Box::new(source.filter(node_predicates(node))),
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
