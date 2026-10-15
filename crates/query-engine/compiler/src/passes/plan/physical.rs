use super::requirements::{
    Column, OutputValue, Predicate, Projection, node_outputs, node_predicates,
};
use crate::error::{QueryError, Result};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, EDGE_RESERVED_COLUMNS, SOURCE_TAGS_COLUMN,
    TARGET_TAGS_COLUMN,
};

use super::{EdgeRelation, NodePlan};

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub subplans: Vec<Subplan>,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SubplanId(pub usize);

pub struct Subplan {
    pub id: SubplanId,
    pub plan: PhysicalPlan,
    pub materialized: bool,
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
pub enum PhysicalPlan {
    Project {
        source: PhysicalSource,
        outputs: Vec<Projection>,
    },
    Distinct {
        source: PhysicalSource,
        outputs: Vec<Projection>,
    },
}

impl PhysicalPlan {
    pub fn candidate_keys(node: &NodePlan) -> Result<Option<Self>> {
        let predicates: Vec<_> = node
            .filters
            .iter()
            .filter(|(_, filter)| filter.is_candidate())
            .map(|(property, filter)| filter.at(Column::new(&node.alias, property)))
            .collect();
        if predicates.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self::Project {
            source: PhysicalSource::node(node, false)?.filter(predicates),
            outputs: vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, DEFAULT_PRIMARY_KEY)),
                DEFAULT_PRIMARY_KEY,
            )],
        }))
    }

    pub fn node_keys(node: &NodePlan, candidate: Option<(SubplanId, String)>) -> Result<Self> {
        let mut source = PhysicalSource::node(node, true)?;
        if let Some((id, key)) = candidate {
            source = source.filter(vec![Predicate::Membership {
                column: Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                definition: format!("_orbit_subplan_{}", id.0),
                key,
            }]);
        }
        Ok(Self::Project {
            source: source.filter(node_predicates(node)),
            outputs: vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, DEFAULT_PRIMARY_KEY)),
                DEFAULT_PRIMARY_KEY,
            )],
        })
    }

    pub fn source(&self) -> &PhysicalSource {
        match self {
            Self::Project { source, .. } | Self::Distinct { source, .. } => source,
        }
    }

    pub fn outputs(&self) -> &[Projection] {
        match self {
            Self::Project { outputs, .. } | Self::Distinct { outputs, .. } => outputs,
        }
    }

    pub fn into_source(self) -> PhysicalSource {
        match self {
            Self::Project { source, .. } | Self::Distinct { source, .. } => source,
        }
    }
}

#[derive(Clone)]
pub enum PhysicalSource {
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
    Use {
        id: SubplanId,
        alias: String,
    },
    Filter {
        predicates: Vec<Predicate>,
        input: Box<Self>,
    },
    Project {
        alias: String,
        outputs: Vec<Projection>,
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
    pub(super) fn edge(relation: &EdgeRelation, alias: &str, final_: bool) -> Self {
        if let Some(table) = relation.single_edge_table() {
            return Self::Scan {
                relationship: Some(relation.input_index),
                table: table.into(),
                alias: alias.into(),
                final_,
            };
        }
        Self::Union {
            alias: alias.into(),
            arms: relation
                .edge_tables
                .iter()
                .map(|table| PhysicalPlan::Project {
                    source: Self::Scan {
                        relationship: Some(relation.input_index),
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
            relationship: relation.input_index,
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
    pub fn node_scan(node: &NodePlan) -> Result<Self> {
        Ok(Self::Project {
            source: PhysicalSource::Scope {
                alias: node.alias.clone(),
                input: Box::new(PhysicalSource::node(node, true)?.filter(node_predicates(node))),
            },
            outputs: node_outputs(node),
        })
    }

    pub fn single_node(node: &NodePlan) -> Result<Self> {
        Ok(Self::Project {
            outputs: node_outputs(node),
            source: PhysicalSource::node(node, true)?.filter(node_predicates(node)),
        })
    }
}
