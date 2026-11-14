//! What later phases read about a planned query: node identities, edges, and the stable result order.

use std::collections::HashMap;

use super::NodePlan;
use super::predicates::{Column, OutputValue};
use super::rows::BindingSource;
use crate::error::{QueryError, Result};
use crate::logical::Query;

#[derive(Clone, Default)]
pub struct PlanMetadata {
    pub nodes: HashMap<String, NodeBinding>,
    pub edges: Vec<PlannedEdge>,
    pub stable_order: Vec<Term>,
    pub point_lookup: bool,
}

#[derive(Clone, PartialEq)]
pub enum Term {
    Column(Column),
    Value(Box<OutputValue>),
    Id(orbit_authorization::Key),
    Output(String),
    Text(Column),
    Group(usize),
}

#[derive(Clone)]
pub enum NodeBinding {
    Filtered,
    Values {
        /// One term per part of the node's identity.
        identity: Vec<Term>,
        table_alias: Option<String>,
        traversal_path: Option<Column>,
    },
    Projected,
}

impl NodeBinding {
    pub(super) fn source(
        alias: &str,
        key: &[String],
        table_alias: Option<String>,
        traversal_path: Option<Column>,
    ) -> Self {
        Self::Values {
            identity: key
                .iter()
                .map(|column| Term::Column(Column::new(alias, column)))
                .collect(),
            table_alias,
            traversal_path,
        }
    }

    pub(super) fn identity(&self) -> &[Term] {
        let Self::Values { identity, .. } = self else {
            unreachable!("projected bindings supply their own ordering")
        };
        identity
    }
}

#[derive(Clone)]
pub struct PlannedEdge {
    pub column_prefix: String,
    pub variable_length: bool,
    pub rel_types: Vec<String>,
    pub relationship: usize,
}

pub(super) fn bindings(
    nodes: &HashMap<String, NodePlan>,
    query: Query<'_>,
    matched: &[BindingSource],
) -> Result<HashMap<String, NodeBinding>> {
    let mut bound: HashMap<String, NodeBinding> = matched
        .iter()
        .map(|binding| {
            let source = NodeBinding::source(
                &binding.alias,
                &binding.key,
                binding.property_source.clone(),
                binding.traversal_path.clone(),
            );
            (binding.node.clone(), source)
        })
        .collect();
    for (alias, node) in query
        .bindings()
        .into_iter()
        .filter_map(|binding| Some((binding.name, nodes.get(binding.name)?)))
    {
        if bound.contains_key(alias) {
            continue;
        }
        let missing = || QueryError::Lowering(format!("node '{alias}' has no emitted identity"));
        let source = matched
            .iter()
            .find(|binding| binding.node == alias)
            .ok_or_else(missing)?;
        let binding = match (bound.get(&source.alias), node.node_ids.as_deref()) {
            (
                Some(NodeBinding::Values {
                    table_alias: Some(table),
                    ..
                }),
                _,
            ) => NodeBinding::source(table, &source.key, None, None),
            (_, Some([id])) => NodeBinding::Values {
                identity: vec![Term::Id(id.clone())],
                table_alias: None,
                traversal_path: None,
            },
            _ => return Err(missing()),
        };
        bound.insert(alias.to_string(), binding);
    }
    Ok(bound)
}
