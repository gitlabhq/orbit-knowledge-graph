//! Query lowerer: edge-chain-first, nodes are lazy.

pub(crate) mod build;
mod columns;
pub(crate) mod graph;
pub mod query_types;

use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};
use crate::tp_bounds::TpBounds;
use std::collections::HashMap;

use super::plan::graph::ExecutionPlan;
use super::plan::predicates::{Column, OutputValue};
use super::plan::{Plan, QueryPlan};

#[derive(Clone, Default)]
pub struct LoweredMetadata {
    pub nodes: HashMap<String, NodeBinding>,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<Term>,
}

#[derive(Clone, PartialEq)]
pub enum Term {
    Column(Column),
    Value(Box<OutputValue>),
    Id(i64),
    Output(String),
    Text(Column),
    Group(usize),
}

#[derive(Clone)]
pub enum NodeBinding {
    Filtered,
    Values {
        identity: Term,
        table_alias: Option<String>,
        traversal_path: Option<Column>,
    },
    Projected,
}

impl NodeBinding {
    fn source(
        alias: &str,
        column: &str,
        table_alias: Option<String>,
        traversal_path: Option<Column>,
    ) -> Self {
        Self::Values {
            identity: Term::Column(Column::new(alias, column)),
            table_alias,
            traversal_path,
        }
    }

    fn identity(&self) -> &Term {
        let Self::Values { identity, .. } = self else {
            unreachable!("projected bindings supply their own ordering")
        };
        identity
    }
}

#[derive(Clone)]
pub struct LoweredEdge {
    pub column_prefix: String,
    pub path_column: Option<String>,
    pub rel_types: Vec<String>,
    pub relationship: usize,
}

pub struct LoweredQuery<P> {
    pub plan: P,
    pub metadata: LoweredMetadata,
}

fn bindings<T>(
    plan: &Plan<T>,
    input: &Input,
    mut nodes: HashMap<String, NodeBinding>,
) -> Result<HashMap<String, NodeBinding>> {
    for node in input
        .nodes
        .iter()
        .filter(|node| plan.nodes.contains_key(&node.id))
    {
        if nodes.contains_key(&node.id) {
            continue;
        }
        let source = plan.node_edge_mappings.get(&node.id).ok_or_else(|| {
            QueryError::Lowering(format!("node '{}' has no emitted identity", node.id))
        })?;
        let binding = match (nodes.get(&source.0), node.node_ids.as_slice()) {
            (
                Some(NodeBinding::Values {
                    table_alias: Some(alias),
                    ..
                }),
                _,
            ) => NodeBinding::source(alias, &source.1, None, None),
            (_, [id]) => NodeBinding::Values {
                identity: Term::Id(*id),
                table_alias: None,
                traversal_path: None,
            },
            _ => {
                return Err(QueryError::Lowering(format!(
                    "node '{}' has no emitted identity",
                    node.id
                )));
            }
        };
        nodes.insert(node.id.clone(), binding);
    }
    Ok(nodes)
}

fn redaction_columns<T>(plan: &Plan<T>, execution: &ExecutionPlan) -> Vec<Column> {
    execution
        .bindings
        .iter()
        .filter_map(|binding| {
            let source = binding.property_source.as_deref()?;
            let node = plan.nodes.get(&binding.node)?;
            Some(Column::new(source, &node.redaction_id_column))
        })
        .collect()
}

pub fn emit<M: RelationalModel>(
    plan: &QueryPlan,
    input: &Input,
    tp_bounds: Option<&TpBounds>,
    model: &M,
) -> Result<LoweredQuery<PlanOf<M>>> {
    let catalog = model.catalog();
    let (built, nodes, stable_order) = match plan {
        QueryPlan::Traversal(plan) => {
            let nodes = bindings(plan, input, graph::bindings(&plan.operation.execution))?;
            let shape = query_types::traversal::shape(plan, input)?;
            let built = catalog
                .build(|q| query_types::traversal::emit(q, plan, input, &shape, tp_bounds))
                .map_err(rejected)?;
            let stable_order = query_types::traversal::stable_order(plan, input, &nodes);
            (built, nodes, stable_order)
        }
        QueryPlan::Aggregation(plan) => {
            let nodes = bindings(plan, input, graph::bindings(&plan.operation.execution))?;
            let built = catalog
                .build(|q| query_types::aggregation::emit(q, plan, input, tp_bounds))
                .map_err(rejected)?;
            let stable_order = query_types::aggregation::stable_order(plan);
            (built, nodes, stable_order)
        }
    };
    let edges = plan
        .relations()
        .iter()
        .map(|hop| LoweredEdge {
            path_column: (hop.max_hops > 1).then(|| hop.path_nodes_column()),
            column_prefix: hop.column_prefix(),
            rel_types: hop.rel_types.clone(),
            relationship: hop.input_index,
        })
        .collect();
    Ok(LoweredQuery {
        plan: built,
        metadata: LoweredMetadata {
            nodes,
            edges,
            stable_order,
        },
    })
}
