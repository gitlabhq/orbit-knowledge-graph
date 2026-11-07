//! Query lowerer: edge-chain-first, nodes are lazy.

pub(crate) mod build;
pub mod query_types;

use std::collections::HashMap;

use crate::error::{QueryError, Result};
use crate::input::QueryType;
use crate::logical::Query;
use crate::passes::codegen::{PlanOf, RelationalModel};
use crate::tp_bounds::TpBounds;

use super::plan::pattern::Pattern;
use super::plan::predicates::{Column, OutputValue};
use super::plan::query_types::aggregation::aggregation_plan;
use super::plan::rows::{self, BindingSource};
use super::plan::{NodePlan, Planned};

#[derive(Clone, Default)]
pub struct LoweredMetadata {
    pub nodes: HashMap<String, NodeBinding>,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<Term>,
    pub point_lookup: bool,
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

fn bindings(
    nodes: &HashMap<String, NodePlan>,
    query: Query<'_>,
    matched: &[BindingSource],
) -> Result<HashMap<String, NodeBinding>> {
    let mut bound: HashMap<String, NodeBinding> = matched
        .iter()
        .map(|binding| {
            let source = NodeBinding::source(
                &binding.alias,
                &binding.column,
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
        let binding = match (bound.get(&source.alias), node.node_ids.as_slice()) {
            (
                Some(NodeBinding::Values {
                    table_alias: Some(table),
                    ..
                }),
                _,
            ) => NodeBinding::source(table, &source.column, None, None),
            (_, [id]) => NodeBinding::Values {
                identity: Term::Id(*id),
                table_alias: None,
                traversal_path: None,
            },
            _ => return Err(missing()),
        };
        bound.insert(alias.to_string(), binding);
    }
    Ok(bound)
}

pub fn emit<M: RelationalModel>(
    query: Query<'_>,
    tp_bounds: Option<&TpBounds>,
    model: &M,
) -> Result<Planned<PlanOf<M>>> {
    let limit = query
        .row_limit()
        .ok_or_else(|| QueryError::PipelineInvariant("queries carry a row limit".into()))?;
    let pattern = Pattern::new(query, model)?;
    let mut planned = None;
    let plan = rows::build(model.catalog(), |q| {
        let matched = pattern.execution(q, tp_bounds)?;
        let nodes = bindings(&pattern.nodes, query, &matched.bindings)?;
        let (rows, stable_order) = match query.kind() {
            QueryType::Aggregation => {
                let result = aggregation_plan(query, &pattern.nodes, &matched);
                let stable_order = query_types::aggregation::stable_order(&result);
                let rows = query_types::aggregation::emit(q, matched, &result, limit)?;
                (rows, stable_order)
            }
            _ => {
                let shape = query_types::traversal::shape(&pattern, &matched, query, limit)?;
                let stable_order =
                    query_types::traversal::stable_order(&pattern, &matched, query, &nodes);
                let rows = query_types::traversal::emit(q, matched, &shape)?;
                (rows, stable_order)
            }
        };
        planned = Some((nodes, stable_order));
        Ok(rows)
    })?;
    let (nodes, stable_order) =
        planned.ok_or_else(|| QueryError::PipelineInvariant("a built plan has metadata".into()))?;
    let edges = pattern
        .relations
        .iter()
        .map(|hop| LoweredEdge {
            path_column: (hop.max_hops > 1).then(|| hop.path_nodes_column()),
            column_prefix: hop.column_prefix(),
            rel_types: hop.rel_types.clone(),
            relationship: hop.input_index,
        })
        .collect();
    let point_lookup = !pattern.nodes.is_empty()
        && pattern.nodes.values().all(NodePlan::is_id_pinned_only)
        && pattern
            .relations
            .iter()
            .all(|relation| relation.max_hops == 1);
    Ok(Planned {
        plan,
        metadata: LoweredMetadata {
            nodes,
            edges,
            stable_order,
            point_lookup,
        },
    })
}
