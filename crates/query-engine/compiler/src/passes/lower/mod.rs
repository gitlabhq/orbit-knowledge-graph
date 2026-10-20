//! Query lowerer: edge-chain-first, nodes are lazy.

mod columns;
mod graph;
mod predicates;
pub mod query_types;
pub(crate) mod sql;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use std::collections::HashMap;

use super::plan::graph::ExecutionPlan;
use super::plan::predicates::Column;
use super::plan::{Plan, QueryPlan};

#[derive(Clone, Default)]
pub struct LoweredMetadata {
    pub nodes: HashMap<String, NodeBinding>,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<OrderExpr>,
}

#[derive(Clone)]
pub enum NodeBinding {
    Filtered,
    Values {
        identity: Expr,
        table_alias: Option<String>,
        traversal_path: Option<Expr>,
    },
    Projected {
        role_identity: Option<Expr>,
    },
}

impl NodeBinding {
    fn source(
        alias: &str,
        column: &str,
        table_alias: Option<String>,
        traversal_path: Option<Expr>,
    ) -> Self {
        Self::Values {
            identity: Expr::col(alias, column),
            table_alias,
            traversal_path,
        }
    }

    pub fn role_identity(&self) -> Result<Option<&Expr>> {
        Ok(match self {
            Self::Values {
                identity,
                table_alias: None,
                ..
            } => Some(identity),
            Self::Projected { role_identity } => role_identity.as_ref(),
            Self::Filtered => {
                return Err(QueryError::Lowering(
                    "protected filtered node requires a visible identity".into(),
                ));
            }
            _ => None,
        })
    }

    fn identity(&self) -> &Expr {
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

pub struct LoweredQuery {
    pub ast: Node,
    pub metadata: LoweredMetadata,
}

pub struct EmitOutput {
    pub from: TableRef,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
    pub nodes: HashMap<String, NodeBinding>,
}

impl EmitOutput {
    fn take_bindings<T>(
        &mut self,
        plan: &Plan<T>,
        input: &Input,
    ) -> Result<HashMap<String, NodeBinding>> {
        for node in input
            .nodes
            .iter()
            .filter(|node| plan.nodes.contains_key(&node.id))
        {
            if self.nodes.contains_key(&node.id) {
                continue;
            }
            let source = plan.node_edge_mappings.get(&node.id).ok_or_else(|| {
                QueryError::Lowering(format!("node '{}' has no emitted identity", node.id))
            })?;
            let binding = match (self.nodes.get(&source.0), node.node_ids.as_slice()) {
                (
                    Some(NodeBinding::Values {
                        table_alias: Some(alias),
                        ..
                    }),
                    _,
                ) => NodeBinding::source(alias, &source.1, None, None),
                (_, [id]) => NodeBinding::Values {
                    identity: Expr::lit(*id),
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
            self.nodes.insert(node.id.clone(), binding);
        }
        Ok(std::mem::take(&mut self.nodes))
    }

    pub fn into_query(
        self,
        mut select: Vec<SelectExpr>,
        group_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
        limit: u32,
    ) -> Query {
        select.extend(self.select);
        Query {
            ctes: self.ctes,
            select,
            from: self.from,
            where_clause: Expr::conjoin(self.where_parts),
            group_by,
            order_by,
            limit: Some(limit),
            ..Default::default()
        }
    }
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

pub fn emit(plan: &QueryPlan, input: &Input) -> Result<LoweredQuery> {
    let emitted = match plan {
        QueryPlan::Traversal(plan) => query_types::traversal::emit(plan, input)?,
        QueryPlan::Aggregation(plan) => query_types::aggregation::emit(plan, input)?,
        QueryPlan::Neighbors(plan) => query_types::neighbors::emit(plan, input)?,
        QueryPlan::PathFinding(plan) => query_types::pathfinding::emit(plan, input)?,
        QueryPlan::Hydration(plan) => query_types::hydration::emit(plan)?,
    };
    let edges = plan
        .relations()
        .iter()
        .map(|hop| {
            let prefix = hop.column_prefix();
            LoweredEdge {
                path_column: (hop.max_hops > 1).then(|| format!("{prefix}path_nodes")),
                column_prefix: prefix,
                rel_types: hop.rel_types.clone(),
                relationship: hop.input_index,
            }
        })
        .collect();
    Ok(LoweredQuery {
        ast: emitted.ast,
        metadata: LoweredMetadata {
            nodes: emitted.nodes,
            edges,
            stable_order: emitted.stable_order,
        },
    })
}
