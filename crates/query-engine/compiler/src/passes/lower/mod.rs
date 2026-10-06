//! Query lowerer: edge-chain-first, nodes are lazy.

pub(crate) mod context;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
mod physical;
mod requirements;
pub(crate) mod sql;
pub mod traversal;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use ontology::constants::DEFAULT_PRIMARY_KEY;
use std::collections::HashMap;

use super::plan::QueryPlan;
use crate::config::BindingNames;
use query_data_model::QueryDataModel;
use query_data_model::bindings::{QueryBindings, ScopeId};

#[derive(Clone, Default)]
pub struct LoweredMetadata {
    pub nodes: HashMap<String, NodeBinding>,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<OrderExpr>,
}

pub type NodeBinding = super::plan::NodeBinding<Expr>;

impl super::plan::NodeBinding<Expr> {
    pub fn role_identity(&self) -> Result<Option<&Expr>> {
        Ok(match self {
            Self::Values {
                identity,
                relation: None,
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

    fn property(
        &self,
        scope: ScopeId,
        bindings: &QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        property: &str,
    ) -> Result<Expr> {
        let Self::Values {
            identity, relation, ..
        } = self
        else {
            return Err(QueryError::Lowering(
                "projected result has no node-table properties".into(),
            ));
        };
        if property == DEFAULT_PRIMARY_KEY {
            return Ok(identity.clone());
        }
        let relation = relation.ok_or_else(|| {
            QueryError::Lowering(format!("property '{property}' has no visible node table"))
        })?;
        context::stored_column(model, bindings, scope, relation, property).map(Expr::Column)
    }
}

#[derive(Clone)]
pub struct LoweredEdge {
    pub column_prefix: String,
    pub path_column: Option<String>,
    pub rel_types: Vec<String>,
}

pub struct LoweredQuery {
    pub ast: Node,
    pub metadata: LoweredMetadata,
}

pub fn emit(
    plan: &QueryPlan,
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    bindings: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<LoweredQuery> {
    let mut nodes = HashMap::new();
    let mut family_order = Vec::new();
    let mut node = match plan {
        QueryPlan::Traversal(plan) => {
            let output = physical::execute(bindings.root(), &plan.operation.execution);
            nodes = lower_node_bindings(&plan.operation.execution);
            traversal::emit_traversal(input, output, &nodes, bindings, model)
        }
        QueryPlan::Aggregation(plan) => {
            let result = &plan.operation.result;
            let output = physical::execute(bindings.root(), &plan.operation.execution);
            nodes = lower_node_bindings(&plan.operation.execution);
            Ok(requirements::aggregation(result, output, input.limit))
        }
        QueryPlan::Neighbors(plan) => {
            let (query, binding, order) =
                neighbors::emit_neighbors(plan, input, model, bindings, names)?;
            family_order = order;
            nodes.insert(plan.operation.center.clone(), binding);
            Ok(query)
        }
        QueryPlan::PathFinding(plan) => {
            let (query, order) =
                pathfinding::emit_pathfinding(plan, input, model, bindings, names)?;
            family_order = order;
            Ok(query)
        }
        QueryPlan::Hydration(plan) => hydration::emit_hydration(&plan.operation.nodes, input.limit),
    }?;

    if !input.join_predicates.is_empty()
        && let Node::Query(q) = &mut node
    {
        let column = |alias: &str, property: &str| -> Result<Expr> {
            nodes
                .get(alias)
                .ok_or_else(|| {
                    QueryError::Lowering(format!("node '{alias}' has no lowered binding"))
                })?
                .property(q.scope, bindings, model, property)
        };
        for jp in &input.join_predicates {
            let pred = sql::comparison(
                column(&jp.lhs_node, &jp.lhs_prop)?,
                jp.op,
                column(&jp.rhs_node, &jp.rhs_prop)?,
            )?;
            q.where_clause = Some(match q.where_clause.take() {
                Some(existing) => Expr::and(existing, pred),
                None => pred,
            });
        }
    }

    let edges = plan
        .hops()
        .iter()
        .enumerate()
        .map(|(index, hop)| {
            let prefix = if hop.max_hops > 1 {
                format!("hop_e{index}_")
            } else {
                format!("e{index}_")
            };
            LoweredEdge {
                path_column: (hop.max_hops > 1).then(|| format!("{prefix}path_nodes")),
                column_prefix: prefix,
                rel_types: hop.rel_types.clone(),
            }
        })
        .collect();
    let stable_order = match plan {
        QueryPlan::Hydration(_) => vec![],
        QueryPlan::Aggregation(_) => match &node {
            Node::Query(query) => query.group_by.iter().cloned().map(OrderExpr::asc).collect(),
            Node::Insert(_) => Vec::new(),
        },
        QueryPlan::PathFinding(_) | QueryPlan::Neighbors(_) => family_order,
        QueryPlan::Traversal(_) if input.relationships.is_empty() => input
            .nodes
            .iter()
            .filter_map(|node| nodes.get(&node.id))
            .map(|binding| OrderExpr::asc(binding.identity().clone()))
            .collect(),
        QueryPlan::Traversal(_) => {
            let mut order = Vec::new();
            for node in &input.nodes {
                if let Some(NodeBinding::Values { identity, .. }) = nodes.get(&node.id)
                    && !order.iter().any(|key: &OrderExpr| key.expr == *identity)
                {
                    order.push(OrderExpr::asc(identity.clone()));
                }
            }
            order
        }
    };
    Ok(LoweredQuery {
        ast: node,
        metadata: LoweredMetadata {
            nodes,
            edges,
            stable_order,
        },
    })
}

fn lower_node_bindings(
    plan: &super::plan::physical::ExecutionPlan,
) -> HashMap<String, NodeBinding> {
    use super::plan::physical::NodeIdentity;
    plan.bindings
        .iter()
        .map(|(node, binding)| {
            (
                node.clone(),
                match binding {
                    super::plan::NodeBinding::Filtered => NodeBinding::Filtered,
                    super::plan::NodeBinding::Values {
                        identity,
                        relation,
                        traversal_path,
                    } => NodeBinding::Values {
                        identity: match identity {
                            NodeIdentity::Column(column) => Expr::Column(*column),
                            NodeIdentity::Pinned(id) => Expr::lit(*id),
                        },
                        relation: *relation,
                        traversal_path: traversal_path.map(Expr::Column),
                    },
                    super::plan::NodeBinding::Projected { .. } => {
                        unreachable!("projected identities are produced by family lowering")
                    }
                },
            )
        })
        .collect()
}
