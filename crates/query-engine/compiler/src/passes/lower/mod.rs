//! Query lowerer: edge-chain-first, nodes are lazy.

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
use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
use std::collections::{BTreeMap, HashMap};

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
    fn source(alias: &str, column: &str, table_alias: Option<String>) -> Self {
        Self::Values {
            identity: Expr::col(alias, column),
            table_alias,
            traversal_path: Some(Expr::col(alias, TRAVERSAL_PATH_COLUMN)),
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

    fn property(&self, property: &str) -> Result<Expr> {
        let Self::Values {
            identity,
            table_alias,
            ..
        } = self
        else {
            return Err(QueryError::Lowering(
                "projected result has no node-table properties".into(),
            ));
        };
        if property == DEFAULT_PRIMARY_KEY {
            return Ok(identity.clone());
        }
        table_alias
            .as_ref()
            .map(|alias| Expr::col(alias, property))
            .ok_or_else(|| {
                QueryError::Lowering(format!("property '{property}' has no visible node table"))
            })
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

pub struct EmitOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
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
                ) => NodeBinding::source(alias, &source.1, None),
                (_, [id]) => NodeBinding::Values {
                    identity: Expr::lit(*id),
                    table_alias: None,
                    traversal_path: None,
                },
                _ if input.query_type == QueryType::Aggregation
                    && plan.nodes.get(&source.0).is_some_and(|holder| {
                        holder.hydration == super::plan::HydrationStrategy::FilterOnly
                    })
                    && !node_group_ids(&input.aggregation.group_by)
                        .any(|alias| alias == node.id) =>
                {
                    NodeBinding::Filtered
                }
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

pub fn emit(plan: &QueryPlan, input: &Input) -> Result<LoweredQuery> {
    let mut nodes = HashMap::new();
    let mut node = match plan {
        QueryPlan::Traversal(plan) => {
            let mut output = physical::execute(&plan.operation.execution);
            nodes = output.take_bindings(plan, input)?;
            traversal::emit_traversal(plan, input, output)
        }
        QueryPlan::Aggregation(plan) => {
            let result = &plan.operation.result;
            let mut output = physical::execute(&plan.operation.execution);
            nodes = output.take_bindings(plan, input)?;
            Ok(requirements::aggregation(result, output, input.limit))
        }
        QueryPlan::Neighbors(plan) => {
            let (query, binding) = neighbors::emit_neighbors(plan, input)?;
            nodes.insert(plan.operation.center.clone(), binding);
            Ok(query)
        }
        QueryPlan::PathFinding(plan) => pathfinding::emit_pathfinding(plan, input),
        QueryPlan::Hydration(plan) => hydration::emit_hydration(
            &plan.operation.nodes,
            input.limit,
            plan.operation.options.dynamic,
            plan.operation.options.path_segment_budget,
        ),
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
                .property(property)
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
    let stable_order = match input.query_type {
        QueryType::Aggregation => match &node {
            Node::Query(query) => query.group_by.iter().cloned().map(OrderExpr::asc).collect(),
            Node::Insert(_) => Vec::new(),
        },
        QueryType::PathFinding => vec![
            OrderExpr::asc(Expr::func(
                "toString",
                vec![Expr::col("paths", crate::constants::path_column())],
            )),
            OrderExpr::asc(Expr::func(
                "toString",
                vec![Expr::col("paths", crate::constants::edge_kinds_column())],
            )),
        ],
        QueryType::Neighbors => match plan {
            QueryPlan::Neighbors(plan) if plan.operation.direction == Direction::Both => vec![
                OrderExpr::asc(Expr::ident(crate::constants::redaction_id_column(
                    &plan.operation.center,
                ))),
                OrderExpr::asc(Expr::ident(crate::constants::neighbor_id_column())),
                OrderExpr::asc(Expr::ident(crate::constants::relationship_type_column())),
                OrderExpr::asc(Expr::ident(crate::constants::neighbor_is_outgoing_column())),
            ],
            _ => vec![
                OrderExpr::asc(Expr::col("e", ontology::constants::SOURCE_ID_COLUMN)),
                OrderExpr::asc(Expr::col("e", ontology::constants::TARGET_ID_COLUMN)),
                OrderExpr::asc(Expr::col(
                    "e",
                    ontology::constants::RELATIONSHIP_KIND_COLUMN,
                )),
            ],
        },
        _ if input.relationships.is_empty() => input
            .nodes
            .iter()
            .filter_map(|node| nodes.get(&node.id))
            .map(|binding| OrderExpr::asc(binding.identity().clone()))
            .collect(),
        _ => {
            let mappings = match plan {
                QueryPlan::Traversal(plan) => &plan.node_edge_mappings,
                QueryPlan::Aggregation(plan) => &plan.node_edge_mappings,
                _ => unreachable!("only edge-chain queries use mapped ordering"),
            };
            mappings
                .iter()
                .filter_map(|(node, source)| {
                    nodes.get(node).map(|binding| (source, binding.identity()))
                })
                .collect::<BTreeMap<_, _>>()
                .into_values()
                .map(|identity| OrderExpr::asc(identity.clone()))
                .collect()
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
