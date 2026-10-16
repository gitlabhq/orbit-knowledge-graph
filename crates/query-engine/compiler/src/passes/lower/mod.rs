//! Query lowerer: edge-chain-first, nodes are lazy.

pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
mod physical;
mod requirements;
pub(crate) mod sql;

use crate::ast::*;
use crate::constants::EDGE_TYPE_SUFFIX;
use crate::error::{QueryError, Result};
use crate::input::*;
use std::collections::{BTreeMap, HashMap};

use super::plan::{EdgeRelation, Plan, QueryPlan};

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

fn multi_type_edge_types(node: &Node, relations: &[EdgeRelation]) -> Vec<Expr> {
    let Node::Query(query) = node else {
        return Vec::new();
    };
    relations
        .iter()
        .enumerate()
        .filter(|(_, hop)| {
            hop.max_hops == 1
                && (hop.rel_types.len() > 1
                    || crate::passes::normalize::is_wildcard(&hop.rel_types))
        })
        .filter_map(|(index, hop)| edge_type(query, hop, index))
        .collect()
}

fn edge_type(query: &Query, relation: &EdgeRelation, index: usize) -> Option<Expr> {
    query
        .selected(&format!(
            "{}{}",
            relation.column_prefix(index),
            EDGE_TYPE_SUFFIX
        ))
        .cloned()
}

fn emit_traversal(
    input: &Input,
    output: EmitOutput,
    relations: &[EdgeRelation],
    execution: &crate::passes::plan::physical::ExecutionPlan,
) -> Result<Node> {
    let mut query = output.into_query(vec![], vec![], vec![], input.limit);
    query.order_by = match (&input.order_by, &input.relationship_return.sort) {
        (Some(order), _) => vec![ordered(
            requirements::column(&execution.property(&order.node, &order.property)),
            order.direction,
        )],
        (None, Some(sort)) => {
            let edge_type = relations
                .iter()
                .enumerate()
                .find(|(_, hop)| hop.input_index == sort.relationship)
                .and_then(|(index, hop)| edge_type(&query, hop, index))
                .ok_or_else(|| {
                    QueryError::Lowering("sorted relationship has no edge type column".into())
                })?;
            vec![ordered(edge_type, sort.direction)]
        }
        (None, None) => Vec::new(),
    };
    Ok(Node::Query(Box::new(query)))
}

fn ordered(expr: Expr, direction: OrderDirection) -> OrderExpr {
    if direction == OrderDirection::Desc {
        OrderExpr::desc(expr)
    } else {
        OrderExpr::asc(expr)
    }
}

pub fn emit(plan: &QueryPlan, input: &Input) -> Result<LoweredQuery> {
    let mut nodes = HashMap::new();
    let node = match plan {
        QueryPlan::Traversal(plan) => {
            let mut output = physical::execute(&plan.operation.execution);
            nodes = output.take_bindings(plan, input)?;
            emit_traversal(input, output, &plan.relations, &plan.operation.execution)
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
        QueryPlan::Hydration(plan) => hydration::emit_hydration(&plan.operation.nodes, input.limit),
    }?;

    let edges = plan
        .relations()
        .iter()
        .enumerate()
        .map(|(index, hop)| {
            let prefix = hop.column_prefix(index);
            LoweredEdge {
                path_column: (hop.max_hops > 1).then(|| format!("{prefix}path_nodes")),
                column_prefix: prefix,
                rel_types: hop.rel_types.clone(),
                relationship: hop.input_index,
            }
        })
        .collect();
    let stable_order = match plan {
        QueryPlan::Hydration(_) => vec![],
        QueryPlan::Aggregation(_) => match &node {
            Node::Query(query) => query.group_by.iter().cloned().map(OrderExpr::asc).collect(),
            Node::Insert(_) => Vec::new(),
        },
        QueryPlan::PathFinding(_) => vec![
            OrderExpr::asc(Expr::func(
                Function::ToString,
                vec![Expr::col("paths", crate::constants::path_column())],
            )),
            OrderExpr::asc(Expr::func(
                Function::ToString,
                vec![Expr::col("paths", crate::constants::edge_kinds_column())],
            )),
        ],
        QueryPlan::Neighbors(plan) => match plan.operation.direction {
            Direction::Both => vec![
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
        QueryPlan::Traversal(_) if input.relationships.is_empty() => input
            .nodes
            .iter()
            .filter_map(|node| nodes.get(&node.id))
            .map(|binding| OrderExpr::asc(binding.identity().clone()))
            .collect(),
        QueryPlan::Traversal(plan) => plan
            .node_edge_mappings
            .iter()
            .filter_map(|(node, source)| {
                nodes.get(node).map(|binding| (source, binding.identity()))
            })
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .cloned()
            .chain(multi_type_edge_types(&node, &plan.relations))
            .map(OrderExpr::asc)
            .collect(),
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
