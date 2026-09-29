//! Query lowerer: edge-chain-first, nodes are lazy.

pub mod aggregation;
mod fk;
mod flat_chain;
mod helpers;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
mod single_node;
pub mod traversal;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::plan::{Plan, PlanBody, Strategy};
use super::shared;

#[derive(Clone, Default)]
pub struct LoweredMetadata {
    pub nodes: HashMap<String, NodeBinding>,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<OrderExpr>,
}

#[derive(Clone)]
pub struct NodeBinding {
    pub identity: Expr,
    pub table_alias: Option<String>,
    pub traversal_path: Option<Expr>,
    pub projected: bool,
}

impl NodeBinding {
    fn property(&self, property: &str) -> Result<Expr> {
        if property == DEFAULT_PRIMARY_KEY {
            return Ok(self.identity.clone());
        }
        self.table_alias
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

impl Plan {
    pub fn emit_edge_chain(&self) -> Result<EmitOutput> {
        match self.strategy {
            Strategy::SingleNode => single_node::emit_single_node(self),
            Strategy::Fk(ref shape) => fk::emit_fk(self, shape),
            Strategy::Flat => flat_chain::emit_flat_chain(self),
        }
    }
}

pub struct EmitOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
    pub node_tables: HashSet<String>,
    /// Edge predicates for `-If` aggregate combinators. When set, the
    /// aggregation pass emits `countIf(cond)` / `sumIf(col, cond)` / etc.
    /// and the predicates are already in the LIMIT BY subquery's WHERE.
    pub edge_if_predicates: Option<Expr>,
}

impl EmitOutput {
    fn node_bindings(&self, plan: &Plan, input: &Input) -> Result<HashMap<String, NodeBinding>> {
        input
            .nodes
            .iter()
            .filter(|node| plan.nodes.contains_key(&node.id))
            .map(|node| {
                let source = plan.node_edge_mappings.get(&node.id);
                let visible = |alias: &str| {
                    self.node_tables.contains(alias)
                        || self.edge_aliases.iter().any(|edge| edge == alias)
                };
                let identity = match source {
                    Some((alias, column)) if visible(alias) => Expr::col(alias, column),
                    Some(_) if node.node_ids.len() == 1 => Expr::lit(node.node_ids[0]),
                    None if visible(&node.id) => Expr::col(&node.id, &node.id_property),
                    _ => {
                        return Err(QueryError::Lowering(format!(
                            "node '{}' has no emitted identity",
                            node.id
                        )));
                    }
                };
                let path_alias = source.map_or(node.id.as_str(), |(alias, _)| alias.as_str());
                let traversal_path =
                    visible(path_alias).then(|| Expr::col(path_alias, TRAVERSAL_PATH_COLUMN));
                Ok((
                    node.id.clone(),
                    NodeBinding {
                        identity,
                        table_alias: self.node_tables.contains(&node.id).then(|| node.id.clone()),
                        traversal_path,
                        projected: false,
                    },
                ))
            })
            .collect()
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

pub fn emit(plan: &Plan, input: &Input) -> Result<LoweredQuery> {
    let mut nodes = HashMap::new();
    let mut node = match &plan.body {
        PlanBody::Traversal => {
            let output = plan.emit_edge_chain()?;
            nodes = output.node_bindings(plan, input)?;
            traversal::emit_traversal(plan, input, output)
        }
        PlanBody::Aggregation {
            aggregations,
            agg_sort,
        } => {
            let output = plan.emit_edge_chain()?;
            nodes = output.node_bindings(plan, input)?;
            aggregation::emit_aggregation(
                plan,
                input,
                aggregations,
                &input.aggregation.group_by,
                agg_sort.as_ref(),
                output,
            )
        }
        PlanBody::Neighbors {
            center,
            direction,
            edge,
            has_non_denorm,
            center_tp_lookup,
        } => {
            let center_plan = &plan.nodes[center];
            nodes.insert(
                center.clone(),
                NodeBinding {
                    identity: Expr::col("e", direction.edge_columns().0),
                    table_alias: (*has_non_denorm || !center_plan.uses_default_pk())
                        .then(|| center.clone()),
                    traversal_path: None,
                    projected: true,
                },
            );
            neighbors::emit_neighbors(
                plan,
                input,
                center,
                *direction,
                edge,
                *has_non_denorm,
                center_tp_lookup.as_ref(),
            )
        }
        PlanBody::PathFinding(pf) => pathfinding::emit_pathfinding(plan, input, pf),
        PlanBody::Hydration { nodes, options } => hydration::emit_hydration(
            nodes,
            input.limit,
            options.dynamic,
            options.path_segment_budget,
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
            let pred = shared::comparison(
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
        .hops
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
        QueryType::Neighbors => match &plan.body {
            PlanBody::Neighbors {
                center, direction, ..
            } if *direction == Direction::Both => vec![
                OrderExpr::asc(Expr::ident(crate::constants::redaction_id_column(center))),
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
            .map(|binding| OrderExpr::asc(binding.identity.clone()))
            .collect(),
        _ => plan
            .node_edge_mappings
            .iter()
            .filter_map(|(node, source)| nodes.get(node).map(|binding| (source, &binding.identity)))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .map(|identity| OrderExpr::asc(identity.clone()))
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
