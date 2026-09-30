use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::constants::{
    EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX, EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX,
};
use crate::error::{QueryError, Result};
use crate::input::Direction;
use crate::passes::shared::{latest_node_predicates, node_select_columns};
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::{Hop, NodePlan};

pub enum EdgeRead {
    Plain,
    Final { narrow_inside: bool },
    Latest { sort_key: Vec<String> },
    MultiHop,
}

pub fn edge_reads(
    hops: &[Hop],
    aggregate: bool,
    sort_keys: &HashMap<String, Vec<String>>,
) -> Result<Vec<EdgeRead>> {
    hops.iter()
        .map(|hop| {
            Ok(if hop.max_hops > 1 {
                EdgeRead::MultiHop
            } else if hops.len() > 1 {
                let (start, end) = hop.direction.edge_columns();
                EdgeRead::Final {
                    narrow_inside: sort_keys.get(&hop.edge_table).is_some_and(|keys| {
                        keys.iter().take(4).any(|key| key == start || key == end)
                    }),
                }
            } else if aggregate {
                let sort_key = sort_keys
                    .get(&hop.edge_table)
                    .filter(|key| !key.is_empty())
                    .ok_or_else(|| {
                        QueryError::Lowering(format!(
                            "no sort key for edge table '{}'; cannot plan latest rows",
                            hop.edge_table
                        ))
                    })?;
                EdgeRead::Latest {
                    sort_key: sort_key.clone(),
                }
            } else {
                EdgeRead::Plain
            })
        })
        .collect()
}

pub struct PhysicalPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<SelectExpr>,
}

pub enum PhysicalSource {
    Scan {
        table: String,
        alias: String,
        final_: bool,
    },
    Filter {
        predicate: Expr,
        input: Box<Self>,
    },
    Scope {
        alias: String,
        input: Box<Self>,
    },
    Join {
        kind: JoinType,
        condition: Expr,
        left: Box<Self>,
        right: Box<Self>,
    },
    Latest {
        sort_key: Vec<String>,
        alias: String,
        input: Box<Self>,
    },
}

impl PhysicalPlan {
    pub fn single_node(node: &NodePlan) -> Result<Self> {
        let table = node
            .table
            .clone()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        Ok(Self {
            outputs: node_select_columns(&node.alias, node),
            source: PhysicalSource::Filter {
                predicate: Expr::conjoin(latest_node_predicates(&node.alias, node))
                    .expect("current-row scan has a deletion predicate"),
                input: Box::new(PhysicalSource::Scan {
                    table,
                    alias: node.alias.clone(),
                    final_: true,
                }),
            },
        })
    }

    pub fn fk_chain(
        hops: &[Hop],
        nodes: &HashMap<String, NodePlan>,
        project_edges: bool,
    ) -> Result<Self> {
        let root = &hops
            .first()
            .ok_or_else(|| QueryError::Lowering("FK chain requires a hop".into()))?
            .from_node;
        let node = |alias: &str| {
            nodes
                .get(alias)
                .ok_or_else(|| QueryError::Lowering(format!("FK chain node '{alias}' not found")))
        };
        let mut plan = Self::single_node(node(root)?)?;
        plan.source = PhysicalSource::Scope {
            alias: root.clone(),
            input: Box::new(plan.source),
        };
        let mut reached = HashSet::from([root.as_str()]);
        for (index, hop) in hops.iter().enumerate() {
            let fk = hop
                .fk
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("FK chain hop missing FK metadata".into()))?;
            let alias = if reached.contains(hop.from_node.as_str()) {
                &hop.to_node
            } else {
                &hop.from_node
            };
            let next = Self::single_node(node(alias)?)?;
            plan.source = PhysicalSource::Join {
                kind: JoinType::Inner,
                condition: Expr::eq(
                    Expr::col(&fk.fk_node, &fk.fk_column),
                    Expr::col(&fk.target_node, &fk.referenced_column),
                ),
                left: Box::new(plan.source),
                right: Box::new(PhysicalSource::Scope {
                    alias: alias.clone(),
                    input: Box::new(next.source),
                }),
            };
            plan.outputs.extend(next.outputs);
            reached.insert(hop.from_node.as_str());
            reached.insert(hop.to_node.as_str());
            if project_edges {
                let (source, target) = match hop.direction {
                    Direction::Incoming => (&hop.to_node, &hop.from_node),
                    Direction::Outgoing | Direction::Both => (&hop.from_node, &hop.to_node),
                };
                let fields = [
                    (
                        EDGE_TYPE_SUFFIX,
                        Expr::string(hop.rel_types.first().map(String::as_str).unwrap_or("")),
                    ),
                    (EDGE_SRC_SUFFIX, Expr::col(source, DEFAULT_PRIMARY_KEY)),
                    (
                        EDGE_SRC_TYPE_SUFFIX,
                        Expr::string(node(source)?.entity.as_deref().unwrap_or("")),
                    ),
                    (EDGE_DST_SUFFIX, Expr::col(target, DEFAULT_PRIMARY_KEY)),
                    (
                        EDGE_DST_TYPE_SUFFIX,
                        Expr::string(node(target)?.entity.as_deref().unwrap_or("")),
                    ),
                ];
                plan.outputs
                    .extend(fields.into_iter().map(|(suffix, expression)| {
                        SelectExpr::new(expression, format!("e{index}_{suffix}"))
                    }));
            }
        }
        Ok(plan)
    }
}
