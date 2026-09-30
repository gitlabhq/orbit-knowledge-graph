use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::error::{QueryError, Result};
use crate::passes::shared::{
    filter_to_expr, id_list_predicate, id_range_predicate, latest_node_predicates,
    node_select_columns,
};
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::{Hop, NodePlan};

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub edge_if_predicates: Option<Expr>,
    pub definitions: Vec<(String, PhysicalPlan)>,
    pub outputs: Vec<SelectExpr>,
    pub bindings: Vec<BindingSource>,
    pub edge_aliases: Vec<String>,
}

pub struct BindingSource {
    pub node: String,
    pub alias: String,
    pub column: String,
    pub joined: bool,
}

pub(super) fn key_membership(alias: &str, column: &str, name: String) -> Expr {
    Expr::InSubquery {
        expr: Box::new(Expr::col(alias, column)),
        cte_name: name,
        column: DEFAULT_PRIMARY_KEY.into(),
    }
}

#[derive(Clone)]
pub struct PhysicalPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<SelectExpr>,
}

#[derive(Clone)]
pub enum PhysicalSource {
    KeyFilter {
        value: Expr,
        keys: Box<PhysicalPlan>,
        input: Box<Self>,
    },
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

impl PhysicalSource {
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

    fn where_all(self, predicates: Vec<Expr>) -> Self {
        Self::Filter {
            predicate: Expr::conjoin(predicates).expect("node scan predicates"),
            input: Box::new(self),
        }
    }

    pub(crate) fn filter(self, predicates: Vec<Expr>) -> Self {
        predicates
            .into_iter()
            .fold(self, |input, predicate| Self::Filter {
                predicate,
                input: Box::new(input),
            })
    }

    pub(super) fn cascade(self, hop: &Hop, alias: &str, upstream: Option<&PhysicalPlan>) -> Self {
        match upstream {
            Some(keys) => Self::KeyFilter {
                value: Expr::col(
                    alias,
                    &hop.join_prev.as_ref().expect("cascade join").curr_col,
                ),
                keys: Box::new(keys.clone()),
                input: Box::new(self),
            },
            None => self,
        }
    }

    pub(super) fn edge_keys(
        hop: &Hop,
        alias: &str,
        predicates: Vec<Expr>,
        upstream: Option<&PhysicalPlan>,
    ) -> Self {
        let source = Self::Filter {
            predicate: Expr::conjoin(predicates).expect("edge predicates"),
            input: Box::new(Self::Scan {
                table: hop.edge_table.clone(),
                alias: alias.into(),
                final_: false,
                relationship: Some(hop.input_index),
            }),
        };
        source.cascade(hop, alias, upstream)
    }
}

impl PhysicalPlan {
    pub fn candidate_keys(node: &NodePlan, column: &str, extra: Vec<Expr>) -> Result<Self> {
        Self::keys(node, column, false, extra)
    }

    pub fn filtered_keys(node: &NodePlan, column: &str) -> Result<Self> {
        Self::keys(node, column, true, vec![])
    }

    fn keys(node: &NodePlan, column: &str, final_: bool, extra: Vec<Expr>) -> Result<Self> {
        let mut predicates = latest_node_predicates(&node.alias, node);
        predicates.extend(extra);
        Ok(Self {
            source: PhysicalSource::node(node, final_)?.where_all(predicates),
            outputs: vec![SelectExpr::new(
                Expr::col(&node.alias, column),
                DEFAULT_PRIMARY_KEY,
            )],
        })
    }

    pub fn node_scan(
        node: &NodePlan,
        narrowing: Option<Expr>,
        sort_key: &[String],
    ) -> Result<Self> {
        let mut source = PhysicalSource::node(node, narrowing.is_none())?;
        if let Some(narrowing) = narrowing {
            if sort_key.is_empty() {
                return Err(QueryError::Lowering(format!(
                    "node '{}' has no latest-row key",
                    node.alias
                )));
            }
            let mut predicates = vec![narrowing];
            predicates.extend(
                node.filters
                    .iter()
                    .filter(|(column, filter)| {
                        sort_key.contains(column) && filter.filter.rhs_column.is_none()
                    })
                    .map(|(column, filter)| filter_to_expr(&node.alias, column, filter)),
            );
            if sort_key.iter().any(|column| column == DEFAULT_PRIMARY_KEY) {
                if !node.node_ids.is_empty() {
                    predicates.push(id_list_predicate(
                        &node.alias,
                        DEFAULT_PRIMARY_KEY,
                        &node.node_ids,
                    ));
                }
                if let Some(range) = &node.id_range {
                    predicates.push(id_range_predicate(&node.alias, range));
                }
            }
            source = PhysicalSource::Latest {
                sort_key: sort_key.to_vec(),
                alias: node.alias.clone(),
                input: Box::new(source.where_all(predicates)),
            };
        }
        Ok(Self {
            source: PhysicalSource::Scope {
                alias: node.alias.clone(),
                input: Box::new(source.where_all(latest_node_predicates(&node.alias, node))),
            },
            outputs: node_select_columns(&node.alias, node),
        })
    }

    pub fn single_node(node: &NodePlan) -> Result<Self> {
        Ok(Self {
            outputs: node_select_columns(&node.alias, node),
            source: PhysicalSource::node(node, true)?
                .where_all(latest_node_predicates(&node.alias, node)),
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
        let mut plan = Self::node_scan(node(root)?, None, &[])?;
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
            let next = Self::node_scan(node(alias)?, None, &[])?;
            plan.source = PhysicalSource::Join {
                kind: JoinType::Inner,
                condition: Expr::eq(
                    Expr::col(&fk.fk_node, &fk.fk_column),
                    Expr::col(&fk.target_node, &fk.referenced_column),
                ),
                left: Box::new(plan.source),
                right: Box::new(next.source),
            };
            plan.outputs.extend(next.outputs);
            reached.insert(hop.from_node.as_str());
            reached.insert(hop.to_node.as_str());
            if project_edges {
                plan.outputs.extend(super::fk::edge_outputs(
                    hop,
                    index,
                    nodes,
                    Expr::col(&hop.from_node, DEFAULT_PRIMARY_KEY),
                    Expr::col(&hop.to_node, DEFAULT_PRIMARY_KEY),
                ));
            }
        }
        Ok(plan)
    }
}
