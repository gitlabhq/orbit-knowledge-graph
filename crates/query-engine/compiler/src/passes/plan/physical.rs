use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::error::{QueryError, Result};
use crate::passes::shared::{
    filter_to_expr, id_list_predicate, id_range_predicate, latest_node_predicates,
    node_select_columns,
};
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::edge_predicates::{node_id_pin_predicates, push_edge_predicates};
use super::{DenormalizedKey, DenormalizedProperty, Hop, HydrationStrategy, NodePlan};

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

impl ExecutionPlan {
    pub fn flat(
        hops: &[Hop],
        aggregate: bool,
        sort_keys: &HashMap<String, Vec<String>>,
        nodes: &HashMap<String, NodePlan>,
        table_columns: &HashMap<String, HashSet<String>>,
        denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    ) -> Result<Self> {
        let mut narrowing = HashMap::new();
        for alias in hops.iter().flat_map(|hop| [&hop.from_node, &hop.to_node]) {
            let Some(node) = nodes.get(alias) else {
                continue;
            };
            if node.hydration == HydrationStrategy::FilterOnly && hops.len() >= 2 {
                if !narrowing.contains_key(alias) {
                    narrowing.insert(
                        alias.clone(),
                        PhysicalPlan::filtered_keys(node, DEFAULT_PRIMARY_KEY)?,
                    );
                }
                continue;
            }
            if node.hydration != HydrationStrategy::Join
                || !node.has_selective_filters()
                || narrowing.contains_key(alias)
            {
                continue;
            }
            let table = node
                .table
                .as_deref()
                .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' has no table")))?;
            let sort_key = sort_keys
                .get(table)
                .filter(|key| !key.is_empty())
                .ok_or_else(|| {
                    QueryError::Lowering(format!(
                        "no sort key for node table '{table}'; cannot plan narrowing"
                    ))
                })?;
            let mut keys = PhysicalPlan::candidate_keys(node, DEFAULT_PRIMARY_KEY, vec![])?;
            keys.source = PhysicalSource::Latest {
                alias: alias.clone(),
                sort_key: sort_key.clone(),
                input: Box::new(keys.source),
            };
            narrowing.insert(alias.clone(), keys);
        }
        let cascades = super::cascade::plan(hops, nodes, table_columns, denormalized, &narrowing);
        let mut emitted = HashSet::new();
        let mut definitions = Vec::new();
        let filters: Vec<_> = hops
            .iter()
            .enumerate()
            .map(|(index, hop)| {
                let mut predicates = Vec::new();
                let (start, end) = hop.direction.edge_columns();
                for filter_only in [false, true] {
                    for (alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
                        if !narrowing.contains_key(alias)
                            || (nodes[alias].hydration == HydrationStrategy::FilterOnly)
                                != filter_only
                        {
                            continue;
                        }
                        let first_use = emitted.insert(alias.clone());
                        if first_use {
                            definitions
                                .push((format!("_filter_{alias}"), narrowing[alias].clone()));
                        }
                        if first_use || !filter_only {
                            predicates.push(Expr::InSubquery {
                                expr: Box::new(Expr::col(format!("e{index}"), column)),
                                cte_name: format!("_filter_{alias}"),
                                column: DEFAULT_PRIMARY_KEY.into(),
                            });
                        }
                    }
                }
                predicates
            })
            .collect();
        let (mut source, edge_if_predicates) = super::flat::edge_source(
            hops,
            aggregate,
            sort_keys,
            nodes,
            table_columns,
            denormalized,
            &filters,
            &cascades,
        )?;
        let mut outputs = Vec::new();
        let mut bindings = Vec::new();
        let mut visited = HashSet::new();
        for (index, hop) in hops.iter().enumerate() {
            let (start, end) = hop.direction.edge_columns();
            for (node_alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
                let Some(node) = nodes.get(node_alias).filter(|_| visited.insert(node_alias))
                else {
                    continue;
                };
                let edge = format!("e{index}");
                let joined = node.hydration != HydrationStrategy::Skip
                    && !(node.hydration == HydrationStrategy::FilterOnly
                        && narrowing.contains_key(node_alias));
                bindings.push(BindingSource {
                    node: node_alias.clone(),
                    alias: edge.clone(),
                    column: column.into(),
                    joined,
                });
                if !joined {
                    continue;
                }
                let membership = if node.hydration == HydrationStrategy::Join && node.use_narrowing
                {
                    let alias = format!("e{index}n");
                    let mut predicates = Vec::new();
                    push_edge_predicates(&mut predicates, &alias, hop, nodes, table_columns, false);
                    predicates.extend(node_id_pin_predicates(&alias, hop, nodes));
                    let source = PhysicalSource::edge_keys(
                        hop,
                        &alias,
                        predicates,
                        cascades[index].as_ref(),
                    );
                    let outputs = vec![SelectExpr::new(
                        Expr::col(&alias, column),
                        DEFAULT_PRIMARY_KEY,
                    )];
                    definitions.push((
                        format!("_narrow_{node_alias}"),
                        PhysicalPlan { source, outputs },
                    ));
                    Some(Expr::InSubquery {
                        expr: Box::new(Expr::col(node_alias, DEFAULT_PRIMARY_KEY)),
                        cte_name: format!("_narrow_{node_alias}"),
                        column: DEFAULT_PRIMARY_KEY.into(),
                    })
                } else {
                    None
                };
                let table = node.table.as_ref().ok_or_else(|| {
                    QueryError::Lowering(format!("node '{node_alias}' has no table"))
                })?;
                let sort_key = sort_keys.get(table).ok_or_else(|| {
                    QueryError::Lowering(format!("no sort key for node table '{table}'"))
                })?;
                let scan = PhysicalPlan::node_scan(node, membership, sort_key)?;
                outputs.extend(scan.outputs);
                source = PhysicalSource::Join {
                    kind: JoinType::Inner,
                    condition: Expr::eq(
                        Expr::col(node_alias, DEFAULT_PRIMARY_KEY),
                        Expr::col(&edge, column),
                    ),
                    left: Box::new(source),
                    right: Box::new(scan.source),
                };
            }
        }
        Ok(Self {
            source,
            edge_if_predicates,
            definitions,
            outputs,
            bindings,
            edge_aliases: (0..hops.len()).map(|index| format!("e{index}")).collect(),
        })
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
    pub(super) fn filter(self, predicates: Vec<Expr>) -> Self {
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
        let mut plan = Self::filtered_keys(node, column)?;
        let PhysicalSource::Filter { predicate, input } = &mut plan.source else {
            unreachable!()
        };
        let PhysicalSource::Scan { final_, .. } = input.as_mut() else {
            unreachable!()
        };
        *final_ = false;
        for additional in extra {
            *predicate = Expr::and(predicate.clone(), additional);
        }
        Ok(plan)
    }

    pub fn filtered_keys(node: &NodePlan, column: &str) -> Result<Self> {
        let mut plan = Self::single_node(node)?;
        plan.outputs = vec![SelectExpr::new(
            Expr::col(&node.alias, column),
            DEFAULT_PRIMARY_KEY,
        )];
        Ok(plan)
    }

    pub fn node_scan(
        node: &NodePlan,
        narrowing: Option<Expr>,
        sort_key: &[String],
    ) -> Result<Self> {
        let mut plan = Self::single_node(node)?;
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
            let PhysicalSource::Filter {
                predicate,
                mut input,
            } = plan.source
            else {
                unreachable!()
            };
            let PhysicalSource::Scan { final_, .. } = input.as_mut() else {
                unreachable!()
            };
            *final_ = false;
            plan.source = PhysicalSource::Filter {
                predicate,
                input: Box::new(PhysicalSource::Latest {
                    sort_key: sort_key.to_vec(),
                    alias: node.alias.clone(),
                    input: Box::new(PhysicalSource::Filter {
                        predicate: Expr::conjoin(predicates).expect("narrowing predicate"),
                        input,
                    }),
                }),
            };
        }
        plan.source = PhysicalSource::Scope {
            alias: node.alias.clone(),
            input: Box::new(plan.source),
        };
        Ok(plan)
    }

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
                    relationship: None,
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
