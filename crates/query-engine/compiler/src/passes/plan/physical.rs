use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::constants::{
    EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX, EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX,
};
use crate::error::{QueryError, Result};
use crate::input::Direction;
use crate::passes::shared::{
    filter_to_expr, id_list_predicate, id_range_predicate, latest_node_predicates,
    node_select_columns,
};
use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::{DenormalizedKey, DenormalizedProperty, Hop, HydrationStrategy, NodePlan};

pub struct FlatPlan {
    pub reads: Vec<EdgeRead>,
    pub narrowing: HashMap<String, PhysicalPlan>,
    pub cascades: Vec<Option<PhysicalPlan>>,
}

impl FlatPlan {
    pub fn new(
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
            let mut keys =
                PhysicalPlan::filtered_keys(node, ontology::constants::DEFAULT_PRIMARY_KEY)?;
            let PhysicalSource::Filter { input, .. } = &mut keys.source else {
                unreachable!()
            };
            let PhysicalSource::Scan { final_, .. } = input.as_mut() else {
                unreachable!()
            };
            *final_ = false;
            keys.source = PhysicalSource::Latest {
                alias: alias.clone(),
                sort_key: sort_key.clone(),
                input: Box::new(keys.source),
            };
            narrowing.insert(alias.clone(), keys);
        }
        Ok(Self {
            reads: edge_reads(hops, aggregate, sort_keys, nodes)?,
            cascades: super::cascade::plan(hops, nodes, table_columns, denormalized, &narrowing),
            narrowing,
        })
    }
}

pub enum EdgeRead {
    Plain,
    Final { narrow_inside: bool },
    Latest { sort_key: Vec<String> },
    MultiHop(Box<PhysicalSource>),
}

pub fn edge_reads(
    hops: &[Hop],
    aggregate: bool,
    sort_keys: &HashMap<String, Vec<String>>,
    nodes: &HashMap<String, NodePlan>,
) -> Result<Vec<EdgeRead>> {
    hops.iter()
        .enumerate()
        .map(|(index, hop)| {
            Ok(if hop.max_hops > 1 {
                EdgeRead::MultiHop(Box::new(super::hops::multi_hop(
                    hop,
                    &format!("e{index}"),
                    nodes,
                )))
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
