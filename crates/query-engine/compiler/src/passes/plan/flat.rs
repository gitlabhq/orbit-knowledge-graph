use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::error::{QueryError, Result};
use crate::passes::shared::{deleted_false, filter_to_expr};

use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource, key_membership};
use super::{DenormalizedKey, DenormalizedProperty, Hop, HydrationStrategy, NodePlan};

pub(super) struct PlanningFacts<'a> {
    pub hops: &'a [Hop],
    pub nodes: &'a HashMap<String, NodePlan>,
    pub sort_keys: &'a HashMap<String, Vec<String>>,
    pub table_columns: &'a HashMap<String, HashSet<String>>,
    pub denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
}

struct FlatBuilder<'a> {
    facts: PlanningFacts<'a>,
    definitions: Vec<(String, PhysicalPlan)>,
    filtered: HashSet<String>,
    tagged: HashSet<(String, String)>,
}

pub(super) fn plan(facts: PlanningFacts<'_>, aggregate: bool) -> Result<ExecutionPlan> {
    FlatBuilder {
        facts,
        definitions: Vec::new(),
        filtered: HashSet::new(),
        tagged: HashSet::new(),
    }
    .build(aggregate)
}

impl FlatBuilder<'_> {
    fn build(mut self, aggregate: bool) -> Result<ExecutionPlan> {
        let mut source = None;
        let mut edge_if_predicates = None;
        let mut cascades = Vec::new();
        for (index, hop) in self.facts.hops.iter().enumerate() {
            let membership = self.filter_keys(index)?;
            let cascade = self.cascade(index, cascades.last().and_then(Option::as_ref));
            let (edge, condition) = self.edge(index, membership, cascade.as_ref(), aggregate)?;
            edge_if_predicates = condition.or(edge_if_predicates);
            source = Some(match source {
                Some(previous) => {
                    let join = hop
                        .join_prev
                        .as_ref()
                        .expect("non-first hop must have join_prev");
                    PhysicalSource::Join {
                        kind: JoinType::Inner,
                        condition: Expr::eq(
                            Expr::col(&join.prev_alias, &join.prev_col),
                            Expr::col(format!("e{index}"), &join.curr_col),
                        ),
                        left: Box::new(previous),
                        right: Box::new(edge),
                    }
                }
                None => edge,
            });
            cascades.push(cascade);
        }
        let mut plan = ExecutionPlan {
            source: source.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?,
            definitions: self.definitions,
            outputs: Vec::new(),
            bindings: Vec::new(),
            edge_aliases: (0..self.facts.hops.len())
                .map(|index| format!("e{index}"))
                .collect(),
            edge_if_predicates,
        };
        let mut visited = HashSet::new();
        for (index, hop) in self.facts.hops.iter().enumerate() {
            let (start, end) = hop.direction.edge_columns();
            for (alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
                let Some(node) = self
                    .facts
                    .nodes
                    .get(alias)
                    .filter(|_| visited.insert(alias))
                else {
                    continue;
                };
                let edge = &plan.edge_aliases[index];
                let joined = node.hydration != HydrationStrategy::Skip
                    && !(node.hydration == HydrationStrategy::FilterOnly
                        && self.filtered.contains(alias));
                plan.bindings.push(BindingSource {
                    node: alias.clone(),
                    alias: edge.clone(),
                    column: column.into(),
                    joined,
                });
                if !joined {
                    continue;
                }
                let membership = if node.hydration == HydrationStrategy::Join && node.use_narrowing
                {
                    let scan_alias = format!("{edge}n");
                    let mut predicates = self.facts.edge_predicates(&scan_alias, hop, false);
                    predicates.extend(self.facts.node_id_predicates(&scan_alias, hop));
                    let keys = PhysicalPlan {
                        source: PhysicalSource::edge_keys(
                            hop,
                            &scan_alias,
                            predicates,
                            cascades[index].as_ref(),
                        ),
                        outputs: vec![SelectExpr::new(
                            Expr::col(&scan_alias, column),
                            DEFAULT_PRIMARY_KEY,
                        )],
                    };
                    let name = format!("_narrow_{alias}");
                    plan.definitions.push((name.clone(), keys));
                    Some(key_membership(alias, DEFAULT_PRIMARY_KEY, name))
                } else {
                    None
                };
                let scan =
                    PhysicalPlan::node_scan(node, membership, self.facts.node_sort_key(node)?)?;
                plan.outputs.extend(scan.outputs);
                plan.source = PhysicalSource::Join {
                    kind: JoinType::Inner,
                    condition: Expr::eq(
                        Expr::col(alias, DEFAULT_PRIMARY_KEY),
                        Expr::col(edge, column),
                    ),
                    left: Box::new(plan.source),
                    right: Box::new(scan.source),
                };
            }
        }
        Ok(plan)
    }

    fn filter_keys(&mut self, index: usize) -> Result<Vec<Expr>> {
        let hop = &self.facts.hops[index];
        let (start, end) = hop.direction.edge_columns();
        let mut predicates = Vec::new();
        for filter_only in [false, true] {
            for (alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
                let Some(node) = self.facts.nodes.get(alias) else {
                    continue;
                };
                let eligible = if filter_only {
                    node.hydration == HydrationStrategy::FilterOnly && self.facts.hops.len() >= 2
                } else {
                    node.hydration == HydrationStrategy::Join && node.has_selective_filters()
                };
                if !eligible {
                    continue;
                }
                let first = self.filtered.insert(alias.clone());
                let name = format!("_filter_{alias}");
                if first {
                    let keys = if filter_only {
                        PhysicalPlan::filtered_keys(node, DEFAULT_PRIMARY_KEY)?
                    } else {
                        let sort_key = self.facts.node_sort_key(node)?;
                        if sort_key.is_empty() {
                            return Err(QueryError::Lowering(format!(
                                "no sort key for node '{alias}'; cannot plan narrowing"
                            )));
                        }
                        let mut keys =
                            PhysicalPlan::candidate_keys(node, DEFAULT_PRIMARY_KEY, vec![])?;
                        keys.source = PhysicalSource::Latest {
                            alias: alias.clone(),
                            sort_key: sort_key.to_vec(),
                            input: Box::new(keys.source),
                        };
                        keys
                    };
                    self.definitions.push((name.clone(), keys));
                }
                if first || !filter_only {
                    predicates.push(key_membership(&format!("e{index}"), column, name));
                }
            }
        }
        Ok(predicates)
    }

    fn cascade(&self, index: usize, upstream: Option<&PhysicalPlan>) -> Option<PhysicalPlan> {
        let hop = &self.facts.hops[index];
        let join = hop
            .join_prev
            .as_ref()
            .filter(|_| hop.cascade_anchor && index > 0)?;
        let previous = &self.facts.hops[index - 1];
        let selective =
            [&previous.from_node, &previous.to_node]
                .into_iter()
                .any(|alias| {
                    self.filtered.contains(alias)
                        || self.facts.nodes.get(alias).is_some_and(|node| {
                            !node.node_ids.is_empty() || node.id_range.is_some()
                        })
                });
        if !selective && upstream.is_none() {
            return None;
        }
        let alias = format!("{}p", join.prev_alias);
        let mut predicates =
            self.facts
                .filtered_edge_predicates(&alias, previous, &mut HashSet::new());
        let (start, end) = previous.direction.edge_columns();
        for (node, column) in [(&previous.from_node, start), (&previous.to_node, end)] {
            if self.filtered.contains(node) {
                predicates.push(key_membership(&alias, column, format!("_filter_{node}")));
            }
        }
        Some(PhysicalPlan {
            source: PhysicalSource::edge_keys(previous, &alias, predicates, upstream),
            outputs: vec![SelectExpr::col(&alias, &join.prev_col)],
        })
    }

    fn edge(
        &mut self,
        index: usize,
        membership: Vec<Expr>,
        cascade: Option<&PhysicalPlan>,
        aggregate: bool,
    ) -> Result<(PhysicalSource, Option<Expr>)> {
        let hop = &self.facts.hops[index];
        let alias = format!("e{index}");
        let scan = |final_| PhysicalSource::Scan {
            table: hop.edge_table.clone(),
            alias: alias.clone(),
            final_,
            relationship: Some(hop.input_index),
        };
        let multi_hop = hop.max_hops > 1;
        let dedup = self.facts.hops.len() >= 2;
        if !multi_hop && !dedup && aggregate {
            let sort_key = self
                .facts
                .sort_keys
                .get(&hop.edge_table)
                .filter(|key| !key.is_empty())
                .ok_or_else(|| {
                    QueryError::Lowering(format!(
                        "no sort key for edge table '{}'; cannot plan latest rows",
                        hop.edge_table
                    ))
                })?;
            let mut predicates = self
                .facts
                .filtered_edge_predicates(&alias, hop, &mut self.tagged);
            predicates.extend(membership);
            let condition = Expr::conjoin(predicates.clone());
            return Ok((
                PhysicalSource::Latest {
                    sort_key: sort_key.clone(),
                    alias: alias.clone(),
                    input: Box::new(scan(false).filter(predicates)),
                },
                condition,
            ));
        }
        let edge = if multi_hop {
            super::hops::multi_hop(hop, &alias, self.facts.nodes)
                .filter(membership)
                .cascade(hop, &alias, cascade)
        } else if dedup {
            let (start, end) = hop.direction.edge_columns();
            let narrow_inside = self
                .facts
                .sort_keys
                .get(&hop.edge_table)
                .is_some_and(|keys| keys.iter().take(4).any(|key| key == start || key == end));
            let mut input = scan(true).filter(self.facts.node_id_predicates(&alias, hop));
            let outside = if narrow_inside {
                input = input.filter(membership).cascade(hop, &alias, cascade);
                Vec::new()
            } else {
                membership
            };
            let scoped = PhysicalSource::Scope {
                alias: alias.clone(),
                input: Box::new(input.filter(vec![deleted_false(&alias)])),
            };
            if narrow_inside {
                scoped
            } else {
                scoped.filter(outside).cascade(hop, &alias, cascade)
            }
        } else {
            scan(false).filter(membership).cascade(hop, &alias, cascade)
        };
        let mut predicates = if multi_hop {
            Vec::new()
        } else {
            self.facts.edge_predicates(&alias, hop, dedup)
        };
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| filter_to_expr(&alias, property, filter)),
        );
        self.facts
            .push_denorm_tags(&mut predicates, hop, &alias, &mut self.tagged);
        if !dedup || multi_hop {
            predicates.extend(self.facts.node_id_predicates(&alias, hop));
        }
        Ok((edge.filter(predicates), None))
    }
}

impl PlanningFacts<'_> {
    fn node_sort_key(&self, node: &NodePlan) -> Result<&[String]> {
        let table = node
            .table
            .as_ref()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        self.sort_keys
            .get(table)
            .map(Vec::as_slice)
            .ok_or_else(|| QueryError::Lowering(format!("no sort key for node table '{table}'")))
    }
}
