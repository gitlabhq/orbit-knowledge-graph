use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;
use crate::passes::shared::id_list_predicate;

use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource};
use super::{Hop, HydrationStrategy, NodePlan};

struct StarCandidates {
    pub definitions: Vec<(String, PhysicalPlan)>,
    pub center_filter: Option<Expr>,
    pub targets: HashMap<String, TargetNarrowing>,
}

enum TargetNarrowing {
    Reference(String),
    Define { name: String, keys: PhysicalPlan },
}

pub(super) fn star(
    center: &str,
    hops: &[Hop],
    nodes: &HashMap<String, NodePlan>,
    traversal: bool,
    sort_keys: &HashMap<String, Vec<String>>,
) -> Result<ExecutionPlan> {
    let candidates = StarCandidates::plan(center, hops, nodes, traversal)?;
    let mut definitions = candidates.definitions;
    let mut root = PhysicalPlan::single_node(&nodes[center])?;
    let mut center_predicates: Vec<_> = candidates.center_filter.into_iter().collect();
    for hop in hops {
        let fk = hop.fk.as_ref().expect("validated FK star hop");
        let target = &nodes[&fk.target_node];
        if fk.fk_node == center
            && !target.node_ids.is_empty()
            && fk.referenced_column == DEFAULT_PRIMARY_KEY
        {
            center_predicates.push(id_list_predicate(center, &fk.fk_column, &target.node_ids));
        }
    }
    root.source = PhysicalSource::Scope {
        alias: center.into(),
        input: Box::new(root.source.filter(center_predicates)),
    };
    let mut bindings = vec![BindingSource {
        node: center.into(),
        alias: center.into(),
        column: DEFAULT_PRIMARY_KEY.into(),
        joined: true,
    }];
    for hop in hops {
        let fk = hop.fk.as_ref().expect("validated FK star hop");
        let target = &nodes[&fk.target_node];
        if fk.fk_node != center
            && !target.node_ids.is_empty()
            && fk.referenced_column == DEFAULT_PRIMARY_KEY
        {
            root.source = root.source.filter(vec![id_list_predicate(
                &fk.fk_node,
                &fk.fk_column,
                &target.node_ids,
            )]);
        }
        if target.fk_needs_join {
            let membership = candidates.targets.get(&fk.target_node).map(|narrowing| {
                let name = match narrowing {
                    TargetNarrowing::Reference(name) => name,
                    TargetNarrowing::Define { name, keys } => {
                        definitions.push((name.clone(), keys.clone()));
                        name
                    }
                };
                Expr::InSubquery {
                    expr: Box::new(Expr::col(&target.alias, &fk.referenced_column)),
                    cte_name: name.clone(),
                    column: DEFAULT_PRIMARY_KEY.into(),
                }
            });
            let table = target.table.as_ref().ok_or_else(|| {
                QueryError::Lowering(format!("node '{}' has no table", target.alias))
            })?;
            let sort_key = sort_keys.get(table).ok_or_else(|| {
                QueryError::Lowering(format!("no sort key for node table '{table}'"))
            })?;
            let scan = PhysicalPlan::node_scan(target, membership, sort_key)?;
            root.source = PhysicalSource::Join {
                kind: JoinType::Inner,
                condition: Expr::eq(
                    Expr::col(&target.alias, &fk.referenced_column),
                    Expr::col(&fk.fk_node, &fk.fk_column),
                ),
                left: Box::new(root.source),
                right: Box::new(scan.source),
            };
            root.outputs.extend(scan.outputs);
        } else if target.hydration == HydrationStrategy::FilterOnly {
            let name = format!("_filter_{}", target.alias);
            definitions.push((
                name.clone(),
                PhysicalPlan::filtered_keys(target, &fk.referenced_column)?,
            ));
            root.source = root.source.filter(vec![Expr::InSubquery {
                expr: Box::new(Expr::col(&fk.fk_node, &fk.fk_column)),
                cte_name: name,
                column: DEFAULT_PRIMARY_KEY.into(),
            }]);
        }
        let (alias, column) = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
            (fk.fk_node.clone(), fk.fk_column.clone())
        } else {
            (fk.target_node.clone(), DEFAULT_PRIMARY_KEY.into())
        };
        bindings.push(BindingSource {
            node: fk.target_node.clone(),
            alias,
            column,
            joined: target.fk_needs_join,
        });
    }
    let mut edge_aliases = Vec::new();
    if traversal {
        for (index, hop) in hops.iter().enumerate() {
            let fk = hop.fk.as_ref().expect("validated FK star hop");
            let target_id = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
                Expr::col(center, &fk.fk_column)
            } else {
                Expr::col(&fk.target_node, DEFAULT_PRIMARY_KEY)
            };
            let center_id = Expr::col(center, DEFAULT_PRIMARY_KEY);
            let (from_id, to_id) = if fk.fk_node == hop.from_node {
                (center_id, target_id)
            } else {
                (target_id, center_id)
            };
            let edge = format!("e{index}");
            root.outputs
                .extend(edge_outputs(hop, index, nodes, from_id, to_id));
            edge_aliases.push(edge);
        }
    }
    Ok(ExecutionPlan {
        source: root.source,
        outputs: root.outputs,
        definitions,
        bindings,
        edge_aliases,
        edge_if_predicates: None,
    })
}

pub(super) fn edge_outputs(
    hop: &Hop,
    index: usize,
    nodes: &HashMap<String, NodePlan>,
    from_id: Expr,
    to_id: Expr,
) -> [SelectExpr; 5] {
    let (source, source_id, target, target_id) = match hop.direction {
        Direction::Incoming => (&hop.to_node, to_id, &hop.from_node, from_id),
        Direction::Outgoing | Direction::Both => (&hop.from_node, from_id, &hop.to_node, to_id),
    };
    [
        (
            EDGE_TYPE_SUFFIX,
            Expr::string(hop.rel_types.first().map(String::as_str).unwrap_or("")),
        ),
        (EDGE_SRC_SUFFIX, source_id),
        (
            EDGE_SRC_TYPE_SUFFIX,
            Expr::string(nodes[source].entity.as_deref().unwrap_or("")),
        ),
        (EDGE_DST_SUFFIX, target_id),
        (
            EDGE_DST_TYPE_SUFFIX,
            Expr::string(nodes[target].entity.as_deref().unwrap_or("")),
        ),
    ]
    .map(|(suffix, value)| SelectExpr::new(value, format!("e{index}_{suffix}")))
}

impl StarCandidates {
    pub fn plan(
        center: &str,
        hops: &[Hop],
        nodes: &HashMap<String, NodePlan>,
        traversal: bool,
    ) -> Result<Self> {
        let node = |alias: &str| {
            nodes
                .get(alias)
                .ok_or_else(|| QueryError::Lowering(format!("FK node '{alias}' not found")))
        };
        let center_node = node(center)?;
        let mut extra: HashMap<String, Vec<Expr>> = HashMap::new();
        for hop in hops {
            let fk = hop
                .fk
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("FK star hop missing metadata".into()))?;
            let target = node(&fk.target_node)?;
            if !target.node_ids.is_empty() && fk.referenced_column == DEFAULT_PRIMARY_KEY {
                extra
                    .entry(fk.fk_node.clone())
                    .or_default()
                    .push(id_list_predicate(
                        &fk.fk_node,
                        &fk.fk_column,
                        &target.node_ids,
                    ));
            }
        }

        let mut definitions = Vec::new();
        let mut references = HashMap::new();
        let mut visited = HashSet::new();
        for hop in hops {
            let fk = hop.fk.as_ref().expect("validated FK star hop");
            let target = node(&fk.target_node)?;
            if !target.fk_needs_join || !visited.insert(&fk.target_node) {
                continue;
            }
            let additional = extra.get(&fk.target_node).cloned().unwrap_or_default();
            if target.filters.is_empty()
                && target.node_ids.is_empty()
                && target.id_range.is_none()
                && additional.is_empty()
            {
                continue;
            }
            let name = format!("_candidate_{}", fk.target_node);
            definitions.push((
                name.clone(),
                PhysicalPlan::candidate_keys(target, &fk.referenced_column, additional)?,
            ));
            references.insert(fk.target_node.clone(), name);
        }
        for hop in hops {
            let fk = hop.fk.as_ref().expect("validated FK star hop");
            if let Some(name) = references.get(&fk.target_node) {
                extra
                    .entry(fk.fk_node.clone())
                    .or_default()
                    .push(Expr::InSubquery {
                        expr: Box::new(Expr::col(&fk.fk_node, &fk.fk_column)),
                        cte_name: name.clone(),
                        column: DEFAULT_PRIMARY_KEY.into(),
                    });
            }
        }
        let center_extra = extra.remove(center).unwrap_or_default();
        let center_filter = if hops.iter().any(|hop| {
            nodes[&hop.fk.as_ref().expect("validated FK star hop").target_node].fk_needs_join
        }) && !center_extra.is_empty()
        {
            let name = format!("_candidate_{center}");
            definitions.push((
                name.clone(),
                PhysicalPlan::candidate_keys(
                    center_node,
                    DEFAULT_PRIMARY_KEY,
                    center_extra.clone(),
                )?,
            ));
            references.insert(center.into(), name.clone());
            Some(Expr::InSubquery {
                expr: Box::new(Expr::col(center, DEFAULT_PRIMARY_KEY)),
                cte_name: name,
                column: DEFAULT_PRIMARY_KEY.into(),
            })
        } else {
            None
        };

        let mut targets = HashMap::new();
        for hop in hops {
            let fk = hop.fk.as_ref().expect("validated FK star hop");
            let target = node(&fk.target_node)?;
            if !target.fk_needs_join {
                continue;
            }
            let narrowing = if let Some(name) = references.get(&fk.target_node) {
                TargetNarrowing::Reference(name.clone())
            } else if (traversal || fk.fk_node != center)
                && target.filters.is_empty()
                && target.node_ids.is_empty()
                && target.id_range.is_none()
                && center_node.has_selective_filters()
            {
                TargetNarrowing::Define {
                    name: format!("_narrow_{}", fk.target_node),
                    keys: PhysicalPlan::candidate_keys(
                        center_node,
                        &fk.fk_column,
                        center_extra.clone(),
                    )?,
                }
            } else {
                continue;
            };
            targets.insert(fk.target_node.clone(), narrowing);
        }
        Ok(Self {
            definitions,
            center_filter,
            targets,
        })
    }
}
