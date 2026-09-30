use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::ast::Expr;
use crate::error::{QueryError, Result};
use crate::passes::shared::id_list_predicate;

use super::physical::PhysicalPlan;
use super::{Hop, NodePlan};

pub struct StarCandidates {
    pub definitions: Vec<(String, PhysicalPlan)>,
    pub center_filter: Option<Expr>,
    pub targets: HashMap<String, TargetNarrowing>,
}

pub enum TargetNarrowing {
    Reference(String),
    Define { name: String, keys: PhysicalPlan },
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
