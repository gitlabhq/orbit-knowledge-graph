use query_data_model::QueryDataModel;
use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::ast::{Expr, SelectExpr};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;
use crate::passes::shared::id_list_predicate;

use super::context::PlanningContext;
use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource, key_membership};
use super::{Hop, HydrationStrategy, NodePlan};

pub(super) fn star<M: QueryDataModel + ?Sized>(
    context: &PlanningContext<'_, M>,
    center: &str,
) -> Result<ExecutionPlan> {
    let hops = &context.hops;
    let nodes = &context.nodes;
    let traversal = !context.aggregate();
    let center_node = context.node(center)?;
    let root = PhysicalPlan::single_node(center_node)?;
    let mut plan = ExecutionPlan {
        source: root.source,
        outputs: root.outputs,
        definitions: Vec::new(),
        bindings: vec![BindingSource::table(center)],
        edge_aliases: Vec::new(),
        edge_if_predicates: None,
    };
    let mut extra: HashMap<String, Vec<Expr>> = HashMap::new();
    for hop in hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FK star hop missing metadata".into()))?;
        let target = context.node(&fk.target_node)?;
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
    let center_pins = extra.get(center).cloned().unwrap_or_default();
    let mut references = HashMap::new();
    let mut visited = HashSet::new();
    for hop in hops {
        let fk = hop.fk.as_ref().expect("validated FK star hop");
        let target = &nodes[&fk.target_node];
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
        plan.definitions.push((
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
                .push(key_membership(&fk.fk_node, &fk.fk_column, name.clone()));
        }
    }
    let center_extra = extra.remove(center).unwrap_or_default();
    if !visited.is_empty() && !center_extra.is_empty() {
        let name = format!("_candidate_{center}");
        plan.definitions.push((
            name.clone(),
            PhysicalPlan::candidate_keys(center_node, DEFAULT_PRIMARY_KEY, center_extra.clone())?,
        ));
        references.insert(center.into(), name.clone());
        plan.source = plan
            .source
            .filter(vec![key_membership(center, DEFAULT_PRIMARY_KEY, name)]);
    }
    plan.source = PhysicalSource::Scope {
        alias: center.into(),
        input: Box::new(plan.source.filter(center_pins)),
    };

    for hop in hops {
        let fk = hop.fk.as_ref().expect("validated FK star hop");
        let target = &nodes[&fk.target_node];
        if fk.fk_node != center
            && !target.node_ids.is_empty()
            && fk.referenced_column == DEFAULT_PRIMARY_KEY
        {
            plan.source = plan.source.filter(vec![id_list_predicate(
                &fk.fk_node,
                &fk.fk_column,
                &target.node_ids,
            )]);
        }
        if target.fk_needs_join {
            let name = if let Some(name) = references.get(&fk.target_node) {
                Some(name.clone())
            } else if (traversal || fk.fk_node != center)
                && target.filters.is_empty()
                && target.node_ids.is_empty()
                && target.id_range.is_none()
                && center_node.has_selective_filters()
            {
                let name = format!("_narrow_{}", fk.target_node);
                plan.definitions.push((
                    name.clone(),
                    PhysicalPlan::candidate_keys(center_node, &fk.fk_column, center_extra.clone())?,
                ));
                Some(name)
            } else {
                None
            };
            let membership =
                name.map(|name| key_membership(&target.alias, &fk.referenced_column, name));
            let scan = PhysicalPlan::node_scan(target, membership, context.node_sort_key(target)?)?;
            plan.source = plan.source.inner_join(
                scan.source,
                Expr::eq(
                    Expr::col(&target.alias, &fk.referenced_column),
                    Expr::col(&fk.fk_node, &fk.fk_column),
                ),
            );
            plan.outputs.extend(scan.outputs);
        } else if target.hydration == HydrationStrategy::FilterOnly {
            let name = format!("_filter_{}", target.alias);
            plan.definitions.push((
                name.clone(),
                PhysicalPlan::filtered_keys(target, &fk.referenced_column)?,
            ));
            plan.source =
                plan.source
                    .filter(vec![key_membership(&fk.fk_node, &fk.fk_column, name)]);
        }
        let (alias, column) = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
            (fk.fk_node.clone(), fk.fk_column.clone())
        } else {
            (fk.target_node.clone(), DEFAULT_PRIMARY_KEY.into())
        };
        plan.bindings.push(BindingSource {
            node: fk.target_node.clone(),
            alias,
            column,
            joined: target.fk_needs_join,
        });
    }
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
            plan.outputs
                .extend(edge_outputs(hop, index, nodes, from_id, to_id));
            plan.edge_aliases.push(format!("e{index}"));
        }
    }
    Ok(plan)
}

pub(super) fn chain<M: QueryDataModel + ?Sized>(
    context: &PlanningContext<'_, M>,
) -> Result<ExecutionPlan> {
    let root = &context
        .hops
        .first()
        .ok_or_else(|| QueryError::Lowering("FK chain requires a hop".into()))?
        .from_node;
    let scan = PhysicalPlan::node_scan(context.node(root)?, None, &[])?;
    let mut plan = ExecutionPlan {
        source: scan.source,
        outputs: scan.outputs,
        bindings: vec![BindingSource::table(root)],
        definitions: vec![],
        edge_aliases: vec![],
        edge_if_predicates: None,
    };
    let mut reached = HashSet::from([root.as_str()]);
    for (index, hop) in context.hops.iter().enumerate() {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FK chain hop missing FK metadata".into()))?;
        let alias = if reached.contains(hop.from_node.as_str()) {
            &hop.to_node
        } else {
            &hop.from_node
        };
        let next = PhysicalPlan::node_scan(context.node(alias)?, None, &[])?;
        plan.source = plan.source.inner_join(
            next.source,
            Expr::eq(
                Expr::col(&fk.fk_node, &fk.fk_column),
                Expr::col(&fk.target_node, &fk.referenced_column),
            ),
        );
        plan.outputs.extend(next.outputs);
        plan.bindings.push(BindingSource::table(alias));
        reached.insert(hop.from_node.as_str());
        reached.insert(hop.to_node.as_str());
        if !context.aggregate() {
            plan.outputs.extend(edge_outputs(
                hop,
                index,
                &context.nodes,
                Expr::col(&hop.from_node, DEFAULT_PRIMARY_KEY),
                Expr::col(&hop.to_node, DEFAULT_PRIMARY_KEY),
            ));
        }
    }
    Ok(plan)
}

fn edge_outputs(
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
