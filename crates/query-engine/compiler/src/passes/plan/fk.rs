use query_data_model::QueryDataModel;
use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::requirements::{Column, OutputValue, Projection, id_list};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;

use super::context::PlanningContext;
use super::physical::{BindingSource, ExecutionPlan, key_membership};
use super::{Hop, HydrationStrategy, NodePlan};

pub(super) fn star<M: QueryDataModel + ?Sized>(
    context: &mut PlanningContext<'_, M>,
    center: &str,
) -> Result<ExecutionPlan> {
    let foreign_keys = context
        .hops
        .iter()
        .map(|hop| {
            hop.fk
                .clone()
                .ok_or_else(|| QueryError::Lowering("FK star hop missing metadata".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    let traversal = !context.aggregate();
    let root = context.node_plan(center)?;
    let mut plan = ExecutionPlan {
        source: root.source,
        outputs: root.outputs,
        definitions: Vec::new(),
        bindings: vec![BindingSource::table(center)],
    };
    let mut center_pins = Vec::new();
    for fk in &foreign_keys {
        let target = context.node(&fk.target_node)?;
        if !target.node_ids.is_empty() && fk.referenced_column == DEFAULT_PRIMARY_KEY {
            center_pins.push(id_list(&fk.fk_node, &fk.fk_column, &target.node_ids));
        }
    }
    let mut references = HashMap::new();
    let mut visited = HashSet::new();
    for fk in &foreign_keys {
        let target = context.node(&fk.target_node)?;
        if !target.fk_needs_join || !visited.insert(&fk.target_node) {
            continue;
        }
        if target.filters.is_empty() && target.node_ids.is_empty() && target.id_range.is_none() {
            continue;
        }
        let (name, keys) = context
            .candidate_keys(&fk.target_node, &fk.referenced_column, vec![])?
            .define(format!("_candidate_{}", fk.target_node));
        plan.definitions.push((name.clone(), keys));
        references.insert(fk.target_node.clone(), name);
    }
    let mut center_extra = center_pins.clone();
    for fk in &foreign_keys {
        if let Some(name) = references.get(&fk.target_node) {
            center_extra.push(key_membership(&fk.fk_node, &fk.fk_column, name.clone()));
        }
    }
    if !visited.is_empty() && !center_extra.is_empty() {
        let (name, keys) = context
            .candidate_keys(center, DEFAULT_PRIMARY_KEY, center_extra.clone())?
            .define(format!("_candidate_{center}"));
        plan.definitions.push((name.clone(), keys));
        plan.source = plan
            .source
            .filter(vec![key_membership(center, DEFAULT_PRIMARY_KEY, name)]);
    }
    plan.source = plan.source.filter(center_pins).scoped(center);

    for fk in &foreign_keys {
        let target = context.node(&fk.target_node)?;
        if target.fk_needs_join {
            let name = if let Some(name) = references.get(&fk.target_node) {
                Some(name.clone())
            } else if traversal
                && target.filters.is_empty()
                && target.node_ids.is_empty()
                && target.id_range.is_none()
                && context.node(center)?.has_selective_filters()
            {
                let (name, keys) = context
                    .candidate_keys(center, &fk.fk_column, center_extra.clone())?
                    .define(format!("_narrow_{}", fk.target_node));
                plan.definitions.push((name.clone(), keys));
                Some(name)
            } else {
                None
            };
            let membership =
                name.map(|name| key_membership(&fk.target_node, &fk.referenced_column, name));
            let scan = context.node_scan(&fk.target_node, membership)?;
            plan.source = plan.source.inner_join(
                scan.source,
                (
                    Column::new(&fk.target_node, &fk.referenced_column),
                    Column::new(&fk.fk_node, &fk.fk_column),
                ),
            );
            plan.outputs.extend(scan.outputs);
        } else if target.hydration == HydrationStrategy::FilterOnly {
            let (name, keys) = context
                .filtered_keys(&fk.target_node, &fk.referenced_column)?
                .define(format!("_filter_{}", fk.target_node));
            plan.definitions.push((name.clone(), keys));
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
            joined: context.node(&fk.target_node)?.fk_needs_join,
        });
    }
    if traversal {
        for (index, (hop, fk)) in context.hops.iter().zip(&foreign_keys).enumerate() {
            let target_id = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
                Column::new(center, &fk.fk_column)
            } else {
                Column::new(&fk.target_node, DEFAULT_PRIMARY_KEY)
            };
            let center_id = Column::new(center, DEFAULT_PRIMARY_KEY);
            let (from_id, to_id) = if fk.fk_node == hop.from_node {
                (center_id, target_id)
            } else {
                (target_id, center_id)
            };
            plan.outputs
                .extend(edge_outputs(hop, index, &context.nodes, from_id, to_id));
        }
    }
    Ok(plan)
}

pub(super) fn chain<M: QueryDataModel + ?Sized>(
    context: &mut PlanningContext<'_, M>,
) -> Result<ExecutionPlan> {
    let root = context
        .hops
        .first()
        .ok_or_else(|| QueryError::Lowering("FK chain requires a hop".into()))?
        .from_node
        .clone();
    let scan = context.node_scan(&root, None)?;
    let mut plan = ExecutionPlan {
        source: scan.source,
        outputs: scan.outputs,
        bindings: vec![BindingSource::table(&root)],
        definitions: vec![],
    };
    let mut reached = HashSet::from([root]);
    for index in 0..context.hops.len() {
        let hop = &context.hops[index];
        let alias = if reached.contains(hop.from_node.as_str()) {
            &hop.to_node
        } else {
            &hop.from_node
        }
        .clone();
        let next = context.node_scan(&alias, None)?;
        let hop = &context.hops[index];
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FK chain hop missing FK metadata".into()))?;
        plan.source = plan.source.inner_join(
            next.source,
            (
                Column::new(&fk.fk_node, &fk.fk_column),
                Column::new(&fk.target_node, &fk.referenced_column),
            ),
        );
        plan.outputs.extend(next.outputs);
        plan.bindings.push(BindingSource::table(&alias));
        reached.insert(hop.from_node.clone());
        reached.insert(hop.to_node.clone());
        if !context.aggregate() {
            plan.outputs.extend(edge_outputs(
                hop,
                index,
                &context.nodes,
                Column::new(&hop.from_node, DEFAULT_PRIMARY_KEY),
                Column::new(&hop.to_node, DEFAULT_PRIMARY_KEY),
            ));
        }
    }
    Ok(plan)
}

fn edge_outputs(
    hop: &Hop,
    index: usize,
    nodes: &HashMap<String, NodePlan>,
    from_id: Column,
    to_id: Column,
) -> [Projection; 5] {
    let (source, source_id, target, target_id) = match hop.direction {
        Direction::Incoming => (&hop.to_node, to_id, &hop.from_node, from_id),
        Direction::Outgoing | Direction::Both => (&hop.from_node, from_id, &hop.to_node, to_id),
    };
    [
        (
            EDGE_TYPE_SUFFIX,
            OutputValue::Text(hop.rel_types.first().cloned().unwrap_or_default()),
        ),
        (EDGE_SRC_SUFFIX, OutputValue::Column(source_id)),
        (
            EDGE_SRC_TYPE_SUFFIX,
            OutputValue::Text(
                nodes
                    .get(source)
                    .and_then(|node| node.entity.clone())
                    .unwrap_or_default(),
            ),
        ),
        (EDGE_DST_SUFFIX, OutputValue::Column(target_id)),
        (
            EDGE_DST_TYPE_SUFFIX,
            OutputValue::Text(
                nodes
                    .get(target)
                    .and_then(|node| node.entity.clone())
                    .unwrap_or_default(),
            ),
        ),
    ]
    .map(|(suffix, value)| Projection::new(value, format!("e{index}_{suffix}")))
}
