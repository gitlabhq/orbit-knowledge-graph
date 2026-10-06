use query_data_model::{QueryBackendCatalog, QueryDataModel};
use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::requirements::{Column, OutputValue, id_list};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;

use super::context::PlanningContext;
use super::physical::{ExecutionPlan, KeyConstraint};
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
    let center_scope = context.child_scope(context.bindings.root())?;
    let source = context.node_source(center_scope, center, true)?;
    let center_relation = source.relation();
    let predicates = context.node_predicates(center_relation, context.node(center)?)?;
    let mut plan = ExecutionPlan {
        source: source.filter(predicates),
        outputs: vec![],
        definitions: Vec::new(),
        bindings: HashMap::new(),
    };
    let mut center_pins = Vec::new();
    let mut center_extra = Vec::new();
    let center_table = context
        .node(center)?
        .table
        .as_deref()
        .expect("center table")
        .to_owned();
    for fk in &foreign_keys {
        let target = context.node(&fk.target_node)?;
        if !target.node_ids.is_empty() && fk.referenced_column == DEFAULT_PRIMARY_KEY {
            let stored = context
                .model
                .query_backend()
                .storage()
                .resolve_column(&center_table, &fk.fk_column)
                .map_err(|error| QueryError::Lowering(error.to_string()))?;
            center_extra.push(KeyConstraint::Ids(stored, target.node_ids.clone()));
            center_pins.push(id_list(
                context.column(center_relation, &fk.fk_column)?,
                &target.node_ids,
            ));
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
        let keys = context.candidate_keys(&fk.target_node, &fk.referenced_column, vec![])?;
        let (name, keys) = context.define(
            context.bindings.root(),
            format!("_candidate_{}", fk.target_node),
            keys,
        )?;
        plan.definitions.push((name, keys));
        references.insert(fk.target_node.clone(), name);
    }
    for fk in &foreign_keys {
        if let Some(name) = references.get(&fk.target_node) {
            let column = context
                .model
                .query_backend()
                .storage()
                .resolve_column(&center_table, &fk.fk_column)
                .map_err(|error| QueryError::Lowering(error.to_string()))?;
            center_extra.push(KeyConstraint::Membership(column, *name));
        }
    }
    if !visited.is_empty() && !center_extra.is_empty() {
        let keys = context.candidate_keys(center, DEFAULT_PRIMARY_KEY, center_extra.clone())?;
        let (name, keys) = context.define(
            context.bindings.root(),
            format!("_candidate_{center}"),
            keys,
        )?;
        plan.definitions.push((name, keys));
        plan.source =
            plan.source.filter(vec![context.key_membership(
                context.column(center_relation, DEFAULT_PRIMARY_KEY)?,
                name,
            )?]);
    }
    plan.source = context.scoped(
        context.bindings.root(),
        center_scope,
        plan.source.filter(center_pins),
    )?;
    let center_relation = plan.source.relation();
    plan.bindings.extend([context.node_binding(
        center,
        context.column(center_relation, DEFAULT_PRIMARY_KEY)?,
        Some(center_relation),
    )?]);
    context
        .node_relations
        .insert(center.into(), center_relation);
    let mut relations = HashMap::from([(center.to_owned(), center_relation)]);
    plan.outputs = context.node_outputs(center_relation, center)?;

    for fk in &foreign_keys {
        let target = context.node(&fk.target_node)?;
        if target.fk_needs_join {
            let name = if let Some(name) = references.get(&fk.target_node) {
                Some(*name)
            } else if traversal
                && target.filters.is_empty()
                && target.node_ids.is_empty()
                && target.id_range.is_none()
                && context.node(center)?.has_selective_filters()
            {
                let keys = context.candidate_keys(center, &fk.fk_column, center_extra.clone())?;
                let (name, keys) = context.define(
                    context.bindings.root(),
                    format!("_narrow_{}", fk.target_node),
                    keys,
                )?;
                plan.definitions.push((name, keys));
                Some(name)
            } else {
                None
            };
            let membership = name.map(|name| (fk.referenced_column.as_str(), name));
            let scan = context.node_scan(&fk.target_node, membership)?;
            relations.insert(fk.target_node.clone(), scan.source.relation());
            plan.source = plan.source.inner_join(
                scan.source,
                (
                    context.column(relations[&fk.target_node], &fk.referenced_column)?,
                    context.column(center_relation, &fk.fk_column)?,
                ),
            );
            plan.outputs.extend(scan.outputs);
        } else if target.hydration == HydrationStrategy::FilterOnly {
            let keys = context.filtered_keys(&fk.target_node, &fk.referenced_column)?;
            let (name, keys) = context.define(
                context.bindings.root(),
                format!("_filter_{}", fk.target_node),
                keys,
            )?;
            plan.definitions.push((name, keys));
            plan.source = plan.source.filter(vec![
                context.key_membership(context.column(center_relation, &fk.fk_column)?, name)?,
            ]);
        }
        let identity = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
            context.column(center_relation, &fk.fk_column)?
        } else {
            context.column(relations[&fk.target_node], DEFAULT_PRIMARY_KEY)?
        };
        plan.bindings.extend([context.node_binding(
            &fk.target_node,
            identity,
            relations.get(&fk.target_node).copied(),
        )?]);
    }
    if traversal {
        for (index, fk) in foreign_keys.iter().enumerate() {
            let hop = &context.hops[index];
            let target_id = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
                context.column(center_relation, &fk.fk_column)?
            } else {
                context.column(relations[&fk.target_node], DEFAULT_PRIMARY_KEY)?
            };
            let center_id = context.column(center_relation, DEFAULT_PRIMARY_KEY)?;
            let (from_id, to_id) = if fk.fk_node == hop.from_node {
                (center_id, target_id)
            } else {
                (target_id, center_id)
            };
            let outputs = edge_outputs(hop, &context.nodes, from_id, to_id);
            for (suffix, value) in outputs {
                plan.outputs.push(context.projection(
                    context.bindings.root(),
                    value,
                    format!("e{index}_{suffix}"),
                )?);
            }
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
    let root_binding = context.node_binding(
        &root,
        context.column(scan.source.relation(), DEFAULT_PRIMARY_KEY)?,
        Some(scan.source.relation()),
    )?;
    let mut relations = HashMap::from([(root.clone(), scan.source.relation())]);
    let mut plan = ExecutionPlan {
        source: scan.source,
        outputs: scan.outputs,
        bindings: HashMap::from([root_binding]),
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
        relations.insert(alias.clone(), next.source.relation());
        let hop = &context.hops[index];
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FK chain hop missing FK metadata".into()))?;
        plan.source = plan.source.inner_join(
            next.source,
            (
                context.column(relations[&fk.fk_node], &fk.fk_column)?,
                context.column(relations[&fk.target_node], &fk.referenced_column)?,
            ),
        );
        plan.outputs.extend(next.outputs);
        plan.bindings.extend([context.node_binding(
            &alias,
            context.column(relations[&alias], DEFAULT_PRIMARY_KEY)?,
            Some(relations[&alias]),
        )?]);
        reached.insert(hop.from_node.clone());
        reached.insert(hop.to_node.clone());
        if !context.aggregate() {
            let outputs = edge_outputs(
                hop,
                &context.nodes,
                context.column(relations[&hop.from_node], DEFAULT_PRIMARY_KEY)?,
                context.column(relations[&hop.to_node], DEFAULT_PRIMARY_KEY)?,
            );
            for (suffix, value) in outputs {
                plan.outputs.push(context.projection(
                    context.bindings.root(),
                    value,
                    format!("e{index}_{suffix}"),
                )?);
            }
        }
    }
    Ok(plan)
}

fn edge_outputs(
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    from_id: Column,
    to_id: Column,
) -> [(&'static str, OutputValue); 5] {
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
}
