use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::context::PlanningContext;
use super::edge_chain::denorm_hop;
use super::helpers::{FilterOwner, bind_filter};
use super::physical::BindingSource;
use super::requirements::{Column, Predicate, tag_filter};
use super::{BoundFilter, DenormalizedKey, DenormalizedProperty, Hop, NodePlan};
use crate::error::{QueryError, Result};
use crate::input::{BooleanExpression, Condition, Input, PredicateTarget};
use query_data_model::OrbitQueryModel;

pub(super) struct Placement {
    pub after_joins: Vec<bool>,
    pub joined_nodes: HashSet<String>,
}

pub(super) fn place_conditions(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    nodes: &HashMap<String, NodePlan>,
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Result<Placement> {
    let mut placement = Placement {
        after_joins: Vec::new(),
        joined_nodes: HashSet::new(),
    };
    for root in &input.predicates {
        let mut untagged = HashSet::new();
        for condition in root.leaves() {
            let (column, bound) = bind_leaf(condition.clone(), input, model, hops)?;
            let tagged = matches!(condition.target, PredicateTarget::Node(_))
                && edge_tag(&column.source, &bound, hops, denormalized).is_some();
            if !tagged {
                untagged.extend(
                    condition
                        .node_properties()
                        .filter(|(_, property)| *property != DEFAULT_PRIMARY_KEY)
                        .map(|(alias, _)| alias.to_string()),
                );
            } else if let PredicateTarget::Node(alias) = &condition.target
                && nodes[alias].has_untagged_filter(hops, denormalized)
            {
                placement.joined_nodes.insert(alias.clone());
            }
        }
        let scan = root
            .local_node()
            .filter(|_| hops.is_empty() || !untagged.is_empty());
        placement.after_joins.push(scan.is_none());
        placement
            .joined_nodes
            .extend(scan.map(str::to_string).into_iter().chain(untagged));
    }
    Ok(placement)
}

pub(super) fn bind_scan_conditions<M: OrbitQueryModel + ?Sized>(
    context: &PlanningContext<'_, M>,
    nodes: &mut HashMap<String, NodePlan>,
    placement: &Placement,
) -> Result<()> {
    for (root, after_joins) in context.input.predicates.iter().zip(&placement.after_joins) {
        if let Some(alias) = root.local_node()
            && !after_joins
        {
            let predicate = bind_tree(root, context, None)?;
            nodes
                .get_mut(alias)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant(format!("node '{alias}' has no plan"))
                })?
                .predicates
                .push(predicate);
        }
    }
    Ok(())
}

pub(super) fn bind_join_conditions<M: OrbitQueryModel + ?Sized>(
    context: &PlanningContext<'_, M>,
    bindings: &[BindingSource],
    placement: &Placement,
) -> Result<Vec<Predicate>> {
    context
        .input
        .predicates
        .iter()
        .zip(&placement.after_joins)
        .filter(|(_, after_joins)| **after_joins)
        .map(|(root, _)| bind_tree(root, context, Some(bindings)))
        .collect()
}

fn edge_tag(
    alias: &str,
    bound: &BoundFilter,
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<Predicate> {
    let (position, facts) = denorm_hop(bound, alias, hops, denormalized)?;
    let hop = &hops[position];
    if bound.filter.rhs_column.is_some() || facts.nullable || hop.max_hops != 1 || hop.fk.is_some()
    {
        return None;
    }
    tag_filter(
        &format!("e{position}"),
        &facts.edge_column,
        &facts.tag_key,
        &bound.filter,
    )
}

fn bind_leaf(
    condition: Condition,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
) -> Result<(Column, BoundFilter)> {
    let (column, owner) = match &condition.target {
        PredicateTarget::Node(alias) => {
            let entity = input
                .entity_of(alias)
                .and_then(|entity| model.graph().entity_id(entity))
                .ok_or_else(|| {
                    QueryError::PipelineInvariant(format!("condition node '{alias}' has no entity"))
                })?;
            (
                Column::new(alias, &condition.property),
                FilterOwner::Entity(entity),
            )
        }
        PredicateTarget::Relationship(index) => {
            let (position, hop) = hops
                .iter()
                .enumerate()
                .find(|(_, hop)| hop.input_index == *index)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant("condition relationship has no edge scan".into())
                })?;
            (
                Column::new(format!("e{position}"), &condition.property),
                FilterOwner::Table(&hop.edge_table),
            )
        }
    };
    Ok((
        column,
        bind_filter(&condition.property, condition.filter, &owner, model),
    ))
}

fn bind_tree<M: OrbitQueryModel + ?Sized>(
    root: &BooleanExpression<Condition>,
    context: &PlanningContext<'_, M>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let expression = root
        .clone()
        .try_map(&mut |condition| bind_condition(condition, context, bindings).map(Box::new))?;
    Ok(match expression {
        BooleanExpression::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}

fn bind_condition<M: OrbitQueryModel + ?Sized>(
    condition: Condition,
    context: &PlanningContext<'_, M>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let node = matches!(condition.target, PredicateTarget::Node(_));
    let (column, mut bound) = bind_leaf(condition, context.input, context.model, &context.hops)?;
    if node
        && bindings.is_some()
        && let Some(tag) = edge_tag(&column.source, &bound, &context.hops, &context.denormalized)
    {
        return Ok(match resolve_column(bindings, column) {
            Ok(column) => Predicate::Boolean(BooleanExpression::And(vec![
                BooleanExpression::Leaf(Box::new(bound.at(column))),
                BooleanExpression::Leaf(Box::new(tag)),
            ])),
            Err(_) => tag,
        });
    }
    let column = if node {
        resolve_column(bindings, column)?
    } else {
        column
    };
    if let Some((alias, property)) = &bound.filter.rhs_column {
        let rhs = resolve_column(bindings, Column::new(alias, property))?;
        bound.filter.rhs_column = Some((rhs.source, rhs.name));
    }
    Ok(bound.at(column))
}

fn resolve_column(bindings: Option<&[BindingSource]>, column: Column) -> Result<Column> {
    let Some(bindings) = bindings else {
        return Ok(column);
    };
    bindings
        .iter()
        .find(|binding| binding.node == column.source)
        .and_then(|binding| binding.column(&column.name))
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "node '{}' has no visible '{}' column",
                column.source, column.name
            ))
        })
}
