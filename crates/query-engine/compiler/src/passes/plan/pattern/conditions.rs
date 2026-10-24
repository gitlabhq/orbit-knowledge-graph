//! Places WHERE conditions: on one node's scan when they only touch that node, else on the joined rows.

use std::collections::HashSet;

use query_data_model::OrbitQueryModel;

use super::Pattern;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::filters::{BoundFilter, FilterOwner, bind_filter};
use crate::passes::plan::graph::BindingSource;
use crate::passes::plan::predicates::{Column, Predicate, tag_filter};

#[derive(Default)]
pub(super) struct ConditionPlacement {
    /// Per condition tree, the node whose scan takes it, or `None` when it binds against the joined rows.
    pub scans: Vec<Option<String>>,
    pub joined_nodes: HashSet<String>,
}

pub(super) fn place_conditions<M>(pattern: &Pattern<'_, M>) -> Result<ConditionPlacement>
where
    M: OrbitQueryModel + ?Sized,
{
    let mut placement = ConditionPlacement::default();
    for root in &pattern.input.predicates {
        let mut untagged = HashSet::new();
        for condition in root.leaves() {
            let (column, bound) = bind_leaf(pattern, condition.clone())?;
            let tagged = matches!(condition.target, PredicateTarget::Node(_))
                && edge_tag(pattern, &column.source, &bound).is_some();
            if !tagged {
                untagged.extend(
                    condition
                        .node_properties()
                        .filter(|(_, property)| {
                            *property != ontology::constants::DEFAULT_PRIMARY_KEY
                        })
                        .map(|(alias, _)| alias.to_string()),
                );
            } else if let PredicateTarget::Node(alias) = &condition.target
                && pattern.has_uncovered_filter(&pattern.nodes[alias])
            {
                placement.joined_nodes.insert(alias.clone());
            }
        }
        let scan = root
            .local_node()
            .filter(|_| !untagged.is_empty())
            .map(str::to_string);
        placement
            .joined_nodes
            .extend(scan.iter().cloned().chain(untagged));
        placement.scans.push(scan);
    }
    Ok(placement)
}

pub(super) fn bind_scan_conditions<M>(
    pattern: &mut Pattern<'_, M>,
    placement: &ConditionPlacement,
) -> Result<()>
where
    M: OrbitQueryModel + ?Sized,
{
    let bound = pattern
        .input
        .predicates
        .iter()
        .zip(&placement.scans)
        .filter_map(|(root, scan)| Some((scan.as_ref()?, root)))
        .map(|(alias, root)| Ok((alias, bind_tree(pattern, root, None)?)))
        .collect::<Result<Vec<_>>>()?;
    for (alias, predicate) in bound {
        pattern
            .nodes
            .get_mut(alias)
            .ok_or_else(|| QueryError::PipelineInvariant(format!("node '{alias}' has no plan")))?
            .predicates
            .push(predicate);
    }
    Ok(())
}

pub(super) fn bind_tree<M>(
    pattern: &Pattern<'_, M>,
    root: &BooleanExpression<Condition>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let expression = root
        .clone()
        .try_map(&mut |condition| bind_condition(pattern, condition, bindings).map(Box::new))?;
    Ok(match expression {
        BooleanExpression::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}

fn bind_condition<M>(
    pattern: &Pattern<'_, M>,
    condition: Condition,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let node = matches!(condition.target, PredicateTarget::Node(_));
    let (column, mut bound) = bind_leaf(pattern, condition)?;
    if node
        && bindings.is_some()
        && let Some(tag) = edge_tag(pattern, &column.source, &bound)
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
        bound.null_as_epoch.rhs = pattern
            .nodes
            .get(alias)
            .is_some_and(|node| node.null_as_epoch.contains(property));
        let rhs = resolve_column(bindings, Column::new(alias, property))?;
        bound.filter.rhs_column = Some((rhs.source, rhs.name));
    }
    Ok(bound.at(column))
}

fn bind_leaf<M>(pattern: &Pattern<'_, M>, condition: Condition) -> Result<(Column, BoundFilter)>
where
    M: OrbitQueryModel + ?Sized,
{
    let (column, owner) = match &condition.target {
        PredicateTarget::Node(alias) => {
            let entity = pattern
                .input
                .entity_of(alias)
                .and_then(|entity| pattern.model.graph().entity_id(entity))
                .ok_or_else(|| {
                    QueryError::PipelineInvariant(format!("condition node '{alias}' has no entity"))
                })?;
            (
                Column::new(alias, &condition.property),
                FilterOwner::Entity(entity),
            )
        }
        PredicateTarget::Relationship(index) => {
            let relation = pattern
                .relations
                .iter()
                .find(|relation| relation.input_index == *index)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant("condition relationship has no edge scan".into())
                })?;
            (
                Column::new(&relation.alias, &condition.property),
                FilterOwner::Tables(&relation.edge_tables),
            )
        }
    };
    Ok((
        column,
        bind_filter(&condition.property, condition.filter, &owner, pattern.model),
    ))
}

fn edge_tag<M>(pattern: &Pattern<'_, M>, node: &str, bound: &BoundFilter) -> Option<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let (position, facts) = pattern.denorm_hop(node, bound)?;
    let relation = &pattern.relations[position];
    if bound.filter.rhs_column.is_some() || facts.nullable || relation.max_hops != 1 {
        return None;
    }
    tag_filter(
        &relation.alias,
        &facts.edge_column,
        &facts.tag_key,
        &bound.filter,
    )
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
