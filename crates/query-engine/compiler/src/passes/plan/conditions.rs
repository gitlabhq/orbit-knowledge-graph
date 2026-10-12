use std::collections::HashMap;

use super::context::PlanningContext;
use super::edge_chain::denorm_hop;
use super::helpers::{FilterOwner, bind_filter};
use super::physical::BindingSource;
use super::requirements::{Column, Predicate, tag_filter};
use super::{BoundFilter, DenormalizedKey, DenormalizedProperty, Hop};
use crate::error::{QueryError, Result};
use crate::input::{BooleanExpression, Condition, Input, PredicateTarget};
use query_data_model::OrbitQueryModel;

pub(super) fn edge_tag(
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

pub(super) fn bind_leaf(
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

pub(super) fn bind_tree<M: OrbitQueryModel + ?Sized>(
    root: &BooleanExpression<Condition>,
    context: &PlanningContext<'_, M>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let resolve = |column: Column| -> Result<Column> {
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
    };
    let expression = root
        .clone()
        .try_map(&mut |condition| -> Result<Box<Predicate>> {
            let node = matches!(condition.target, PredicateTarget::Node(_));
            let (column, mut bound) =
                bind_leaf(condition, context.input, context.model, &context.hops)?;
            if node
                && bindings.is_some()
                && let Some(tag) =
                    edge_tag(&column.source, &bound, &context.hops, &context.denormalized)
            {
                return Ok(Box::new(match resolve(column) {
                    Ok(column) => Predicate::Boolean(BooleanExpression::And(vec![
                        BooleanExpression::Leaf(Box::new(bound.at(column))),
                        BooleanExpression::Leaf(Box::new(tag)),
                    ])),
                    Err(_) => tag,
                }));
            }
            let column = if node { resolve(column)? } else { column };
            if let Some((alias, property)) = &bound.filter.rhs_column {
                let rhs = resolve(Column::new(alias, property))?;
                bound.filter.rhs_column = Some((rhs.source, rhs.name));
            }
            Ok(Box::new(bound.at(column)))
        })?;
    Ok(match expression {
        BooleanExpression::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}
