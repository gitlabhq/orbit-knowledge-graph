use std::collections::HashMap;

use super::Hop;
use super::edge_chain::denorm_hop;
use super::helpers::{FilterOwner, bind_filter};
use super::physical::BindingSource;
use super::requirements::{Column, Predicate, tag_filter};
use crate::input::{BooleanExpression, Condition, Input, PredicateTarget};
use crate::{QueryError, Result};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::{DenormalizedKey, DenormalizedProperty, OrbitQueryModel};

pub(super) fn edge_tag(
    condition: &Condition,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<(usize, Predicate)> {
    let PredicateTarget::Node(alias) = &condition.target else {
        return None;
    };
    if condition.filter.rhs_column.is_some() {
        return None;
    }
    let entity = model.graph().entity_id(input.entity_of(alias)?)?;
    let bound = bind_filter(
        &condition.property,
        condition.filter.clone(),
        &FilterOwner::Entity(entity),
        model,
    );
    let (position, facts) = denorm_hop(&bound, alias, hops, denormalized)?;
    if facts.nullable || hops[position].max_hops != 1 || hops[position].fk.is_some() {
        return None;
    }
    tag_filter(
        &format!("e{position}"),
        &facts.edge_column,
        &facts.tag_key,
        &bound.filter,
    )
    .map(|predicate| (position, predicate))
}

pub(super) fn edge_covered(
    condition: &Condition,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> bool {
    match &condition.target {
        PredicateTarget::Relationship(_) => true,
        PredicateTarget::Node(_) => {
            (condition.property == DEFAULT_PRIMARY_KEY && condition.filter.rhs_column.is_none())
                || edge_tag(condition, input, model, hops, denormalized).is_some()
        }
    }
}

pub(super) fn bind(
    expression: &BooleanExpression<Condition>,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    bindings: Option<(
        &[BindingSource],
        &HashMap<DenormalizedKey, DenormalizedProperty>,
    )>,
) -> Result<Predicate> {
    let resolve = |alias: &str, property: &str| -> Result<Column> {
        let Some((bindings, _)) = bindings else {
            return Ok(Column::new(alias, property));
        };
        let binding = bindings
            .iter()
            .find(|binding| binding.node == alias)
            .ok_or_else(|| {
                QueryError::Lowering(format!("node '{alias}' has no lowered binding"))
            })?;
        if property == DEFAULT_PRIMARY_KEY {
            Ok(Column::new(&binding.alias, &binding.column))
        } else if binding.joined {
            Ok(Column::new(alias, property))
        } else {
            Err(QueryError::Lowering(format!(
                "property '{property}' has no visible node table"
            )))
        }
    };
    let expression = expression
        .clone()
        .try_map(&mut |predicate| -> Result<Box<Predicate>> {
            if let Some((_, denormalized)) = bindings
                && let Some((_, tag)) = edge_tag(&predicate, input, model, hops, denormalized)
            {
                return Ok(Box::new(tag));
            }
            let (column, owner) = match &predicate.target {
                PredicateTarget::Node(alias) => {
                    let entity = input
                        .entity_of(alias)
                        .and_then(|entity| model.graph().entity_id(entity))
                        .ok_or_else(|| {
                            QueryError::PipelineInvariant(format!(
                                "condition node '{alias}' has no entity"
                            ))
                        })?;
                    (
                        resolve(alias, &predicate.property)?,
                        FilterOwner::Entity(entity),
                    )
                }
                PredicateTarget::Relationship(index) => {
                    let (position, hop) = hops
                        .iter()
                        .enumerate()
                        .find(|(_, hop)| hop.input_index == *index)
                        .ok_or_else(|| {
                            QueryError::PipelineInvariant(
                                "condition relationship has no edge scan".into(),
                            )
                        })?;
                    (
                        Column::new(format!("e{position}"), &predicate.property),
                        FilterOwner::Table(&hop.edge_table),
                    )
                }
            };
            let mut bound = bind_filter(&predicate.property, predicate.filter, &owner, model);
            if let Some((alias, property)) = &bound.filter.rhs_column {
                let rhs = resolve(alias, property)?;
                bound.filter.rhs_column = Some((rhs.source, rhs.name));
            }
            Ok(Box::new(Predicate::Property {
                column,
                filter: bound.filter,
                data_type: bound.data_type,
                in_sort_key: bound.in_sort_key,
            }))
        })?;
    Ok(match expression {
        BooleanExpression::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}
