use super::Hop;
use super::helpers::{FilterOwner, bind_filter};
use super::requirements::{Predicate, property_filter};
use crate::input::{BooleanExpression, Input, PredicateTarget, PropertyPredicate};
use crate::{QueryError, Result};
use query_data_model::QueryDataModel;

pub(super) fn local_node(expression: &BooleanExpression<PropertyPredicate>) -> Option<&str> {
    let PredicateTarget::Node(alias) = &expression.leaves().next()?.target else {
        return None;
    };
    expression
        .leaves()
        .all(|leaf| {
            matches!(&leaf.target, PredicateTarget::Node(node) if node == alias)
                && leaf
                    .filter
                    .rhs_column
                    .as_ref()
                    .is_none_or(|(node, _)| node == alias)
        })
        .then_some(alias)
}

pub(super) fn references(input: &Input, target: PredicateTarget) -> bool {
    input.predicate_leaves().any(|leaf| {
        leaf.target == target
            || match &target {
                PredicateTarget::Node(alias) => leaf
                    .filter
                    .rhs_column
                    .as_ref()
                    .is_some_and(|(node, _)| node == alias),
                PredicateTarget::Relationship(_) => false,
            }
    })
}

pub(super) fn bind(
    expression: &BooleanExpression<PropertyPredicate>,
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    hops: &[Hop],
) -> Result<Predicate> {
    expression
        .clone()
        .try_map(&mut |leaf| {
            let (alias, owner) = match &leaf.target {
                PredicateTarget::Node(alias) => {
                    let entity = input
                        .nodes
                        .iter()
                        .find(|node| &node.id == alias)
                        .and_then(|node| node.entity.as_deref())
                        .and_then(|entity| model.graph().entity_id(entity))
                        .ok_or_else(|| {
                            QueryError::Lowering(format!("predicate node '{alias}' has no entity"))
                        })?;
                    (alias.clone(), FilterOwner::Entity(entity))
                }
                PredicateTarget::Relationship(index) => {
                    let (position, hop) = hops
                        .iter()
                        .enumerate()
                        .find(|(_, hop)| hop.input_index == *index)
                        .ok_or_else(|| {
                            QueryError::Lowering("predicate relationship was removed".into())
                        })?;
                    (format!("e{position}"), FilterOwner::Table(&hop.edge_table))
                }
            };
            let bound = bind_filter(&leaf.property, leaf.filter, &owner, model);
            Ok(Box::new(property_filter(&alias, &leaf.property, &bound)))
        })
        .map(Predicate::Boolean)
}
