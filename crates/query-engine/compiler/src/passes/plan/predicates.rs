use super::Hop;
use super::helpers::{FilterOwner, bind_filter};
use super::physical::BindingSource;
use super::requirements::{Column, Predicate};
use crate::input::{BooleanExpression, Condition, Input, PredicateTarget};
use crate::{QueryError, Result};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::QueryDataModel;

pub(super) fn bind(
    expression: &BooleanExpression<Condition>,
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    hops: &[Hop],
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let resolve = |alias: &str, property: &str| -> Result<Column> {
        let Some(bindings) = bindings else {
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
        .try_map(&mut |condition| -> Result<Box<Predicate>> {
            let Condition::Property(predicate) = condition;
            let (column, owner) = match &predicate.target {
                PredicateTarget::Node(alias) => {
                    let entity = input
                        .nodes
                        .iter()
                        .find(|node| &node.id == alias)
                        .and_then(|node| node.entity.as_deref())
                        .and_then(|entity| model.graph().entity_id(entity))
                        .ok_or_else(|| {
                            QueryError::Lowering(format!("condition node '{alias}' has no entity"))
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
                            QueryError::Lowering("condition relationship has no edge scan".into())
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
