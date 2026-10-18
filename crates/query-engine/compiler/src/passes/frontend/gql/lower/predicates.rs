use std::collections::HashMap;

use crate::Result;
use crate::input::{BooleanExpression, Condition, FilterOp, InputFilter, PredicateTarget};

use super::super::ast::{Comparison, MapEntry, Predicate};
use super::super::errors::invalid;
use super::Lowering;

impl Lowering {
    pub(super) fn map_filters(
        entries: Vec<MapEntry<'_>>,
        filters: &mut HashMap<String, Vec<InputFilter>>,
    ) {
        for entry in entries {
            filters
                .entry(entry.key.value)
                .or_default()
                .push(InputFilter {
                    op: Some(FilterOp::Eq),
                    value: Some(entry.value),
                    ..Default::default()
                });
        }
    }

    pub(super) fn predicates(
        &mut self,
        predicates: Vec<BooleanExpression<Predicate<'_>>>,
    ) -> Result<()> {
        let expression = BooleanExpression::And(predicates)
            .try_map(&mut |predicate| self.condition(predicate))?;
        self.input.absorb(expression)
    }

    fn condition(&self, predicate: Predicate<'_>) -> Result<Condition> {
        let comparison = match predicate {
            Predicate::Comparison(comparison) => comparison,
            Predicate::RelationshipType {
                span,
                variable,
                op,
                value,
            } => {
                let index = self.relationship_index(span, &variable)?;
                self.one_hop(span, &variable)?;
                return Ok(Condition {
                    target: PredicateTarget::Relationship(index),
                    property: Condition::RELATIONSHIP_TYPE.into(),
                    filter: InputFilter {
                        op: Some(op),
                        value,
                        rhs_column: None,
                    },
                });
            }
        };
        let Comparison {
            span,
            property,
            op,
            value,
            rhs_property,
        } = *comparison;
        let target = self.predicate_target(&property.node.value, span)?;
        let rhs_column = match rhs_property {
            Some(rhs) => {
                let rhs_target = self.predicate_target(&rhs.node.value, span)?;
                if !matches!(
                    (&target, &rhs_target),
                    (PredicateTarget::Node(_), PredicateTarget::Node(_))
                ) {
                    return Err(invalid(
                        span,
                        "property comparisons involving relationships are unsupported",
                    ));
                }
                if !matches!(
                    op,
                    FilterOp::Eq
                        | FilterOp::Ne
                        | FilterOp::Gt
                        | FilterOp::Lt
                        | FilterOp::Gte
                        | FilterOp::Lte
                ) {
                    return Err(invalid(
                        span,
                        "property-to-property comparisons only support =, <>, !=, <, >, <=, >=",
                    ));
                }
                Some((rhs.node.value, rhs.property.value))
            }
            None => None,
        };
        Ok(Condition {
            target,
            property: property.property.value,
            filter: InputFilter {
                op: Some(op),
                value,
                rhs_column,
            },
        })
    }

    fn predicate_target(&self, alias: &str, span: pest::Span<'_>) -> Result<PredicateTarget> {
        if self.input.nodes.iter().any(|node| node.id == alias) {
            Ok(PredicateTarget::Node(alias.into()))
        } else if let Some(index) = self.edges.get(alias) {
            if self.input.relationships[*index].hops.max != 1 {
                return Err(invalid(
                    span,
                    "a variable-length relationship binds a list; relationship-list predicates are unsupported",
                ));
            }
            Ok(PredicateTarget::Relationship(*index))
        } else {
            Err(invalid(span, &format!("undefined variable {alias}")))
        }
    }
}
