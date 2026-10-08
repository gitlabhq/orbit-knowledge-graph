use std::collections::HashMap;

use crate::input::{
    BooleanExpression, FilterOp, InputFilter, InputIdRange, PredicateTarget, PropertyPredicate,
};
use crate::{QueryError, Result};
use serde_json::Value;

use super::super::ast::{Comparison, MapEntry};
use super::super::invalid;
use super::Lowering;

impl Lowering {
    pub(super) fn predicates(
        &mut self,
        expression: BooleanExpression<Comparison<'_>>,
    ) -> Result<()> {
        match expression {
            BooleanExpression::And(children) => {
                for child in children {
                    self.predicates(child)?;
                }
            }
            BooleanExpression::Leaf(comparison) => self.predicate(comparison)?,
            expression => {
                let expression =
                    expression.try_map(&mut |comparison| self.property_predicate(comparison))?;
                self.input.predicates.push(expression);
            }
        }
        Ok(())
    }

    fn property_predicate(&self, comparison: Comparison<'_>) -> Result<PropertyPredicate> {
        let target = self.predicate_target(&comparison.property.node.value, comparison.span)?;
        let rhs_column = if let Some(rhs) = comparison.rhs_property {
            let rhs_target = self.predicate_target(&rhs.node.value, rhs.span)?;
            if !matches!(
                (&target, rhs_target),
                (PredicateTarget::Node(_), PredicateTarget::Node(_))
            ) {
                return Err(invalid(
                    comparison.span,
                    "property comparisons involving relationships are unsupported",
                ));
            }
            Some((rhs.node.value, rhs.property.value))
        } else {
            None
        };
        Ok(PropertyPredicate {
            target,
            property: comparison.property.property.value,
            filter: InputFilter {
                op: Some(comparison.op),
                value: comparison.value,
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

    pub(super) fn predicate(&mut self, comparison: Comparison<'_>) -> Result<()> {
        let PropertyPredicate {
            target,
            property,
            filter,
        } = self.property_predicate(comparison)?;
        match target {
            PredicateTarget::Node(alias) => {
                if let Some((rhs_node, rhs_prop)) = &filter.rhs_column
                    && &alias != rhs_node
                {
                    self.input
                        .join_predicates
                        .push(crate::input::JoinPredicate {
                            lhs_node: alias,
                            lhs_prop: property,
                            op: filter.op.unwrap_or(FilterOp::Eq),
                            rhs_node: rhs_node.clone(),
                            rhs_prop: rhs_prop.clone(),
                        });
                } else {
                    self.input
                        .nodes
                        .iter_mut()
                        .find(|node| node.id == alias)
                        .expect("resolved node")
                        .filters
                        .entry(property)
                        .or_default()
                        .push(filter);
                }
            }
            PredicateTarget::Relationship(index) => {
                self.input.relationships[index]
                    .filters
                    .entry(property)
                    .or_default()
                    .push(filter);
            }
        }
        Ok(())
    }

    pub(super) fn promote_ids(&mut self) -> Result<()> {
        for node in &mut self.input.nodes {
            let Some(filters) = node.filters.get_mut("id") else {
                continue;
            };
            if node.node_ids.is_empty()
                && let Some((index, values)) =
                    filters
                        .iter()
                        .enumerate()
                        .find_map(|(index, f)| match (&f.op, &f.value) {
                            (Some(FilterOp::In), Some(Value::Array(values)))
                                if !values.is_empty() =>
                            {
                                Some((index, values))
                            }
                            _ => None,
                        })
            {
                node.node_ids = values
                    .iter()
                    .map(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            .ok_or_else(|| {
                                QueryError::Validation(
                                    "node IDs must be signed 64-bit integers".into(),
                                )
                            })
                    })
                    .collect::<Result<_>>()?;
                filters.remove(index);
            }
            if let [left, right] = filters.as_slice() {
                let lower = [left, right]
                    .into_iter()
                    .find(|f| matches!(f.op, Some(FilterOp::Gte | FilterOp::Gt)));
                let upper = [left, right]
                    .into_iter()
                    .find(|f| matches!(f.op, Some(FilterOp::Lte | FilterOp::Lt)));
                if let (Some(lower), Some(upper)) = (lower, upper)
                    && let (Some(start), Some(end)) = (
                        lower.value.as_ref().and_then(Value::as_i64),
                        upper.value.as_ref().and_then(Value::as_i64),
                    )
                {
                    let start = start.checked_add(i64::from(lower.op == Some(FilterOp::Gt)));
                    let end = end.checked_sub(i64::from(upper.op == Some(FilterOp::Lt)));
                    if let (Some(start), Some(end)) = (start, end) {
                        node.id_range = Some(InputIdRange { start, end });
                        filters.clear();
                    }
                }
            }
            if filters.is_empty() {
                node.filters.remove("id");
            }
        }
        Ok(())
    }
}
