use std::collections::HashMap;

use crate::input::{
    BooleanExpression, Condition, FilterOp, InputFilter, InputIdRange, PredicateTarget,
    PropertyPredicate,
};
use crate::{QueryError, Result};
use ontology::constants::TRAVERSAL_PATH_COLUMN;
use serde_json::Value;

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
        if predicates.is_empty() {
            return Ok(());
        }
        let expression = BooleanExpression::And(predicates);
        if let Some(Predicate::Comparison(comparison)) =
            expression
                .negated_leaves()
                .find(|predicate| match predicate {
                    Predicate::Comparison(comparison) => std::iter::once(&comparison.property)
                        .chain(&comparison.rhs_property)
                        .any(|property| property.property.value == TRAVERSAL_PATH_COLUMN),
                })
        {
            return Err(invalid(
                comparison.span,
                "traversal_path cannot be used under NOT; Orbit scopes every query to your authorized paths",
            ));
        }
        let expression = expression
            .try_map(&mut |predicate| self.condition(predicate))?
            .normalize();
        for conjunct in expression.conjuncts() {
            match conjunct {
                BooleanExpression::Leaf(Condition::Property(predicate)) => {
                    self.push_filter(predicate)
                }
                root => self.input.predicates.push(root),
            }
        }
        Ok(())
    }

    fn condition(&self, predicate: Predicate<'_>) -> Result<Condition> {
        let Predicate::Comparison(comparison) = predicate;
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
        Ok(Condition::Property(PropertyPredicate {
            target,
            property: property.property.value,
            filter: InputFilter {
                op: Some(op),
                value,
                rhs_column,
            },
        }))
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

    fn push_filter(&mut self, predicate: PropertyPredicate) {
        if predicate.compares_aliases() {
            self.input
                .predicates
                .push(BooleanExpression::Leaf(Condition::Property(predicate)));
            return;
        }
        let filters = match &predicate.target {
            PredicateTarget::Node(alias) => {
                &mut self
                    .input
                    .nodes
                    .iter_mut()
                    .find(|node| node.id == *alias)
                    .expect("predicate targets resolve to declared nodes")
                    .filters
            }
            PredicateTarget::Relationship(index) => &mut self.input.relationships[*index].filters,
        };
        filters
            .entry(predicate.property)
            .or_default()
            .push(predicate.filter);
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
