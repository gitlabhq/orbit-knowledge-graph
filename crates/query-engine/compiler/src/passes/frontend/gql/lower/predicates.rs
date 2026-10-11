use std::collections::HashMap;

use crate::input::{FilterOp, InputFilter, InputIdRange};
use crate::{QueryError, Result};
use serde_json::Value;

use super::super::ast::{Comparison, MapEntry, Name, Predicate};
use super::super::errors::invalid;
use super::Lowering;
use super::is_type_shaped;
use crate::passes::validate::validate_identifier;

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

    pub(super) fn predicate(&mut self, predicate: Predicate<'_>) -> Result<()> {
        match predicate {
            Predicate::Comparison(comparison) => self.comparison(*comparison),
            Predicate::RelationshipType {
                span,
                variable,
                op,
                value,
            } => self.relationship_type(span, &variable, op, value),
        }
    }

    fn relationship_type(
        &mut self,
        span: pest::Span<'_>,
        variable: &Name<'_>,
        op: FilterOp,
        value: Option<Value>,
    ) -> Result<()> {
        let types = relationship_type_names(span, &variable.value, op, value)?;
        let index = self.relationship_index(span, variable)?;
        self.one_hop(span, variable)?;
        let edge = &mut self.input.relationships[index];
        if edge.types == ["*"] {
            edge.types = types;
        } else {
            edge.types.retain(|kind| types.contains(kind));
        }
        if edge.types.is_empty() {
            return Err(invalid(
                span,
                &format!(
                    "type({}) excludes every relationship type the pattern allows for it",
                    variable.value
                ),
            ));
        }
        Ok(())
    }

    fn comparison(&mut self, comparison: Comparison<'_>) -> Result<()> {
        let Comparison {
            span,
            property,
            op,
            value,
            rhs_property,
        } = comparison;

        if let Some(rhs) = rhs_property {
            let lhs_node = property.node.value;
            let lhs_prop = property.property.value;
            let rhs_node = rhs.node.value;
            let rhs_prop = rhs.property.value;
            let lhs_known = self.input.nodes.iter().any(|n| n.id == lhs_node)
                || self.edges.contains_key(&lhs_node);
            let rhs_known = self.input.nodes.iter().any(|n| n.id == rhs_node)
                || self.edges.contains_key(&rhs_node);
            if !lhs_known {
                return Err(invalid(span, &format!("undefined variable {lhs_node}")));
            }
            if !rhs_known {
                return Err(invalid(span, &format!("undefined variable {rhs_node}")));
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
            if lhs_node == rhs_node {
                self.input
                    .nodes
                    .iter_mut()
                    .find(|n| n.id == lhs_node)
                    .ok_or_else(|| invalid(span, &format!("undefined variable {lhs_node}")))?
                    .filters
                    .entry(lhs_prop)
                    .or_default()
                    .push(InputFilter {
                        op: Some(op),
                        rhs_column: Some((rhs_node, rhs_prop)),
                        ..Default::default()
                    });
            } else {
                self.input
                    .join_predicates
                    .push(crate::input::JoinPredicate {
                        lhs_node,
                        lhs_prop,
                        op,
                        rhs_node,
                        rhs_prop,
                    });
            }
            return Ok(());
        }

        let filter = InputFilter {
            op: Some(op),
            value,
            ..Default::default()
        };
        let node = property.node.value;
        let key = property.property.value;
        if let Some(node) = self.input.nodes.iter_mut().find(|n| n.id == node) {
            node.filters.entry(key).or_default().push(filter);
        } else if let Some(index) = self.edges.get(&node) {
            let edge = &mut self.input.relationships[*index];
            if edge.hops.max != 1 {
                return Err(invalid(
                    span,
                    "a variable-length relationship binds a list; relationship-list predicates are unsupported",
                ));
            }
            edge.filters.entry(key).or_default().push(filter);
        } else {
            return Err(invalid(span, &format!("undefined variable {node}")));
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

fn relationship_type_names(
    span: pest::Span<'_>,
    name: &str,
    op: FilterOp,
    value: Option<Value>,
) -> Result<Vec<String>> {
    let unsupported = || {
        invalid(
            span,
            &format!(
                "type({name}) supports = or IN with relationship type names, such as type({name}) = 'CLOSES' or type({name}) IN ['CLOSES', 'MENTIONS']"
            ),
        )
    };
    let values = match (op, value) {
        (FilterOp::Eq, Some(value @ Value::String(_))) => vec![value],
        (FilterOp::In, Some(Value::Array(values))) if !values.is_empty() => values,
        _ => return Err(unsupported()),
    };
    values
        .into_iter()
        .map(|value| match value {
            Value::String(kind) if is_type_shaped(&kind) && validate_identifier(&kind).is_ok() => {
                Ok(kind)
            }
            Value::String(kind) if validate_identifier(&kind).is_ok() => Err(invalid(
                span,
                &format!(
                    "relationship type names are uppercase and case-sensitive, such as type({name}) = 'CLOSES'"
                ),
            )),
            _ => Err(unsupported()),
        })
        .collect()
}
