use std::collections::HashMap;

use crate::input::{FilterOp, InputFilter, InputIdRange};
use crate::{QueryError, Result};
use serde_json::Value;

use super::super::ast::{Comparison, MapEntry};
use super::super::invalid;
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

    pub(super) fn predicate(&mut self, comparison: Comparison<'_>) -> Result<()> {
        let Comparison {
            span,
            property,
            op,
            value,
        } = comparison;
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
            let Some(filters) = node.filters.get("id") else {
                continue;
            };
            match filters.as_slice() {
                [
                    InputFilter {
                        op: Some(FilterOp::In),
                        value: Some(Value::Array(values)),
                        ..
                    },
                ] if node.node_ids.is_empty() && !values.is_empty() => {
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
                    node.filters.remove("id");
                }
                [left, right] => {
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
                            node.filters.remove("id");
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
