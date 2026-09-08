use std::collections::{HashMap, HashSet};

use compiler::input::{FilterOp, InputFilter, InputIdRange};
use compiler::{QueryError, Result};
use pest::iterators::Pair;
use serde_json::Value;

use super::Lowering;
use crate::value::{Bindings, value};
use crate::{Rule, invalid, name, property};

impl Lowering<'_> {
    pub(super) fn map_filters(
        pair: Pair<'_, Rule>,
        bindings: &Bindings<'_>,
        filters: &mut HashMap<String, Vec<InputFilter>>,
    ) -> Result<()> {
        let mut keys = HashSet::new();
        for entry in pair.into_inner() {
            let mut parts = entry.clone().into_inner();
            let key = name(parts.next().expect("map entry has a key"))?;
            if !keys.insert(key.clone()) {
                return Err(invalid(&entry, "duplicate property in a map"));
            }
            let val = value(parts.next().expect("map entry has a value"), bindings)?;
            filters.entry(key).or_default().push(InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(val),
                ..Default::default()
            });
        }
        Ok(())
    }

    pub(super) fn predicates(&mut self, pair: Pair<'_, Rule>) -> Result<()> {
        match pair.as_rule() {
            Rule::Where | Rule::AndExpression | Rule::ParenthesizedExpression => {
                for child in pair.into_inner() {
                    self.predicates(child)?;
                }
            }
            Rule::ComparisonExpression | Rule::TokenPredicate => {
                let mut parts = pair.clone().into_inner();
                let (prop, operator) = if pair.as_rule() == Rule::TokenPredicate {
                    let operator = parts.next().expect("token predicate has a function");
                    (
                        property(parts.next().expect("token predicate has a property"))?,
                        operator,
                    )
                } else {
                    (
                        property(parts.next().expect("comparison has a property"))?,
                        parts.next().expect("comparison has an operator"),
                    )
                };
                let op = match operator.as_rule() {
                    Rule::NullOperator => {
                        if operator.into_inner().next().is_some() {
                            FilterOp::IsNotNull
                        } else {
                            FilterOp::IsNull
                        }
                    }
                    Rule::StringOperator => {
                        let token = operator.as_str().to_ascii_lowercase();
                        if token.starts_with("starts") {
                            FilterOp::StartsWith
                        } else if token.starts_with("ends") {
                            FilterOp::EndsWith
                        } else {
                            FilterOp::Contains
                        }
                    }
                    _ => match operator.as_str().to_ascii_lowercase().as_str() {
                        "=" => FilterOp::Eq,
                        ">" => FilterOp::Gt,
                        "<" => FilterOp::Lt,
                        ">=" => FilterOp::Gte,
                        "<=" => FilterOp::Lte,
                        "in" => FilterOp::In,
                        "token_match" => FilterOp::TokenMatch,
                        "all_tokens" => FilterOp::AllTokens,
                        "any_tokens" => FilterOp::AnyTokens,
                        _ => return Err(invalid(&operator, "unsupported predicate operator")),
                    },
                };
                let value = parts.next().map(|p| value(p, &self.bindings)).transpose()?;
                let filter = InputFilter {
                    op: Some(op),
                    value,
                    ..Default::default()
                };
                if let Some(node) = self.input.nodes.iter_mut().find(|n| n.id == prop.node) {
                    node.filters.entry(prop.property).or_default().push(filter);
                } else if let Some(index) = self.edges.get(&prop.node) {
                    let edge = &mut self.input.relationships[*index];
                    if edge.hops.max != 1 {
                        return Err(invalid(
                            &pair,
                            "a variable-length relationship binds a list; relationship-list predicates are unsupported",
                        ));
                    }
                    edge.filters.entry(prop.property).or_default().push(filter);
                } else {
                    return Err(invalid(&pair, &format!("undefined variable {}", prop.node)));
                }
            }
            _ => {
                return Err(invalid(
                    &pair,
                    "only AND-combined property predicates are supported",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn promote_ids(&mut self) -> Result<()> {
        for node in &mut self.input.nodes {
            if !node.node_ids.is_empty() {
                continue;
            }
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
                ] if !values.is_empty() => {
                    node.node_ids = values
                        .iter()
                        .map(|v| {
                            v.as_i64().ok_or_else(|| {
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
