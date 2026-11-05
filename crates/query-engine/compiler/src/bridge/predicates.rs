use query_data_model::plan::logical::{
    Comparison, EdgeField, Expr, Literal, Matching, Operand, Predicate,
};
use serde_json::Value;

use super::Bridge;
use crate::input::{BooleanExpression, Condition, FilterOp, InputFilter, PredicateTarget};
use crate::{QueryError, Result};

impl Bridge<'_> {
    pub(super) fn condition(&self, predicate: &Predicate) -> Result<BooleanExpression<Condition>> {
        let leaf = |left: &Expr, op: FilterOp, value: Option<Value>, temporal: bool| {
            let (target, property) = self.column(left)?;
            Ok(BooleanExpression::Leaf(Condition {
                target,
                property,
                filter: InputFilter {
                    op: Some(op),
                    value,
                    rhs_column: None,
                    from_temporal_literal: temporal,
                },
            }))
        };
        match predicate {
            Predicate::And(all) => Ok(BooleanExpression::And(
                all.iter()
                    .map(|child| self.condition(child))
                    .collect::<Result<_>>()?,
            )),
            Predicate::Not(inner) => Ok(BooleanExpression::Not(Box::new(self.condition(inner)?))),
            Predicate::Null { left, null } => {
                let op = if *null {
                    FilterOp::IsNull
                } else {
                    FilterOp::IsNotNull
                };
                leaf(left, op, None, false)
            }
            Predicate::In { left, values } => {
                let temporal = values
                    .iter()
                    .any(|value| matches!(value, Literal::Temporal(_)));
                let values = values.iter().map(value).collect();
                leaf(left, FilterOp::In, Some(Value::Array(values)), temporal)
            }
            Predicate::Text {
                left,
                matching,
                needle,
            } => {
                let op = match matching {
                    Matching::Contains => FilterOp::Contains,
                    Matching::StartsWith => FilterOp::StartsWith,
                    Matching::EndsWith => FilterOp::EndsWith,
                    Matching::Token => FilterOp::TokenMatch,
                    Matching::AllTokens => FilterOp::AllTokens,
                    Matching::AnyTokens => FilterOp::AnyTokens,
                };
                leaf(left, op, Some(Value::String(needle.clone())), false)
            }
            Predicate::Compare {
                left,
                comparison,
                right,
            } => {
                let op = match comparison {
                    Comparison::Eq => FilterOp::Eq,
                    Comparison::Ne => FilterOp::Ne,
                    Comparison::Lt => FilterOp::Lt,
                    Comparison::Le => FilterOp::Lte,
                    Comparison::Gt => FilterOp::Gt,
                    Comparison::Ge => FilterOp::Gte,
                };
                match right {
                    Operand::Literal(literal) => leaf(
                        left,
                        op,
                        Some(value(literal)),
                        matches!(literal, Literal::Temporal(_)),
                    ),
                    Operand::Expr(right) => {
                        let mut compared = leaf(left, op, None, false)?;
                        let (PredicateTarget::Node(node), property) = self.column(right)? else {
                            return Err(QueryError::PipelineInvariant(
                                "relationship columns are never compared with properties".into(),
                            ));
                        };
                        compared.visit_mut(&mut |condition| {
                            condition.filter.rhs_column = Some((node.clone(), property.clone()))
                        });
                        Ok(compared)
                    }
                }
            }
        }
    }

    fn column(&self, expr: &Expr) -> Result<(PredicateTarget, String)> {
        match expr {
            Expr::Property { binding, property } => Ok((
                PredicateTarget::Node(self.alias(*binding)?),
                self.model.graph().property(*property).name.clone(),
            )),
            Expr::Edge { binding, field } => {
                let index =
                    self.edges.get(binding).copied().ok_or_else(|| {
                        QueryError::PipelineInvariant("unbound relationship".into())
                    })?;
                let property = match field {
                    EdgeField::Kind => Condition::RELATIONSHIP_TYPE,
                    EdgeField::Source => "source_id",
                    EdgeField::SourceKind => "source_kind",
                    EdgeField::Target => "target_id",
                    EdgeField::TargetKind => "target_kind",
                };
                Ok((PredicateTarget::Relationship(index), property.to_string()))
            }
            Expr::Attribute { binding, attribute } => {
                let index =
                    self.edges.get(binding).copied().ok_or_else(|| {
                        QueryError::PipelineInvariant("unbound relationship".into())
                    })?;
                let name = &self.model.graph().attribute(*attribute).name;
                Ok((PredicateTarget::Relationship(index), name.clone()))
            }
            other => Err(QueryError::PipelineInvariant(format!(
                "{other:?} is not a filterable column"
            ))),
        }
    }
}

fn value(literal: &Literal) -> Value {
    match literal {
        Literal::String(text) | Literal::Temporal(text) => Value::String(text.clone()),
        Literal::Int(number) => Value::from(*number),
        Literal::Float(number) => Value::from(*number),
        Literal::Bool(flag) => Value::Bool(*flag),
    }
}
