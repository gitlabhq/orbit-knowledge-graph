use std::collections::HashSet;

use serde_json::Value;

use super::super::ast::{BooleanExpression, Comparison, FilterOp, MapEntry, Predicate as Leaf};
use super::super::errors::invalid;
use super::scope::{Scope, Shape};
use crate::plan::logical::{
    Binding, Comparison as Compare, EdgeField, Expr, Literal, Matching, PlanError, Predicate, Rows,
};
use crate::surface::SurfaceError;

pub(super) fn referenced(predicates: &[BooleanExpression<Leaf<'_>>]) -> HashSet<String> {
    fn visit(tree: &BooleanExpression<Leaf<'_>>, into: &mut HashSet<String>) {
        match tree {
            BooleanExpression::Leaf(Leaf::Comparison(comparison)) => {
                into.insert(comparison.property.node.value.clone());
                if let Some(rhs) = &comparison.rhs_property {
                    into.insert(rhs.node.value.clone());
                }
            }
            BooleanExpression::Leaf(Leaf::RelationshipType { .. }) => {}
            BooleanExpression::And(all) => all.iter().for_each(|tree| visit(tree, into)),
            BooleanExpression::Not(inner) => visit(inner, into),
        }
    }
    let mut into = HashSet::new();
    predicates.iter().for_each(|tree| visit(tree, &mut into));
    into
}

pub(super) fn tree(
    tree: BooleanExpression<Leaf<'_>>,
    rows: &Rows<'_>,
    scope: &Scope<'_>,
) -> Result<Predicate, SurfaceError> {
    Ok(match tree {
        BooleanExpression::Leaf(leaf) => self::leaf(leaf, rows, scope)?,
        BooleanExpression::And(all) => Predicate::And(
            all.into_iter()
                .map(|child| self::tree(child, rows, scope))
                .collect::<Result<_, _>>()?,
        ),
        BooleanExpression::Not(inner) => !self::tree(*inner, rows, scope)?,
    })
}

pub(super) fn entries(
    entries: &[MapEntry<'_>],
    mut column: impl FnMut(&str) -> Result<Expr, SurfaceError>,
) -> Result<Vec<Predicate>, SurfaceError> {
    entries
        .iter()
        .map(|entry| {
            let value = literal(&entry.value, entry.from_temporal_literal)?;
            Ok(column(&entry.key.value)?.eq(value))
        })
        .collect()
}

pub(super) fn edge_field(link: Binding<'_>, name: &str) -> Result<Expr, SurfaceError> {
    let field: EdgeField = name.parse().map_err(|_| PlanError::UnknownColumn {
        name: name.to_string(),
        available: <EdgeField as strum::VariantNames>::VARIANTS
            .iter()
            .map(|name| name.to_string())
            .collect(),
    })?;
    Ok(link.field(field)?)
}

fn leaf(leaf: Leaf<'_>, rows: &Rows<'_>, scope: &Scope<'_>) -> Result<Predicate, SurfaceError> {
    let comparison = match leaf {
        Leaf::Comparison(comparison) => *comparison,
        Leaf::RelationshipType {
            span,
            variable,
            op,
            value,
        } => {
            let name = &variable.value;
            let kind = match scope.edge(name) {
                Some((_, edge)) if edge.hops.max > 1 => {
                    return Err(invalid(
                        span,
                        &format!(
                            "{name} has a variable length and binds a list; type({name}) and RETURN {name} need a one-hop relationship, so list the types in the pattern instead, such as -[{name}:CLOSES|MENTIONS*1..3]->"
                        ),
                    ));
                }
                Some(_) => edge_field(rows.binding(name)?, "relationship_kind")?,
                None => return Err(not_relationship(span, name, scope)),
            };
            let value = value.ok_or_else(|| invalid(span, "type() compares with a value"))?;
            return compare(kind, op, &value, false, span);
        }
    };
    let Comparison {
        span,
        property,
        op,
        value,
        from_temporal_literal,
        rhs_property,
    } = comparison;
    let left = column(
        &property.node.value,
        &property.property.value,
        rows,
        scope,
        span,
    )?;
    match (rhs_property, value) {
        (Some(rhs), _) => {
            if scope.edge(&property.node.value).is_some() || scope.edge(&rhs.node.value).is_some() {
                return Err(invalid(
                    span,
                    "property comparisons involving relationships are unsupported",
                ));
            }
            let right = column(&rhs.node.value, &rhs.property.value, rows, scope, span)?;
            let comparison = comparison_of(op).ok_or_else(|| {
                invalid(
                    span,
                    "property-to-property comparisons only support =, <>, !=, <, >, <=, >=",
                )
            })?;
            Ok(left.compare(comparison, right))
        }
        (None, None) => match op {
            FilterOp::IsNull => Ok(left.is_null(true)),
            FilterOp::IsNotNull => Ok(left.is_null(false)),
            _ => Err(invalid(span, "this comparison needs a value")),
        },
        (None, Some(value)) => compare(left, op, &value, from_temporal_literal, span),
    }
}

fn column(
    alias: &str,
    property: &str,
    rows: &Rows<'_>,
    scope: &Scope<'_>,
    span: pest::Span<'_>,
) -> Result<Expr, SurfaceError> {
    if scope.node(alias).is_some() {
        return Ok(rows.binding(alias)?.property(property)?);
    }
    match scope.edge(alias) {
        Some((_, edge)) if edge.hops.max != 1 => Err(invalid(
            span,
            "a variable-length relationship binds a list; relationship-list predicates are unsupported",
        )),
        Some(_) => edge_field(rows.binding(alias)?, property),
        None => Err(invalid(span, &format!("undefined variable {alias}"))),
    }
}

fn not_relationship(span: pest::Span<'_>, name: &str, scope: &Scope<'_>) -> SurfaceError {
    let kind = match &scope.shape {
        Shape::Path(path) if path == name => "a path",
        _ if scope.dynamic(name) || scope.node(name).is_some() => "a node",
        _ => return invalid(span, &format!("undefined variable {name}")),
    };
    invalid(
        span,
        &format!("type() takes a relationship variable, such as r in -[r]->, but {name} is {kind}"),
    )
}

fn comparison_of(op: FilterOp) -> Option<Compare> {
    Some(match op {
        FilterOp::Eq => Compare::Eq,
        FilterOp::Ne => Compare::Ne,
        FilterOp::Gt => Compare::Gt,
        FilterOp::Lt => Compare::Lt,
        FilterOp::Gte => Compare::Ge,
        FilterOp::Lte => Compare::Le,
        _ => return None,
    })
}

fn compare(
    left: Expr,
    op: FilterOp,
    value: &Value,
    temporal: bool,
    span: pest::Span<'_>,
) -> Result<Predicate, SurfaceError> {
    let text = |matching| match value {
        Value::String(needle) => Ok(left.matches(matching, needle.clone())),
        _ => Err(invalid(span, "text matching takes a string")),
    };
    match op {
        FilterOp::In => match value {
            Value::Array(values) => Ok(left.one_of(
                values
                    .iter()
                    .map(|value| literal(value, temporal))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            _ => Err(invalid(span, "IN takes a list")),
        },
        FilterOp::Contains => text(Matching::Contains),
        FilterOp::StartsWith => text(Matching::StartsWith),
        FilterOp::EndsWith => text(Matching::EndsWith),
        FilterOp::TokenMatch => text(Matching::Token),
        FilterOp::AllTokens => text(Matching::AllTokens),
        FilterOp::AnyTokens => text(Matching::AnyTokens),
        FilterOp::IsNull => Ok(left.is_null(true)),
        FilterOp::IsNotNull => Ok(left.is_null(false)),
        op => {
            let comparison = comparison_of(op).expect("the remaining operators compare");
            Ok(left.compare(comparison, literal(value, temporal)?))
        }
    }
}

fn literal(value: &Value, temporal: bool) -> Result<Literal, SurfaceError> {
    match value {
        Value::String(text) if temporal => Ok(Literal::Temporal(text.clone())),
        Value::String(text) => Ok(Literal::String(text.clone())),
        Value::Bool(flag) => Ok(Literal::Bool(*flag)),
        Value::Number(number) => Ok(number.as_i64().map_or_else(
            || Literal::Float(number.as_f64().unwrap_or_default()),
            Literal::Int,
        )),
        other => Err(SurfaceError::schema(format!(
            "{other} is not a scalar value"
        ))),
    }
}
