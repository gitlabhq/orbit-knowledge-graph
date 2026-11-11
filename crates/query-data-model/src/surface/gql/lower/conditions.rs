use super::super::ast::{
    BooleanExpression, Literal, MapEntry, Operand, Operator, Predicate as Leaf, Property, Subject,
    Value,
};
use super::super::errors::invalid;
use super::{Built, Conditions};
use crate::plan::logical::{EdgeField, Expr, LogicalBuilder, LogicalType, Predicate, Rows};
use crate::surface::SurfaceError;

const STRUCTURAL: &str = "MATCHES ast(...) must be a top-level condition of a single-node match";

/// Applies WHERE as one filter, then each top-level structural match.
pub(super) fn filter<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    predicates: Conditions<'_>,
) -> Built<'m> {
    let (patterns, predicates) = structural(predicates);
    let mut rows = match predicates.is_empty() {
        true => rows,
        false => {
            let predicate = tree(BooleanExpression::And(predicates), &rows)?;
            q.filter(rows, predicate)?
        }
    };
    for (property, pattern, language) in patterns {
        let target = column(&rows, &property)?;
        rows = q.structural(rows, target, &pattern, &language)?;
    }
    Ok(rows)
}

type Pattern<'i> = (Property<'i>, String, String);

/// Splits top-level structural matches from the other conditions; without one, the conditions
/// stay as written.
fn structural(predicates: Conditions<'_>) -> (Vec<Pattern<'_>>, Conditions<'_>) {
    fn top_level(tree: &BooleanExpression<Leaf<'_>>) -> bool {
        match tree {
            BooleanExpression::Leaf(Leaf::Structural { .. }) => true,
            BooleanExpression::And(all) => all.iter().any(top_level),
            _ => false,
        }
    }
    if !predicates.iter().any(top_level) {
        return (Vec::new(), predicates);
    }
    let mut patterns = Vec::new();
    let mut rest = Vec::new();
    for conjunct in flatten(predicates) {
        match conjunct {
            BooleanExpression::Leaf(Leaf::Structural {
                property,
                pattern,
                language,
                ..
            }) => patterns.push((property, pattern, language)),
            other => rest.push(other),
        }
    }
    (patterns, rest)
}

fn flatten(predicates: Conditions<'_>) -> Conditions<'_> {
    predicates
        .into_iter()
        .flat_map(|tree| match tree {
            BooleanExpression::And(all) => flatten(all),
            tree => vec![tree],
        })
        .collect()
}

fn tree(tree: BooleanExpression<Leaf<'_>>, rows: &Rows<'_>) -> Result<Predicate, SurfaceError> {
    Ok(match tree {
        BooleanExpression::Leaf(leaf) => self::leaf(leaf, rows)?,
        BooleanExpression::And(all) => Predicate::And(
            all.into_iter()
                .map(|child| self::tree(child, rows))
                .collect::<Result<_, _>>()?,
        ),
        BooleanExpression::Not(inner) => !self::tree(*inner, rows)?,
    })
}

pub(super) fn entries(
    entries: &[MapEntry<'_>],
    model: &crate::Model,
    mut column: impl FnMut(&str) -> Result<Expr, SurfaceError>,
) -> Result<Vec<Predicate>, SurfaceError> {
    entries
        .iter()
        .map(|entry| {
            let key = &entry.key.value;
            match &entry.value {
                Value::Scalar(literal) => {
                    let left = column(key)?;
                    let literal = pinned(&left, model, literal.clone());
                    Ok(left.eq(literal))
                }
                Value::List(_) => Err(invalid(
                    entry.key.span,
                    &format!("{key} takes one value in a property map; write WHERE n.{key} IN [...] for several"),
                )),
            }
        })
        .collect()
}

fn leaf(leaf: Leaf<'_>, rows: &Rows<'_>) -> Result<Predicate, SurfaceError> {
    let (span, subject, op, operand) = match leaf {
        Leaf::Comparison {
            span,
            subject,
            op,
            operand,
        } => (span, subject, op, operand),
        Leaf::Structural { span, .. } => return Err(invalid(span, STRUCTURAL)),
    };
    let (left, binding) = match &subject {
        Subject::Property(property) => {
            (column(rows, property)?, rows.binding(&property.node.value)?)
        }
        Subject::Type(variable) => {
            let binding = rows.binding(&variable.value)?;
            (binding.field(EdgeField::Kind)?, binding)
        }
    };
    let model = binding.catalog().model();
    let comparison = op.comparison();
    Ok(match (op, operand) {
        (Operator::IsNull, _) => left.is_null(true),
        (Operator::IsNotNull, _) => left.is_null(false),
        (_, Some(Operand::Property(right))) => {
            let comparison = comparison.ok_or_else(|| mismatch(span))?;
            left.compare(comparison, column(rows, &right)?)
        }
        (Operator::In, Some(Operand::Value(Value::List(values)))) => {
            let values: Vec<_> = values
                .into_iter()
                .map(|value| pinned(&left, model, value))
                .collect();
            left.one_of(values)
        }
        (_, Some(Operand::Value(Value::Scalar(literal)))) => match (op.matching(), comparison) {
            (Some(matching), _) => match literal {
                Literal::String(needle) | Literal::Temporal(needle) => {
                    left.matches(matching, needle)
                }
                _ => return Err(invalid(span, "text matching takes a string")),
            },
            (None, Some(comparison)) => {
                let literal = match comparison {
                    crate::plan::logical::Comparison::Eq => pinned(&left, model, literal),
                    _ => literal,
                };
                left.compare(comparison, literal)
            }
            (None, None) => return Err(mismatch(span)),
        },
        (_, Some(Operand::Value(Value::List(_)))) => {
            return Err(invalid(
                span,
                "only IN takes a list; compare with one value",
            ));
        }
        (_, None) => return Err(mismatch(span)),
    })
}

/// A property of a node, or a field or attribute of a relationship.
pub(super) fn column(rows: &Rows<'_>, property: &Property<'_>) -> Result<Expr, SurfaceError> {
    let binding = rows.binding(&property.node.value)?;
    let name = &property.property.value;
    Ok(match binding.kind() {
        LogicalType::Edge => rows.edge_column(binding, name)?,
        _ => binding.property(name)?,
    })
}

/// An integer identity arrives as a number, so a quoted integer still pins the node.
fn pinned(left: &Expr, model: &crate::Model, literal: Literal) -> Literal {
    let graph = model.graph();
    match (left, literal) {
        (Expr::Property { property, .. }, Literal::String(text))
            if graph.property(*property).data_type == crate::DataType::Int
                && graph.entity(graph.property(*property).entity).identity == [*property] =>
        {
            text.parse()
                .map_or_else(|_| Literal::String(text), Literal::Int)
        }
        (_, other) => other,
    }
}

fn mismatch(span: pest::Span<'_>) -> SurfaceError {
    SurfaceError::Syntax(format!(
        "grammar produced an operator and operand that no condition takes at {:?}",
        span.as_str()
    ))
}
