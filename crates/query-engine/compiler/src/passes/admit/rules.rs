use std::collections::HashSet;

use crate::passes::codegen::RelationalModel;
use query_data_model::plan::logical::{Comparison, Expr, Literal, Predicate};
use query_data_model::plan::tracking::Ref;

use super::{node_property, served};
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::logical::columns::free_text;
use crate::logical::facts::Fact;

fn sides(fact: &Fact<'_>) -> impl Iterator<Item = Expr> {
    std::iter::once(fact.left).chain(fact.right())
}

pub(super) fn negated(
    query: Query<'_>,
    facts: &[Fact<'_>],
    model: &impl RelationalModel,
) -> Result<()> {
    let negated = facts.iter().filter(|fact| fact.negated).flat_map(sides);
    for expr in negated {
        let Some((_, entity, property)) = node_property(query, expr) else {
            continue;
        };
        if served(model, expr) {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: virtual columns cannot be used under NOT"
            )));
        }
    }
    Ok(())
}

pub(super) fn epoch(
    query: Query<'_>,
    facts: &[Fact<'_>],
    model: &impl RelationalModel,
) -> Result<()> {
    for fact in facts {
        let Some((_, entity, property)) = node_property(query, fact.left) else {
            continue;
        };
        let epoch_null = matches!(fact.left, Expr::Property { property, .. } if model.catalog().null_as_epoch(property));
        let early = fact.literals().iter().any(|literal| match literal {
            Literal::String(text) | Literal::Temporal(text) => at_or_before_epoch(text),
            _ => false,
        });
        if epoch_null && early {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: \
                 values on or before 1970-01-01 stand for null; use is_null or is_not_null"
            )));
        }
    }
    Ok(())
}

fn at_or_before_epoch(value: &str) -> bool {
    let date = value.get(..10).filter(|date| {
        date.bytes().enumerate().all(|(i, byte)| match i {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
    });
    date.is_some_and(|date| date <= "1970-01-01")
}

pub(super) fn aggregation_text(
    query: Query<'_>,
    facts: &[Fact<'_>],
    model: &impl RelationalModel,
) -> Result<()> {
    let Some((groups, _)) = query.aggregate() else {
        return Ok(());
    };
    let rows: HashSet<Ref> = groups
        .iter()
        .filter_map(|group| match group {
            Expr::Node(binding) => Some(*binding),
            Expr::Property { binding, property } => {
                query.entity_name(*binding)?;
                free_text(model.catalog().graph(), *property).then_some(*binding)
            }
            _ => None,
        })
        .collect();
    for fact in facts {
        let matching = match fact.leaf {
            Predicate::Text { .. } => true,
            Predicate::Compare { .. } => !matches!(
                fact.comparison,
                Some(Comparison::Eq | Comparison::Ne) | None
            ),
            _ => false,
        };
        if !matching {
            continue;
        }
        for expr in sides(fact) {
            let (
                Expr::Property {
                    binding,
                    property: id,
                },
                Some((alias, entity, property)),
            ) = (expr, node_property(query, expr))
            else {
                continue;
            };
            let by_namespace = query
                .entity(binding)
                .and_then(|entity| model.catalog().redaction(entity))
                .is_some_and(|redaction| {
                    matches!(redaction.resource.as_str(), "project" | "group")
                });
            if rows.contains(&binding) || by_namespace || !free_text(model.catalog().graph(), id) {
                continue;
            }
            return Err(QueryError::Validation(format!(
                "aggregation filter on {entity}.{property}: text matching and ordering require grouping by \"{alias}\""
            )));
        }
    }
    Ok(())
}

pub(super) fn order(query: Query<'_>, model: &impl RelationalModel) -> Result<()> {
    for key in query.ordered() {
        let Some((alias, _, property)) = node_property(query, key) else {
            continue;
        };
        if served(model, key) {
            return Err(QueryError::Validation(format!(
                "order_by property {alias}.{property} is virtual and cannot be read in SQL"
            )));
        }
    }
    Ok(())
}
