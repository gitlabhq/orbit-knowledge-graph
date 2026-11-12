use crate::DataType;

use super::check::{Typed, scalar};
use super::{Comparison, Expr, Literal, Logical, Matching, Operand, PlanError, Predicate};
use crate::Model;
use crate::plan::tracking::Construction;

const MIN_PATTERN: usize = 3;

pub(super) fn predicate(
    model: &Model,
    construction: &Construction<Logical>,
    predicate: &Predicate,
) -> Result<(), PlanError> {
    looked_up(model, predicate)?;
    match predicate {
        Predicate::And(all) => all
            .iter()
            .try_for_each(|predicate| self::predicate(model, construction, predicate)),
        Predicate::Not(inner) => {
            negated(model, inner)?;
            self::predicate(model, construction, inner)
        }
        Predicate::Null { left, .. } => filterable(model, construction, *left).map(drop),
        Predicate::In { left, values } => {
            let typed = filterable(model, construction, *left)?;
            values.iter().try_for_each(|value| literal(&typed, value))
        }
        Predicate::InTuple { left, values } => {
            let typed = left
                .iter()
                .map(|left| filterable(model, construction, *left))
                .collect::<Result<Vec<_>, _>>()?;
            values.iter().try_for_each(|tuple| {
                if tuple.len() != typed.len() {
                    return Err(PlanError::shape(
                        "every tuple of a tuple IN has one value per column",
                    ));
                }
                typed
                    .iter()
                    .zip(tuple)
                    .try_for_each(|(typed, value)| literal(typed, value))
            })
        }
        Predicate::Compare {
            left,
            comparison,
            right,
        } => {
            let typed = filterable(model, construction, *left)?;
            let ordering = !matches!(comparison, Comparison::Eq | Comparison::Ne);
            if ordering && typed.data_type == DataType::Enum {
                return Err(PlanError::Type {
                    expr: typed.name,
                    expected: "an ordered value; enum values compare only with = or <>".to_string(),
                    found: "Enum".to_string(),
                });
            }
            match right {
                Operand::Literal(value) => literal(&typed, value),
                Operand::Expr(right) => {
                    let other = filterable(model, construction, *right)?;
                    comparable(model, &typed, &other)
                }
            }
        }
        Predicate::Text {
            left,
            matching,
            needle,
        } => {
            let typed = filterable(model, construction, *left)?;
            text(&typed, *matching, needle)
        }
    }
}

fn negated(model: &Model, predicate: &Predicate) -> Result<(), PlanError> {
    for leaf in predicate.leaf_predicates() {
        if let Some(Expr::Property { property, .. }) = leaf.exprs().first().copied()
            && model.lookup(property).is_some()
        {
            return Err(PlanError::shape(format!(
                "{}: virtual columns cannot be used under NOT",
                model.graph().property(property).name
            )));
        }
    }
    Ok(())
}

fn looked_up(model: &Model, leaf: &Predicate) -> Result<(), PlanError> {
    let (Some(Expr::Property { property, .. }), Some(operator)) =
        (leaf.exprs().first().copied(), leaf.operator())
    else {
        return Ok(());
    };
    let Some(lookup) = model.lookup(property) else {
        return Ok(());
    };
    let name = &model.graph().property(property).name;
    if lookup.disabled {
        return Err(PlanError::shape(format!(
            "{name} is looked up from {} that is not available yet",
            lookup.service
        )));
    }
    if !lookup
        .operators
        .iter()
        .any(|allowed| allowed == operator.as_ref())
    {
        return Err(PlanError::shape(format!(
            "{operator} is not supported on the looked-up property {name}; allowed: {}",
            lookup.operators.join(", ")
        )));
    }
    Ok(())
}

pub(super) fn filterable<'m>(
    model: &'m Model,
    construction: &Construction<Logical>,
    expr: Expr,
) -> Result<Typed<'m>, PlanError> {
    let typed = scalar(model, construction, expr)?;
    if typed.access.filterable {
        Ok(typed)
    } else {
        Err(PlanError::NotFilterable(typed.name))
    }
}

fn text(typed: &Typed<'_>, matching: Matching, needle: &str) -> Result<(), PlanError> {
    if !matches!(
        typed.data_type,
        DataType::String | DataType::Enum | DataType::Uuid
    ) {
        return Err(PlanError::Type {
            expr: typed.name.clone(),
            expected: "text".to_string(),
            found: format!("{:?}", typed.data_type),
        });
    }
    if !typed.access.like {
        return Err(PlanError::NoSubstring(typed.name.clone()));
    }
    let length = needle.chars().count();
    if length < MIN_PATTERN {
        return Err(PlanError::shape(format!(
            "the search pattern on {} must be at least {MIN_PATTERN} characters",
            typed.name
        )));
    }
    if matching == Matching::Token
        && needle
            .chars()
            .any(|c| c.is_ascii() && !c.is_ascii_alphanumeric())
    {
        return Err(PlanError::shape(format!(
            "a token match on {} takes one word of letters and digits; use all_tokens for several words",
            typed.name
        )));
    }
    let labels = typed
        .property
        .and_then(|property| property.enum_values.as_ref());
    match labels {
        Some(labels) if !matching.tokens() => {
            let needle = needle.to_lowercase();
            let found = labels.values().any(|label| {
                let label = label.to_lowercase();
                match matching {
                    Matching::Contains => label.contains(&needle),
                    Matching::StartsWith => label.starts_with(&needle),
                    _ => label.ends_with(&needle),
                }
            });
            let verb = match matching {
                Matching::Contains => "contains",
                Matching::StartsWith => "starts with",
                _ => "ends with",
            };
            found.then_some(()).ok_or_else(|| PlanError::Invalid {
                expr: typed.name.clone(),
                reason: format!(
                    "no enum value {verb} \"{needle}\". Valid values: {}",
                    allowed(typed).join(", ")
                ),
            })
        }
        _ => Ok(()),
    }
}

fn comparable(model: &Model, left: &Typed<'_>, right: &Typed<'_>) -> Result<(), PlanError> {
    if let Some(property) = [left.property, right.property]
        .into_iter()
        .flatten()
        .find(|property| model.lookup(property.id).is_some())
    {
        return Err(PlanError::shape(format!(
            "{} is a looked-up property and cannot be compared with another property",
            property.name
        )));
    }
    let temporal = |data_type| matches!(data_type, DataType::Date | DataType::DateTime);
    let same =
        left.data_type == right.data_type || temporal(left.data_type) && temporal(right.data_type);
    let enums = left.data_type == DataType::Enum
        && left.property.map(|p| (left.enum_type, &p.enum_values))
            != right.property.map(|p| (right.enum_type, &p.enum_values));
    if same && enums {
        return Err(PlanError::Invalid {
            expr: format!("{} and {}", left.name, right.name),
            reason: "they are enums with different values and cannot be compared".to_string(),
        });
    }
    if !same {
        return Err(PlanError::Type {
            expr: right.name.clone(),
            expected: format!("{:?} like {}", left.data_type, left.name),
            found: format!("{:?}", right.data_type),
        });
    }
    Ok(())
}

pub(super) fn literal(typed: &Typed<'_>, literal: &Literal) -> Result<(), PlanError> {
    let invalid = |reason: String| PlanError::Invalid {
        expr: typed.name.clone(),
        reason,
    };
    let date = |valid: bool, formats: &str| {
        valid
            .then_some(())
            .ok_or_else(|| invalid(format!("the value is not a valid date; use {formats}")))
    };
    match (typed.data_type, literal) {
        (DataType::Enum, Literal::String(_) | Literal::Int(_)) => enum_value(typed, literal),
        (DataType::Date, Literal::String(text) | Literal::Temporal(text)) => {
            date(super::time::date(text), "YYYY-MM-DD")
        }
        (DataType::DateTime, Literal::String(text) | Literal::Temporal(text)) => date(
            super::time::date_time(text),
            "YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS[.ffffff][Z]",
        ),
        (found, Literal::Temporal(_)) => Err(invalid(format!(
            "datetime() and date() compare only with date fields, but this field is {found:?}"
        ))),
        (DataType::String | DataType::Uuid, Literal::String(_))
        | (DataType::Int, Literal::Int(_))
        | (DataType::Float, Literal::Float(_) | Literal::Int(_))
        | (DataType::Bool, Literal::Bool(_)) => Ok(()),
        (expected, found) => {
            let noun = |data_type| match data_type {
                DataType::Int => "an integer",
                DataType::Float => "a number",
                DataType::Bool => "a boolean",
                _ => "a string",
            };
            let found = match found {
                Literal::String(_) => "a string",
                Literal::Int(_) => "an integer",
                Literal::Float(_) => "a float",
                Literal::Bool(_) => "a boolean",
                Literal::Temporal(_) => "a date",
            };
            Err(invalid(format!(
                "the value is {found}, not {}",
                noun(expected)
            )))
        }
    }
}

fn enum_value(typed: &Typed<'_>, literal: &Literal) -> Result<(), PlanError> {
    let Some(property) = typed.property else {
        return Ok(());
    };
    let labels = property.enum_values.iter().flatten();
    let declared = match literal {
        Literal::String(text) => labels.clone().any(|(_, label)| label == text),
        Literal::Int(key) => {
            typed.enum_type == Some(crate::EnumType::Int)
                && labels.clone().any(|(known, _)| known == key)
        }
        _ => false,
    };
    declared.then_some(()).ok_or_else(|| PlanError::Value {
        property: typed.name.clone(),
        value: match literal {
            Literal::String(text) => text.clone(),
            Literal::Int(key) => key.to_string(),
            other => format!("{other:?}"),
        },
        allowed: allowed(typed),
    })
}

fn allowed(typed: &Typed<'_>) -> Vec<String> {
    let Some(property) = typed.property else {
        return Vec::new();
    };
    property
        .enum_values
        .iter()
        .flatten()
        .map(|(key, label)| match typed.enum_type {
            Some(crate::EnumType::Int) => format!("{key} ({label})"),
            _ => label.clone(),
        })
        .collect()
}
