use ontology::DataType;

use super::{Expr, Literal, Logical, LogicalType, Measure, PlanError, Predicate};
use crate::plan::tracking::Construction;
use crate::{Model, Property};

pub(super) struct Typed<'m> {
    pub(super) data_type: DataType,
    property: Option<&'m Property>,
    name: String,
}

pub(super) fn scalar<'m>(
    model: &'m Model,
    construction: &Construction<Logical>,
    expr: Expr,
) -> Result<Typed<'m>, PlanError> {
    match expr {
        Expr::Property { binding, property } => {
            let property = model.graph().property(property);
            let entity = &model.graph().entity(property.entity).name;
            let name = format!("{entity}.{}", property.name);
            match construction.value(binding)? {
                (_, LogicalType::Entity(bound)) if *bound == property.entity => Ok(Typed {
                    data_type: property.data_type,
                    property: Some(property),
                    name,
                }),
                (_, found) => Err(mismatch(name, entity, *found)),
            }
        }
        Expr::Output(output) => match construction.value(output)? {
            (name, LogicalType::Scalar(data_type)) => Ok(Typed {
                data_type: *data_type,
                property: None,
                name: name.unwrap_or("output").to_string(),
            }),
            (name, found) => Err(mismatch(name.unwrap_or("output"), "a scalar", *found)),
        },
    }
}

fn mismatch(expr: impl Into<String>, expected: &str, found: LogicalType) -> PlanError {
    PlanError::Type {
        expr: expr.into(),
        expected: expected.to_string(),
        found: format!("{found:?}"),
    }
}

pub(super) fn predicate(
    model: &Model,
    construction: &Construction<Logical>,
    predicate: &Predicate,
) -> Result<(), PlanError> {
    match predicate {
        Predicate::And(all) => all
            .iter()
            .try_for_each(|predicate| self::predicate(model, construction, predicate)),
        Predicate::Compare { left, right, .. } => {
            let typed = filterable(model, construction, *left)?;
            literal(&typed, right)
        }
        Predicate::Contains { left, .. } => {
            let typed = filterable(model, construction, *left)?;
            if typed.data_type != DataType::String {
                return Err(PlanError::Type {
                    expr: typed.name,
                    expected: "String".to_string(),
                    found: format!("{:?}", typed.data_type),
                });
            }
            match typed.property {
                Some(property) if !model.access().property(property.id).like => {
                    Err(PlanError::NoSubstring(typed.name))
                }
                _ => Ok(()),
            }
        }
    }
}

fn filterable<'m>(
    model: &'m Model,
    construction: &Construction<Logical>,
    expr: Expr,
) -> Result<Typed<'m>, PlanError> {
    let typed = scalar(model, construction, expr)?;
    match typed.property {
        Some(property) if !model.access().property(property.id).filterable => {
            Err(PlanError::NotFilterable(typed.name))
        }
        _ => Ok(typed),
    }
}

fn literal(typed: &Typed<'_>, literal: &Literal) -> Result<(), PlanError> {
    let allowed = typed
        .property
        .and_then(|property| property.enum_values.as_ref());
    match (typed.data_type, literal) {
        (DataType::Enum, Literal::String(value)) => match allowed {
            Some(values) if !values.values().any(|allowed| allowed == value) => {
                Err(PlanError::Value {
                    property: typed.name.clone(),
                    value: value.clone(),
                    allowed: values.values().cloned().collect(),
                })
            }
            _ => Ok(()),
        },
        (
            DataType::String | DataType::Date | DataType::DateTime | DataType::Uuid,
            Literal::String(_),
        )
        | (DataType::Int | DataType::Enum, Literal::Int(_))
        | (DataType::Float, Literal::Float(_) | Literal::Int(_))
        | (DataType::Bool, Literal::Bool(_)) => Ok(()),
        (expected, found) => Err(PlanError::Type {
            expr: typed.name.clone(),
            expected: format!("{expected:?}"),
            found: format!("{found:?}"),
        }),
    }
}

pub(super) fn measure(
    model: &Model,
    construction: &Construction<Logical>,
    mut measure: Measure,
) -> Result<DataType, PlanError> {
    let Some(input) = measure.input() else {
        return Ok(DataType::Int);
    };
    let typed = scalar(model, construction, *input)?;
    match (measure, typed.data_type) {
        (Measure::Sum(_), DataType::Int | DataType::Float)
        | (Measure::Min(_) | Measure::Max(_), _) => Ok(typed.data_type),
        (_, found) => Err(PlanError::Type {
            expr: typed.name,
            expected: "a number".to_string(),
            found: format!("{found:?}"),
        }),
    }
}
