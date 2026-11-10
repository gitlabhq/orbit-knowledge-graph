use query_data_model::EnumType;
use query_data_model::Model;
use query_data_model::plan::logical::{Expr, Literal, Operand, Predicate};

pub(super) fn coerce(predicate: Option<Predicate>, model: &Model) -> Option<Predicate> {
    predicate.map(|predicate| rewrite(predicate, model))
}

fn rewrite(predicate: Predicate, model: &Model) -> Predicate {
    match predicate {
        Predicate::And(all) => {
            Predicate::And(all.into_iter().map(|inner| rewrite(inner, model)).collect())
        }
        Predicate::Not(inner) => Predicate::Not(Box::new(rewrite(*inner, model))),
        Predicate::Compare {
            left,
            comparison,
            right: Operand::Literal(literal),
        } => Predicate::Compare {
            left,
            comparison,
            right: Operand::Literal(label(left, literal, model)),
        },
        Predicate::In { left, values } => Predicate::In {
            left,
            values: values
                .into_iter()
                .map(|value| label(left, value, model))
                .collect(),
        },
        other => other,
    }
}

fn label(left: Expr, literal: Literal, model: &Model) -> Literal {
    let (Expr::Property { property, .. }, Literal::Int(code)) = (left, &literal) else {
        return literal;
    };
    let property = model.graph().property(property);
    match (&property.enum_type, &property.enum_values) {
        (EnumType::Int, Some(values)) => values
            .get(code)
            .map_or(literal, |label| Literal::String(label.clone())),
        _ => literal,
    }
}
