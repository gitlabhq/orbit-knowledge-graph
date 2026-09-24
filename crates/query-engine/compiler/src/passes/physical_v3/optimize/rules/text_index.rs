use super::super::super::*;
use crate::input::FilterOp;
use crate::passes::logical_v3::{Expr, Value};

pub fn apply<B: Backend>(plan: PhysicalPlan<B>, catalog: &PhysicalCatalog<'_>) -> PhysicalPlan<B> {
    plan.map_expressions(&mut |expression| match expression {
        Expr::Filter {
            op: FilterOp::Contains,
            left,
            right: Some(right),
            ..
        } => {
            let rewrite = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(Value::String(token))) => catalog
                    .text_index(column)
                    .map(|tokenizer| (token.clone(), tokenizer.to_string())),
                _ => None,
            };
            match rewrite {
                Some((token, tokenizer)) => Expr::TokenMatch {
                    value: left,
                    token: Value::String(token),
                    tokenizer,
                },
                None => Expr::Filter {
                    op: FilterOp::Contains,
                    left,
                    right: Some(right),
                    data_type: Some(ontology::DataType::String),
                },
            }
        }
        other => other,
    })
}
