pub mod gql;
pub mod json;

use crate::plan::logical::{Literal, LogicalPlan, PlanError};

#[derive(Debug)]
pub struct Statement {
    pub plan: LogicalPlan,
    pub debug: bool,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SurfaceError {
    #[error("parse error: {0}")]
    Syntax(String),
    #[error("schema violation: {0}")]
    Schema(String),
    #[error("limit exceeded: {0}")]
    Limit(String),
    #[error(transparent)]
    Plan(#[from] PlanError),
}

impl SurfaceError {
    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema(message.into())
    }
}

fn pinned(property: &str, literal: Literal) -> Literal {
    match literal {
        Literal::String(text) if property == "id" => text
            .parse()
            .map_or_else(|_| Literal::String(text), Literal::Int),
        other => other,
    }
}
