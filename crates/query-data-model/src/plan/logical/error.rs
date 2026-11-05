use crate::catalog::Unresolved;
use crate::plan::tracking::Rejected;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Unresolved(#[from] Unresolved),
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("{0} is hidden")]
    Hidden(String),
    #[error("{0} cannot be filtered")]
    NotFilterable(String),
    #[error("{0} does not support substring matching")]
    NoSubstring(String),
    #[error("{expr} is {found}, not {expected}")]
    Type {
        expr: String,
        expected: String,
        found: String,
    },
    #[error("{property} has no value {value}; allowed: {}", .allowed.join(", "))]
    Value {
        property: String,
        value: String,
        allowed: Vec<String>,
    },
    #[error("{relationship} reaches several entities; pick one of: {}", .endpoints.join(", "))]
    Ambiguous {
        relationship: String,
        endpoints: Vec<String>,
    },
    #[error("these rows have no entity to follow or read properties from")]
    NoEntity,
    #[error("unknown column {name}; available: {}", .available.join(", "))]
    UnknownColumn {
        name: String,
        available: Vec<String>,
    },
}
