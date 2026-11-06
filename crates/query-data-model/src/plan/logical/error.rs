use crate::catalog::Unresolved;
use crate::plan::tracking::Rejected;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    Name,
    Reference,
    Type,
    Policy,
    Connectivity,
    Shape,
    Limit,
    Construction,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Unresolved(#[from] Unresolved),
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("{0} is hidden")]
    Hidden(String),
    #[error("field \"{name}\" on {owner} is not available in this data model")]
    Unavailable { owner: String, name: String },
    #[error("{0} is available to administrators only")]
    AdminOnly(String),
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
    #[error("unknown variable {name}; available: {}", .available.join(", "))]
    UnknownBinding {
        name: String,
        available: Vec<String>,
    },
    #[error("{0} is bound twice")]
    Duplicate(String),
    #[error("{0}")]
    Shape(String),
    #[error("{what} is {found}; the maximum is {max}")]
    Limit {
        what: String,
        found: usize,
        max: usize,
    },
}

impl PlanError {
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::Unresolved(Unresolved::NotConnected { .. }) | Self::Ambiguous { .. } => {
                ErrorClass::Connectivity
            }
            Self::Unresolved(_) | Self::Unavailable { .. } => ErrorClass::Name,
            Self::Rejected(_) => ErrorClass::Construction,
            Self::Hidden(_)
            | Self::AdminOnly(_)
            | Self::NotFilterable(_)
            | Self::NoSubstring(_) => ErrorClass::Policy,
            Self::Type { .. } | Self::Value { .. } | Self::NoEntity => ErrorClass::Type,
            Self::UnknownColumn { .. } | Self::UnknownBinding { .. } | Self::Duplicate(_) => {
                ErrorClass::Reference
            }
            Self::Shape(_) => ErrorClass::Shape,
            Self::Limit { .. } => ErrorClass::Limit,
        }
    }

    pub fn shape(message: impl Into<String>) -> Self {
        Self::Shape(message.into())
    }

    pub fn limit(what: impl Into<String>, found: usize, max: usize) -> Self {
        Self::Limit {
            what: what.into(),
            found,
            max,
        }
    }
}
