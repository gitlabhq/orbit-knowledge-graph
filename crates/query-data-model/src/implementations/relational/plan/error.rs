use crate::plan::tracking::Rejected;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RelationalError {
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("unknown column {name}; available: {}", .available.join(", "))]
    UnknownColumn {
        name: String,
        available: Vec<String>,
    },
    #[error("column {0} is ambiguous; qualify it with its relation")]
    Ambiguous(String),
    #[error("this backend does not support {0}")]
    Unsupported(&'static str),
    #[error("{0} cannot follow the rows' last clause; read them through a subquery")]
    Misplaced(&'static str),
    #[error("{0} needs a complete query; select its columns first")]
    Incomplete(&'static str),
    #[error("{function} does not accept {count} arguments")]
    Arity { function: String, count: usize },
    #[error("variable {0} is not bound by a lambda")]
    Unbound(String),
    #[error("expected {expected} columns, found {found}")]
    Width { expected: usize, found: usize },
    #[error("computed column {0} is not selected")]
    Unselected(String),
    #[error("{0} is not the version column of a versioned store")]
    NotVersion(String),
}
