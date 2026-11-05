#[derive(Debug, thiserror::Error)]
pub enum DataModelError {
    #[error("unknown {kind} '{name}' while deriving the query data model")]
    UnknownReference { kind: &'static str, name: String },
    #[error("duplicate {kind} '{name}' while deriving the query data model")]
    Duplicate { kind: &'static str, name: String },
    #[error("invalid query data model: {0}")]
    Invalid(String),
    #[error("invalid model spec:\n{}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"))]
    Spec(Vec<crate::catalog::SpecError>),
}
