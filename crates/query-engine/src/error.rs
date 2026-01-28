//! Error types for the query engine

use ontology::OntologyError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("lowering error: {0}")]
    Lowering(String),

    #[error("codegen error: {0}")]
    Codegen(String),

    #[error("security error: {0}")]
    Security(String),

    #[error("ontology error: {0}")]
    Ontology(#[from] OntologyError),
}

pub type Result<T> = std::result::Result<T, QueryError>;
