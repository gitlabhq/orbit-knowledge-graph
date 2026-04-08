//! Error types for the query engine.
//!
//! Each variant maps to at most one threat-model counter in [`crate::metrics`].
//! Adding a new variant that represents a security-relevant rejection? Update
//! [`crate::metrics::threat_counter`] to wire it to the right instrument.

use ontology::OntologyError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("schema violation: {0}")]
    Validation(String),

    #[error("reference error: {0}")]
    ReferenceError(String),

    #[error("pagination error: {0}")]
    PaginationError(String),

    #[error("allowlist rejected: {0}")]
    AllowlistRejected(String),

    #[error("depth exceeded: {0}")]
    DepthExceeded(String),

    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    #[error("lowering error: {0}")]
    Lowering(String),

    #[error("enforce error: {0}")]
    Enforcement(String),

    #[error("codegen error: {0}")]
    Codegen(String),

    #[error("security error: {0}")]
    Security(String),

    #[error("pipeline invariant violated: {0}")]
    PipelineInvariant(String),

    #[error("ontology error: {0}")]
    Ontology(#[from] OntologyError),
}

impl QueryError {
    /// Whether this error only contains user-input validation details
    /// and is safe to include verbatim in client-facing responses.
    pub fn is_client_safe(&self) -> bool {
        matches!(
            self,
            Self::Parse(_)
                | Self::Validation(_)
                | Self::ReferenceError(_)
                | Self::PaginationError(_)
                | Self::DepthExceeded(_)
                | Self::LimitExceeded(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;
