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
                | Self::AllowlistRejected(_)
                | Self::DepthExceeded(_)
                | Self::LimitExceeded(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Exhaustive test for is_client_safe. If a new QueryError variant is
    /// added, this test will fail to compile until the new variant is
    /// explicitly handled here, forcing a decision about whether it's safe.
    #[test]
    fn is_client_safe_covers_all_variants() {
        let cases: Vec<(QueryError, bool)> = vec![
            (
                QueryError::Parse(serde_json::from_str::<()>("!").unwrap_err()),
                true,
            ),
            (QueryError::Validation("bad".into()), true),
            (QueryError::ReferenceError("bad".into()), true),
            (QueryError::PaginationError("bad".into()), true),
            (QueryError::DepthExceeded("bad".into()), true),
            (QueryError::LimitExceeded("bad".into()), true),
            (QueryError::AllowlistRejected("bad".into()), true),
            (QueryError::Lowering("bad".into()), false),
            (QueryError::Enforcement("bad".into()), false),
            (QueryError::Codegen("bad".into()), false),
            (QueryError::Security("bad".into()), false),
            (QueryError::PipelineInvariant("bad".into()), false),
            (
                QueryError::Ontology(OntologyError::Validation("bad".into())),
                false,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(
                error.is_client_safe(),
                expected,
                "{error} should be client_safe={expected}"
            );
        }
    }
}
