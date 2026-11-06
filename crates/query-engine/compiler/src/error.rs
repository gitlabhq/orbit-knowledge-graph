//! Error types for the query engine.
//!
//! Each variant maps to at most one threat-model counter in [`crate::metrics`].
//! Adding a new variant that represents a security-relevant rejection? Update
//! [`crate::metrics::failure_reason`] to wire it to the right instrument.

use ontology::OntologyError;
use query_data_model::plan::logical::{ErrorClass, PlanError};
use query_data_model::surface::SurfaceError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("schema violation: {0}")]
    Validation(String),

    #[error("reference error: {0}")]
    ReferenceError(String),

    #[error("allowlist rejected: {0}")]
    AllowlistRejected(String),

    #[error("authorization error: {0}")]
    Authorization(String),

    #[error("restriction error: {0}")]
    Restrict(String),

    #[error("depth exceeded: {0}")]
    DepthExceeded(String),

    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    #[error("lowering error: {0}")]
    Lowering(String),

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
                | Self::AllowlistRejected(_)
                | Self::Authorization(_)
                | Self::Restrict(_)
                | Self::DepthExceeded(_)
                | Self::LimitExceeded(_)
        )
    }

    pub fn rejection_reason(&self) -> RejectionReason {
        match self {
            Self::Parse(_) => RejectionReason::Parse,
            Self::Validation(_) => RejectionReason::Schema,
            Self::ReferenceError(_) => RejectionReason::Reference,
            Self::AllowlistRejected(_) => RejectionReason::Ontology,
            Self::Authorization(_) => RejectionReason::Authorization,
            Self::Restrict(_) => RejectionReason::Restrict,
            Self::Ontology(_) => RejectionReason::OntologyInternal,
            Self::DepthExceeded(_) => RejectionReason::Depth,
            Self::LimitExceeded(_) => RejectionReason::Limit,
            Self::Security(_) => RejectionReason::Security,
            Self::Lowering(_) => RejectionReason::Lowering,
            Self::Codegen(_) => RejectionReason::Codegen,
            Self::PipelineInvariant(_) => RejectionReason::Pipeline,
        }
    }
}

impl From<SurfaceError> for QueryError {
    fn from(error: SurfaceError) -> Self {
        let message = error.to_string();
        match error {
            SurfaceError::Syntax(message) => Self::Parse(message),
            SurfaceError::Schema(message) => Self::Validation(message),
            SurfaceError::Limit(message) => Self::LimitExceeded(message),
            SurfaceError::Plan(PlanError::AdminOnly(_)) => Self::Restrict(message),
            SurfaceError::Plan(PlanError::Hidden(_)) => Self::AllowlistRejected(message),
            SurfaceError::Plan(error) => match error.class() {
                ErrorClass::Name => Self::AllowlistRejected(message),
                ErrorClass::Reference => Self::ReferenceError(message),
                ErrorClass::Construction => Self::PipelineInvariant(message),
                _ => Self::Validation(message),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum RejectionReason {
    Parse,
    Schema,
    Reference,
    Ontology,
    Authorization,
    Restrict,
    OntologyInternal,
    Depth,
    Limit,
    Security,
    Lowering,
    Codegen,
    Pipeline,
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
            (QueryError::Parse("bad".into()), true),
            (QueryError::Validation("bad".into()), true),
            (QueryError::ReferenceError("bad".into()), true),
            (QueryError::DepthExceeded("bad".into()), true),
            (QueryError::LimitExceeded("bad".into()), true),
            (QueryError::AllowlistRejected("bad".into()), true),
            (QueryError::Authorization("bad".into()), true),
            (QueryError::Restrict("bad".into()), true),
            (QueryError::Lowering("bad".into()), false),
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
