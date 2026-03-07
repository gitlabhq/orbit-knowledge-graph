//! JSON DSL frontend — parses the query-engine's JSON query format into an IR plan.
//!
//! This frontend will replace the bespoke AST/codegen path in `query-engine`.
//! The JSON DSL spec lives in `docs/design-documents/querying/`.
//!
//! TODO: implement once query-engine migration is in progress.

use crate::ir::plan::Plan;

/// JSON DSL frontend configuration.
pub struct JsonDslFrontend;

#[derive(Debug, thiserror::Error)]
pub enum JsonDslError {
    #[error("JSON DSL frontend not yet implemented")]
    NotImplemented,
}

impl super::Frontend for JsonDslFrontend {
    type Input = serde_json::Value;
    type Error = JsonDslError;

    fn lower(&self, _input: Self::Input) -> Result<Plan, Self::Error> {
        Err(JsonDslError::NotImplemented)
    }
}
