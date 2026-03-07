//! ETL config frontend — parses ontology YAML ETL definitions into IR plans.
//!
//! This frontend will replace the bespoke SQL generation in the indexer.
//! ETL config is driven by YAML in `fixtures/ontology/nodes/`.
//!
//! TODO: implement once indexer migration is in progress.

use crate::ir::plan::Plan;

/// ETL config frontend.
pub struct EtlFrontend;

#[derive(Debug, thiserror::Error)]
pub enum EtlError {
    #[error("ETL frontend not yet implemented")]
    NotImplemented,
}

impl super::Frontend for EtlFrontend {
    type Input = serde_json::Value;
    type Error = EtlError;

    fn lower(&self, _input: Self::Input) -> Result<Plan, Self::Error> {
        Err(EtlError::NotImplemented)
    }
}
