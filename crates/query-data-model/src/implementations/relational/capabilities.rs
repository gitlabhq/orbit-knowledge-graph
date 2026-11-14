use super::RelationalFacts;
use crate::catalog::StorageBackend;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentRows {
    Final,
    Stored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlCapabilities {
    pub current_rows: CurrentRows,
    pub limit_by: bool,
    pub settings: bool,
    pub materialized_bodies: bool,
    pub token_search: bool,
    /// Node properties may load in a later hydration query instead of joining the base read.
    pub deferred_nodes: bool,
}

pub trait RelationalBackend: StorageBackend<Store: RelationalFacts> {
    const SQL: SqlCapabilities;
}
