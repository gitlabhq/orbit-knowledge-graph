use super::RelationalFacts;
use crate::catalog::StorageBackend;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentRows {
    Final,
    Stored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    pub current_rows: CurrentRows,
    pub limit_by: bool,
    pub settings: bool,
    pub materialized_bodies: bool,
    pub token_search: bool,
}

pub trait RelationalBackend: StorageBackend<Store: RelationalFacts> {
    const CAPABILITIES: Capabilities;
}
