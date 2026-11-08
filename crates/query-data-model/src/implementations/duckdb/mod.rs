pub mod backend;
mod functions;
pub mod render;

use super::relational::{Capabilities, CurrentRows, RelationalBackend};

#[derive(Debug, Clone)]
pub struct DuckDb;

impl RelationalBackend for DuckDb {
    const CAPABILITIES: Capabilities = Capabilities {
        current_rows: CurrentRows::Stored,
        limit_by: false,
        settings: false,
        materialized_bodies: false,
        token_search: false,
        deferred_nodes: false,
    };
}
