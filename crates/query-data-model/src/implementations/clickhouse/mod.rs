pub mod backend;
pub mod facts;
pub mod render;
pub mod schema;
pub mod spec;
mod versioned;

pub use versioned::versioned;

use super::relational::{Capabilities, CurrentRows, RelationalBackend};

#[derive(Debug, Clone)]
pub struct ClickHouse;

impl RelationalBackend for ClickHouse {
    const CAPABILITIES: Capabilities = Capabilities {
        current_rows: CurrentRows::Final,
        limit_by: true,
        settings: true,
        materialized_bodies: true,
        token_search: true,
        deferred_nodes: true,
    };
}
