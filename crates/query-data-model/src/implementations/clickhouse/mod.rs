pub mod backend;
pub mod facts;
pub mod schema;
pub mod spec;

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
    };
}
