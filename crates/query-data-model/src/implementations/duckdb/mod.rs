pub mod backend;
mod ddl;
mod functions;
pub mod render;

use std::sync::Arc;

use super::relational::{Capabilities, CurrentRows, RelationalBackend, Table};
use crate::{Catalog, DataModelError, Model, Packages};

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

impl Catalog<DuckDb> {
    pub const CONNECTION: &'static str = "local";

    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        model.physical(Self::CONNECTION)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }

    /// The tables the connection creates, entity tables before edge tables.
    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        let layouts = || self.stores().flat_map(|(_, store)| &store.tables);
        layouts()
            .filter(|table| table.entity.is_some())
            .chain(layouts().filter(|table| table.entity.is_none()))
    }
}
