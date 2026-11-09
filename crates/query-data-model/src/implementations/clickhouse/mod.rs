pub mod backend;
pub mod facts;
pub mod render;
pub mod schema;
pub mod spec;
mod versioned;

pub use versioned::versioned;

use std::sync::Arc;

use super::relational::{Capabilities, CurrentRows, RelationalBackend};
use facts::{DenormalizedKey, DenormalizedProperty};

use crate::{Catalog, DataModelError, Model, Packages, RelationshipId};

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

impl Catalog<ClickHouse> {
    pub const CONNECTION: &'static str = "main";

    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        model.physical(Self::CONNECTION)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }

    pub fn versioned(packages: &Packages, table_prefix: &str) -> Result<Self, DataModelError> {
        let fragments = versioned(packages.fragments()?, table_prefix)?;
        Self::from_model(&Arc::new(Model::load(&fragments)?))
    }

    pub fn denormalized_properties(
        &self,
    ) -> impl Iterator<Item = (RelationshipId, DenormalizedKey, &DenormalizedProperty)> {
        self.stores()
            .flat_map(|(_, store)| store.denormalized.declared())
    }
}
