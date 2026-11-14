pub mod backend;
pub mod facts;
pub mod render;
pub mod schema;
pub mod spec;
mod versioned;

pub use versioned::versioned;

use std::sync::Arc;

use super::relational::{CurrentRows, RelationalBackend, SqlCapabilities};
use facts::{DenormalizedKey, DenormalizedProperty};

use crate::{Catalog, DataModelError, Model, Packages, RelationshipId};

#[derive(Debug, Clone)]
pub struct ClickHouse;

impl RelationalBackend for ClickHouse {
    const SQL: SqlCapabilities = SqlCapabilities {
        current_rows: CurrentRows::Final,
        limit_by: true,
        settings: true,
        materialized_bodies: true,
        token_search: true,
        deferred_nodes: true,
    };
}

impl Catalog<ClickHouse> {
    /// The catalog of `packages` as a deployment of the shape `shape` reads them, with every
    /// table under `table_prefix`.
    pub fn versioned(
        packages: &Packages,
        shape: &str,
        table_prefix: &str,
    ) -> Result<Self, DataModelError> {
        let fragments = packages
            .fragments()?
            .deployment(&crate::Deployments::embedded(), shape)?;
        let fragments = versioned(fragments, table_prefix)?;
        Self::from_model(&Arc::new(Model::load(&fragments)?))
    }

    pub fn denormalized_properties(
        &self,
    ) -> impl Iterator<Item = (RelationshipId, DenormalizedKey, &DenormalizedProperty)> {
        self.stores()
            .flat_map(|(_, store)| store.read_paths.denormalized.declared())
    }
}
