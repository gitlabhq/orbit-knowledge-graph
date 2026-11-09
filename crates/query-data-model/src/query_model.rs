use std::sync::Arc;

use crate::catalog::{Catalog, Model, StorageBackend};
use crate::implementations::relational::RelationalFacts;
use crate::{DataModelError, GraphCatalog, OrbitQueryModel};

#[derive(Debug)]
pub struct QueryModel<B: StorageBackend>
where
    B::Store: RelationalFacts,
{
    catalog: Catalog<B>,
}

impl<B: StorageBackend> QueryModel<B>
where
    B::Store: RelationalFacts,
{
    pub fn new(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
        Ok(Self {
            catalog: model.physical::<B>(connection)?,
        })
    }

    pub fn catalog(&self) -> &Catalog<B> {
        &self.catalog
    }
}

impl<B: StorageBackend> OrbitQueryModel for QueryModel<B>
where
    B::Store: RelationalFacts,
{
    fn graph(&self) -> &GraphCatalog {
        self.catalog.graph()
    }
}
