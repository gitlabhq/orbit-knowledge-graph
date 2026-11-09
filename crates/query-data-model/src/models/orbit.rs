use std::sync::Arc;

use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::duckdb::DuckDb;
use crate::{DataModelError, GraphCatalog, Model, Packages, QueryModel};

pub type ClickHouseDataModel = QueryModel<ClickHouse>;
pub type DuckDbDataModel = QueryModel<DuckDb>;

impl ClickHouseDataModel {
    pub const CONNECTION: &'static str = "main";

    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        Self::new(model, Self::CONNECTION)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }

    pub fn versioned(packages: &Packages, table_prefix: &str) -> Result<Self, DataModelError> {
        let fragments =
            crate::implementations::clickhouse::versioned(packages.fragments()?, table_prefix)?;
        Self::from_model(&Arc::new(Model::load(&fragments)?))
    }
}

impl DuckDbDataModel {
    pub const CONNECTION: &'static str = "local";

    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        Self::new(model, Self::CONNECTION)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }
}

pub trait OrbitQueryModel {
    fn graph(&self) -> &GraphCatalog;
}
