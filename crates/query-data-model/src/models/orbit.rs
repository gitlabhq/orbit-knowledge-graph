use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::duckdb::DuckDb;
use crate::{
    DataModelError, DenormalizedCatalog, EntityId, GraphCatalog, MaterializedJoin, Model, Packages,
    PropertyId, QueryModel,
};

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

#[derive(Debug, Clone)]
pub enum PropertyRealization {
    Stored { column: String },
    Virtual(ontology::VirtualSource),
}

pub trait OrbitQueryModel {
    fn graph(&self) -> &GraphCatalog;
    fn null_as_epoch(&self, property: PropertyId) -> bool;
    fn null_as_epoch_properties(&self, entity: EntityId) -> HashMap<String, ontology::DataType>;
    fn high_fan_out(&self, entity: EntityId) -> bool;
    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType>;
    fn table_columns(&self, table: &str) -> Option<&HashSet<String>>;
    fn table_sort_key(&self, table: &str) -> Option<&[String]>;
    fn equivalent_layouts(&self, table: &str) -> Vec<&str>;
    fn in_sort_key(&self, table: &str, column: &str) -> bool;
    fn has_text_index(&self, property: PropertyId) -> bool;
    fn denormalized(&self) -> &DenormalizedCatalog;
    fn traversal_path_lookup(
        &self,
        entity: &str,
        kind: ontology::TraversalPathKind,
    ) -> Option<(&str, &str)>;
    fn property_is_foreign_key(&self, property: PropertyId) -> bool;
    fn materialized_joins(&self) -> &[MaterializedJoin];
}
