mod edges;
mod engine;
mod entities;
mod joins;
mod layout;
mod render;
mod resolve;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::ClickHouse;
pub use super::spec::EngineTable;
use crate::{Catalog, DataModelError, Model};

pub use crate::implementations::relational::{
    Column, Engine, Index, PathColumn, Projection, Table,
};
pub(super) use layout::check_indexes;
pub use layout::default_index_name;
pub use render::DictionaryCredentials;
pub(super) use resolve::resolve;
pub use types::{data_type, nullable};

impl Catalog<ClickHouse> {
    pub fn schema(&self) -> Result<&Schema, DataModelError> {
        self.stores()
            .find_map(|(_, store)| store.schema())
            .ok_or_else(|| {
                DataModelError::Invalid(format!("connection {} has no schema", self.connection()))
            })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    tables: Vec<Table>,
    auxiliary: Vec<AuxiliaryTable>,
    dictionaries: Vec<Dictionary>,
    views: Vec<MaterializedView>,
    refreshable_views: Vec<RefreshableView>,
    graph_tables: Vec<GraphTable>,
    joins: Vec<String>,
    dependencies: BTreeMap<String, BTreeSet<String>>,
    writers: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryTable {
    pub table: Table,
    pub versioned: bool,
    pub role: Option<EngineTable>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dictionary {
    pub name: String,
    pub source_table: String,
    pub key: String,
    pub attributes: Vec<Column>,
    pub layout: String,
    pub size_in_cells: Option<u64>,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
    pub version_column: String,
    pub deleted_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedView {
    pub name: String,
    pub versioned: bool,
    pub to_table: Option<String>,
    pub select_query: String,
    pub engine: Option<Engine>,
    pub order_by: Vec<String>,
    pub populate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshableView {
    pub name: String,
    pub versioned: bool,
    pub select_query: String,
    pub append_to: String,
    pub refresh: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTable {
    pub name: String,
    pub global: bool,
    pub has_path_column: bool,
}

impl Schema {
    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        Catalog::<ClickHouse>::from_model(model)?.schema().cloned()
    }

    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|table| table.name == name)
    }

    /// The unprefixed name of the auxiliary table declared for `role`.
    pub fn engine_table(&self, role: EngineTable) -> Result<&str, DataModelError> {
        self.auxiliary
            .iter()
            .find(|table| table.role == Some(role))
            .map(|table| table.table.name.as_str())
            .ok_or_else(|| {
                DataModelError::Invalid(format!("no auxiliary table has the role {role}"))
            })
    }

    pub fn auxiliary_tables(&self) -> &[AuxiliaryTable] {
        &self.auxiliary
    }

    pub fn versioned_tables(&self) -> impl Iterator<Item = &Table> {
        self.auxiliary
            .iter()
            .filter(|table| table.versioned)
            .map(|table| &table.table)
            .chain(&self.tables)
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.auxiliary
            .iter()
            .map(|table| &table.table)
            .chain(&self.tables)
            .map(|table| table.name.as_str())
    }

    pub fn dictionaries(&self) -> &[Dictionary] {
        &self.dictionaries
    }

    pub fn views(&self) -> &[MaterializedView] {
        &self.views
    }

    pub fn refreshable_views(&self) -> &[RefreshableView] {
        &self.refreshable_views
    }

    pub fn graph_tables(&self) -> &[GraphTable] {
        &self.graph_tables
    }

    pub fn join_sources(&self) -> impl Iterator<Item = &str> {
        self.joins
            .iter()
            .flat_map(|join| &self.dependencies[join])
            .map(String::as_str)
    }

    pub fn dependencies(&self) -> &BTreeMap<String, BTreeSet<String>> {
        &self.dependencies
    }

    pub fn writers(&self, table: &str) -> Option<&BTreeSet<String>> {
        self.writers.get(table)
    }
}
