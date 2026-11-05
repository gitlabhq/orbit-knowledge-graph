mod engine;
mod joins;
mod render;
mod resolve;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::ClickHouse;
use crate::{Catalog, ClickHouseDataModel, DataModelError, Model};

pub use render::DictionaryCredentials;
pub use resolve::default_index_name;
pub(super) use resolve::{check_indexes, resolve};
pub use types::{data_type, storage_type};

impl Catalog<ClickHouse> {
    pub fn schema(&self) -> Result<&Schema, DataModelError> {
        self.stores()
            .find_map(|(_, store)| store.schema())
            .ok_or_else(|| {
                DataModelError::Invalid(format!("connection {} has no schema", self.connection()))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub projections: Vec<Projection>,
    pub engine: Engine,
    pub order_by: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub settings: Vec<(String, String)>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub storage_type: String,
    pub default: Option<String>,
    pub codecs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub name: String,
    pub column: String,
    pub lowercase: bool,
    pub kind: String,
    pub granularity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    Reorder {
        name: String,
        order_by: Vec<String>,
    },
    Lightweight {
        name: String,
        order_by: Vec<String>,
    },
    Aggregate {
        name: String,
        select: Vec<String>,
        group_by: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Engine {
    pub name: String,
    pub arguments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryTable {
    pub table: Table,
    pub versioned: bool,
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
        model
            .physical::<ClickHouse>(ClickHouseDataModel::CONNECTION)?
            .schema()
            .cloned()
    }

    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|table| table.name == name)
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
