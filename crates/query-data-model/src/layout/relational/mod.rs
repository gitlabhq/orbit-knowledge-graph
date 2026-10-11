use std::collections::BTreeMap;
pub mod mapping;
pub use mapping::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationalMapping, RelationshipRoute,
    TraversalPathLookup,
};
use std::fmt::Debug;
use std::marker::PhantomData;

use super::{Layout, LayoutModel};

pub trait RelationalBackend: Debug + Clone {
    type StorageType: Debug + Clone;
    type TableOptions: Debug + Clone;
    type ColumnOptions: Debug + Clone;
    type Metadata: Debug;
    type Mapping: Debug;
}

#[derive(Debug)]
pub struct Relational<B: RelationalBackend>(PhantomData<B>);

impl<B: RelationalBackend> LayoutModel for Relational<B> {
    type Schema = Schema<B>;
    type Mapping = B::Mapping;
}

#[derive(Debug)]
pub struct Schema<B: RelationalBackend> {
    pub tables: Vec<Table<B>>,
    pub metadata: B::Metadata,
}

#[derive(Debug, Clone)]
pub struct Table<B: RelationalBackend> {
    pub name: String,
    pub columns: Vec<Column<B>>,
    pub column_types: BTreeMap<String, ontology::DataType>,
    pub sort_key: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub options: B::TableOptions,
}

#[derive(Debug, Clone)]
pub struct Column<B: RelationalBackend> {
    pub name: String,
    pub storage_type: B::StorageType,
    pub default: Option<String>,
    pub options: B::ColumnOptions,
}

impl<B: RelationalBackend> Layout<Relational<B>> {
    pub fn tables(&self) -> &[Table<B>] {
        self.schema.tables()
    }

    pub fn table(&self, name: &str) -> Option<&Table<B>> {
        self.schema.table(name)
    }
}

impl<B: RelationalBackend> Schema<B> {
    pub fn tables(&self) -> &[Table<B>] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&Table<B>> {
        self.tables.iter().find(|table| table.name == name)
    }
}
