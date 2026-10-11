use std::collections::BTreeMap;
pub mod mapping;
pub use mapping::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationalMapping, RelationshipRoute,
    TraversalPathLookup,
};
use std::fmt::Debug;

pub trait RelationalBackend: Debug + Clone {
    type StorageType: Debug + Clone;
    type TableOptions: Debug + Clone;
    type ColumnOptions: Debug + Clone;
    type Metadata: Debug;
}

#[derive(Debug)]
pub struct Relational<B: RelationalBackend> {
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

impl<B: RelationalBackend> Relational<B> {
    pub fn tables(&self) -> &[Table<B>] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&Table<B>> {
        self.tables.iter().find(|table| table.name == name)
    }
}
