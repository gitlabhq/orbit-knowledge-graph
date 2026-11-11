use crate::catalog::{Source, Store};
use crate::{DataType, EntityId, PropertyId};

/// A table a store holds: the columns plans read and, for a table the store creates, its DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub entity: Option<EntityId>,
    pub columns: Vec<Column>,
    pub sort_key: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: Vec<Index>,
    pub projections: Vec<Projection>,
    pub engine: Option<Engine>,
    pub settings: Vec<(String, String)>,
    pub ttl: Option<String>,
    pub path_columns: Vec<PathColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub property: Option<PropertyId>,
    pub data_type: Option<DataType>,
    /// The backend's type; empty on a table the store does not create.
    pub storage_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub codecs: Vec<String>,
    pub null_as_epoch: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathColumn {
    pub name: String,
    pub entity: Option<EntityId>,
    /// The table whose rows the column scopes: the entity's own, or an edge table.
    pub table: String,
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

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            entity: None,
            columns,
            sort_key: Vec::new(),
            primary_key: None,
            indexes: Vec::new(),
            projections: Vec::new(),
            engine: None,
            settings: Vec::new(),
            ttl: None,
            path_columns: Vec::new(),
        }
    }

    /// The table that stores an entity, filtered by the source's path column.
    pub fn of_source(
        source: &Source,
        table: &str,
        sort_key: Vec<String>,
        columns: Vec<Column>,
    ) -> Self {
        let path_columns = source.path_column.iter().map(|name| PathColumn {
            name: name.clone(),
            entity: Some(source.entity),
            table: table.to_string(),
        });
        Self {
            entity: Some(source.entity),
            sort_key,
            path_columns: path_columns.collect(),
            ..Self::new(table, columns)
        }
    }

    /// An edge table, filtered by its store's path column.
    pub fn edge(name: &str, store: &Store, sort_key: Vec<String>, columns: Vec<Column>) -> Self {
        let path_columns = store.path_column.iter().map(|column| PathColumn {
            name: column.clone(),
            entity: None,
            table: name.to_string(),
        });
        Self {
            sort_key,
            path_columns: path_columns.collect(),
            ..Self::new(name, columns)
        }
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.column(name).is_some()
    }
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: Option<DataType>) -> Self {
        Self {
            name: name.into(),
            property: None,
            data_type,
            storage_type: String::new(),
            nullable: false,
            default: None,
            codecs: Vec::new(),
            null_as_epoch: false,
        }
    }
}
