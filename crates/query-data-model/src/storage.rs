use crate::{ColumnId, DataModelError, TableId};
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use ontology::{EdgeTableConfig, NodeEntity, StorageColumn};
use std::collections::HashMap;

mod local;
pub use local::{LocalType, local_tables};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredColumnRef {
    pub table: TableId,
    pub column: ColumnId,
}

#[derive(Debug)]
pub struct StorageCatalog {
    tables: Vec<TableLayout>,
    table_ids: HashMap<String, TableId>,
    column_ids: Vec<HashMap<String, ColumnId>>,
}

impl StorageCatalog {
    pub fn new(tables: impl IntoIterator<Item = TableLayout>) -> Result<Self, DataModelError> {
        let mut tables: Vec<_> = tables.into_iter().collect();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        let mut table_ids = HashMap::new();
        let mut column_ids = Vec::new();
        for (index, table) in tables.iter().enumerate() {
            if table_ids
                .insert(table.name.clone(), TableId(index))
                .is_some()
            {
                return Err(DataModelError::Duplicate {
                    kind: "stored table",
                    name: table.name.clone(),
                });
            }
            let mut columns = HashMap::new();
            for (index, column) in table.columns.iter().enumerate() {
                let name = column.name.trim_matches('`');
                if columns.insert(name.to_owned(), ColumnId(index)).is_some() {
                    return Err(DataModelError::Duplicate {
                        kind: "stored column",
                        name: format!("{}.{name}", table.name),
                    });
                }
            }
            column_ids.push(columns);
        }
        Ok(Self {
            tables,
            table_ids,
            column_ids,
        })
    }

    pub fn table_id(&self, name: &str) -> Option<TableId> {
        self.table_ids.get(name).copied()
    }

    pub fn resolve_table(&self, name: &str) -> Result<TableId, DataModelError> {
        self.table_id(name)
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "stored table",
                name: name.to_owned(),
            })
    }

    pub fn table(&self, id: TableId) -> &TableLayout {
        &self.tables[id.index()]
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.tables.iter()
    }

    pub fn column_ref(&self, table: TableId, name: &str) -> Option<StoredColumnRef> {
        self.column_ids[table.index()]
            .get(name)
            .map(|column| StoredColumnRef {
                table,
                column: *column,
            })
    }

    pub fn column(&self, reference: StoredColumnRef) -> &StoredColumn {
        &self.table(reference.table).columns[reference.column.index()]
    }

    pub fn resolve_column(
        &self,
        table: &str,
        column: &str,
    ) -> Result<StoredColumnRef, DataModelError> {
        self.table_id(table)
            .and_then(|table| self.column_ref(table, column))
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "stored column",
                name: format!("{table}.{column}"),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageType {
    ClickHouse(String),
    DuckDb(LocalType),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredColumn {
    pub name: String,
    pub data_type: StorageType,
    pub default: Option<String>,
    pub codec: Option<Vec<String>>,
    pub query_type: Option<ontology::DataType>,
}

impl StoredColumn {
    pub fn clickhouse_type(&self) -> &str {
        match &self.data_type {
            StorageType::ClickHouse(value) => value,
            StorageType::DuckDb(_) => panic!("expected ClickHouse storage"),
        }
    }

    pub fn auxiliary(column: &ontology::AuxiliaryColumn) -> Self {
        let mut data_type = match column.data_type {
            ontology::DataType::String | ontology::DataType::Uuid => "String",
            ontology::DataType::Int => "Int64",
            ontology::DataType::Bool => "Bool",
            ontology::DataType::DateTime => "DateTime64(6, 'UTC')",
            ontology::DataType::Date => "Date32",
            _ => "String",
        }
        .to_owned();
        if column.nullable {
            data_type = format!("Nullable({data_type})");
        }
        Self {
            name: column.name.clone(),
            data_type: StorageType::ClickHouse(data_type),
            default: column.default.clone(),
            codec: column.codec.clone(),
            query_type: Some(column.data_type),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSemantics {
    Current,
    Versioned { engine_deletes: bool },
}

#[derive(Debug, Clone)]
pub struct TableLayout {
    pub name: String,
    pub columns: Vec<StoredColumn>,
    pub sort_key: Vec<ColumnId>,
    pub entity: Option<crate::EntityId>,
    pub path_columns: Vec<crate::PathColumn>,
    pub path_scopable: bool,
    pub row_semantics: RowSemantics,
}

impl TableLayout {
    pub fn new(
        name: impl Into<String>,
        columns: Vec<StoredColumn>,
        sort_key: &[String],
        row_semantics: RowSemantics,
    ) -> Result<Self, DataModelError> {
        let mut table = Self {
            name: name.into(),
            columns,
            sort_key: vec![],
            entity: None,
            path_columns: vec![],
            path_scopable: false,
            row_semantics,
        };
        table.sort_key = sort_key
            .iter()
            .map(|column| table.column_id(column))
            .collect::<Result<_, _>>()?;
        Ok(table)
    }

    pub fn column_id(&self, name: &str) -> Result<ColumnId, DataModelError> {
        self.columns
            .iter()
            .position(|column| column.name.trim_matches('`') == name)
            .map(ColumnId)
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "stored column",
                name: format!("{}.{name}", self.name),
            })
    }

    pub fn sort_columns(&self) -> impl Iterator<Item = &StoredColumn> {
        self.sort_key
            .iter()
            .map(|column| &self.columns[column.index()])
    }

    pub fn add_path_column(
        &mut self,
        name: &str,
        entity: Option<crate::EntityId>,
    ) -> Result<(), DataModelError> {
        self.path_columns.push(crate::PathColumn {
            column: self.column_id(name)?,
            entity,
        });
        Ok(())
    }

    pub fn column(&self, name: &str) -> Option<&StoredColumn> {
        self.columns
            .iter()
            .find(|column| column.name.trim_matches('`') == name)
    }
}

fn stored_columns(
    columns: impl IntoIterator<Item = StorageColumn>,
    query_type: impl Fn(&str) -> Option<ontology::DataType>,
) -> Vec<StoredColumn> {
    columns
        .into_iter()
        .map(|storage| StoredColumn {
            query_type: query_type(storage.name.trim_matches('`')),
            name: storage.name,
            data_type: StorageType::ClickHouse(storage.ch_type),
            default: storage.default,
            codec: storage.codec,
        })
        .collect()
}

pub fn system_columns(version_type: Option<&str>) -> Vec<StoredColumn> {
    let version = match version_type {
        Some("uint64") => StoredColumn {
            name: VERSION_COLUMN.into(),
            data_type: StorageType::ClickHouse("UInt64".into()),
            default: None,
            codec: None,
            query_type: None,
        },
        _ => StoredColumn {
            name: VERSION_COLUMN.into(),
            data_type: StorageType::ClickHouse("DateTime64(6, 'UTC')".into()),
            default: Some("now64(6)".into()),
            codec: Some(vec!["Delta(8)".into(), "ZSTD(1)".into()]),
            query_type: None,
        },
    };
    vec![
        version,
        StoredColumn {
            name: DELETED_COLUMN.into(),
            data_type: StorageType::ClickHouse("Bool".into()),
            default: Some("false".into()),
            codec: None,
            query_type: None,
        },
    ]
}

pub fn remote_node_columns(node: &NodeEntity) -> Vec<StoredColumn> {
    let mut columns = stored_columns(node.storage.columns.iter().cloned(), |name| {
        node.fields
            .iter()
            .find(|field| field.name == name && field.column_name().is_some())
            .map(|field| field.data_type)
    });
    columns.extend(system_columns(None));
    columns
}

pub fn remote_edge_columns(config: &EdgeTableConfig) -> Vec<StoredColumn> {
    let mut columns = stored_columns(
        config
            .storage
            .columns
            .iter()
            .chain(&config.storage.denormalized_columns)
            .cloned(),
        |name| {
            config
                .columns
                .iter()
                .find(|column| column.name.trim_matches('`') == name)
                .map(|column| column.data_type)
        },
    );
    columns.extend(system_columns(None));
    columns
}

pub fn denormalized_columns<'a>(
    join: &'a ontology::denormalized::DenormalizedJoin,
    source: impl Fn(usize) -> &'a [StoredColumn],
) -> Vec<StoredColumn> {
    let anchor = join.anchor_table();
    let mut columns: Vec<_> = source(anchor)
        .iter()
        .filter(|column| column.name == ontology::TRAVERSAL_PATH_COLUMN)
        .cloned()
        .collect();
    for index in 0..join.tables.len() {
        columns.extend(
            source(index)
                .iter()
                .filter(|column| {
                    ontology::denormalized::copies(&column.name)
                        && !(index == anchor && column.name == ontology::TRAVERSAL_PATH_COLUMN)
                })
                .map(|column| {
                    let mut column = column.clone();
                    column.name = join.column_for(index, &column.name);
                    column
                }),
        );
    }
    columns.extend(system_columns(None));
    columns
}
