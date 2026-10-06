use std::collections::HashMap;

use crate::{ColumnId, DataModelError, TableId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoredColumnRef {
    pub table: TableId,
    pub column: ColumnId,
}

#[derive(Debug)]
pub struct StorageCatalog<T> {
    tables: Vec<TableLayout<T>>,
    table_ids: HashMap<String, TableId>,
}

impl<T> StorageCatalog<T> {
    pub fn new(tables: impl IntoIterator<Item = TableLayout<T>>) -> Result<Self, DataModelError> {
        let mut tables: Vec<_> = tables.into_iter().collect();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        let mut table_ids = HashMap::new();
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
        }
        Ok(Self { tables, table_ids })
    }

    pub fn table_id(&self, name: &str) -> Option<TableId> {
        self.table_ids.get(name).copied()
    }

    pub fn resolve_table(&self, name: &str) -> Result<TableId, DataModelError> {
        self.table_id(name)
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "stored table",
                name: name.into(),
            })
    }

    pub fn table(&self, id: TableId) -> &TableLayout<T> {
        &self.tables[id.index()]
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout<T>> {
        self.tables.iter()
    }

    pub fn column_ref(&self, table: TableId, name: &str) -> Option<StoredColumnRef> {
        self.table(table)
            .column_ids
            .get(name)
            .map(|column| StoredColumnRef {
                table,
                column: *column,
            })
    }

    pub fn column(&self, reference: StoredColumnRef) -> &StoredColumn<T> {
        self.table(reference.table).column_by_id(reference.column)
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
pub struct StoredColumn<T> {
    name: String,
    storage: T,
}

impl<T> StoredColumn<T> {
    pub fn new(name: impl Into<String>, storage: T) -> Self {
        Self {
            name: name.into(),
            storage,
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn storage(&self) -> &T {
        &self.storage
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSemantics {
    Current,
    Versioned {
        version: ColumnId,
        deletion: Option<Deletion>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deletion {
    pub column: ColumnId,
    pub applied_on_merge: bool,
}

#[derive(Debug, Clone)]
pub struct TableLayout<T> {
    name: String,
    columns: Vec<StoredColumn<T>>,
    column_ids: HashMap<String, ColumnId>,
    sort_key: Vec<ColumnId>,
    row_semantics: RowSemantics,
}

impl<T> TableLayout<T> {
    pub fn new(
        name: impl Into<String>,
        columns: Vec<StoredColumn<T>>,
        sort_key: &[String],
    ) -> Result<Self, DataModelError> {
        let name = name.into();
        let mut column_ids = HashMap::new();
        for (index, column) in columns.iter().enumerate() {
            if column_ids
                .insert(column.name.clone(), ColumnId(index))
                .is_some()
            {
                return Err(DataModelError::Duplicate {
                    kind: "stored column",
                    name: format!("{name}.{}", column.name),
                });
            }
        }
        let mut table = Self {
            name,
            columns,
            column_ids,
            sort_key: vec![],
            row_semantics: RowSemantics::Current,
        };
        table.sort_key = sort_key
            .iter()
            .map(|name| table.column_id(name))
            .collect::<Result<_, _>>()?;
        Ok(table)
    }

    pub fn versioned(
        mut self,
        version: &str,
        deletion: Option<(&str, bool)>,
    ) -> Result<Self, DataModelError> {
        self.row_semantics = RowSemantics::Versioned {
            version: self.column_id(version)?,
            deletion: deletion
                .map(|(name, applied_on_merge)| {
                    self.column_id(name).map(|column| Deletion {
                        column,
                        applied_on_merge,
                    })
                })
                .transpose()?,
        };
        Ok(self)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn columns(&self) -> &[StoredColumn<T>] {
        &self.columns
    }
    pub fn sort_key(&self) -> &[ColumnId] {
        &self.sort_key
    }
    pub fn row_semantics(&self) -> &RowSemantics {
        &self.row_semantics
    }
    pub fn column_by_id(&self, id: ColumnId) -> &StoredColumn<T> {
        &self.columns[id.index()]
    }

    pub fn column_id(&self, name: &str) -> Result<ColumnId, DataModelError> {
        self.column_ids
            .get(name)
            .copied()
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "stored column",
                name: format!("{}.{name}", self.name),
            })
    }

    pub fn column(&self, name: &str) -> Option<&StoredColumn<T>> {
        self.column_ids
            .get(name)
            .map(|column| self.column_by_id(*column))
    }

    pub fn sort_columns(&self) -> impl Iterator<Item = &StoredColumn<T>> {
        self.sort_key
            .iter()
            .map(|column| self.column_by_id(*column))
    }
}
