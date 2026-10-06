use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TABLE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId {
    table: TableId,
    slot: usize,
}

#[derive(Debug)]
pub struct StoredColumn<T = ontology::DataType> {
    pub name: String,
    pub data_type: Option<T>,
    pub array: bool,
}

#[derive(Debug)]
pub struct StoredTable<T = ontology::DataType> {
    id: TableId,
    name: String,
    columns: Vec<StoredColumn<T>>,
    names: HashMap<String, usize>,
}

impl<T> StoredTable<T> {
    pub fn new(name: String, columns: impl IntoIterator<Item = StoredColumn<T>>) -> Self {
        let mut columns = columns.into_iter().collect::<Vec<_>>();
        columns.sort_by(|left, right| left.name.cmp(&right.name));
        let names = columns
            .iter()
            .enumerate()
            .map(|(slot, column)| (column.name.clone(), slot))
            .collect::<HashMap<_, _>>();
        assert_eq!(names.len(), columns.len(), "duplicate stored column");
        Self {
            id: TableId(NEXT_TABLE.fetch_add(1, Ordering::Relaxed)),
            name,
            columns,
            names,
        }
    }

    pub fn reference(&self) -> StoredTableRef<'_, T> {
        StoredTableRef(self)
    }
}

#[derive(Debug)]
pub struct StoredTableRef<'a, T = ontology::DataType>(&'a StoredTable<T>);

impl<T> Copy for StoredTableRef<'_, T> {}
impl<T> Clone for StoredTableRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> PartialEq for StoredTableRef<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl<T> Eq for StoredTableRef<'_, T> {}

impl<'a, T> StoredTableRef<'a, T> {
    pub fn id(self) -> TableId {
        self.0.id
    }
    pub fn name(self) -> &'a str {
        &self.0.name
    }
    pub fn column(self, name: &str) -> Option<StoredColumnRef<'a, T>> {
        self.0.names.get(name).map(|slot| StoredColumnRef {
            table: self,
            id: ColumnId {
                table: self.id(),
                slot: *slot,
            },
        })
    }
    pub fn resolve(self, id: ColumnId) -> Option<StoredColumnRef<'a, T>> {
        (id.table == self.id() && id.slot < self.0.columns.len())
            .then_some(StoredColumnRef { table: self, id })
    }
    pub fn columns(self) -> impl Iterator<Item = StoredColumnRef<'a, T>> {
        (0..self.0.columns.len()).map(move |slot| StoredColumnRef {
            table: self,
            id: ColumnId {
                table: self.id(),
                slot,
            },
        })
    }
}

#[derive(Debug)]
pub struct StoredColumnRef<'a, T = ontology::DataType> {
    table: StoredTableRef<'a, T>,
    id: ColumnId,
}

impl<T> Copy for StoredColumnRef<'_, T> {}
impl<T> Clone for StoredColumnRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> PartialEq for StoredColumnRef<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<T> Eq for StoredColumnRef<'_, T> {}

impl<'a, T> StoredColumnRef<'a, T> {
    pub fn id(self) -> ColumnId {
        self.id
    }
    pub fn table(self) -> StoredTableRef<'a, T> {
        self.table
    }
    pub fn name(self) -> &'a str {
        &self.table.0.columns[self.id.slot].name
    }
    pub fn data_type(self) -> Option<&'a T> {
        self.table.0.columns[self.id.slot].data_type.as_ref()
    }
    pub fn ordinal(self) -> usize {
        self.id.slot
    }

    pub fn is_array(self) -> bool {
        self.table.0.columns[self.id.slot].array
    }
}
