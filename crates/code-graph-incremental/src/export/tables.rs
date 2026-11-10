//! The local tables an export writes, read from the model's local store.

use query_data_model::{Catalog, DataType, DuckDb};

use crate::error::LoadError;

/// A local entity and the columns its table stores, in property order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityTable {
    pub entity: String,
    pub table: String,
    pub columns: Vec<(String, DataType, bool)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportTables {
    pub entities: Vec<EntityTable>,
    pub edge_table: String,
    pub edge_columns: Vec<(String, DataType)>,
}

impl ExportTables {
    pub fn from_catalog(catalog: &Catalog<DuckDb>) -> Result<Self, LoadError> {
        let graph = catalog.graph();
        let mut entities: Vec<EntityTable> = graph
            .entities()
            .filter_map(|entity| {
                let (store, source) = catalog.source(entity.id)?;
                let columns = entity
                    .properties
                    .iter()
                    .filter_map(|property| {
                        let column = store.column(*property)?;
                        let property = graph.property(*property);
                        Some((column.to_string(), property.data_type, property.nullable))
                    })
                    .collect();
                Some(EntityTable {
                    entity: entity.name.clone(),
                    table: source.table.clone(),
                    columns,
                })
            })
            .collect();
        entities.sort_by(|a, b| a.entity.cmp(&b.entity));

        let edge_table = catalog
            .default_edge_table()
            .ok_or_else(|| LoadError::new("the local store has no default edge table"))?;
        let edge_columns = catalog
            .physical_table(edge_table)
            .ok_or_else(|| LoadError::new(format!("the local store has no table {edge_table}")))?
            .columns
            .iter()
            .map(|column| {
                column
                    .data_type
                    .map(|data_type| (column.name.clone(), data_type))
                    .ok_or_else(|| {
                        LoadError::new(format!("{edge_table}.{} has no type", column.name))
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            entities,
            edge_table: edge_table.to_string(),
            edge_columns,
        })
    }

    pub fn embedded() -> &'static Self {
        static TABLES: std::sync::LazyLock<ExportTables> = std::sync::LazyLock::new(|| {
            let catalog = Catalog::<DuckDb>::embedded().expect("embedded model must load");
            ExportTables::from_catalog(&catalog).expect("embedded local store must export")
        });
        &TABLES
    }

    pub fn entity(&self, name: &str) -> Option<&EntityTable> {
        self.entities.iter().find(|entity| entity.entity == name)
    }
}
