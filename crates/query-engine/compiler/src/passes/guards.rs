//! The path columns a scan must filter and the role each one needs, read from
//! the loader's guards and the store's table facts.

use query_data_model::RequiredRole;
use query_data_model::catalog::RowFilter;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::RelationalFacts;
use query_data_model::implementations::relational::plan::Table;
use query_data_model::{Catalog, EntityId};

pub struct PathGuard {
    pub column: String,
    pub min_role: u32,
}

pub fn scan<B: RelationalBackend>(catalog: &Catalog<B>, table: Table) -> Option<Vec<PathGuard>> {
    let (store, facts) = catalog.stores().nth(table.store())?;
    let name = catalog.table_name(table);
    if let Some(source) = store.sources().find(|source| source.table == name) {
        let guard = store.guard(source.entity)?;
        return Some(match &guard.filter {
            RowFilter::Path { column } => vec![PathGuard {
                column: column.clone(),
                min_role: guard.min_role.unwrap_or_else(reporter),
            }],
            RowFilter::None | RowFilter::PathThrough { .. } => Vec::new(),
        });
    }
    let columns = &facts.table(name)?.path_columns;
    Some(
        columns
            .iter()
            .map(|column| PathGuard {
                column: column.name.clone(),
                min_role: column
                    .entity
                    .and_then(|entity| role(catalog, entity))
                    .unwrap_or_else(reporter),
            })
            .collect(),
    )
}

fn reporter() -> u32 {
    RequiredRole::Reporter.as_access_level()
}

fn role<B: RelationalBackend>(catalog: &Catalog<B>, entity: EntityId) -> Option<u32> {
    let (store, _) = catalog.home(entity)?;
    store.guard(entity)?.min_role
}
