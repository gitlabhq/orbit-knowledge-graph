//! The scope key columns a scan must filter, read from the loader's guards, and the caller's
//! scope sets that bound them.

use orbit_authorization::{Grants, ScopeSet};
use query_data_model::catalog::{Guard, PermissionId, Rule, ScopeId};
use query_data_model::implementations::relational::plan::Table;
use query_data_model::implementations::relational::{RelationalBackend, RelationalFacts};
use query_data_model::{Catalog, Model};

use crate::error::{QueryError, Result};

pub struct ScanGuard {
    pub column: String,
    pub scope: ScopeId,
    pub permission: PermissionId,
}

/// Every scope key column of the scanned table; none when the table is unknown or its rows
/// reach their key through relationships, which a relational scan cannot filter.
pub fn scan<B: RelationalBackend>(catalog: &Catalog<B>, table: Table) -> Option<Vec<ScanGuard>> {
    let (store, facts) = catalog.stores().nth(table.store())?;
    let name = catalog.table_name(table);
    let keyed = |guard: &Guard, column: &str| match &guard.rule {
        Rule::Column(_) | Rule::Owner(_) => Some(vec![ScanGuard {
            column: column.to_string(),
            scope: guard.scope,
            permission: guard.permission,
        }]),
        Rule::Trusted | Rule::Global | Rule::Checked => Some(Vec::new()),
        Rule::Via(_) => None,
    };
    if let Some(source) = store
        .sources()
        .find(|source| source.table.as_deref() == Some(name))
    {
        let guard = store.guard(source.entity)?;
        return match &guard.rule {
            Rule::Column(column) | Rule::Owner(column) => keyed(guard, column),
            _ => keyed(guard, ""),
        };
    }
    let mut guards = Vec::new();
    for column in &facts.relational().table(name)?.path_columns {
        let guard = match column.entity {
            Some(entity) => catalog.guard(entity)?,
            None => store.edge_guard(&column.table)?,
        };
        guards.extend(keyed(guard, &column.name)?);
    }
    Some(guards)
}

/// The entity needs a permission that no relationship's edge rows carry, so an edge row does
/// not prove the caller may read it.
pub fn stricter_than_edges<B: RelationalBackend>(
    catalog: &Catalog<B>,
    entity: query_data_model::EntityId,
) -> bool {
    let Some(guard) = catalog
        .guard(entity)
        .filter(|guard| guard.rule != Rule::Trusted)
    else {
        return false;
    };
    let access = catalog.model().access();
    catalog
        .graph()
        .relationships()
        .all(|relationship| access.relationship(relationship.id).permission != guard.permission)
}

/// The caller's scope keys for a guard's scope and permission; an unanswered pair fails closed.
pub fn scope_set<'g>(
    grants: &'g Grants,
    model: &Model,
    scope: ScopeId,
    permission: PermissionId,
) -> Result<&'g ScopeSet> {
    let access = model.access();
    grants
        .scope_set(
            &access.scope(scope).name,
            &access.permission(permission).name,
        )
        .map_err(|error| QueryError::Security(error.to_string()))
}
