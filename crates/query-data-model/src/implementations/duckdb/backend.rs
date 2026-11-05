use serde::{Deserialize, Serialize};

use super::layout::DuckDb;
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{
    RelationalFacts, RelationalOptions, RelationalRoute, RelationalStore,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
}

#[derive(Debug)]
pub struct DuckDbStore {
    pub relational: RelationalStore,
}

impl RelationalFacts for DuckDbStore {
    fn relational(&self) -> &RelationalStore {
        &self.relational
    }
}

impl StorageBackend for DuckDb {
    const KIND: &'static str = "duckdb";
    type Options = RelationalOptions;
    type SourceOptions = DuckDbSource;
    type RouteOptions = RelationalRoute;
    type Store = DuckDbStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> DuckDbStore {
        let relational = RelationalStore::resolve(
            &input,
            &input.options,
            input
                .sources
                .iter()
                .map(|source| (source.entity, source.options.sort_key.clone())),
            diagnostics,
        );
        DuckDbStore { relational }
    }
}
