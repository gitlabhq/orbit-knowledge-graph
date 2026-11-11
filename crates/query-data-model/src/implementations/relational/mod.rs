use indexmap::IndexMap;
use serde::Deserialize;

use crate::catalog::{Catalog, StorageBackend};
use crate::{ForeignKey, RelationshipVariantId};

mod capabilities;
mod catalog;
pub mod plan;
pub mod render;
mod store;
mod tables;

pub use capabilities::{Capabilities, CurrentRows, RelationalBackend};
pub use catalog::RelationshipRoute;
pub use store::{ReadPaths, RelationalStore, Versioning, entity_layout};
pub use tables::{Column, Engine, Index, PathColumn, Projection, Table};

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationalOptions {
    #[serde(default)]
    pub version_column: Option<String>,
    #[serde(default)]
    pub deleted_column: Option<String>,
    #[serde(default)]
    pub edge_tables: IndexMap<String, EdgeTableSpec>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTableSpec {
    #[serde(default)]
    pub shared: bool,
    #[serde(default)]
    pub sort_key: Vec<String>,
    #[serde(default)]
    pub columns: IndexMap<String, String>,
}

pub trait RelationalFacts {
    fn relational(&self) -> &RelationalStore;

    fn read_paths(&self) -> Option<&ReadPaths> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeRoute<'a> {
    pub store: &'a str,
    pub table: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRoute<'a> {
    pub store: &'a str,
    pub key: ForeignKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route<'a> {
    pub edge: Option<EdgeRoute<'a>>,
    pub foreign_key: Option<KeyRoute<'a>>,
}

impl<B: StorageBackend> Catalog<B>
where
    B::Store: RelationalFacts,
{
    pub fn routes(&self, variant: RelationshipVariantId) -> impl Iterator<Item = Route<'_>> {
        self.stores().filter_map(move |(store, facts)| {
            let edge = facts
                .relational()
                .edge_table(variant)
                .map(|table| EdgeRoute {
                    store: &store.name,
                    table,
                });
            let foreign_key = store.foreign_key(variant).map(|key| KeyRoute {
                store: &store.name,
                key,
            });
            (edge.is_some() || foreign_key.is_some()).then_some(Route { edge, foreign_key })
        })
    }
}
