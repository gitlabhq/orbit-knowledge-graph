use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::catalog::{Catalog, StorageBackend};
use crate::plan::logical::EdgeField;
use crate::{ForeignKey, RelationshipVariantId};

mod capabilities;
mod catalog;
pub mod plan;
pub mod render;
mod store;
mod tables;

pub use capabilities::{CurrentRows, RelationalBackend, SqlCapabilities};
pub use catalog::RelationshipRoute;
pub use store::{ReadPaths, RelationalStore, Versioning, entity_layout};
pub use tables::{Column, ColumnId, Engine, Index, PathColumn, Projection, Table, TableId, Target};

#[derive(Debug, Clone, Default, PartialEq, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RelationalOptions {
    pub version_column: Option<String>,
    pub deleted_column: Option<String>,
    pub edge_tables: IndexMap<String, EdgeTableSpec>,
    pub edge_columns: Option<EdgeColumns>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EdgeColumns {
    pub relationship_kind: String,
    pub source_id: String,
    pub source_kind: String,
    pub target_id: String,
    pub target_kind: String,
}

impl EdgeColumns {
    pub fn column(&self, field: EdgeField) -> &str {
        match field {
            EdgeField::Kind => &self.relationship_kind,
            EdgeField::SourceId => &self.source_id,
            EdgeField::SourceKind => &self.source_kind,
            EdgeField::TargetId => &self.target_id,
            EdgeField::TargetKind => &self.target_kind,
        }
    }

    pub fn all(&self) -> [(EdgeField, &str); 5] {
        [
            EdgeField::Kind,
            EdgeField::SourceId,
            EdgeField::SourceKind,
            EdgeField::TargetId,
            EdgeField::TargetKind,
        ]
        .map(|field| (field, self.column(field)))
    }
}

impl crate::spec::WrittenBlock for RelationalOptions {}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EdgeTableSpec {
    pub shared: bool,
    pub sort_key: Vec<String>,
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
