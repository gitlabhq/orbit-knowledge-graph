use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use ontology::DataType;
use serde::{Deserialize, Serialize};

use crate::catalog::{Catalog, Diagnostics, StorageBackend, StoreInput};
use crate::spec::parse_word;
use crate::{EntityId, ForeignKey, PathColumn, RelationshipVariantId};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationalOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_column: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub edge_tables: IndexMap<String, EdgeTableSpec>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub route_all_variants: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTableSpec {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub default: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct TableFacts {
    pub entity: Option<EntityId>,
    pub columns: Option<HashSet<String>>,
    pub column_types: HashMap<String, DataType>,
    pub sort_key: Vec<String>,
    pub path_columns: Option<Vec<PathColumn>>,
}

impl EdgeTableSpec {
    pub fn column_types(
        &self,
        package: &str,
        table: &str,
        diagnostics: &mut Diagnostics,
    ) -> HashMap<String, DataType> {
        self.columns
            .iter()
            .filter_map(|(column, word)| {
                let data_type =
                    diagnostics.require(parse_word::<DataType>(word), package, || {
                        format!("edge table {table} column {column} has unknown type `{word}`")
                    })?;
                Some((column.clone(), data_type))
            })
            .collect()
    }
}

impl RelationalOptions {
    pub fn edge_table_for(
        &self,
        source_domain: Option<&str>,
        target_domain: Option<&str>,
    ) -> Option<&str> {
        let in_domain = |domain: &str| {
            self.edge_tables
                .iter()
                .find(|(_, table)| table.domains.iter().any(|d| d == domain))
        };
        let shared = source_domain
            .filter(|domain| Some(*domain) == target_domain)
            .and_then(in_domain);
        shared
            .or_else(|| self.edge_tables.iter().find(|(_, table)| table.default))
            .map(|(name, _)| name.as_str())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationalRoute {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Versioning {
    pub version: String,
    pub deleted: String,
}

#[derive(Debug, Clone)]
pub struct EdgeTable {
    pub name: String,
    pub sort_key: Vec<String>,
    pub domains: Vec<String>,
    pub default: bool,
}

#[derive(Debug)]
pub struct RelationalStore {
    pub versioning: Option<Versioning>,
    pub edge_tables: Vec<EdgeTable>,
    sort_keys: Vec<Option<Vec<String>>>,
    edge_routes: Vec<Option<String>>,
}

impl RelationalStore {
    pub fn default_edge_table(&self) -> Option<&EdgeTable> {
        self.edge_tables.iter().find(|table| table.default)
    }

    pub fn sort_key(&self, entity: EntityId) -> Option<&[String]> {
        self.sort_keys[entity.index()].as_deref()
    }

    pub fn edge_table(&self, variant: RelationshipVariantId) -> Option<&str> {
        self.edge_routes[variant.index()].as_deref()
    }

    pub fn resolve<B: StorageBackend<RouteOptions = RelationalRoute>>(
        input: &StoreInput<'_, B>,
        options: &RelationalOptions,
        sort_keys: impl Iterator<Item = (EntityId, Vec<String>)>,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let store = input.store;
        let graph = input.graph;
        let versioning = match (&options.version_column, &options.deleted_column) {
            (Some(version), Some(deleted)) => Some(Versioning {
                version: version.clone(),
                deleted: deleted.clone(),
            }),
            (None, None) => None,
            _ => {
                diagnostics.error(
                    &store.package,
                    format!(
                        "store {} must name both version and deleted columns",
                        store.name
                    ),
                );
                None
            }
        };
        let mut domains = std::collections::HashSet::new();
        for domain in options
            .edge_tables
            .values()
            .flat_map(|table| &table.domains)
        {
            if !domains.insert(domain) {
                diagnostics.error(
                    &store.package,
                    format!(
                        "store {} assigns domain {domain} to two edge tables",
                        store.name
                    ),
                );
            }
        }
        if options
            .edge_tables
            .values()
            .filter(|table| table.default)
            .count()
            > 1
        {
            diagnostics.error(
                &store.package,
                format!("store {} has more than one default edge table", store.name),
            );
        }
        let mut keys = vec![None; graph.entities().count()];
        for (entity, key) in sort_keys {
            keys[entity.index()] = Some(if key.is_empty() {
                options.sort_key.clone()
            } else {
                key
            });
        }
        let on_connection = |entity: EntityId| {
            input
                .connection
                .iter()
                .any(|other| other.source(entity).is_some())
        };
        let mut edge_routes = vec![None; graph.variants().count()];
        for variant in graph.variants() {
            let bound = store.source(variant.source).is_some() && on_connection(variant.target);
            if !bound && !options.route_all_variants {
                continue;
            }
            let route = input
                .routes
                .iter()
                .find(|route| route.relationship == variant.relationship);
            let narrow = route.and_then(|route| {
                route
                    .variants
                    .iter()
                    .find(|(id, _)| *id == variant.id)
                    .map(|(_, options)| options)
            });
            let table = narrow
                .and_then(|options| options.table.as_deref())
                .or_else(|| route?.options.table.as_deref())
                .or_else(|| {
                    options.edge_table_for(
                        graph.entity(variant.source).domain.as_deref(),
                        graph.entity(variant.target).domain.as_deref(),
                    )
                });
            let Some(table) = table else {
                continue;
            };
            if !options.edge_tables.contains_key(table) {
                let package = route.map_or(store.package.as_str(), |route| route.package);
                diagnostics.error(
                    package,
                    format!(
                        "store {} routes {} to undeclared edge table {table}",
                        store.name,
                        graph.relationship(variant.relationship).name
                    ),
                );
                continue;
            }
            edge_routes[variant.id.index()] = Some(table.to_string());
        }
        Self {
            versioning,
            edge_tables: options
                .edge_tables
                .iter()
                .map(|(name, table)| EdgeTable {
                    name: name.clone(),
                    sort_key: table.sort_key.clone(),
                    domains: table.domains.clone(),
                    default: table.default,
                })
                .collect(),
            sort_keys: keys,
            edge_routes,
        }
    }
}

pub trait RelationalFacts {
    fn relational(&self) -> &RelationalStore;
    fn table(&self, name: &str) -> Option<&TableFacts>;
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
