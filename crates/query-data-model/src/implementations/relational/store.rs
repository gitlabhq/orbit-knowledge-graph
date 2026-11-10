use std::collections::{HashMap, HashSet};

use super::{RelationalFacts, RelationalOptions, RelationalRoute, Table};
use crate::catalog::{Diagnostics, StorageBackend, Store, StoreInput};
use crate::{
    DenormalizedCatalog, EntityId, GraphCatalog, MaterializedJoin, RelationshipVariantId,
    TraversalPathKind, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct Versioning {
    pub version: String,
    pub deleted: String,
}

/// What a relational store holds: its tables, where each variant's edges live, and the
/// ClickHouse read paths, which stay empty on DuckDB.
#[derive(Debug)]
pub struct RelationalStore {
    pub versioning: Option<Versioning>,
    pub default_edge_table: Option<String>,
    pub tables: Vec<Table>,
    pub denormalized: DenormalizedCatalog,
    pub joins: Vec<MaterializedJoin>,
    pub high_fan_out: HashSet<EntityId>,
    pub path_lookups: HashMap<(EntityId, TraversalPathKind), TraversalPathLookup>,
    pub copies: Vec<(String, String)>,
    edge_routes: Vec<Option<String>>,
}

impl RelationalFacts for RelationalStore {
    fn relational(&self) -> &RelationalStore {
        self
    }
}

impl RelationalStore {
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|table| table.name == name)
    }

    pub fn entity_table(&self, entity: EntityId) -> Option<&Table> {
        self.tables
            .iter()
            .find(|table| table.entity == Some(entity))
    }

    pub fn sort_key(&self, entity: EntityId) -> Option<&[String]> {
        Some(&self.entity_table(entity)?.sort_key)
    }

    pub fn edge_table(&self, variant: RelationshipVariantId) -> Option<&str> {
        self.edge_routes[variant.index()].as_deref()
    }

    pub fn resolve<B: StorageBackend<RouteOptions = RelationalRoute>>(
        input: &StoreInput<'_, B>,
        options: &RelationalOptions,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let store = input.store;
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
        let mut domains = HashSet::new();
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
        let mut defaults = options
            .edge_tables
            .iter()
            .filter(|(_, table)| table.default);
        let default_edge_table = defaults.next().map(|(name, _)| name.clone());
        if defaults.next().is_some() {
            diagnostics.error(
                &store.package,
                format!("store {} has more than one default edge table", store.name),
            );
        }
        Self {
            versioning,
            default_edge_table,
            tables: Vec::new(),
            denormalized: DenormalizedCatalog::default(),
            joins: Vec::new(),
            high_fan_out: HashSet::new(),
            path_lookups: HashMap::new(),
            copies: Vec::new(),
            edge_routes: edge_routes(input, options, diagnostics),
        }
    }

    pub fn check_attribute_columns(
        &self,
        store: &Store,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        for variant in graph.variants() {
            let relationship = &graph.relationship(variant.relationship).name;
            let label = || {
                let (source, target) = (
                    &graph.entity(variant.source).name,
                    &graph.entity(variant.target).name,
                );
                format!("store {} {relationship}({source} -> {target})", store.name)
            };
            let mut bound = store.attribute_columns(variant.id).peekable();
            if bound.peek().is_none() {
                continue;
            }
            let Some(name) = self.edge_table(variant.id) else {
                diagnostics.error(
                    &store.package,
                    format!("{} binds attributes but uses no edge table", label()),
                );
                continue;
            };
            let table = self.table(name);
            for (attribute, column) in bound {
                let attribute = graph.attribute(attribute);
                let found = table
                    .and_then(|table| table.column(column))
                    .and_then(|column| column.data_type);
                match found {
                    Some(found) if found == attribute.data_type => {}
                    Some(found) => diagnostics.error(
                        &store.package,
                        format!(
                            "{} binds {} to {name}.{column}, which is {found:?}, not {:?}",
                            label(),
                            attribute.name,
                            attribute.data_type
                        ),
                    ),
                    None => diagnostics.error(
                        &store.package,
                        format!(
                            "{} binds {} to {name}.{column}, which does not exist",
                            label(),
                            attribute.name
                        ),
                    ),
                }
            }
        }
    }
}

fn edge_routes<B: StorageBackend<RouteOptions = RelationalRoute>>(
    input: &StoreInput<'_, B>,
    options: &RelationalOptions,
    diagnostics: &mut Diagnostics,
) -> Vec<Option<String>> {
    let (graph, store) = (input.graph, input.store);
    let on_connection = |entity: EntityId| {
        input
            .connection
            .iter()
            .any(|other| other.source(entity).is_some())
    };
    let mut routes = vec![None; graph.variants().count()];
    for variant in graph.variants() {
        if store.source(variant.source).is_none() || !on_connection(variant.target) {
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
        routes[variant.id.index()] = Some(table.to_string());
    }
    routes
}
