use std::collections::{HashMap, HashSet};

use super::{EdgeColumns, RelationalFacts, Table};
use crate::catalog::{Diagnostics, Source, SourceInput, StorageBackend, StoreInput};
use crate::plan::logical::EdgeField;
use crate::{
    DenormalizedCatalog, EntityId, MaterializedJoin, RelationshipVariantId, TraversalPathKind,
    TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct Versioning {
    pub version: String,
    pub deleted: String,
}

/// What a relational store holds: its tables and where each variant's edges live.
#[derive(Debug)]
pub struct RelationalStore {
    pub versioning: Option<Versioning>,
    pub shared_edge_table: Option<String>,
    pub edge_columns: Option<EdgeColumns>,
    pub tables: Vec<Table>,
    edge_routes: Vec<Option<String>>,
}

/// The read paths a store declares beyond its tables, which only ClickHouse stores have.
#[derive(Debug, Default)]
pub struct ReadPaths {
    pub denormalized: DenormalizedCatalog,
    pub joins: Vec<MaterializedJoin>,
    pub high_fan_out: HashSet<EntityId>,
    pub path_lookups: HashMap<(EntityId, TraversalPathKind), TraversalPathLookup>,
    pub copies: Vec<(String, String)>,
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

    /// The store's versioning and the edge table of every variant it binds to one, checked
    /// against the edge tables it declares.
    pub fn resolve<B: StorageBackend>(
        input: &StoreInput<'_, B>,
        version: Option<&String>,
        deleted: Option<&String>,
        edge_tables: &[(&str, bool)],
        edge_columns: Option<&EdgeColumns>,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let store = input.store;
        match (edge_tables.is_empty(), edge_columns) {
            (false, None) => diagnostics.error(
                &store.package,
                format!(
                    "store {} declares edge tables but no edge_columns",
                    store.name
                ),
            ),
            (true, Some(_)) => diagnostics.error(
                &store.package,
                format!(
                    "store {} declares edge_columns but no edge tables",
                    store.name
                ),
            ),
            _ => {}
        }
        let mut shared = edge_tables.iter().filter(|(_, shared)| *shared);
        let shared_edge_table = shared.next().map(|(name, _)| name.to_string());
        if shared.next().is_some() {
            diagnostics.error(
                &store.package,
                format!("store {} shares more than one edge table", store.name),
            );
        }
        let versioning = match (version, deleted) {
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
        let mut edge_routes = vec![None; input.graph.variants().count()];
        for (variant, package, _) in store.bindings() {
            let Some(table) = store.edge_table(variant) else {
                continue;
            };
            if !edge_tables.iter().any(|(name, _)| *name == table) {
                diagnostics.error(
                    package,
                    format!(
                        "store {} binds {} to undeclared edge table {table}",
                        store.name,
                        input
                            .graph
                            .relationship(input.graph.variant(variant).relationship)
                            .name
                    ),
                );
                continue;
            }
            edge_routes[variant.index()] = Some(table.to_string());
        }
        Self {
            versioning,
            shared_edge_table,
            edge_columns: edge_columns.cloned(),
            tables: Vec::new(),
            edge_routes,
        }
    }

    fn edge_field(&self, column: &str) -> bool {
        self.edge_columns
            .as_ref()
            .is_some_and(|columns| columns.all().iter().any(|(_, name)| *name == column))
    }

    /// Every edge table stores every edge field, every attribute of a variant's relationship is a
    /// column of the variant's edge table, and the table's ID columns have the types of both
    /// endpoint identities.
    pub fn check_edge_columns<B: StorageBackend>(
        &self,
        input: &StoreInput<'_, B>,
        diagnostics: &mut Diagnostics,
    ) {
        let (graph, store) = (input.graph, input.store);
        if let Some(columns) = &self.edge_columns {
            for table in self.tables.iter().filter(|table| table.edge) {
                for (_, column) in columns.all() {
                    if !table.has_column(column) {
                        diagnostics.error(
                            &store.package,
                            format!(
                                "store {} edge table {} has no edge column {column}",
                                store.name, table.name
                            ),
                        );
                    }
                }
            }
        }
        for (variant, package, _) in store.bindings() {
            let variant = graph.variant(variant);
            let relationship = graph.relationship(variant.relationship);
            let label = || {
                let (source, target) = (
                    &graph.entity(variant.source).name,
                    &graph.entity(variant.target).name,
                );
                format!(
                    "store {} {}({source} -> {target})",
                    store.name, relationship.name
                )
            };
            let table = self
                .edge_table(variant.id)
                .and_then(|name| self.table(name));
            if let (Some(table), Some(columns)) = (table, &self.edge_columns) {
                for (field, endpoint) in [
                    (EdgeField::SourceId, variant.source),
                    (EdgeField::TargetId, variant.target),
                ] {
                    let column = columns.column(field);
                    let entity = graph.entity(endpoint);
                    let identity = match entity.identity.as_slice() {
                        [identity] => Some(graph.property(*identity).data_type),
                        _ => None,
                    };
                    let found = table.column(column).and_then(|column| column.data_type);
                    if found.is_none() || found != identity {
                        let found = found.map_or("untyped".to_string(), |t| format!("{t:?}"));
                        let identity =
                            identity.map_or("composite".to_string(), |t| format!("{t:?}"));
                        diagnostics.error(
                            package,
                            format!(
                                "{} binds edge table {} whose {column} is {found}, but {}'s identity is {identity}; keys are never cast",
                                label(),
                                table.name,
                                entity.name
                            ),
                        );
                    }
                }
            }
            for attribute in &relationship.attributes {
                let attribute = graph.attribute(*attribute);
                if self.edge_field(&attribute.name)
                    || store.path_column.as_ref() == Some(&attribute.name)
                {
                    diagnostics.error(
                        package,
                        format!(
                            "{} declares attribute {} on an edge column the store reserves",
                            label(),
                            attribute.name
                        ),
                    );
                    continue;
                }
                let found = table.and_then(|table| table.column(&attribute.name));
                match found.and_then(|column| column.data_type) {
                    Some(found) if found == attribute.data_type => {}
                    Some(found) => diagnostics.error(
                        package,
                        format!(
                            "{} stores attribute {} as {found:?}, not {:?}",
                            label(),
                            attribute.name,
                            attribute.data_type
                        ),
                    ),
                    None => diagnostics.error(
                        package,
                        format!(
                            "{} has no edge column for attribute {}",
                            label(),
                            attribute.name
                        ),
                    ),
                }
            }
        }
    }
}

/// The table and sort key of an entity in a relational store: the sort key its backend block
/// overrides, else the store's path column followed by the entity's access order.
pub fn entity_layout<'a, B: StorageBackend>(
    input: &'a StoreInput<'_, B>,
    entity: &SourceInput<'_, B>,
    sort_key: &[String],
    diagnostics: &mut Diagnostics,
) -> Option<(&'a Source, &'a str, Vec<String>)> {
    let (graph, store) = (input.graph, input.store);
    let source = store.source(entity.entity)?;
    let name = &graph.entity(entity.entity).name;
    let table = source.table.as_deref()?;
    let sort_key = match sort_key {
        [] => {
            let order = source
                .access
                .order
                .iter()
                .filter_map(|id| store.column(*id));
            source
                .path_column
                .iter()
                .map(String::as_str)
                .chain(order)
                .map(str::to_string)
                .collect()
        }
        declared => declared.to_vec(),
    };
    if sort_key.is_empty() {
        diagnostics.error(
            entity.package,
            format!("store {} declares no access order for {name}", store.name),
        );
    }
    Some((source, table, sort_key))
}
