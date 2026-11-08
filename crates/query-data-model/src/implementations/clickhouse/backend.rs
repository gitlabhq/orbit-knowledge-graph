use std::collections::{HashMap, HashSet};

use ontology::{DataType, FieldSelectivity, TraversalPathKind};

use super::ClickHouse;
use super::schema::{self, Schema};
use super::spec::{ClickHouseOptions, ClickHouseSource};
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{
    RelationalFacts, RelationalRoute, RelationalStore, TableFacts, TableSchema,
};
use crate::spec::parse_word;
use crate::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, EntityId,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, PathColumn, PropertyId,
    TraversalPathLookup,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Text,
    Bloom,
    Set,
    MinMax,
    NGram,
    Token,
}

impl IndexKind {
    pub fn parse(word: &str) -> Option<Self> {
        match word.split('(').next()?.trim() {
            "text" => Some(Self::Text),
            "bloom_filter" => Some(Self::Bloom),
            "set" => Some(Self::Set),
            "minmax" => Some(Self::MinMax),
            "ngrambf_v1" => Some(Self::NGram),
            "tokenbf_v1" => Some(Self::Token),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClickHouseColumn {
    pub indexes: Vec<IndexKind>,
    pub selectivity: FieldSelectivity,
    pub null_as_epoch: bool,
}

#[derive(Debug)]
pub struct ClickHouseStore {
    pub relational: RelationalStore,
    pub denormalized: DenormalizedCatalog,
    pub joins: Vec<MaterializedJoin>,
    columns: Vec<Option<ClickHouseColumn>>,
    high_fan_out: Vec<bool>,
    path_lookups: HashMap<(EntityId, TraversalPathKind), TraversalPathLookup>,
    copies: Vec<(String, String)>,
    tables: HashMap<String, TableFacts>,
    schema: Option<Schema>,
}

impl ClickHouseStore {
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    pub fn column(&self, property: PropertyId) -> Option<&ClickHouseColumn> {
        self.columns[property.index()].as_ref()
    }

    pub fn high_fan_out(&self, entity: EntityId) -> bool {
        self.high_fan_out[entity.index()]
    }

    pub fn path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.path_lookups.get(&(entity, kind))
    }
}

impl RelationalFacts for ClickHouseStore {
    fn relational(&self) -> &RelationalStore {
        &self.relational
    }

    fn table(&self, name: &str) -> Option<&TableFacts> {
        self.tables.get(name)
    }
}

impl StorageBackend for ClickHouse {
    const KIND: &'static str = "clickhouse";
    type Options = ClickHouseOptions;
    type SourceOptions = ClickHouseSource;
    type RouteOptions = RelationalRoute;
    type Store = ClickHouseStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> ClickHouseStore {
        let relational = RelationalStore::resolve(
            &input,
            &input.options.relational(),
            input
                .sources
                .iter()
                .map(|source| (source.entity, source.options.sort_key.clone())),
            diagnostics,
        );
        let mut store = ClickHouseStore {
            relational,
            denormalized: DenormalizedCatalog::default(),
            joins: Vec::new(),
            columns: vec![None; input.graph.properties().count()],
            high_fan_out: vec![false; input.graph.entities().count()],
            path_lookups: HashMap::new(),
            copies: Vec::new(),
            tables: HashMap::new(),
            schema: None,
        };
        store.annotate_sources(&input, diagnostics);
        store.register_entity_tables(&input);
        store.register_edge_tables(&input);
        store.resolve_denormalized(&input, diagnostics);
        store.resolve_joins(&input, diagnostics);
        store.resolve_copies(&input, diagnostics);
        store.schema = schema::resolve(&input, &store.relational, diagnostics);
        store.relational.tables = store.table_schemas();
        store.check_attribute_columns(input.store, input.graph, diagnostics);
        store
    }
}

impl ClickHouseStore {
    fn annotate_sources(
        &mut self,
        input: &StoreInput<'_, ClickHouse>,
        diagnostics: &mut Diagnostics,
    ) {
        let graph = input.graph;
        let store = input.store;
        for source in &input.sources {
            let entity = source.entity;
            for property in &graph.entity(entity).properties {
                if store.column(*property).is_some() {
                    self.columns[property.index()] = Some(ClickHouseColumn {
                        indexes: Vec::new(),
                        selectivity: graph.property(*property).selectivity,
                        null_as_epoch: false,
                    });
                }
            }
            let name = &graph.entity(entity).name;
            let stored = |property: &str, diagnostics: &mut Diagnostics| {
                let id = graph
                    .property_id(entity, property)
                    .filter(|id| store.column(*id).is_some());
                diagnostics.require(id, source.package, || {
                    format!("store {} does not store {name}.{property}", store.name)
                })
            };
            let options = &source.options;
            let shared = &input.options.columns;
            schema::check_indexes(
                source.package,
                &format!("store {} {name}", store.name),
                &options.indexes,
                |target| {
                    shared.contains_key(target)
                        || graph
                            .property_id(entity, target)
                            .is_some_and(|id| store.column(id).is_some())
                },
                diagnostics,
            );
            for (property, specs) in &options.indexes {
                let column = graph
                    .property_id(entity, property)
                    .and_then(|id| self.columns[id.index()].as_mut());
                if let Some(column) = column {
                    let kinds = specs.specs().iter().map(|spec| spec.kind());
                    column.indexes.extend(kinds.filter_map(IndexKind::parse));
                }
            }
            for property in &options.epoch_nulls {
                if let Some(column) =
                    stored(property, diagnostics).and_then(|id| self.columns[id.index()].as_mut())
                {
                    column.null_as_epoch = true;
                }
            }
            self.high_fan_out[entity.index()] = options
                .high_fan_out
                .iter()
                .filter_map(|property| stored(property, diagnostics))
                .count()
                > 0;
            for (kind, lookup) in &options.path_lookups {
                let kind_word = diagnostics.require(
                    parse_word::<TraversalPathKind>(kind),
                    source.package,
                    || format!("{name} has unknown path lookup kind `{kind}`"),
                );
                let property = diagnostics.require(
                    graph.property_id(entity, &lookup.key),
                    source.package,
                    || format!("{name} path lookup key {} is not a property", lookup.key),
                );
                if let (Some(kind), Some(property)) = (kind_word, property) {
                    self.path_lookups.insert(
                        (entity, kind),
                        TraversalPathLookup {
                            table: lookup.table.clone(),
                            property,
                        },
                    );
                }
            }
        }
    }

    fn register_entity_tables(&mut self, input: &StoreInput<'_, ClickHouse>) {
        let graph = input.graph;
        let system = system_columns(
            input.options.version_column.as_deref(),
            input.options.deleted_column.as_deref(),
        );
        for source in input.store.sources() {
            let properties = &graph.entity(source.entity).properties;
            let stored = properties
                .iter()
                .filter_map(|property| Some((*property, input.store.column(*property)?)));
            let columns: HashSet<String> = stored
                .clone()
                .map(|(_, column)| column.to_string())
                .chain(system.iter().map(|(name, _)| name.clone()))
                .collect();
            let column_types = stored
                .map(|(property, column)| (column.to_string(), graph.property(property).data_type))
                .chain(system.iter().cloned())
                .collect();
            let path_columns = source
                .path_column
                .iter()
                .map(|name| PathColumn {
                    name: name.clone(),
                    entity: Some(source.entity),
                })
                .collect();
            self.tables.insert(
                source.table.clone(),
                TableFacts {
                    entity: Some(source.entity),
                    columns: Some(columns),
                    column_types,
                    sort_key: self
                        .relational
                        .sort_key(source.entity)
                        .unwrap_or_default()
                        .to_vec(),
                    path_columns: Some(path_columns),
                },
            );
        }
    }

    fn register_edge_tables(&mut self, input: &StoreInput<'_, ClickHouse>) {
        let system = system_columns(
            input.options.version_column.as_deref(),
            input.options.deleted_column.as_deref(),
        );
        for (name, spec) in &input.options.edge_tables {
            let mut columns: HashSet<String> = spec.columns.keys().cloned().collect();
            let mut column_types: HashMap<String, DataType> = spec
                .columns
                .iter()
                .filter_map(|(column, layout)| {
                    let data_type = schema::data_type(layout.storage_type.as_deref()?)?;
                    Some((column.clone(), data_type))
                })
                .collect();
            columns.extend(
                input
                    .options
                    .denormalized
                    .iter()
                    .map(|property| property.column.clone()),
            );
            columns.extend(system.iter().map(|(column, _)| column.clone()));
            column_types.extend(system.iter().cloned());
            self.tables.insert(
                name.clone(),
                TableFacts {
                    entity: None,
                    columns: Some(columns),
                    column_types,
                    sort_key: spec.sort_key.clone(),
                    path_columns: Some(
                        input
                            .store
                            .path_column
                            .iter()
                            .map(|name| PathColumn {
                                name: name.clone(),
                                entity: None,
                            })
                            .collect(),
                    ),
                },
            );
        }
    }

    fn resolve_denormalized(
        &mut self,
        input: &StoreInput<'_, ClickHouse>,
        diagnostics: &mut Diagnostics,
    ) {
        let graph = input.graph;
        let package = &input.store.package;
        let mut properties = HashMap::new();
        for spec in &input.options.denormalized {
            let Some(entity) = diagnostics.require(graph.entity_id(&spec.entity), package, || {
                format!("denormalized property names unknown entity {}", spec.entity)
            }) else {
                continue;
            };
            let (Some(property), Some(relationship)) = (
                graph.property_id(entity, &spec.property),
                graph.relationship_id(&spec.relationship),
            ) else {
                continue;
            };
            let direction = match spec.side.as_str() {
                "source" => DenormalizedDirection::Source,
                "target" => DenormalizedDirection::Target,
                side => {
                    diagnostics.error(
                        package,
                        format!("denormalized property side `{side}` is not source or target"),
                    );
                    continue;
                }
            };
            let variants: HashSet<_> = graph
                .relationship(relationship)
                .variants
                .iter()
                .map(|variant| graph.variant(*variant))
                .filter(|variant| match direction {
                    DenormalizedDirection::Source => variant.source == entity,
                    DenormalizedDirection::Target => variant.target == entity,
                })
                .map(|variant| (relationship, variant.source, variant.target))
                .collect();
            properties
                .entry(DenormalizedKey {
                    property,
                    direction,
                })
                .and_modify(|existing: &mut DenormalizedProperty| {
                    existing.variants.extend(&variants)
                })
                .or_insert_with(|| DenormalizedProperty {
                    edge_column: spec.column.clone(),
                    tag_key: spec.tag.clone(),
                    variants,
                    nullable: spec.nullable,
                });
        }
        self.denormalized = DenormalizedCatalog::new(properties);
    }

    fn resolve_joins(&mut self, input: &StoreInput<'_, ClickHouse>, diagnostics: &mut Diagnostics) {
        let graph = input.graph;
        let package = &input.store.package;
        for (table, spec) in &input.options.joins {
            let nodes: Option<Vec<MaterializedNode>> = spec
                .nodes
                .iter()
                .map(|node| {
                    let entity =
                        diagnostics.require(graph.entity_id(&node.entity), package, || {
                            format!("join {table} names unknown entity {}", node.entity)
                        })?;
                    let source = spec.sources.get(node.source)?;
                    Some(MaterializedNode {
                        entity,
                        source_occurrence: node.source,
                        identity_column: node.key.clone(),
                        properties: graph
                            .entity(entity)
                            .properties
                            .iter()
                            .filter_map(|property| {
                                let column = source.columns.get(&graph.property(*property).name)?;
                                Some((*property, column.clone()))
                            })
                            .collect(),
                    })
                })
                .collect();
            let Some(nodes) = nodes else {
                continue;
            };
            let relationships: Option<Vec<MaterializedRelationship>> = spec
                .relationships
                .iter()
                .map(|relationship| {
                    let (source, target) = (
                        nodes.get(relationship.source)?,
                        nodes.get(relationship.target)?,
                    );
                    let variant = graph.variant_named(
                        &relationship.relationship,
                        &graph.entity(source.entity).name,
                        &graph.entity(target.entity).name,
                    );
                    let variant = diagnostics.require(variant, package, || {
                        format!(
                            "join {table} names unknown variant {}",
                            relationship.relationship
                        )
                    })?;
                    Some(MaterializedRelationship {
                        variant: variant.id,
                        source_slot: relationship.source,
                        target_slot: relationship.target,
                        edge_occurrence: relationship.edge,
                        source_id_column: source.identity_column.clone(),
                        target_id_column: target.identity_column.clone(),
                        columns: relationship
                            .edge
                            .and_then(|edge| spec.sources.get(edge))
                            .map(|edge| edge.columns.clone())
                            .unwrap_or_default(),
                    })
                })
                .collect();
            let Some(relationships) = relationships else {
                continue;
            };
            let mut columns = HashSet::new();
            let mut column_types = HashMap::new();
            let mut path_columns = Vec::new();
            for source in &spec.sources {
                let facts = self.tables.get(&source.table);
                for (column, renamed) in &source.columns {
                    columns.insert(renamed.clone());
                    if let Some(data_type) = facts.and_then(|facts| facts.column_types.get(column))
                    {
                        column_types.insert(renamed.clone(), *data_type);
                    }
                }
                if let Some(path) = &source.path_column {
                    columns.insert(path.clone());
                    path_columns.push(PathColumn {
                        name: path.clone(),
                        entity: facts.and_then(|facts| facts.entity),
                    });
                }
            }
            let system = system_columns(
                input.options.version_column.as_deref(),
                input.options.deleted_column.as_deref(),
            );
            columns.extend(system.iter().map(|(name, _)| name.clone()));
            column_types.extend(system.iter().cloned());
            self.tables.insert(
                table.clone(),
                TableFacts {
                    entity: None,
                    columns: Some(columns),
                    column_types,
                    sort_key: spec.sort_key.clone(),
                    path_columns: Some(path_columns),
                },
            );
            self.joins.push(MaterializedJoin {
                table: table.clone(),
                nodes,
                relationships,
                sources: spec
                    .sources
                    .iter()
                    .map(|source| source.table.clone())
                    .collect(),
            });
        }
    }

    fn resolve_copies(
        &mut self,
        input: &StoreInput<'_, ClickHouse>,
        diagnostics: &mut Diagnostics,
    ) {
        for (name, copy) in &input.options.copies {
            let Some(source) = diagnostics.require(
                self.tables.get(&copy.source).cloned(),
                &input.store.package,
                || format!("copy {name} names unknown table {}", copy.source),
            ) else {
                continue;
            };
            self.tables.insert(
                name.clone(),
                TableFacts {
                    sort_key: copy.sort_key.clone(),
                    ..source
                },
            );
            self.copies.push((name.clone(), copy.source.clone()));
        }
    }
}

fn system_columns(version: Option<&str>, deleted: Option<&str>) -> Vec<(String, DataType)> {
    version
        .map(|column| (column.to_string(), DataType::DateTime))
        .into_iter()
        .chain(deleted.map(|column| (column.to_string(), DataType::Bool)))
        .collect()
}

impl crate::query_model::QueryFacts for ClickHouseStore {
    const SELECTIVE_LATEST_ROWS: bool = true;
    const DEFERRED_NODE_LOADING: bool = true;

    fn text_index(&self, property: PropertyId) -> bool {
        self.column(property)
            .is_some_and(|column| column.indexes.contains(&IndexKind::Text))
    }

    fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.column(property)
            .is_some_and(|column| column.null_as_epoch)
    }

    fn high_fan_out(&self, entity: EntityId) -> bool {
        ClickHouseStore::high_fan_out(self, entity)
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        ClickHouseStore::path_lookup(self, entity, kind)
    }

    fn copies_of<'a>(&'a self, table: &str) -> Vec<&'a str> {
        self.copies
            .iter()
            .filter(|(_, source)| source == table)
            .map(|(copy, _)| copy.as_str())
            .collect()
    }

    fn joins(&self) -> &[MaterializedJoin] {
        &self.joins
    }
}

impl ClickHouseStore {
    fn table_schemas(&self) -> Vec<TableSchema> {
        let mut tables: Vec<_> = self.schema.iter().flat_map(physical_tables).collect();
        let mut names: Vec<_> = self.tables.keys().collect();
        names.sort();
        for name in names {
            let facts = &self.tables[name];
            let index = match tables.iter().position(|table| &table.name == name) {
                Some(index) => index,
                None => {
                    tables.push(TableSchema::new(name, []));
                    tables.len() - 1
                }
            };
            let mut columns: Vec<_> = facts.columns.iter().flatten().collect();
            columns.sort();
            tables[index].extend(
                columns
                    .into_iter()
                    .map(|column| (column.clone(), facts.column_types.get(column).copied())),
            );
        }
        tables
    }
}

fn physical_tables(schema: &Schema) -> impl Iterator<Item = TableSchema> + '_ {
    schema
        .tables()
        .iter()
        .chain(schema.auxiliary_tables().iter().map(|table| &table.table))
        .map(|table| {
            TableSchema::new(
                &table.name,
                table
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), schema::data_type(&column.storage_type))),
            )
        })
}
