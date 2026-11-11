use std::collections::{HashMap, HashSet};

use super::ClickHouse;
use super::schema::{self, Schema};
use super::spec::{ClickHouseOptions, ClickHouseSource, ColumnSpec};
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{ReadPaths, RelationalFacts, RelationalStore};
use crate::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, TraversalPathLookup,
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

#[derive(Debug)]
pub struct ClickHouseStore {
    pub relational: RelationalStore,
    pub read_paths: ReadPaths,
    schema: Option<Schema>,
}

impl ClickHouseStore {
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }
}

impl RelationalFacts for ClickHouseStore {
    fn relational(&self) -> &RelationalStore {
        &self.relational
    }

    fn read_paths(&self) -> Option<&ReadPaths> {
        Some(&self.read_paths)
    }
}

impl StorageBackend for ClickHouse {
    const KIND: &'static str = "clickhouse";
    type Options = ClickHouseOptions;
    type SourceOptions = ClickHouseSource;
    type ColumnOptions = Option<ColumnSpec>;
    type Store = ClickHouseStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> ClickHouseStore {
        let options = input.options;
        let edge_tables: Vec<(&str, bool)> = options
            .edge_tables
            .iter()
            .map(|(name, table)| (name.as_str(), table.shared))
            .collect();
        let mut relational = RelationalStore::resolve(
            &input,
            options.version_column.as_ref(),
            options.deleted_column.as_ref(),
            &edge_tables,
            diagnostics,
        );
        let mut read_paths = ReadPaths::default();
        annotate_sources(&input, &mut read_paths, diagnostics);
        resolve_denormalized(&input, &mut read_paths, diagnostics);
        resolve_joins(&input, &mut read_paths, diagnostics);
        let schema = schema::resolve(&input, &mut relational, &mut read_paths, diagnostics);
        relational.check_attribute_columns(&input, diagnostics);
        ClickHouseStore {
            relational,
            read_paths,
            schema,
        }
    }
}

fn annotate_sources(
    input: &StoreInput<'_, ClickHouse>,
    paths: &mut ReadPaths,
    diagnostics: &mut Diagnostics,
) {
    let graph = input.graph;
    let store = input.store;
    for source in &input.entities {
        let entity = source.entity;
        let name = &graph.entity(entity).name;
        let stored = |property: &str, diagnostics: &mut Diagnostics| {
            let id = graph
                .property_id(entity, property)
                .filter(|id| store.column(*id).is_some());
            diagnostics.require(id, source.package, || {
                format!("store {} does not store {name}.{property}", store.name)
            })
        };
        let options = source.options;
        let shared = &input.options.columns;
        if let Some(indexes) = &options.indexes {
            schema::check_indexes(
                source.package,
                &format!("store {} {name}", store.name),
                indexes,
                |target| {
                    shared.contains_key(target)
                        || graph
                            .property_id(entity, target)
                            .is_some_and(|id| store.column(id).is_some())
                },
                diagnostics,
            );
        }
        for property in &options.epoch_nulls {
            stored(property, diagnostics);
        }
        let fans_out = options
            .high_fan_out
            .iter()
            .filter_map(|property| stored(property, diagnostics))
            .count()
            > 0;
        if fans_out {
            paths.high_fan_out.insert(entity);
        }
        for (kind, lookup) in &options.path_lookups {
            let property = diagnostics.require(
                graph.property_id(entity, &lookup.key),
                source.package,
                || format!("{name} path lookup key {} is not a property", lookup.key),
            );
            if let Some(property) = property {
                paths.path_lookups.insert(
                    (entity, *kind),
                    TraversalPathLookup {
                        table: lookup.table.clone(),
                        property,
                    },
                );
            }
        }
    }
}

fn resolve_denormalized(
    input: &StoreInput<'_, ClickHouse>,
    paths: &mut ReadPaths,
    diagnostics: &mut Diagnostics,
) {
    let graph = input.graph;
    let package = &input.store.package;
    let mut properties = HashMap::new();
    let mut declared = Vec::new();
    for spec in &input.options.denormalized {
        let Some(entity) = diagnostics.require(graph.entity_id(&spec.entity), package, || {
            format!("denormalized property names unknown entity {}", spec.entity)
        }) else {
            continue;
        };
        let label = format!("denormalized property {}.{}", spec.entity, spec.property);
        let property =
            diagnostics.require(graph.property_id(entity, &spec.property), package, || {
                format!("{label} names an unknown property")
            });
        let relationship =
            diagnostics.require(graph.relationship_id(&spec.relationship), package, || {
                format!("{label} names unknown relationship {}", spec.relationship)
            });
        let (Some(property), Some(relationship)) = (property, relationship) else {
            continue;
        };
        let direction = spec.side;
        let key = DenormalizedKey {
            property,
            direction,
        };
        declared.push((relationship, key));
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
            .entry(key)
            .and_modify(|existing: &mut DenormalizedProperty| existing.variants.extend(&variants))
            .or_insert_with(|| DenormalizedProperty {
                edge_column: spec.column.clone(),
                tag_key: spec.tag.clone(),
                variants,
                nullable: spec.nullable,
            });
    }
    paths.denormalized = DenormalizedCatalog::new(properties, declared);
}

fn resolve_joins(
    input: &StoreInput<'_, ClickHouse>,
    paths: &mut ReadPaths,
    diagnostics: &mut Diagnostics,
) {
    let graph = input.graph;
    let package = &input.store.package;
    for (table, spec) in &input.options.joins {
        let nodes: Option<Vec<MaterializedNode>> = spec
            .nodes
            .iter()
            .map(|node| {
                let entity = diagnostics.require(graph.entity_id(&node.entity), package, || {
                    format!("join {table} names unknown entity {}", node.entity)
                })?;
                let source = diagnostics.require(spec.sources.get(node.source), package, || {
                    format!(
                        "join {table} reads {} from unknown source {}",
                        node.entity, node.source
                    )
                })?;
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
                let mut node = |slot: usize| {
                    diagnostics.require(nodes.get(slot), package, || {
                        let name = &relationship.relationship;
                        format!("join {table} relationship {name} names unknown node {slot}")
                    })
                };
                let (source, target) = (node(relationship.source)?, node(relationship.target)?);
                let edge = match relationship.edge {
                    Some(edge) => {
                        let source =
                            diagnostics.require(spec.sources.get(edge), package, || {
                                format!("join {table} reads an edge from unknown source {edge}")
                            })?;
                        source.columns.clone()
                    }
                    None => Default::default(),
                };
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
                    columns: edge,
                })
            })
            .collect();
        let Some(relationships) = relationships else {
            continue;
        };
        paths.joins.push(MaterializedJoin {
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
