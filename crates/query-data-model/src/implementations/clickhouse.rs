pub mod storage;
use crate::storage::TableLayout;
use std::collections::{BTreeSet, HashMap};
use storage::{ClickHouseColumn, edge_columns, node_columns};

use super::{PropertyBackendFacts, derive_property_backend_facts};
use crate::{
    DataModelError, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey,
    DenormalizedProperty, Endpoint, EntityId, ForeignKey, GraphCatalog, PropertyId,
    PropertyRealization, QueryBackendCatalog, RelationshipId, TableId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct EntityLayout {
    pub table: TableId,
    pub has_traversal_path: bool,
    pub global: bool,
    pub default_properties: Vec<PropertyId>,
}

#[derive(Debug)]
pub struct ClickHouseCatalog {
    default_edge_table: TableId,
    entities: Vec<Option<EntityLayout>>,
    relationships: Vec<Option<TableId>>,
    variants: Vec<Option<ForeignKey>>,
    property_facts: Vec<PropertyBackendFacts>,
    storage: crate::storage::StorageCatalog<ClickHouseColumn>,
    scopes: HashMap<TableId, TableScope>,
    denormalized: DenormalizedCatalog,
    traversal_path_lookups: HashMap<(EntityId, ontology::TraversalPathKind), TraversalPathLookup>,
}

#[derive(Debug)]
struct TableScope {
    columns: Vec<crate::PathColumn>,
    scopable: bool,
}

impl ClickHouseCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&EntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        QueryBackendCatalog::relationship_table(self, id)
    }

    pub fn property_column(&self, id: PropertyId) -> Option<&str> {
        QueryBackendCatalog::property_column(self, id)
    }

    pub fn table(&self, name: &str) -> Option<&TableLayout<ClickHouseColumn>> {
        QueryBackendCatalog::table(self, name)
    }

    pub fn table_for_entity(&self, entity: EntityId) -> Option<&TableLayout<ClickHouseColumn>> {
        self.entity(entity)
            .map(|layout| self.storage.table(layout.table))
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout<ClickHouseColumn>> {
        self.storage.tables()
    }

    pub fn edge_tables(&self) -> impl Iterator<Item = &TableLayout<ClickHouseColumn>> {
        self.relationships
            .iter()
            .filter_map(|id| *id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|id| self.storage.table(id))
    }

    pub fn default_edge_table(&self) -> &str {
        self.storage.table(self.default_edge_table).name()
    }

    pub fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.traversal_path_lookups.get(&(entity, kind))
    }
}

impl QueryBackendCatalog for ClickHouseCatalog {
    type ColumnStorage = ClickHouseColumn;
    fn storage(&self) -> &crate::storage::StorageCatalog<ClickHouseColumn> {
        &self.storage
    }
    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        let column = self
            .storage
            .column_ref(self.storage.table_id(table)?, column)?;
        self.storage.column(column).storage().query_type
    }
    fn table_path_scopable(&self, table: &str) -> bool {
        self.storage
            .table_id(table)
            .and_then(|id| self.scopes.get(&id))
            .is_some_and(|scope| scope.scopable)
    }
    fn table_path_columns(&self, table: &str) -> Option<&[crate::PathColumn]> {
        self.scopes
            .get(&self.storage.table_id(table)?)
            .map(|scope| scope.columns.as_slice())
    }
    fn derive(ontology: &ontology::Ontology, graph: &GraphCatalog) -> Result<Self, DataModelError> {
        Self::from_ontology(ontology, graph)
    }

    fn entity_table_id(&self, entity: EntityId) -> Option<TableId> {
        self.entity(entity).map(|layout| layout.table)
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, entity: EntityId) -> bool {
        self.entity(entity).is_some_and(|layout| layout.global)
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.entity(entity)
            .map(|layout| layout.default_properties.as_slice())
            .unwrap_or_default()
    }

    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization> {
        self.property_facts
            .get(property.index())
            .and_then(|facts| facts.realization.as_ref())
    }

    fn property_selectivity(&self, property: PropertyId) -> Option<ontology::FieldSelectivity> {
        self.property_facts
            .get(property.index())
            .map(|facts| facts.selectivity)
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_some_and(|facts| facts.has_text_index)
    }

    fn default_edge_table_id(&self) -> TableId {
        self.default_edge_table
    }

    fn relationship_table_id(&self, relationship: RelationshipId) -> Option<TableId> {
        *self.relationships.get(relationship.index())?
    }

    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        if relationships.is_empty() {
            return self
                .edge_tables()
                .map(|table| table.name().to_owned())
                .collect();
        }
        relationships
            .iter()
            .filter_map(|relationship| self.relationship_table(*relationship))
            .map(String::from)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn foreign_key(
        &self,
        graph: &GraphCatalog,
        relationships: &[RelationshipId],
        source: EntityId,
        target: EntityId,
    ) -> Option<ForeignKey> {
        let mut foreign_keys = relationships.iter().map(|relationship| {
            let variant = graph.variant_id(*relationship, source, target)?;
            *self.variants.get(variant.index())?
        });
        let first = foreign_keys.next()??;
        foreign_keys
            .all(|foreign_key| foreign_key == Some(first))
            .then_some(first)
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        ClickHouseCatalog::traversal_path_lookup(self, entity, kind)
    }
}

impl ClickHouseCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        let mut tables = HashMap::new();

        for node in ontology.nodes() {
            let table =
                TableLayout::new(&node.destination_table, node_columns(node), &node.sort_key)?
                    .versioned(
                        ontology::VERSION_COLUMN,
                        Some((ontology::DELETED_COLUMN, !node.storage.version_only_engine)),
                    )?;
            tables.insert(node.destination_table.clone(), table);
        }

        for table_name in ontology.edge_tables() {
            let config = ontology.edge_table_config(table_name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "edge table",
                    name: table_name.to_string(),
                }
            })?;
            let columns = edge_columns(config);
            let table = TableLayout::new(table_name, columns, &config.sort_key)?.versioned(
                ontology::VERSION_COLUMN,
                Some((ontology::DELETED_COLUMN, true)),
            )?;
            tables.insert(table_name.to_string(), table);
        }

        let mut variants = vec![None; graph.variants().count()];
        for relationship in graph.relationships() {
            let ontology_variants = ontology.get_edge(&relationship.name).unwrap_or_default();
            for edge in ontology_variants {
                if edge.source_kind.is_empty() || edge.target_kind.is_empty() {
                    continue;
                }
                let source = graph.entity_id(&edge.source_kind).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "source entity",
                        name: edge.source_kind.clone(),
                    }
                })?;
                let target = graph.entity_id(&edge.target_kind).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "target entity",
                        name: edge.target_kind.clone(),
                    }
                })?;
                let variant_id = graph
                    .variant_id(relationship.id, source, target)
                    .ok_or_else(|| {
                        DataModelError::Invalid(format!(
                            "missing variant {}({}->{})",
                            relationship.name, edge.source_kind, edge.target_kind
                        ))
                    })?;
                let foreign_key = edge.fk_column.as_deref().and_then(|column| {
                    let (holder, property, referenced) = match graph.property_id(source, column) {
                        Some(property) => (Endpoint::Source, property, target),
                        None => (Endpoint::Target, graph.property_id(target, column)?, source),
                    };
                    Some(ForeignKey {
                        holder,
                        property,
                        referenced_key: graph
                            .property_id(referenced, ontology::constants::DEFAULT_PRIMARY_KEY)?,
                    })
                });
                variants[variant_id.index()] = foreign_key;
            }
        }

        let mut denormalized = HashMap::new();
        for property in ontology.denormalized_properties() {
            let entity = graph.entity_id(&property.node_kind).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "entity",
                    name: property.node_kind.clone(),
                }
            })?;
            let Some(property_id) = graph.property_id(entity, &property.property_name) else {
                continue;
            };
            let direction = match property.direction {
                ontology::DenormDirection::Source => DenormalizedDirection::Source,
                ontology::DenormDirection::Target => DenormalizedDirection::Target,
            };
            let Some(relationship) = graph.relationship_id(&property.relationship_kind) else {
                continue;
            };
            denormalized
                .entry(DenormalizedKey {
                    property: property_id,
                    direction,
                })
                .and_modify(|layout: &mut DenormalizedProperty| {
                    layout.relationships.push(relationship)
                })
                .or_insert_with(|| DenormalizedProperty {
                    edge_column: property.edge_column.clone(),
                    tag_key: property.tag_key.clone(),
                    relationships: vec![relationship],
                });
        }

        for join in ontology.denormalized_joins() {
            let columns = storage::denormalized_columns(join, |index| {
                tables[&join.tables[index].table].columns()
            });
            let table = TableLayout::new(&join.table, columns, &join.sort_key())?.versioned(
                ontology::VERSION_COLUMN,
                Some((ontology::DELETED_COLUMN, true)),
            )?;
            tables.insert(join.table.clone(), table);
        }

        let storage = crate::storage::StorageCatalog::new(tables.into_values())?;
        let mut scopes = HashMap::new();
        for node in ontology.nodes() {
            let table = storage.resolve_table(&node.destination_table)?;
            let columns = if node.global {
                vec![]
            } else {
                vec![crate::PathColumn {
                    column: storage
                        .table(table)
                        .column_id(ontology::TRAVERSAL_PATH_COLUMN)?,
                    entity: graph.entity_id(&node.name),
                }]
            };
            scopes.insert(
                table,
                TableScope {
                    columns,
                    scopable: node.has_traversal_path
                        && !node.global
                        && node
                            .sort_key
                            .first()
                            .is_some_and(|column| column == ontology::TRAVERSAL_PATH_COLUMN),
                },
            );
        }
        for name in ontology.edge_tables() {
            let table = storage.resolve_table(name)?;
            scopes.insert(
                table,
                TableScope {
                    columns: vec![crate::PathColumn {
                        column: storage
                            .table(table)
                            .column_id(ontology::TRAVERSAL_PATH_COLUMN)?,
                        entity: None,
                    }],
                    scopable: false,
                },
            );
        }
        for join in ontology.denormalized_joins() {
            let table = storage.resolve_table(&join.table)?;
            let columns = join
                .traversal_path_columns()
                .map(|(index, name)| {
                    let entity = ontology
                        .nodes()
                        .find(|node| node.destination_table == join.tables[index].table)
                        .and_then(|node| graph.entity_id(&node.name));
                    Ok(crate::PathColumn {
                        column: storage.table(table).column_id(&name)?,
                        entity,
                    })
                })
                .collect::<Result<_, DataModelError>>()?;
            scopes.insert(
                table,
                TableScope {
                    columns,
                    scopable: true,
                },
            );
        }
        let entities = graph
            .entities()
            .map(|entity| {
                let node = ontology.get_node(&entity.name).expect("catalog entity");
                Ok(Some(EntityLayout {
                    table: storage.resolve_table(&node.destination_table)?,
                    has_traversal_path: node.has_traversal_path,
                    global: node.global,
                    default_properties: entity
                        .properties
                        .iter()
                        .copied()
                        .filter(|property| {
                            node.default_columns
                                .contains(&graph.property(*property).name)
                        })
                        .collect(),
                }))
            })
            .collect::<Result<_, DataModelError>>()?;
        let relationships = graph
            .relationships()
            .map(|relationship| {
                storage
                    .resolve_table(ontology.edge_table_for_relationship(&relationship.name))
                    .map(Some)
            })
            .collect::<Result<_, _>>()?;
        let default_edge_table = storage.resolve_table(ontology.edge_table())?;
        let property_facts = derive_property_backend_facts(ontology, graph, &storage, false)?;
        let traversal_path_lookups = ontology
            .traversal_path_lookups()
            .iter()
            .map(|lookup| {
                let entity = graph.entity_id(&lookup.entity).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "entity",
                        name: lookup.entity.clone(),
                    }
                })?;
                let key = storage.resolve_column(&lookup.source_table, &lookup.key_column)?;
                Ok(((entity, lookup.kind), TraversalPathLookup { key }))
            })
            .collect::<Result<_, DataModelError>>()?;
        Ok(ClickHouseCatalog {
            default_edge_table,
            entities,
            relationships,
            variants,
            property_facts,
            storage,
            scopes,
            denormalized: DenormalizedCatalog::new(denormalized),
            traversal_path_lookups,
        })
    }
}
