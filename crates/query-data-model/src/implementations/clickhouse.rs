use std::collections::{BTreeSet, HashMap, HashSet};

use super::GitLabAuthzCatalog;
use crate::{
    Backend, DataModelError, DenormalizedCatalog, EntityId, ForeignKey, GraphCatalog, PropertyId,
    QueryBackendCatalog, RelationshipId, RelationshipVariantId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct TableLayout {
    pub name: String,
    pub columns: HashSet<String>,
    pub column_types: HashMap<String, ontology::DataType>,
    pub sort_key: Vec<String>,
    pub entity: Option<EntityId>,
    pub path_columns: Vec<PathColumn>,
    pub path_scopable: bool,
}

impl TableLayout {
    pub fn minimum_access_level(&self, authz: &GitLabAuthzCatalog) -> u32 {
        self.path_columns
            .iter()
            .filter_map(|path| {
                path.entity
                    .and_then(|entity| authz.entity(entity))
                    .map(|policy| policy.required_access_level)
            })
            .max()
            .unwrap_or(20)
    }
}

#[derive(Debug, Clone)]
pub struct PathColumn {
    pub name: String,
    pub entity: Option<EntityId>,
}

#[derive(Debug, Clone)]
pub struct EntityLayout {
    pub table: String,
    pub has_traversal_path: bool,
    pub global: bool,
    pub default_properties: Vec<PropertyId>,
}

#[derive(Debug, Clone)]
pub struct VariantLayout {
    pub foreign_key: Option<PropertyId>,
}

#[derive(Debug)]
pub struct ClickHouseCatalog {
    default_edge_table: String,
    entities: HashMap<EntityId, EntityLayout>,
    relationships: HashMap<RelationshipId, String>,
    variants: HashMap<RelationshipVariantId, VariantLayout>,
    properties: HashMap<PropertyId, String>,
    tables: HashMap<String, TableLayout>,
    denormalized: DenormalizedCatalog,
    text_indexes: HashSet<PropertyId>,
    traversal_path_lookups: HashMap<(EntityId, ontology::TraversalPathKind), TraversalPathLookup>,
}

impl ClickHouseCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&EntityLayout> {
        self.entities.get(&id)
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        self.relationships.get(&id).map(String::as_str)
    }

    pub fn variant(&self, id: RelationshipVariantId) -> Option<&VariantLayout> {
        self.variants.get(&id)
    }

    pub fn property_column(&self, id: PropertyId) -> Option<&str> {
        self.properties.get(&id).map(String::as_str)
    }

    pub fn table(&self, name: &str) -> Option<&TableLayout> {
        self.tables.get(name)
    }

    pub fn table_for_entity(&self, entity: EntityId) -> Option<&TableLayout> {
        self.entity(entity)
            .and_then(|layout| self.table(&layout.table))
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.tables.values()
    }

    pub fn edge_tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.relationships
            .values()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|name| self.tables.get(name))
    }

    pub fn default_edge_table(&self) -> &str {
        &self.default_edge_table
    }

    pub fn has_text_index(&self, property: PropertyId) -> bool {
        self.text_indexes.contains(&property)
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
    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.entity(entity).map(|layout| layout.table.as_str())
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, entity: EntityId) -> bool {
        self.entity(entity).is_some_and(|layout| layout.global)
    }

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        ClickHouseCatalog::property_column(self, property)
    }

    fn default_edge_table(&self) -> &str {
        ClickHouseCatalog::default_edge_table(self)
    }

    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        ClickHouseCatalog::relationship_table(self, relationship)
    }

    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        if relationships.is_empty() {
            return self.edge_tables().map(|table| table.name.clone()).collect();
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
            let property = self.variant(variant)?.foreign_key?;
            Some(ForeignKey {
                holder: graph.property(property).entity,
                column: self.property_column(property)?.to_string(),
            })
        });
        let first = foreign_keys.next()??;
        foreign_keys
            .all(|foreign_key| {
                foreign_key.is_some_and(|foreign_key| {
                    foreign_key.holder == first.holder && foreign_key.column == first.column
                })
            })
            .then_some(first)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.table(table).map(|layout| &layout.columns)
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.table(table).map(|layout| layout.sort_key.as_slice())
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

pub struct ClickHouse;

impl Backend for ClickHouse {
    type Catalog = ClickHouseCatalog;

    fn derive(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self::Catalog, DataModelError> {
        let mut entities = HashMap::new();
        let mut properties = HashMap::new();
        let mut tables = HashMap::new();
        let mut text_indexes = HashSet::new();

        for node in ontology.nodes() {
            let entity_id =
                graph
                    .entity_id(&node.name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "entity",
                        name: node.name.clone(),
                    })?;
            let mut default_properties = Vec::new();
            for field in &node.fields {
                let property_id = graph.property_id(entity_id, &field.name).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "property",
                        name: format!("{}.{}", node.name, field.name),
                    }
                })?;
                if field.column_name().is_some() {
                    properties.insert(property_id, field.name.clone());
                }
                if node.default_columns.iter().any(|name| name == &field.name) {
                    default_properties.push(property_id);
                }
                if ontology
                    .text_index_tokenizer(&node.name, &field.name)
                    .is_some()
                {
                    text_indexes.insert(property_id);
                }
            }
            if let Some(property) =
                graph.property_id(entity_id, ontology::constants::DEFAULT_PRIMARY_KEY)
            {
                properties
                    .entry(property)
                    .or_insert_with(|| ontology::constants::DEFAULT_PRIMARY_KEY.to_string());
            }
            entities.insert(
                entity_id,
                EntityLayout {
                    table: node.destination_table.clone(),
                    has_traversal_path: node.has_traversal_path,
                    global: node.global,
                    default_properties,
                },
            );
            tables.insert(
                node.destination_table.clone(),
                TableLayout {
                    name: node.destination_table.clone(),
                    columns: node
                        .storage
                        .columns
                        .iter()
                        .map(|column| column.name.trim_matches('`').to_string())
                        .collect(),
                    column_types: node
                        .fields
                        .iter()
                        .filter_map(|field| {
                            field
                                .column_name()
                                .map(|_| (field.name.clone(), field.data_type))
                        })
                        .collect(),
                    sort_key: node.sort_key.clone(),
                    entity: Some(entity_id),
                    path_columns: (!node.global)
                        .then(|| PathColumn {
                            name: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                            entity: Some(entity_id),
                        })
                        .into_iter()
                        .collect(),
                    path_scopable: node.has_traversal_path
                        && !node.global
                        && node.sort_key.first().map(String::as_str)
                            == Some(ontology::constants::TRAVERSAL_PATH_COLUMN),
                },
            );
        }

        for table_name in ontology.edge_tables() {
            let config = ontology.edge_table_config(table_name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "edge table",
                    name: table_name.to_string(),
                }
            })?;
            let columns = config
                .storage
                .columns
                .iter()
                .chain(config.storage.denormalized_columns.iter())
                .map(|column| column.name.trim_matches('`').to_string())
                .collect();
            let column_types = config
                .columns
                .iter()
                .map(|column| (column.name.trim_matches('`').to_string(), column.data_type))
                .collect();
            tables.insert(
                table_name.to_string(),
                TableLayout {
                    name: table_name.to_string(),
                    columns,
                    column_types,
                    sort_key: config.sort_key.clone(),
                    entity: None,
                    path_columns: vec![PathColumn {
                        name: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                        entity: None,
                    }],
                    path_scopable: false,
                },
            );
        }

        let mut relationships = HashMap::new();
        let mut variants = HashMap::new();
        for relationship in graph.relationships() {
            let ontology_variants = ontology.get_edge(&relationship.name).unwrap_or_default();
            let table = ontology
                .edge_table_for_relationship(&relationship.name)
                .to_string();
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
                    graph
                        .property_id(source, column)
                        .or_else(|| graph.property_id(target, column))
                });
                variants.insert(variant_id, VariantLayout { foreign_key });
            }
            relationships.insert(relationship.id, table);
        }

        let mut denormalized_columns = HashMap::new();
        let mut denormalized_relationships: HashMap<_, Vec<_>> = HashMap::new();
        for property in ontology.denormalized_properties() {
            let entity = graph.entity_id(&property.node_kind).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "entity",
                    name: property.node_kind.clone(),
                }
            })?;
            if graph.property_id(entity, &property.property_name).is_none() {
                continue;
            }
            let direction = match property.direction {
                ontology::DenormDirection::Source => "source",
                ontology::DenormDirection::Target => "target",
            };
            let key = (
                property.node_kind.clone(),
                property.property_name.clone(),
                direction.to_string(),
            );
            denormalized_columns.insert(
                key.clone(),
                (property.edge_column.clone(), property.tag_key.clone()),
            );
            denormalized_relationships
                .entry(key)
                .or_default()
                .push(property.relationship_kind.clone());
        }

        let mut traversal_path_lookups = HashMap::new();
        for lookup in ontology.traversal_path_lookups() {
            let entity = graph.entity_id(&lookup.entity).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "entity",
                    name: lookup.entity.clone(),
                }
            })?;
            let property = graph
                .property_id(entity, &lookup.key_column)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "property",
                    name: format!("{}.{}", lookup.entity, lookup.key_column),
                })?;
            traversal_path_lookups.insert(
                (entity, lookup.kind),
                TraversalPathLookup {
                    table: lookup.source_table.clone(),
                    property,
                },
            );
        }

        for join in ontology.denormalized_joins() {
            let path_columns = join
                .traversal_path_columns()
                .map(|(index, name)| PathColumn {
                    name,
                    entity: entities.iter().find_map(|(entity, layout)| {
                        (layout.table == join.tables[index].table).then_some(*entity)
                    }),
                })
                .collect();
            let columns = join
                .tables
                .iter()
                .enumerate()
                .flat_map(|(index, table)| {
                    tables
                        .get(&table.table)
                        .into_iter()
                        .flat_map(move |layout| {
                            layout
                                .columns
                                .iter()
                                .map(move |column| join.column_for(index, column))
                        })
                })
                .collect();
            tables.insert(
                join.table.clone(),
                TableLayout {
                    name: join.table.clone(),
                    columns,
                    column_types: HashMap::new(),
                    sort_key: join.sort_key(),
                    entity: None,
                    path_columns,
                    path_scopable: true,
                },
            );
        }

        Ok(ClickHouseCatalog {
            default_edge_table: ontology.edge_table().to_string(),
            entities,
            relationships,
            variants,
            properties,
            tables,
            denormalized: DenormalizedCatalog {
                columns: denormalized_columns,
                relationships: denormalized_relationships,
            },
            text_indexes,
            traversal_path_lookups,
        })
    }
}
