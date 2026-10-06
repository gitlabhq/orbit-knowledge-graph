use crate::storage::{RowSemantics, TableLayout, remote_edge_columns, remote_node_columns};
use std::collections::{BTreeSet, HashMap};

use super::{PropertyBackendFacts, derive_property_backend_facts};
use crate::{
    DataModelError, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey,
    DenormalizedProperty, Endpoint, EntityId, ForeignKey, GraphCatalog, PropertyId,
    PropertyRealization, QueryBackendCatalog, RelationshipId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct EntityLayout {
    pub table: String,
    pub has_traversal_path: bool,
    pub global: bool,
    pub default_properties: Vec<PropertyId>,
}

#[derive(Debug)]
pub struct ClickHouseCatalog {
    default_edge_table: String,
    entities: Vec<Option<EntityLayout>>,
    relationships: Vec<Option<String>>,
    variants: Vec<Option<ForeignKey>>,
    property_facts: Vec<PropertyBackendFacts>,
    storage: crate::storage::StorageCatalog,
    denormalized: DenormalizedCatalog,
    traversal_path_lookups: HashMap<(EntityId, ontology::TraversalPathKind), TraversalPathLookup>,
}

impl ClickHouseCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&EntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        self.relationships.get(id.index())?.as_deref()
    }

    pub fn property_column(&self, id: PropertyId) -> Option<&str> {
        QueryBackendCatalog::property_column(self, id)
    }

    pub fn table(&self, name: &str) -> Option<&TableLayout> {
        QueryBackendCatalog::table(self, name)
    }

    pub fn table_for_entity(&self, entity: EntityId) -> Option<&TableLayout> {
        self.entity(entity)
            .and_then(|layout| self.table(&layout.table))
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.storage.tables()
    }

    pub fn edge_tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.relationships
            .iter()
            .filter_map(Option::as_deref)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|name| self.table(name))
    }

    pub fn default_edge_table(&self) -> &str {
        &self.default_edge_table
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
    fn storage(&self) -> &crate::storage::StorageCatalog {
        &self.storage
    }
    fn derive(ontology: &ontology::Ontology, graph: &GraphCatalog) -> Result<Self, DataModelError> {
        Self::from_ontology(ontology, graph)
    }

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
        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let mut tables = HashMap::new();

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
                if node.default_columns.iter().any(|name| name == &field.name) {
                    default_properties.push(property_id);
                }
            }
            entities[entity_id.index()] = Some(EntityLayout {
                table: node.destination_table.clone(),
                has_traversal_path: node.has_traversal_path,
                global: node.global,
                default_properties,
            });
            let mut table = TableLayout::new(
                &node.destination_table,
                remote_node_columns(node),
                &node.sort_key,
                RowSemantics::Versioned {
                    engine_deletes: !node.storage.version_only_engine,
                },
            )?;
            table.entity = Some(entity_id);
            if !node.global {
                table.add_path_column(ontology::TRAVERSAL_PATH_COLUMN, Some(entity_id))?;
            }
            table.path_scopable = node.has_traversal_path
                && !node.global
                && table
                    .sort_columns()
                    .next()
                    .is_some_and(|column| column.name == ontology::TRAVERSAL_PATH_COLUMN);
            tables.insert(node.destination_table.clone(), table);
        }

        for table_name in ontology.edge_tables() {
            let config = ontology.edge_table_config(table_name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "edge table",
                    name: table_name.to_string(),
                }
            })?;
            let columns = remote_edge_columns(config);
            let mut table = TableLayout::new(
                table_name,
                columns,
                &config.sort_key,
                RowSemantics::Versioned {
                    engine_deletes: true,
                },
            )?;
            table.add_path_column(ontology::TRAVERSAL_PATH_COLUMN, None)?;
            tables.insert(table_name.to_string(), table);
        }

        let mut relationships = vec![None; graph.relationships().count()];
        let mut variants = vec![None; graph.variants().count()];
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
            relationships[relationship.id.index()] = Some(table);
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
            let columns = crate::storage::denormalized_columns(join, |index| {
                &tables[&join.tables[index].table].columns
            });
            let mut table = TableLayout::new(
                &join.table,
                columns,
                &join.sort_key(),
                RowSemantics::Versioned {
                    engine_deletes: true,
                },
            )?;
            for (index, name) in join.traversal_path_columns() {
                table.add_path_column(&name, tables[&join.tables[index].table].entity)?;
            }
            table.path_scopable = true;
            tables.insert(join.table.clone(), table);
        }

        let storage = crate::storage::StorageCatalog::new(tables.into_values())?;
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
            default_edge_table: ontology.edge_table().to_string(),
            entities,
            relationships,
            variants,
            property_facts,
            storage,
            denormalized: DenormalizedCatalog::new(denormalized),
            traversal_path_lookups,
        })
    }
}
