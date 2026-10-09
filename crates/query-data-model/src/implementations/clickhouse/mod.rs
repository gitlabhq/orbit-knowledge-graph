pub mod storage;

use std::collections::{BTreeSet, HashMap, HashSet};

use super::{PropertyBackendFacts, derive_property_backend_facts};
use crate::{
    DataModelError, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey,
    DenormalizedProperty, Endpoint, EntityId, ForeignKey, GraphCatalog, PathColumn, PropertyId,
    PropertyRealization, QueryBackendCatalog, RelationshipId, TraversalPathLookup,
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
    fn from_storage(
        table: &storage::Table,
        entity: Option<EntityId>,
        path_columns: Vec<PathColumn>,
        path_scopable: bool,
    ) -> Self {
        Self {
            name: table.name.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| ontology::denormalized::copies(&column.name))
                .map(|column| column.name.trim_matches('`').to_string())
                .collect(),
            column_types: table
                .column_types
                .iter()
                .filter(|(name, _)| ontology::denormalized::copies(name))
                .map(|(name, data_type)| (name.clone(), *data_type))
                .collect(),
            sort_key: table.sort_key.clone(),
            entity,
            path_columns,
            path_scopable,
        }
    }
}

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
    tables: HashMap<String, TableLayout>,
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
            .iter()
            .filter_map(Option::as_deref)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|name| self.tables.get(name))
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

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        self.table(table)
            .and_then(|layout| layout.column_types.get(column).copied())
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_some_and(|facts| facts.has_text_index)
    }

    fn table_path_scopable(&self, table: &str) -> bool {
        self.table(table).is_some_and(|layout| layout.path_scopable)
    }

    fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]> {
        self.table(table)
            .map(|layout| layout.path_columns.as_slice())
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

impl ClickHouseCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        let storage = storage::StorageCatalog::derive(ontology)?;
        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let mut property_facts = derive_property_backend_facts(ontology, graph)?;
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
            let text_indexed = ontology.text_indexed_columns(&node.name);
            for field in &node.fields {
                let property_id = graph.property_id(entity_id, &field.name).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "property",
                        name: format!("{}.{}", node.name, field.name),
                    }
                })?;
                property_facts[property_id.index()].realization = Some(match &field.source {
                    ontology::FieldSource::DatabaseColumn(_) => PropertyRealization::Stored {
                        column: field.name.clone(),
                    },
                    ontology::FieldSource::Virtual(source) => {
                        PropertyRealization::Virtual(source.clone())
                    }
                });
                property_facts[property_id.index()].has_text_index =
                    text_indexed.contains(&field.name.as_str());
                if node.default_columns.iter().any(|name| name == &field.name) {
                    default_properties.push(property_id);
                }
            }
            if let Some(property) =
                graph.property_id(entity_id, ontology::constants::DEFAULT_PRIMARY_KEY)
            {
                property_facts[property.index()]
                    .realization
                    .get_or_insert_with(|| PropertyRealization::Stored {
                        column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                    });
            }
            entities[entity_id.index()] = Some(EntityLayout {
                table: node.destination_table.clone(),
                has_traversal_path: node.has_traversal_path,
                global: node.global,
                default_properties,
            });
            tables.insert(
                node.destination_table.clone(),
                TableLayout::from_storage(
                    storage
                        .table(&node.destination_table)
                        .expect("derived node storage"),
                    Some(entity_id),
                    (!node.global)
                        .then(|| PathColumn {
                            name: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                            entity: Some(entity_id),
                        })
                        .into_iter()
                        .collect(),
                    node.has_traversal_path
                        && !node.global
                        && node.sort_key.first().map(String::as_str)
                            == Some(ontology::constants::TRAVERSAL_PATH_COLUMN),
                ),
            );
        }

        for table_name in ontology.edge_tables() {
            tables.insert(
                table_name.to_string(),
                TableLayout::from_storage(
                    storage.table(table_name).expect("derived edge storage"),
                    None,
                    vec![PathColumn {
                        name: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                        entity: None,
                    }],
                    false,
                ),
            );
        }

        let mut relationships = vec![None; graph.relationships().count()];
        let mut variants = vec![None; graph.variants().count()];
        for relationship in graph.relationships() {
            for edge in storage
                .edge_routes()
                .iter()
                .filter(|edge| edge.relationship == relationship.name)
            {
                if edge.source.is_empty() || edge.target.is_empty() {
                    continue;
                }
                let source = graph.entity_id(&edge.source).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "source entity",
                        name: edge.source.clone(),
                    }
                })?;
                let target = graph.entity_id(&edge.target).ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "target entity",
                        name: edge.target.clone(),
                    }
                })?;
                let variant_id = graph
                    .variant_id(relationship.id, source, target)
                    .ok_or_else(|| {
                        DataModelError::Invalid(format!(
                            "missing variant {}({}->{})",
                            relationship.name, edge.source, edge.target
                        ))
                    })?;
                let foreign_key = edge.foreign_key.as_deref().and_then(|column| {
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
            relationships[relationship.id.index()] = storage
                .relationship_tables(&relationship.name)
                .and_then(|tables| tables.first())
                .cloned();
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

        for join in storage.joins() {
            let path_columns = join
                .sources
                .iter()
                .filter_map(|source| {
                    Some(PathColumn {
                        name: source.path_column.clone()?,
                        entity: entities.iter().enumerate().find_map(|(entity, layout)| {
                            (layout.as_ref()?.table == source.table).then_some(EntityId(entity))
                        }),
                    })
                })
                .collect();
            let mut layout = TableLayout::from_storage(
                storage.table(&join.table).expect("derived join storage"),
                None,
                path_columns,
                true,
            );
            layout.column_types.clear();
            tables.insert(join.table.clone(), layout);
        }

        Ok(ClickHouseCatalog {
            default_edge_table: ontology.edge_table().to_string(),
            entities,
            relationships,
            variants,
            property_facts,
            tables,
            denormalized: DenormalizedCatalog::new(denormalized),
            traversal_path_lookups,
        })
    }
}
