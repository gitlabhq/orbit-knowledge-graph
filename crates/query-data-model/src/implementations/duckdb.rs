use crate::storage::TableLayout;
use std::collections::HashMap;

use super::{PropertyBackendFacts, derive_property_backend_facts};
use crate::{
    DataModelError, DenormalizedCatalog, EntityId, GraphCatalog, PropertyId, PropertyRealization,
    QueryBackendCatalog, RelationshipId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct DuckDbEntityLayout {
    pub table: String,
    pub default_properties: Vec<PropertyId>,
    pub has_traversal_path: bool,
}

#[derive(Debug)]
pub struct DuckDbCatalog {
    edge_table: String,
    storage: crate::storage::StorageCatalog,
    entities: Vec<Option<DuckDbEntityLayout>>,
    property_facts: Vec<PropertyBackendFacts>,
    relationships: Vec<String>,
    denormalized: DenormalizedCatalog,
}

impl QueryBackendCatalog for DuckDbCatalog {
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

    fn entity_is_global(&self, _entity: EntityId) -> bool {
        false
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

    fn has_text_index(&self, _property: PropertyId) -> bool {
        false
    }

    fn default_edge_table(&self) -> &str {
        self.edge_table()
    }

    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        DuckDbCatalog::relationship_table(self, relationship)
    }

    fn edge_tables(&self, _relationships: &[RelationshipId]) -> Vec<String> {
        vec![self.edge_table().to_string()]
    }

    fn foreign_key(
        &self,
        _graph: &GraphCatalog,
        _relationships: &[RelationshipId],
        _source: EntityId,
        _target: EntityId,
    ) -> Option<crate::ForeignKey> {
        None
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn traversal_path_lookup(
        &self,
        _entity: EntityId,
        _kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        None
    }
}

impl DuckDbCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&DuckDbEntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        self.relationships.get(id.index()).map(String::as_str)
    }

    pub fn edge_table(&self) -> &str {
        &self.edge_table
    }
}

impl DuckDbCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        let edge_table = ontology
            .local_edge_table_name()
            .unwrap_or_else(|| ontology.edge_table())
            .to_string();
        let mut tables: HashMap<_, _> = crate::storage::local_tables(ontology)
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect();
        tables
            .entry(edge_table.clone())
            .or_insert_with(|| TableLayout::local_edge(&edge_table, ontology.local_edge_columns()));
        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let local_entities = ontology.local_entity_names();
        let entity_names: Vec<_> = if local_entities.is_empty() {
            ontology.node_names().collect()
        } else {
            local_entities
        };
        for entity_name in entity_names {
            let entity_id =
                graph
                    .entity_id(entity_name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "local entity",
                        name: entity_name.to_string(),
                    })?;
            let node =
                ontology
                    .get_node(entity_name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "entity",
                        name: entity_name.to_string(),
                    })?;
            let local_fields = ontology
                .local_entity_fields(entity_name)
                .unwrap_or_else(|| node.fields.iter().collect());
            let has_traversal_path = local_fields
                .iter()
                .any(|field| field.name == ontology::constants::TRAVERSAL_PATH_COLUMN);
            let excluded = ontology
                .local_entity_excludes(entity_name)
                .unwrap_or_default();
            tables
                .entry(node.destination_table.clone())
                .or_insert_with(|| TableLayout::local_node(node, excluded))
                .entity = Some(entity_id);
            entities[entity_id.index()] = Some(DuckDbEntityLayout {
                table: node.destination_table.clone(),
                default_properties: graph.entity(entity_id).properties.clone(),
                has_traversal_path,
            });
        }
        let relationships = graph.relationships().map(|_| edge_table.clone()).collect();
        let storage = crate::storage::StorageCatalog::new(tables.into_values())?;
        let property_facts = derive_property_backend_facts(ontology, graph, &storage, true)?;
        Ok(DuckDbCatalog {
            edge_table,
            storage,
            entities,
            property_facts,
            relationships,
            denormalized: DenormalizedCatalog::default(),
        })
    }
}
