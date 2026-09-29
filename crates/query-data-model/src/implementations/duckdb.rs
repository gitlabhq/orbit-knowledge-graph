use std::collections::{HashMap, HashSet};

use super::{PropertyBackendFacts, derive_property_backend_facts};
use crate::{
    DataModelError, DenormalizedCatalog, EntityId, GraphCatalog, PropertyId, PropertyRealization,
    QueryBackendCatalog, RelationshipId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct DuckDbEntityLayout {
    pub table: String,
    pub default_properties: Vec<PropertyId>,
    pub sort_key: Vec<String>,
    pub has_traversal_path: bool,
}

#[derive(Debug)]
pub struct DuckDbCatalog {
    edge_table: String,
    edge_columns: HashSet<String>,
    edge_column_types: HashMap<String, ontology::DataType>,
    edge_sort_key: Vec<String>,
    entities: Vec<Option<DuckDbEntityLayout>>,
    property_facts: Vec<PropertyBackendFacts>,
    relationships: Vec<String>,
    denormalized: DenormalizedCatalog,
}

impl QueryBackendCatalog for DuckDbCatalog {
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

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        (table == self.edge_table())
            .then(|| self.edge_column_type(column))
            .flatten()
    }

    fn has_text_index(&self, _property: PropertyId) -> bool {
        false
    }

    fn table_path_scopable(&self, _table: &str) -> bool {
        false
    }

    fn table_path_columns(&self, _table: &str) -> Option<&[crate::PathColumn]> {
        None
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

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        (table == self.edge_table()).then(|| self.edge_columns())
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        if table == self.edge_table() {
            return Some(&self.edge_sort_key);
        }
        self.entities
            .iter()
            .flatten()
            .find(|layout| layout.table == table)
            .map(|layout| layout.sort_key.as_slice())
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

    pub fn edge_columns(&self) -> &HashSet<String> {
        &self.edge_columns
    }

    pub fn edge_column_type(&self, column: &str) -> Option<ontology::DataType> {
        self.edge_column_types.get(column).copied()
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
        let edge_columns = ontology
            .local_edge_columns()
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let edge_column_types = ontology
            .local_edge_columns()
            .iter()
            .map(|column| (column.name.clone(), column.data_type))
            .collect();
        let edge_sort_key = ontology
            .sort_key_for_table(&edge_table)
            .unwrap_or_else(|| ontology.edge_sort_key())
            .to_vec();
        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let mut property_facts = derive_property_backend_facts(ontology, graph)?;
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
            for field in local_fields {
                let Some(property) = graph.property_id(entity_id, &field.name) else {
                    continue;
                };
                property_facts[property.index()].realization = Some(match &field.source {
                    ontology::FieldSource::DatabaseColumn(column) => PropertyRealization::Stored {
                        column: column.clone(),
                    },
                    ontology::FieldSource::Virtual(source) => {
                        PropertyRealization::Virtual(source.clone())
                    }
                });
            }
            entities[entity_id.index()] = Some(DuckDbEntityLayout {
                table: node.destination_table.clone(),
                default_properties: graph.entity(entity_id).properties.clone(),
                sort_key: node.sort_key.clone(),
                has_traversal_path,
            });
        }
        let relationships = graph.relationships().map(|_| edge_table.clone()).collect();
        Ok(DuckDbCatalog {
            edge_table,
            edge_columns,
            edge_column_types,
            edge_sort_key,
            entities,
            property_facts,
            relationships,
            denormalized: DenormalizedCatalog::default(),
        })
    }
}
