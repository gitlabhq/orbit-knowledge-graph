use std::collections::{HashMap, HashSet};

use crate::{
    Backend, DataModelError, DenormalizedCatalog, EntityId, GraphCatalog, PropertyId,
    QueryBackendCatalog, RelationshipId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct DuckDbEntityLayout {
    pub table: String,
    pub properties: HashMap<PropertyId, String>,
    pub sort_key: Vec<String>,
}

#[derive(Debug)]
pub struct DuckDbCatalog {
    edge_table: String,
    edge_columns: HashSet<String>,
    edge_column_types: HashMap<String, ontology::DataType>,
    entities: HashMap<EntityId, DuckDbEntityLayout>,
    relationships: HashMap<RelationshipId, String>,
    denormalized: DenormalizedCatalog,
}

impl QueryBackendCatalog for DuckDbCatalog {
    fn supports_foreign_key_elision(&self) -> bool {
        false
    }

    fn requires_node_joins(&self) -> bool {
        true
    }

    fn requires_node_projection(&self) -> bool {
        true
    }

    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.entity(entity).map(|layout| layout.table.as_str())
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity).is_some_and(|layout| {
            layout
                .properties
                .values()
                .any(|column| column == "traversal_path")
        })
    }

    fn entity_is_global(&self, _entity: EntityId) -> bool {
        false
    }

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        let entity = self.entities.iter().find_map(|(entity, layout)| {
            layout.properties.contains_key(&property).then_some(entity)
        })?;
        self.entity(*entity)?
            .properties
            .get(&property)
            .map(String::as_str)
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
        self.entities
            .values()
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
        self.entities.get(&id)
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        self.relationships.get(&id).map(String::as_str)
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

pub struct DuckDb;

impl Backend for DuckDb {
    type Catalog = DuckDbCatalog;

    fn derive(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self::Catalog, DataModelError> {
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
        let mut entities = HashMap::new();
        for entity_name in ontology.local_entity_names() {
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
            let properties = ontology
                .local_entity_fields(entity_name)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|field| {
                    let property = graph.property_id(entity_id, &field.name)?;
                    let column = field.column_name()?.to_string();
                    Some((property, column))
                })
                .collect();
            entities.insert(
                entity_id,
                DuckDbEntityLayout {
                    table: node.destination_table.clone(),
                    properties,
                    sort_key: node.sort_key.clone(),
                },
            );
        }
        let relationships = graph
            .relationships()
            .map(|relationship| (relationship.id, edge_table.clone()))
            .collect();
        Ok(DuckDbCatalog {
            edge_table,
            edge_columns,
            edge_column_types,
            entities,
            relationships,
            denormalized: DenormalizedCatalog::default(),
        })
    }
}
