use std::collections::{HashMap, HashSet};

use crate::{
    Backend, DataModelError, DenormalizedCatalog, DenormalizedJoin, EntityId, GraphCatalog,
    PropertyId, QueryBackendCatalog, RelationshipId, TextIndex, TraversalPathLookup,
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
    entities: HashMap<EntityId, DuckDbEntityLayout>,
    properties: HashMap<PropertyId, String>,
    table_entities: HashMap<String, EntityId>,
    relationships: HashMap<RelationshipId, String>,
    denormalized: DenormalizedCatalog,
}

impl QueryBackendCatalog for DuckDbCatalog {
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

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        self.properties.get(&property).map(String::as_str)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        (table == self.edge_table())
            .then(|| self.edge_column_type(column))
            .flatten()
    }

    fn text_index(&self, _property: PropertyId) -> Option<&TextIndex> {
        None
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
        self.table_entities
            .get(table)
            .and_then(|entity| self.entity(*entity))
            .map(|layout| layout.sort_key.as_slice())
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn denormalized_joins(&self) -> &[DenormalizedJoin] {
        &[]
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
        let edge_sort_key = ontology
            .sort_key_for_table(&edge_table)
            .unwrap_or_else(|| ontology.edge_sort_key())
            .to_vec();
        let mut entities = HashMap::new();
        let mut property_columns = HashMap::new();
        let mut table_entities = HashMap::new();
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
            let properties: HashMap<_, _> = ontology
                .local_entity_fields(entity_name)
                .unwrap_or_else(|| node.fields.iter().collect())
                .into_iter()
                .filter_map(|field| {
                    let property = graph.property_id(entity_id, &field.name)?;
                    let column = field.column_name()?.to_string();
                    Some((property, column))
                })
                .collect();
            let has_traversal_path = properties.values().any(|column| column == "traversal_path");
            property_columns.extend(properties);
            entities.insert(
                entity_id,
                DuckDbEntityLayout {
                    table: node.destination_table.clone(),
                    default_properties: graph.entity(entity_id).properties.clone(),
                    sort_key: node.sort_key.clone(),
                    has_traversal_path,
                },
            );
            table_entities.insert(node.destination_table.clone(), entity_id);
        }
        let relationships = graph
            .relationships()
            .map(|relationship| (relationship.id, edge_table.clone()))
            .collect();
        Ok(DuckDbCatalog {
            edge_table,
            edge_columns,
            edge_column_types,
            edge_sort_key,
            entities,
            properties: property_columns,
            table_entities,
            relationships,
            denormalized: DenormalizedCatalog::default(),
        })
    }
}
