use std::collections::{HashMap, HashSet};

use crate::{Backend, DataModelError, EntityId, GraphCatalog, PropertyId, RelationshipId};

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
        })
    }
}
