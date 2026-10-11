use super::{DuckDbBindings, DuckDbEntityLayout, layout::DuckDb};
use crate::implementations::derive_property_backend_facts;
use crate::{DataModelError, DenormalizedCatalog, GraphCatalog, Mapping, Relational};

impl Mapping<Relational<DuckDb>> for DuckDbBindings {
    fn derive(graph: &GraphCatalog, storage: &Relational<DuckDb>) -> Result<Self, DataModelError> {
        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let mut property_facts = derive_property_backend_facts(&storage.metadata.entities, graph)?;
        for entity in graph.entities() {
            if !storage.metadata.entity_tables.contains_key(&entity.name) {
                for property in &entity.properties {
                    property_facts[property.index()].realization = None;
                }
            }
        }
        for entity_name in storage.metadata.entity_tables.keys().map(String::as_str) {
            let entity_id =
                graph
                    .entity_id(entity_name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "local entity",
                        name: entity_name.to_string(),
                    })?;
            let table = storage
                .entity_table(entity_name)
                .expect("derived local entity table");
            let has_traversal_path = table
                .columns
                .iter()
                .any(|column| column.name == ontology::constants::TRAVERSAL_PATH_COLUMN);
            entities[entity_id.index()] = Some(DuckDbEntityLayout {
                table: table.name.clone(),
                default_properties: graph.entity(entity_id).properties.clone(),
                sort_key: table.sort_key.clone(),
                has_traversal_path,
            });
        }
        let relationships = graph
            .relationships()
            .map(|_| storage.edge().name.clone())
            .collect();
        Ok(DuckDbBindings {
            edge_columns: storage
                .edge()
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            edge_table: storage.edge().name.clone(),
            edge_sort_key: storage.edge().sort_key.clone(),
            edge_column_types: storage.edge().column_types.clone(),
            entities,
            property_facts,
            relationships,
            denormalized: DenormalizedCatalog::default(),
        })
    }
}
