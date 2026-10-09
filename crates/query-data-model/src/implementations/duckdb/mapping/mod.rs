use super::{DuckDbCatalog, DuckDbEntityLayout, storage::StorageCatalog};
use crate::implementations::derive_property_backend_facts;
use crate::{DataModelError, DenormalizedCatalog, GraphCatalog, PropertyRealization};

pub(super) fn derive(
    ontology: &ontology::Ontology,
    graph: &GraphCatalog,
) -> Result<DuckDbCatalog, DataModelError> {
    let storage = StorageCatalog::derive(ontology);
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
        let table = &storage.nodes[entity_name];
        let has_traversal_path = table
            .columns
            .contains(ontology::constants::TRAVERSAL_PATH_COLUMN);
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
            table: table.name.clone(),
            default_properties: graph.entity(entity_id).properties.clone(),
            sort_key: table.sort_key.clone(),
            has_traversal_path,
        });
    }
    let relationships = graph
        .relationships()
        .map(|_| storage.edge.name.clone())
        .collect();
    Ok(DuckDbCatalog {
        storage,
        entities,
        property_facts,
        relationships,
        denormalized: DenormalizedCatalog::default(),
    })
}
