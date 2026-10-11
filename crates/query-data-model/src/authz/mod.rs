pub mod gitlab;
pub mod trusted;

use crate::{DataModelError, GraphCatalog};

#[derive(Debug, Clone)]
struct PropertyPolicy {
    admin_only: bool,
    filterable: bool,
    like_allowed: bool,
}

fn derive_property_policy(
    ontology: &ontology::Ontology,
    graph: &GraphCatalog,
) -> Result<Vec<PropertyPolicy>, DataModelError> {
    let mut properties = vec![
        PropertyPolicy {
            admin_only: false,
            filterable: true,
            like_allowed: true,
        };
        graph.properties().count()
    ];
    for node in ontology.nodes() {
        let entity =
            graph
                .entity_id(&node.name)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "entity",
                    name: node.name.clone(),
                })?;
        for field in &node.fields {
            let property = graph.property_id(entity, &field.name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "property",
                    name: format!("{}.{}", node.name, field.name),
                }
            })?;
            properties[property.index()] = PropertyPolicy {
                admin_only: field.admin_only,
                filterable: field.filterable,
                like_allowed: field.like_allowed,
            };
        }
    }
    Ok(properties)
}
