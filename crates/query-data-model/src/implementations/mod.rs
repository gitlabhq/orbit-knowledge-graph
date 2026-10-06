mod authz;
mod clickhouse;
mod duckdb;

use crate::{DataModelError, GraphCatalog, PropertyRealization};

pub use authz::{EntityAuthConfig, GitLabAuthzCatalog, TrustedLocalCatalog};
pub use clickhouse::{ClickHouseCatalog, EntityLayout};
pub use duckdb::DuckDbCatalog;

#[derive(Debug, Clone)]
pub(super) struct PropertyBackendFacts {
    realization: Option<PropertyRealization>,
    selectivity: ontology::FieldSelectivity,
    has_text_index: bool,
}

fn derive_property_backend_facts(
    ontology: &ontology::Ontology,
    graph: &GraphCatalog,
    storage: &crate::storage::StorageCatalog,
    local: bool,
) -> Result<Vec<PropertyBackendFacts>, DataModelError> {
    let mut facts = vec![
        PropertyBackendFacts {
            realization: None,
            selectivity: ontology::FieldSelectivity::High,
            has_text_index: false,
        };
        graph.properties().count()
    ];
    for node in ontology.nodes() {
        if storage.table_id(&node.destination_table).is_none() {
            continue;
        }
        let entity =
            graph
                .entity_id(&node.name)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "entity",
                    name: node.name.clone(),
                })?;
        for field in &node.fields {
            if local
                && ontology
                    .local_entity_excludes(&node.name)
                    .is_some_and(|excluded| {
                        excluded.contains(&field.name)
                            || matches!(field.source, ontology::FieldSource::Virtual(_))
                    })
            {
                continue;
            }
            let property = graph.property_id(entity, &field.name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "property",
                    name: format!("{}.{}", node.name, field.name),
                }
            })?;
            facts[property.index()] = PropertyBackendFacts {
                realization: Some(match &field.source {
                    ontology::FieldSource::Virtual(source) => {
                        PropertyRealization::Virtual(source.clone())
                    }
                    ontology::FieldSource::DatabaseColumn(_) => PropertyRealization::Stored {
                        column: storage.resolve_column(&node.destination_table, &field.name)?,
                    },
                }),
                selectivity: field.selectivity,
                has_text_index: !local
                    && ontology
                        .text_index_tokenizer(&node.name, &field.name)
                        .is_some(),
            };
        }
        if let Some(property) = graph.property_id(entity, ontology::DEFAULT_PRIMARY_KEY)
            && facts[property.index()].realization.is_none()
        {
            facts[property.index()].realization = Some(PropertyRealization::Stored {
                column: storage
                    .resolve_column(&node.destination_table, ontology::DEFAULT_PRIMARY_KEY)?,
            });
        }
    }
    Ok(facts)
}
