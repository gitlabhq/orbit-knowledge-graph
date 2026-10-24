pub mod clickhouse;
pub mod duckdb;

use std::collections::HashSet;

use crate::{DataModelError, GraphCatalog, PropertyRealization};

pub use clickhouse::{ClickHouseBindings, EntityLayout, TableLayout};
pub use duckdb::DuckDbBindings;

#[derive(Debug, Clone)]
pub(super) struct PropertyBackendFacts {
    realization: Option<PropertyRealization>,
    selectivity: ontology::FieldSelectivity,
    has_text_index: bool,
    unique: bool,
    mutable: bool,
    null_as_epoch: bool,
}

#[derive(Debug)]
pub struct EntityFacts {
    pub name: String,
    pub table: String,
    pub global: bool,
    pub has_traversal_path: bool,
    pub default_properties: Vec<String>,
    pub(crate) properties: Vec<(String, PropertyBackendFacts)>,
}

pub(crate) fn entity_facts(ontology: &ontology::Ontology, local: bool) -> Vec<EntityFacts> {
    ontology
        .nodes()
        .map(|node| {
            let local_fields = local
                .then(|| ontology.local_entity_fields(&node.name))
                .flatten();
            let text_indexed = ontology.text_indexed_columns(&node.name);
            let non_nullable_storage: HashSet<&str> = node
                .storage
                .columns
                .iter()
                .filter(|column| !column.ch_type.starts_with("Nullable("))
                .map(|column| column.name.trim_matches('`'))
                .collect();
            EntityFacts {
                name: node.name.clone(),
                table: node.destination_table.clone(),
                global: node.global,
                has_traversal_path: node.has_traversal_path,
                default_properties: node.default_columns.clone(),
                properties: node
                    .fields
                    .iter()
                    .map(|field| {
                        let available = !local
                            || local_fields.as_ref().is_none_or(|fields| {
                                fields.iter().any(|candidate| candidate.name == field.name)
                            });
                        let realization = available.then(|| match &field.source {
                            ontology::FieldSource::DatabaseColumn(column) => {
                                PropertyRealization::Stored {
                                    column: if local {
                                        column.clone()
                                    } else {
                                        field.name.clone()
                                    },
                                }
                            }
                            ontology::FieldSource::Virtual(source) => {
                                PropertyRealization::Virtual(source.clone())
                            }
                        });
                        (
                            field.name.clone(),
                            PropertyBackendFacts {
                                realization,
                                selectivity: field.selectivity,
                                has_text_index: !local
                                    && text_indexed.contains(&field.name.as_str()),
                                unique: field.unique,
                                mutable: field.mutable,
                                null_as_epoch: !local
                                    && field.nullable
                                    && matches!(
                                        field.data_type,
                                        ontology::DataType::DateTime | ontology::DataType::Date
                                    )
                                    && non_nullable_storage.contains(field.name.as_str()),
                            },
                        )
                    })
                    .collect(),
            }
        })
        .collect()
}

fn derive_property_backend_facts(
    entities: &[EntityFacts],
    graph: &GraphCatalog,
) -> Result<Vec<PropertyBackendFacts>, DataModelError> {
    let mut facts = vec![
        PropertyBackendFacts {
            realization: None,
            selectivity: ontology::FieldSelectivity::High,
            has_text_index: false,
            unique: false,
            mutable: true,
            null_as_epoch: false,
        };
        graph.properties().count()
    ];
    for node in entities {
        let entity =
            graph
                .entity_id(&node.name)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "entity",
                    name: node.name.clone(),
                })?;
        for (name, field) in &node.properties {
            let property = graph.property_id(entity, name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "property",
                    name: format!("{}.{}", node.name, name),
                }
            })?;
            facts[property.index()] = field.clone();
        }
    }
    Ok(facts)
}
