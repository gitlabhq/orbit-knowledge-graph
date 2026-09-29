mod authz;
mod clickhouse;
mod duckdb;

use crate::{DataModelError, GraphCatalog, PropertyRealization};

pub use authz::{EntityAuthConfig, GitLabAuthzCatalog, TrustedLocalCatalog};
pub use clickhouse::{ClickHouseCatalog, EntityLayout, TableLayout};
pub use duckdb::DuckDbCatalog;

fn edge_field_column(
    columns: &std::collections::HashSet<String>,
    field: crate::EdgeField,
) -> Option<&str> {
    use crate::EdgeField;
    use ontology::constants::{
        RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN, TARGET_ID_COLUMN,
        TARGET_KIND_COLUMN,
    };

    let name = match field {
        EdgeField::SourceId => SOURCE_ID_COLUMN,
        EdgeField::TargetId => TARGET_ID_COLUMN,
        EdgeField::SourceKind => SOURCE_KIND_COLUMN,
        EdgeField::TargetKind => TARGET_KIND_COLUMN,
        EdgeField::RelationshipKind => RELATIONSHIP_KIND_COLUMN,
    };

    columns.get(name).map(String::as_str)
}

#[derive(Debug, Clone)]
pub(super) struct PropertyBackendFacts {
    realization: Option<PropertyRealization>,
    selectivity: ontology::FieldSelectivity,
    has_text_index: bool,
}

fn derive_property_backend_facts(
    ontology: &ontology::Ontology,
    graph: &GraphCatalog,
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
            facts[property.index()] = PropertyBackendFacts {
                realization: None,
                selectivity: field.selectivity,
                has_text_index: false,
            };
        }
    }
    Ok(facts)
}
