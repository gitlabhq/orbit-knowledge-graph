use std::collections::BTreeMap;

use ontology::Ontology;
use ontology::migrations::sha256_hex;
use query_data_model::DataModelError;

use crate::schema::{GraphSchema, translate};

pub fn ddl_fingerprints(ontology: &Ontology) -> Result<BTreeMap<String, String>, DataModelError> {
    let schema = GraphSchema::from_ontology(ontology)?;
    let mut fingerprints = BTreeMap::new();

    for table in &schema.tables {
        fingerprints.insert(table.name.clone(), sha256_hex(&table.to_create_sql("")));
    }

    let all_table_names: Vec<String> = schema.table_names().iter().map(|s| s.to_string()).collect();
    for view in schema.views.iter().filter(|view| view.versioned) {
        let resolved = view
            .clone()
            .with_schema_version_prefix("", &all_table_names);
        fingerprints.insert(
            format!("materialized_view/{}", view.name),
            sha256_hex(&resolved.to_create_sql()?),
        );
    }

    Ok(fingerprints)
}

pub fn auxiliary_schema_fingerprints(
    ontology: &Ontology,
) -> Result<BTreeMap<String, String>, DataModelError> {
    let schema = GraphSchema::from_ontology(ontology)?;
    let mut fingerprints = BTreeMap::new();

    for definition in &schema.unversioned_definitions {
        fingerprints.insert(
            format!(
                "{}/{}",
                definition.entity_type.to_lowercase(),
                definition.name
            ),
            sha256_hex(&definition.create_statement),
        );
    }

    let prefix = "v1_";
    for view in &schema.refreshable_views {
        let rendered_select = translate::render_refreshable_view_select(
            &view.select_query,
            &schema.storage,
            1,
            prefix,
        )
        .map_err(|error| {
            DataModelError::Invalid(format!("refreshable view '{}': {error}", view.name))
        })?;
        let view_name = if view.versioned {
            format!("{prefix}{}", view.name)
        } else {
            view.name.clone()
        };
        let create_sql = format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}\n\
                 REFRESH {} APPEND TO {}\nAS {rendered_select}",
            view.refresh, view.append_to
        );
        fingerprints.insert(
            format!("materialized_view/{}", view.name),
            sha256_hex(&create_sql),
        );
    }

    Ok(fingerprints)
}
