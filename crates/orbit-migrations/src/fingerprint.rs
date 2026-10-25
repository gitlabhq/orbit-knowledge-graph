use std::collections::BTreeMap;

use ontology::Ontology;
use ontology::migrations::{
    Fingerprints, embedded_sources, hash_edge_row_sources, sha256_hex, source_fingerprints,
};
use query_data_model::DataModelError;

use crate::schema::{GraphSchema, translate};

pub fn current_fingerprints(ontology: &Ontology) -> Result<Fingerprints, DataModelError> {
    fingerprints_for(ontology, &embedded_sources())
}

fn fingerprints_for(
    ontology: &Ontology,
    sources: &BTreeMap<String, String>,
) -> Result<Fingerprints, DataModelError> {
    Ok(Fingerprints {
        sources: source_fingerprints(sources),
        ddl: ddl_fingerprints(ontology)?,
        edge_rows: hash_edge_row_sources(sources),
        auxiliary_schema: auxiliary_schema_fingerprints(ontology)?,
    })
}

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

#[cfg(test)]
mod tests {
    use ontology::archive::OntologyArchive;
    use ontology::migrations::{EdgeRows, derive_edge_rows};

    use super::*;

    fn load_archive(version: u32) -> (Ontology, Fingerprints) {
        let archive = OntologyArchive::bundled(version)
            .unwrap()
            .unwrap_or_else(|| panic!("v{version} archive is bundled"));
        let ontology = archive.load_ontology().unwrap();
        let fingerprints = fingerprints_for(&ontology, archive.sources()).unwrap();
        (ontology, fingerprints)
    }

    #[test]
    fn only_node_storage_bumps_keep_edge_rows() {
        let archives: Vec<_> = (93..=115)
            .map(|version| (version, load_archive(version)))
            .collect();

        let kept: Vec<u32> = archives
            .windows(2)
            .filter(|pair| {
                let (_, (_, base)) = &pair[0];
                let (_, (ontology, current)) = &pair[1];
                derive_edge_rows(ontology, current, base) == EdgeRows::Unchanged
            })
            .map(|pair| pair[1].0)
            .collect();

        assert_eq!(kept, [101, 113, 115]);
    }
}
