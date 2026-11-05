use std::collections::BTreeMap;

use ontology::migrations::{
    Fingerprints, embedded_sources, hash_edge_row_sources, sha256_hex, source_fingerprints,
};
use query_data_model::{DataModelError, Packages};

use crate::schema::{GraphSchema, Schema, render_refreshable_view_select};

pub fn current_fingerprints() -> Result<Fingerprints, DataModelError> {
    fingerprints_for(&embedded_sources(), &Packages::embedded())
}

fn fingerprints_for(
    sources: &BTreeMap<String, String>,
    packages: &Packages,
) -> Result<Fingerprints, DataModelError> {
    let schema = GraphSchema::new(Schema::from_model(&packages.load()?)?, false);
    Ok(Fingerprints {
        sources: source_fingerprints(sources),
        ddl: ddl_fingerprints(&schema),
        edge_rows: hash_edge_row_sources(sources),
        auxiliary_schema: auxiliary_schema_fingerprints(&schema)?,
    })
}

pub fn ddl_fingerprints(schema: &GraphSchema) -> BTreeMap<String, String> {
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
            sha256_hex(&resolved.to_create_sql()),
        );
    }

    fingerprints
}

pub fn auxiliary_schema_fingerprints(
    schema: &GraphSchema,
) -> Result<BTreeMap<String, String>, DataModelError> {
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
        let rendered_select =
            render_refreshable_view_select(&view.select_query, &schema.physical, 1, prefix)
                .map_err(|error| {
                    DataModelError::Invalid(format!("refreshable view '{}': {error}", view.name))
                })?;
        let view_name = if view.versioned {
            format!("{prefix}{}", view.name)
        } else {
            view.name.clone()
        };
        fingerprints.insert(
            format!("materialized_view/{}", view.name),
            sha256_hex(&view.to_create_sql(&view_name, &rendered_select)),
        );
    }

    Ok(fingerprints)
}

#[cfg(test)]
mod tests {
    use ontology::Ontology;
    use ontology::migrations::{EdgeRows, MigrationScope, derive_edge_rows, derive_scope};

    use super::*;
    use crate::archive::Release;

    struct ArchivedVersion {
        version: u32,
        ontology: Ontology,
        sources: BTreeMap<String, String>,
        fingerprints: Fingerprints,
    }

    fn load_archive(version: u32) -> ArchivedVersion {
        let release = Release::bundled(version)
            .unwrap()
            .unwrap_or_else(|| panic!("v{version} archives are bundled"));
        let archive = release.ontology_archive();
        let ontology = archive.load_ontology().unwrap();
        let fingerprints = fingerprints_for(archive.sources(), &release.packages()).unwrap();
        ArchivedVersion {
            version,
            ontology,
            sources: archive.sources().clone(),
            fingerprints,
        }
    }

    #[test]
    fn only_node_storage_bumps_derive_an_sdlc_scope_that_keeps_edge_rows() {
        let archives: Vec<_> = (93..=115).map(load_archive).collect();

        let kept: Vec<u32> = archives
            .windows(2)
            .filter_map(|pair| {
                let [base, bump] = pair else { return None };
                let (changed_sources, changed_tables) = bump
                    .fingerprints
                    .get_versioned_diff_keys_between(&base.fingerprints);
                let scope = derive_scope(
                    &bump.ontology,
                    &bump.sources,
                    &changed_sources,
                    &changed_tables,
                );
                let edge_rows =
                    derive_edge_rows(&bump.ontology, &bump.fingerprints, &base.fingerprints);
                (matches!(scope, Some(MigrationScope::Sdlc(_))) && edge_rows == EdgeRows::Unchanged)
                    .then_some(bump.version)
            })
            .collect();

        assert_eq!(kept, [113, 115]);
    }
}
