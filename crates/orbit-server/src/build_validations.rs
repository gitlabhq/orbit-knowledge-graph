use std::path::{Path, PathBuf};
use std::sync::Arc;

use orbit_migrations::archive::ModelArchive;

fn config_directory() -> &'static Path {
    Path::new(env!("ORBIT_CONFIG_DIR"))
}

#[test]
fn orbit_skills_match_cli_commands() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    orbit_prompts::validate_skill_pair(
        repository.join("skills/orbit"),
        repository.join("skills/orbit-cli"),
        repository.join("crates/orbit-cli/src/main.rs"),
    )
    .unwrap_or_else(|error| panic!("Orbit skill validation failed: {error}"));
}

#[test]
fn named_queries_render_and_compile() {
    let directory = PathBuf::from(env!("NAMED_QUERIES_DIR"));
    let data_model = Arc::new(
        query_data_model::Catalog::<query_data_model::ClickHouse>::embedded()
            .unwrap_or_else(|error| panic!("embedded model failed to load: {error}")),
    );
    let security_context = compiler::uniform_grants(data_model.model(), false, &["1/"]);
    let queries = named_queries::NamedQueries::load_from_dir(&directory)
        .unwrap_or_else(|error| panic!("named queries failed to load: {error}"));

    for query in queries.iter() {
        let rendered = query
            .render_example()
            .unwrap_or_else(|error| panic!("named query failed to render: {error}"));
        if let Err(error) = compiler::compile(
            &rendered,
            &data_model,
            &security_context,
            query_data_model::testkit::limits(),
        ) {
            panic!("named query `{}` failed to compile: {error}", query.name);
        }
    }
}

/// Fails on a malformed ledger or a current archive that no longer matches the packages.
#[test]
fn migration_ledger_and_current_archives_are_committed() {
    orbit_migrations::ledger::verify_committed(config_directory(), orbit_versions::VERSIONS.schema)
        .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn current_model_archive_is_bundled() {
    let current_version = orbit_versions::VERSIONS.schema;
    let versions = ModelArchive::bundled_versions().unwrap_or_else(|error| panic!("{error}"));
    assert!(
        versions.contains(&current_version),
        "{} is missing; run `mise schema:bump` to write the current archive.",
        ModelArchive::path(config_directory(), current_version).display()
    );
}
