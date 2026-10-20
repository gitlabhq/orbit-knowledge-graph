use std::path::{Path, PathBuf};
use std::sync::Arc;

use ontology::Ontology;
use ontology::archive::OntologyArchive;

fn config_directory() -> &'static Path {
    Path::new(env!("ORBIT_CONFIG_DIR"))
}

fn embedded_ontology() -> Arc<Ontology> {
    Arc::new(
        Ontology::load_embedded()
            .unwrap_or_else(|error| panic!("embedded ontology failed to load: {error}")),
    )
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
    let ontology = embedded_ontology();
    let security_context = compiler::SecurityContext::new(1, vec!["1/".into()])
        .expect("static security context must be valid");
    let queries = named_queries::NamedQueries::load_from_dir(&directory)
        .unwrap_or_else(|error| panic!("named queries failed to load: {error}"));

    for query in queries.iter() {
        for (language, frontend) in [
            (named_queries::Language::Json, compiler::Frontend::JsonDsl),
            (named_queries::Language::Gql, compiler::Frontend::Gql),
        ] {
            let rendered = query
                .render_example_language(language)
                .unwrap_or_else(|error| panic!("named query failed to render: {error}"));
            if let Err(error) = compiler::compile(&rendered, frontend, &ontology, &security_context)
            {
                panic!(
                    "named query `{}` ({language:?}) failed to compile: {error}",
                    query.name
                );
            }
        }
    }
}

/// Fails on ontology/DDL drift from the fingerprint snapshot, a malformed
/// ledger or a stale current archive.
#[test]
fn migration_ledger_matches_fingerprint_snapshot() {
    orbit_migrations::snapshot_check::verify_committed_snapshot(
        &embedded_ontology(),
        config_directory(),
        orbit_versions::VERSIONS.schema,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn current_ontology_archive_is_bundled() {
    let current_version = orbit_versions::VERSIONS.schema;
    let versions = OntologyArchive::bundled_versions().unwrap_or_else(|error| panic!("{error}"));
    assert!(
        versions.contains(&current_version),
        "{} is missing; run `mise schema:snapshot` to seed the current archive.",
        OntologyArchive::path(config_directory(), current_version).display()
    );
}
