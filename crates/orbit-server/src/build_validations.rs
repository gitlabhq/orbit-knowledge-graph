//! Repository-consistency checks that used to run in `build.rs`.
//!
//! They run in the `unit-test` CI job on merge requests and on `main`. Keeping
//! them out of the build script avoids compiling the compiler and ontology
//! crates a second time as host build dependencies.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ontology::Ontology;
use ontology::archive::OntologyArchive;

fn config_directory() -> &'static Path {
    Path::new(env!("CONFIG_DIR"))
}

fn embedded_ontology() -> Arc<Ontology> {
    Arc::new(
        Ontology::load_embedded()
            .unwrap_or_else(|error| panic!("embedded ontology failed to load: {error}")),
    )
}

#[test]
fn prompts_load() {
    let directory = Path::new(env!("PROMPTS_DIR")).join("remote");
    orbit_prompts::Prompts::load_dir(&directory).unwrap_or_else(|error| panic!("{error}"));
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

/// Fails on ontology/DDL drift from the fingerprint snapshot or a malformed
/// ledger. Mirrors `cargo xtask migration-ledger check`.
#[test]
fn migration_ledger_matches_fingerprint_snapshot() {
    let config_directory = config_directory();
    let ledger_path = config_directory.join(ontology::migrations::LEDGER_FILE);
    let fingerprint_path = config_directory.join(ontology::migrations::FINGERPRINT_FILE);
    let ontology = embedded_ontology();

    let current = ontology::migrations::Fingerprints {
        sources: ontology::migrations::source_fingerprints(),
        ddl: orbit_migrations::fingerprint::ddl_fingerprints(&ontology),
        auxiliary_schema: orbit_migrations::fingerprint::auxiliary_schema_fingerprints(&ontology),
    };

    let committed_text = std::fs::read_to_string(&fingerprint_path).unwrap_or_else(|error| {
        panic!(
            "reading {}: {error}. Run `mise schema:bump` to create the fingerprint snapshot.",
            fingerprint_path.display()
        )
    });
    let committed = ontology::migrations::Fingerprints::parse(&committed_text)
        .unwrap_or_else(|error| panic!("{error}"));

    let ledger_text = std::fs::read_to_string(&ledger_path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", ledger_path.display()));
    let ledger = ontology::migrations::MigrationLedger::parse(&ledger_text)
        .unwrap_or_else(|error| panic!("{error}"));

    ontology::migrations::verify_snapshot(
        &ontology,
        &current,
        &committed,
        &ledger,
        orbit_versions::VERSIONS.schema,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn authored_etl_sql_is_valid() {
    ontology::etl_sql::validate_authored_etl_sql(&embedded_ontology())
        .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn bundled_ontology_archives_load() {
    let versions = OntologyArchive::bundled_versions().unwrap_or_else(|error| panic!("{error}"));
    for version in versions {
        OntologyArchive::bundled(version)
            .unwrap_or_else(|error| panic!("{error}"))
            .expect("bundled archive must exist")
            .load_ontology()
            .unwrap_or_else(|error| panic!("bundled archive v{version}: {error}"));
    }
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

#[test]
fn current_ontology_archive_is_not_stale() {
    let current_version = orbit_versions::VERSIONS.schema;
    let archive = OntologyArchive::bundled(current_version)
        .unwrap_or_else(|error| panic!("{error}"))
        .unwrap_or_else(|| {
            panic!("current archive v{current_version} is missing; run `mise schema:snapshot`")
        });
    assert!(
        archive.matches_sources(&ontology::migrations::embedded_sources()),
        "ontology archive is stale; run `mise schema:bump`"
    );
}
