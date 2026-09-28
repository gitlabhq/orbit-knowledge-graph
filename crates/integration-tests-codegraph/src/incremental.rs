//! The same YAML suites, indexed by `code-graph-incremental` instead of
//! `code-graph`: fixtures on disk, production's walk, the full pipeline
//! into a fresh DuckDB, then the suite's queries.

use std::sync::Arc;

use code_graph_incremental::pipeline::{Display, Emit, Export};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Envelope, Scalar, inventory, templates};
use ontology::Ontology;

use super::assertions::TestSuite;
use super::runner::create_test_db;
use super::validator::{load_suite, report, run_suite, write_fixtures};

fn detect_lang(suite: &TestSuite, paths: &[String]) -> SupportLang {
    if let Some(pipeline) = suite.pipeline.as_deref() {
        return SupportLang::from_alias(pipeline)
            .unwrap_or_else(|| panic!("suite {:?}: unknown pipeline {pipeline:?}", suite.name));
    }
    let langs: std::collections::HashSet<_> = paths
        .iter()
        .filter_map(|path| SupportLang::from_path(path))
        .collect();
    match langs.len() {
        1 => *langs.iter().next().unwrap(),
        _ => panic!("suite mixes languages {langs:?}; declare `pipeline:`"),
    }
}

pub fn run_incremental_suite(yaml: &str) {
    let Some(suite) = load_suite(yaml) else {
        return;
    };
    assert!(
        suite.fixture_dir.is_none(),
        "suite {:?}: fixture_dir is not supported by the incremental runner",
        suite.name
    );

    let repo = tempfile::tempdir().expect("temp repository");
    write_fixtures(&suite.fixtures, repo.path());
    let paths: Vec<String> = suite.fixtures.iter().map(|f| f.path.clone()).collect();
    let lang_id = detect_lang(&suite, &paths);
    let ontology = Arc::new(Ontology::load_embedded().expect("embedded ontology"));
    let env = Env::for_lang(lang_id).expect("rules compile");
    let envelope = Envelope::new([
        ("project_id", Scalar::Int(1)),
        ("branch", Scalar::Str("main")),
        ("commit_sha", Scalar::Str("test")),
    ]);

    let inventory = inventory::walk(repo.path())
        .expect("walk fixtures")
        .to_vec();
    let graph = templates::index(Context::new(&env), repo.path(), inventory)
        .expect("suite exceeded the total budget");
    let skipped = &graph.context().report.skipped;
    assert!(
        skipped.is_empty(),
        "files exceeded their budget: {skipped:?}"
    );

    let db = create_test_db().expect("in-memory DuckDB");
    graph
        .then(Display)
        .expect("display rules compile")
        .then(Export {
            ontology: &ontology,
            envelope,
        })
        .expect("export")
        .then(Emit(|table: &str, batch| db.insert_batch(table, &batch)))
        .expect("insert into DuckDB");

    let failures = run_suite(&suite, &db, &ontology);
    report(&suite, &failures);
}
