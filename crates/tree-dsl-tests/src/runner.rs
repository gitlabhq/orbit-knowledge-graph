use std::sync::Arc;

use integration_tests_codegraph::assertions::{FixtureFile, Severity, TestCase, TestSuite};
use integration_tests_codegraph::{Failure, create_test_db, run_suite};
use ontology::Ontology;
use tree_dsl::pipeline::{self, Changes, Display, Emit, Export, Resolved, SourceFile};
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, Envelope, Pipeline, Scalar, State};

fn detect_lang(suite: &TestSuite, fixtures: &[(String, String)]) -> SupportLang {
    if let Some(ref p) = suite.pipeline
        && let Some(lang) = SupportLang::from_alias(p)
    {
        return lang;
    }
    let langs: std::collections::HashSet<_> = fixtures
        .iter()
        .filter_map(|(path, _)| SupportLang::from_path(path))
        .collect();
    match langs.len() {
        1 => *langs.iter().next().unwrap(),
        _ => panic!("suite mixes languages {langs:?}; declare `pipeline:`"),
    }
}

fn workspace_root() -> std::path::PathBuf {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git rev-parse failed");
    assert!(out.status.success(), "git rev-parse --show-toplevel failed");
    std::path::PathBuf::from(String::from_utf8(out.stdout).unwrap().trim())
}

fn load_fixture_dir(dir: &str) -> Vec<(String, String)> {
    let src = workspace_root().join(dir);
    assert!(src.is_dir(), "fixture_dir not found: {}", src.display());
    let mut files: Vec<(String, String)> = walkdir::WalkDir::new(&src)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let rel = e
                .path()
                .strip_prefix(&src)
                .ok()?
                .to_string_lossy()
                .replace('\\', "/");
            let content = std::fs::read_to_string(e.path()).ok()?;
            Some((rel, content))
        })
        .collect();
    files.sort();
    files
}

fn suite_fixtures(suite: &TestSuite) -> Vec<(String, String)> {
    let mut files = match &suite.fixture_dir {
        Some(dir) => load_fixture_dir(dir),
        None => Vec::new(),
    };
    for f in &suite.fixtures {
        files.retain(|(p, _)| p != &f.path);
        files.push((f.path.clone(), f.content.clone()));
    }
    files
}

pub fn sources(files: &[FixtureFile]) -> Vec<SourceFile> {
    files
        .iter()
        .map(|f| SourceFile {
            path: f.path.clone(),
            content: f.content.clone(),
        })
        .collect()
}

/// Export the graph into a fresh DuckDB and run the suite's queries against
/// it. The state comes back for the next incremental step.
fn check(
    graph: Pipeline<'_, Resolved>,
    ontology: &Arc<Ontology>,
    tests: &[TestCase],
) -> (State, Vec<Failure>) {
    let envelope = Envelope::new([
        ("project_id", Scalar::Int(1)),
        ("branch", Scalar::Str("main")),
        ("commit_sha", Scalar::Str("test")),
    ]);
    let db = create_test_db().expect("in-memory DuckDB");
    let displayed = graph
        .then(Display)
        .expect("display rules compile")
        .then(Export { ontology, envelope })
        .expect("export")
        .then(Emit(|table: &str, batch| db.insert_batch(table, &batch)))
        .expect("insert into DuckDB")
        .into_value();
    let suite = TestSuite {
        name: String::new(),
        pipeline: None,
        fixtures: Vec::new(),
        fixture_dir: None,
        trace: false,
        tests: tests.to_vec(),
        steps: Vec::new(),
    };
    let failures = run_suite(&suite, &db, ontology);
    (displayed.state, failures)
}

pub fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = orbit_utils::yaml::from_str(yaml).expect("Failed to parse YAML suite");

    if suite.tests.iter().all(|t| t.skip) && suite.steps.is_empty() {
        eprintln!(
            "[PASS] Suite: {} ({} tests, all skipped)",
            suite.name,
            suite.tests.len()
        );
        return;
    }

    let fixtures = suite_fixtures(&suite);
    let lang_id = detect_lang(&suite, &fixtures);
    let ontology = Arc::new(Ontology::load_embedded().expect("embedded ontology"));
    let env = Env::for_lang(lang_id).expect("rules compile");

    let graph = pipeline::index(
        Context::new(&env),
        fixtures.into_iter().map(SourceFile::from),
    )
    .expect("suite exceeded the total budget");
    let skipped = &graph.context().report.skipped;
    assert!(
        skipped.is_empty(),
        "files exceeded their budget: {skipped:?}"
    );

    let (mut state, mut all_failures) = check(graph, &ontology, &suite.tests);
    let mut total_tests = suite.tests.len();
    let mut total_skipped = suite.tests.iter().filter(|t| t.skip).count();

    for step in &suite.steps {
        let changes = Changes {
            added: sources(&step.add),
            modified: sources(&step.modify),
            removed: step.remove.clone(),
        };
        let graph = pipeline::reindex(Context::new(&env), state, changes)
            .expect("suite exceeded the total budget");
        let (next, failures) = check(graph, &ontology, &step.tests);
        state = next;
        all_failures.extend(failures);
        total_tests += step.tests.len();
        total_skipped += step.tests.iter().filter(|t| t.skip).count();
    }

    let failed = all_failures.len();
    let passed = total_tests
        .saturating_sub(total_skipped)
        .saturating_sub(failed);

    eprintln!("---");
    eprintln!("suite: {:?}", suite.name);
    eprintln!("tests: {total_tests}");
    eprintln!("passed: {passed}");
    eprintln!("failed: {failed}");
    eprintln!("skipped: {total_skipped}");
    if !all_failures.is_empty() {
        eprintln!("failures:");
        for f in &all_failures {
            eprintln!("  - test: {:?}", f.test);
            eprintln!("    severity: {}", f.severity);
            eprintln!("    message: {:?}", f.message);
        }
    }

    if all_failures.iter().any(|f| f.severity == Severity::Error) {
        panic!(
            "suite {:?}: {failed}/{total_tests} tests failed",
            suite.name
        );
    }
}
