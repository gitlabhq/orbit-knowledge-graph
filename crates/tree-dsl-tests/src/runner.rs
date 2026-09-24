use std::sync::Arc;

use integration_tests_codegraph::assertions::{FixtureFile, Severity, TestCase, TestSuite};
use integration_tests_codegraph::{Failure, create_test_db, run_suite};
use ontology::Ontology;
use std::path::Path;

use tree_dsl::pipeline::{Changes, Display, Emit, Export, Resolved};
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, Envelope, Pipeline, Scalar, State};
use tree_dsl::{inventory, templates};

fn detect_lang(suite: &TestSuite, paths: &[String]) -> SupportLang {
    if let Some(ref p) = suite.pipeline
        && let Some(lang) = SupportLang::from_alias(p)
    {
        return lang;
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

fn workspace_root() -> std::path::PathBuf {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git rev-parse failed");
    assert!(out.status.success(), "git rev-parse --show-toplevel failed");
    std::path::PathBuf::from(String::from_utf8(out.stdout).unwrap().trim())
}

/// The suite's files on disk, as a repository: `fixture_dir` copied in,
/// inline fixtures written over it. Returns the relative paths written.
fn write_fixtures(suite: &TestSuite, root: &Path) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(dir) = &suite.fixture_dir {
        let src = workspace_root().join(dir);
        assert!(src.is_dir(), "fixture_dir not found: {}", src.display());
        for entry in walkdir::WalkDir::new(&src)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let rel = entry.path().strip_prefix(&src).unwrap();
            let dst = root.join(rel);
            std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
            std::fs::copy(entry.path(), &dst).unwrap();
            paths.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    paths.extend(write_files(&suite.fixtures, root));
    paths
}

pub fn write_files(files: &[FixtureFile], root: &Path) -> Vec<String> {
    files
        .iter()
        .map(|f| {
            let dst = root.join(&f.path);
            std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
            std::fs::write(&dst, &f.content).unwrap();
            f.path.clone()
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

    let repo = tempfile::tempdir().expect("temp repository");
    let paths = write_fixtures(&suite, repo.path());
    let lang_id = detect_lang(&suite, &paths);
    let ontology = Arc::new(Ontology::load_embedded().expect("embedded ontology"));
    let env = Env::for_lang(lang_id).expect("rules compile");

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

    let (mut state, mut all_failures) = check(graph, &ontology, &suite.tests);
    let mut total_tests = suite.tests.len();
    let mut total_skipped = suite.tests.iter().filter(|t| t.skip).count();

    for step in &suite.steps {
        for removed in &step.remove {
            std::fs::remove_file(repo.path().join(removed)).ok();
        }
        let mut changed = write_files(&step.add, repo.path());
        changed.extend(write_files(&step.modify, repo.path()));
        let changes = Changes {
            changed: inventory::classify(repo.path(), changed),
            removed: step.remove.clone(),
        };
        let graph = templates::reindex(Context::new(&env), state, repo.path(), changes)
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
