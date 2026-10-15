//! The same YAML suites, indexed by `code-graph-incremental` instead of
//! `code-graph`: fixtures on disk, production's walk, the full pipeline
//! into a fresh DuckDB, then the suite's queries.

use std::sync::Arc;

use code_graph_incremental::pipeline::{Display, Emit, Export, Resolved};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Envelope, Limits, Pipeline, Scalar, State, templates};
use ontology::Ontology;

struct FixturePolicy;

impl orbit_utils::vfs::Pass for FixturePolicy {
    type Tag = ();

    fn metadata(&self, file: &orbit_utils::vfs::File<'_, ()>) -> orbit_utils::vfs::Decision<()> {
        if file.path.ends_with(".png") || file.path.ends_with(".min.js") {
            orbit_utils::vfs::Decision::List("excluded_extension")
        } else {
            orbit_utils::vfs::Decision::Pending
        }
    }

    fn content(&self, file: &orbit_utils::vfs::File<'_, ()>) -> orbit_utils::vfs::Decision<()> {
        if file.bytes().is_some_and(|bytes| bytes.contains(&0)) {
            orbit_utils::vfs::Decision::List("binary")
        } else {
            orbit_utils::vfs::Decision::Keep(())
        }
    }
}

use super::assertions::{SkippedFile, TestCase, TestSuite};
use super::runner::create_test_db;
use super::validator::{
    Failure, load_suite, report, run_suite, workspace_root, write_fixtures, write_suite_files,
};

fn detect_lang(suite: &TestSuite, paths: &[String]) -> SupportLang {
    if let Some(pipeline) = suite.pipeline.as_deref().filter(|p| *p != "generic") {
        return SupportLang::from_alias(pipeline)
            .unwrap_or_else(|| panic!("suite {:?}: unknown pipeline {pipeline:?}", suite.name));
    }
    let mut langs: Vec<SupportLang> = paths
        .iter()
        .filter_map(|path| SupportLang::from_path(path))
        .collect();
    langs.sort_by_key(|l| <&str>::from(l));
    langs.dedup();
    let families: std::collections::HashSet<_> = langs.iter().map(|l| l.family()).collect();
    match (langs.first(), families.len()) {
        (None, _) => panic!(
            "suite {:?}: no fixture has a known language extension; declare `pipeline:`",
            suite.name
        ),
        (Some(&lang), 1) => lang,
        _ => panic!(
            "suite {:?} mixes families {families:?}; declare `pipeline:`",
            suite.name
        ),
    }
}

fn suite_limits(suite: &TestSuite) -> Limits {
    match suite.limits.as_deref() {
        None | Some("unlimited") => Limits::UNLIMITED,
        Some("default") => default_limits(),
        Some(other) => panic!("suite {:?}: unknown limits {other:?}", suite.name),
    }
}

/// The production budgets, read from the same file the server reads.
fn default_limits() -> Limits {
    let path = workspace_root().join("config/default.yaml");
    let text = std::fs::read_to_string(&path).expect("config/default.yaml");
    let config: serde_json::Value =
        orbit_utils::yaml::from_str(&text).expect("config/default.yaml parses");
    let pipeline = &config["engine"]["handlers"]["code-indexing-task"]["pipeline"];
    let field = |name: &str| {
        pipeline[name]
            .as_u64()
            .unwrap_or_else(|| panic!("config/default.yaml: pipeline.{name} is not a number"))
    };
    let budget = |name: &str| match field(name) {
        0 => u64::MAX,
        ms => ms,
    };
    Limits {
        max_files: field("max_files") as usize,
        file_parse_ms: budget("per_file_parse_timeout_ms"),
        file_rewrite_ms: budget("per_file_walk_timeout_ms"),
        file_link_ms: budget("per_file_ssa_timeout_ms"),
        file_resolve_ms: budget("per_file_timeout_ms"),
        cross_file_resolve_ms: budget("cross_file_resolve_timeout_ms"),
        max_tree_depth: field("max_tree_depth") as usize,
        max_cross_file_names: field("max_cross_file_names") as usize,
        max_manifest_bytes: field("max_manifest_bytes"),
        max_file_edges: field("max_file_edges") as usize,
        ..Limits::UNLIMITED
    }
}

pub fn run_incremental_suite(yaml: &str) {
    rayon::ThreadPoolBuilder::new()
        .stack_size(templates::WORKER_STACK_BYTES)
        .build()
        .expect("worker pool")
        .install(|| index_and_check(yaml));
}

fn index_and_check(yaml: &str) {
    let Some(suite) = load_suite(yaml) else {
        return;
    };
    let repo = tempfile::tempdir().expect("temp repository");
    let paths: Vec<String> = write_suite_files(&suite, repo.path())
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    let lang_id = detect_lang(&suite, &paths);
    let ontology = Arc::new(Ontology::load_embedded().expect("embedded ontology"));
    let env = Env::with_limits(lang_id, suite_limits(&suite)).expect("rules compile");

    let repository = Arc::new(
        orbit_utils::vfs::Vfs::load(
            orbit_utils::vfs::sources::Directory(repo.path()),
            FixturePolicy,
            Default::default(),
            Default::default(),
        )
        .expect("load fixtures"),
    );
    let graph =
        templates::index(Context::new(&env, repository)).expect("suite exceeded the total budget");
    let (mut state, mut failures) = check(graph, &ontology, &suite.tests, &suite.skipped);
    let mut env = env;

    for step in &suite.steps {
        if step.snapshot {
            let snapshot = repo.path().join("graph.bin");
            state.save(&env, &snapshot).expect("save snapshot");
            (env, state) = State::load(&snapshot, lang_id).expect("load snapshot");
            std::fs::remove_file(&snapshot).ok();
        }
        for removed in &step.remove {
            std::fs::remove_file(repo.path().join(removed)).ok();
        }
        let mut changed = write_fixtures(&step.add, repo.path());
        changed.extend(write_fixtures(&step.modify, repo.path()));
        let changed = changed.into_iter().map(|(path, _)| path);
        let changes = Arc::new(
            orbit_utils::vfs::Vfs::load(
                orbit_utils::vfs::sources::Diff {
                    root: repo.path(),
                    paths: changed.collect(),
                },
                FixturePolicy,
                Default::default(),
                Default::default(),
            )
            .expect("load changed fixtures"),
        );
        let graph = templates::reindex(Context::new(&env, changes), state, step.remove.clone())
            .expect("suite exceeded the total budget");
        let (next, step_failures) = check(graph, &ontology, &step.tests, &[]);
        state = next;
        failures.extend(step_failures);
    }
    report(&suite, &failures);
}

/// Exports the graph into a fresh DuckDB and runs `tests` against it; the
/// graph comes back for the next step.
fn check(
    graph: Pipeline<'_, Resolved>,
    ontology: &Arc<Ontology>,
    tests: &[TestCase],
    skipped: &[SkippedFile],
) -> (State, Vec<Failure>) {
    {
        let mut expected: Vec<&SkippedFile> = skipped.iter().collect();
        expected.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        let mut found: Vec<(&str, &str)> = graph
            .context()
            .report
            .skipped
            .iter()
            .map(|killed| (killed.path.as_str(), killed.label.as_ref()))
            .collect();
        found.sort_unstable();
        let matched = found.len() == expected.len()
            && found.iter().zip(&expected).all(|((path, label), file)| {
                *path == file.path && file.label.as_deref().is_none_or(|want| want == *label)
            });
        assert!(
            matched,
            "files the budgets skipped: {found:?}, expected {expected:?}"
        );
    }
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
        limits: None,
        skipped: Vec::new(),
        tests: tests.to_vec(),
        steps: Vec::new(),
    };
    (displayed.state, run_suite(&suite, &db, ontology, true))
}
