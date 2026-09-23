use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Env, State};

use super::assertions::{Severity, TestSuite};
use super::config::make_graph_config;
use super::export::export;
use super::validator::{Failure, run_suite};

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

async fn build_and_check(env: &Env, state: &mut State, suite: &TestSuite) -> Vec<Failure> {
    tree_dsl::phases::display(env, state);
    let datasets = export(&state.trees, &state.edges, &env.lang).expect("Failed to build datasets");
    let graph_config = make_graph_config().expect("Failed to build graph config");
    run_suite(suite, &datasets, &graph_config).await
}

pub async fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = serde_yaml::from_str(yaml).expect("Failed to parse YAML suite");

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

    let tree_dsl::Indexed {
        env,
        mut state,
        killed,
    } = tree_dsl::index(lang_id, &fixtures).expect("suite exceeded the total budget");
    assert!(killed.is_empty(), "files exceeded their budget: {killed:?}");

    let mut all_failures = Vec::new();
    let mut total_tests = 0usize;
    let mut total_skipped = 0usize;

    if !suite.tests.is_empty() {
        let failures = build_and_check(&env, &mut state, &suite).await;
        total_tests += suite.tests.len();
        total_skipped += suite.tests.iter().filter(|t| t.skip).count();
        all_failures.extend(failures);
    }

    for step in &suite.steps {
        let added: Vec<(String, String)> = step
            .add
            .iter()
            .map(|f| (f.path.clone(), f.content.clone()))
            .collect();
        let modified: Vec<(String, String)> = step
            .modify
            .iter()
            .map(|f| (f.path.clone(), f.content.clone()))
            .collect();
        state = tree_dsl::reindex(&env, state, &added, &modified, &step.remove)
            .expect("suite exceeded the total budget")
            .0;

        if !step.tests.is_empty() {
            let step_suite = TestSuite {
                name: step.name.clone(),
                pipeline: suite.pipeline.clone(),
                fixtures: Vec::new(),
                fixture_dir: None,
                _trace: false,
                tests: step.tests.clone(),
                steps: Vec::new(),
            };
            let failures = build_and_check(&env, &mut state, &step_suite).await;
            total_tests += step.tests.len();
            total_skipped += step.tests.iter().filter(|t| t.skip).count();
            all_failures.extend(failures);
        }
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
