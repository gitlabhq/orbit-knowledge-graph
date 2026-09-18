use tree_dsl::treesitter::SupportLang;

use super::assertions::{Severity, TestSuite};
use super::config::make_graph_config;
use super::export::export;
use super::validator::{Failure, run_suite};

fn detect_lang(suite: &TestSuite) -> SupportLang {
    if let Some(ref p) = suite.pipeline
        && let Some(lang) = SupportLang::from_alias(p)
    {
        return lang;
    }
    for f in &suite.fixtures {
        if let Some(lang) = SupportLang::from_path(&f.path) {
            return lang;
        }
    }
    SupportLang::Python
}

async fn build_and_check(
    result: &mut tree_dsl::IndexResult,
    lang_id: SupportLang,
    suite: &TestSuite,
) -> Vec<Failure> {
    let yaml = tree_dsl::treesitter::lang_yaml(lang_id).expect("no lang yaml");
    let config = tree_dsl::rules::load_lang_full(yaml, &result.lang);
    tree_dsl::display::apply_display(
        &mut result.trees,
        &result.edges,
        &result.lang,
        &config.display_rules,
    );
    let datasets =
        export(&result.trees, &result.edges, &result.lang).expect("Failed to build datasets");
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

    let lang_id = detect_lang(&suite);
    let fixtures: Vec<(String, String)> = suite
        .fixtures
        .iter()
        .map(|f| (f.path.clone(), f.content.clone()))
        .collect();

    let mut result = tree_dsl::index(lang_id, &fixtures);

    let mut all_failures = Vec::new();
    let mut total_tests = 0usize;
    let mut total_skipped = 0usize;

    if !suite.tests.is_empty() {
        let failures = build_and_check(&mut result, lang_id, &suite).await;
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
        result.update(&added, &modified, &step.remove);

        if !step.tests.is_empty() {
            let step_suite = TestSuite {
                name: step.name.clone(),
                pipeline: suite.pipeline.clone(),
                fixtures: Vec::new(),
                _fixture_dir: None,
                _trace: false,
                tests: step.tests.clone(),
                steps: Vec::new(),
            };
            let failures = build_and_check(&mut result, lang_id, &step_suite).await;
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
