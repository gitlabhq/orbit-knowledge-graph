use tree_dsl::grammar::SupportLang;

use super::assertions::{Severity, TestSuite};
use super::config::make_graph_config;
use super::datasets::to_datasets;
use super::validator::run_suite;

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

pub async fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = serde_yaml::from_str(yaml).expect("Failed to parse YAML suite");

    if suite.tests.iter().all(|t| t.skip) {
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
    let datasets = to_datasets(&result.trees, &result.cross_edges, &mut result.lang)
        .expect("Failed to build datasets");

    let config = make_graph_config().expect("Failed to build graph config");
    let failures = run_suite(&suite, &datasets, &config).await;

    let total = suite.tests.len();
    let skipped = suite.tests.iter().filter(|t| t.skip).count();
    let failed = failures.len();
    let passed = total.saturating_sub(skipped).saturating_sub(failed);

    eprintln!("---");
    eprintln!("suite: {:?}", suite.name);
    eprintln!("tests: {total}");
    eprintln!("passed: {passed}");
    eprintln!("failed: {failed}");
    eprintln!("skipped: {skipped}");
    if !failures.is_empty() {
        eprintln!("failures:");
        for f in &failures {
            eprintln!("  - test: {:?}", f.test);
            eprintln!("    severity: {}", f.severity);
            eprintln!("    message: {:?}", f.message);
        }
    }

    if failures.iter().any(|f| f.severity == Severity::Error) {
        panic!("suite {:?}: {failed}/{total} tests failed", suite.name);
    }
}
