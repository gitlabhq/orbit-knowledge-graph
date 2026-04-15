use std::fmt::Write;

use code_graph::v2::{Pipeline, PipelineConfig};
use code_graph_linker::v2::graph::RowContext;

use super::assertions::{Severity, TestSuite};
use super::config::make_graph_config;
use super::datasets::to_lance_datasets;
use super::validator::run_suite;

/// Run a YAML test suite from a string. Panics on any error-severity failure.
pub async fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = serde_yaml::from_str(yaml).expect("Failed to parse YAML suite");

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    for fixture in &suite.fixtures {
        let path = tmp.path().join(&fixture.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("Failed to create dir {}: {e}", parent.display()));
        }
        std::fs::write(&path, &fixture.content)
            .unwrap_or_else(|e| panic!("Failed to write {}: {e}", path.display()));
    }

    let pipeline = Pipeline::new(PipelineConfig::default());
    let result = pipeline.run(tmp.path());
    assert!(
        result.errors.is_empty(),
        "Pipeline errors: {:?}",
        result.errors
    );

    let ctx = RowContext::empty();
    let datasets =
        to_lance_datasets(&result.graph, &ctx).expect("Failed to convert graph to datasets");
    let config = make_graph_config().expect("Failed to build graph config");

    let failures = run_suite(&suite, &datasets, &config).await;
    if failures.is_empty() {
        eprintln!("[PASS] Suite: {} ({} tests)", suite.name, suite.tests.len());
        return;
    }

    let mut msg = format!(
        "\n[FAIL] Suite: {} ({} failures)\n",
        suite.name,
        failures.len()
    );
    for f in &failures {
        writeln!(msg, "  [{}] \"{}\" — {}", f.severity, f.test, f.message).unwrap();
    }

    if failures.iter().any(|f| f.severity == Severity::Error) {
        panic!("{msg}");
    } else {
        eprintln!("{msg}");
    }
}
