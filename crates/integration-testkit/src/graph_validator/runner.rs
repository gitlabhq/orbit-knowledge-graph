//! Test runner: YAML fixtures → CodeGraph → Arrow → Cypher → assert.
//!
//! ```ignore
//! use integration_testkit::graph_validator::runner::run_yaml_suite;
//!
//! #[tokio::test]
//! async fn structural_invariants() {
//!     run_yaml_suite(include_str!("fixtures/structural.yaml")).await;
//! }
//! ```

use std::path::Path;

use code_graph::v2::{Pipeline, PipelineConfig};
use code_graph_linker::v2::graph::RowContext;

use super::assertions::{Severity, TestSuite};
use super::config::make_graph_config;
use super::datasets::to_lance_datasets;
use super::validator::run_suite;

/// Run a YAML test suite from a string. Panics on any error-severity failure.
pub async fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = serde_yaml::from_str(yaml)
        .unwrap_or_else(|e| panic!("Failed to parse YAML suite: {e}"));

    // Write fixtures to a temp directory
    let tmp = tempfile::tempdir()
        .unwrap_or_else(|e| panic!("Failed to create temp dir: {e}"));

    for fixture in &suite.fixtures {
        let path = tmp.path().join(&fixture.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("Failed to create dir {}: {e}", parent.display()));
        }
        std::fs::write(&path, &fixture.content)
            .unwrap_or_else(|e| panic!("Failed to write {}: {e}", path.display()));
    }

    // Run the v2 pipeline
    let pipeline = Pipeline::new(PipelineConfig::default());
    let result = pipeline.run(tmp.path());

    assert!(
        result.errors.is_empty(),
        "Pipeline errors: {:?}",
        result.errors
    );

    // Convert to Arrow datasets
    let ctx = RowContext::empty();
    let datasets = to_lance_datasets(&result.graph, &ctx)
        .unwrap_or_else(|e| panic!("Failed to convert graph to datasets: {e}"));

    let config = make_graph_config()
        .unwrap_or_else(|e| panic!("Failed to build graph config: {e}"));

    // Run the Cypher test suite
    let failures = run_suite(&suite, &datasets, &config).await;

    if !failures.is_empty() {
        let mut msg = format!(
            "\n[FAIL] Suite: {} ({} failures)\n",
            suite.name,
            failures.len()
        );
        for f in &failures {
            let severity = match f.severity {
                Severity::Error => "ERROR",
                Severity::Warning => "WARN",
            };
            msg.push_str(&format!(
                "  [{severity}] \"{}\" — {}\n",
                f.test, f.message
            ));
        }

        let has_errors = failures.iter().any(|f| f.severity == Severity::Error);
        if has_errors {
            panic!("{msg}");
        } else {
            eprintln!("{msg}");
        }
    } else {
        eprintln!(
            "[PASS] Suite: {} ({} tests)",
            suite.name,
            suite.tests.len()
        );
    }
}

/// Run a YAML suite from a file path.
pub async fn run_yaml_suite_file(path: &Path) {
    let yaml = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    run_yaml_suite(&yaml).await;
}
