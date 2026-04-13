//! Execution engine: runs test suites against lance-graph datasets.

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use lance_graph::{CypherQuery, GraphConfig};

use super::assertions::{Assert, Severity, TestCase, TestSuite};
use super::datasets::LanceDatasets;

#[derive(Debug)]
pub struct Failure {
    pub test: String,
    pub severity: Severity,
    pub message: String,
}

/// Run all tests in a suite against the given datasets.
pub async fn run_suite(
    suite: &TestSuite,
    datasets: &LanceDatasets,
    config: &GraphConfig,
) -> Vec<Failure> {
    let mut failures = Vec::new();

    for test in &suite.tests {
        if let Some(failure) = run_test(test, datasets, config).await {
            failures.push(failure);
        }
    }

    failures
}

async fn run_test(
    test: &TestCase,
    datasets: &LanceDatasets,
    config: &GraphConfig,
) -> Option<Failure> {
    let query = match CypherQuery::new(&test.query) {
        Ok(q) => q.with_config(config.clone()),
        Err(e) => {
            return Some(Failure {
                test: test.name.clone(),
                severity: test.severity,
                message: format!("Cypher parse error: {e}"),
            });
        }
    };

    // Clone datasets since execute consumes them
    let ds = datasets.clone();

    let result = match query.execute(ds, None).await {
        Ok(batches) => batches,
        Err(e) => {
            return Some(Failure {
                test: test.name.clone(),
                severity: test.severity,
                message: format!("Query execution error: {e}"),
            });
        }
    };

    check_assertion(test, &result)
}

fn check_assertion(test: &TestCase, batches: &[RecordBatch]) -> Option<Failure> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    match &test.assert {
        Assert::Empty(true) => {
            if total_rows > 0 {
                Some(Failure {
                    test: test.name.clone(),
                    severity: test.severity,
                    message: format!("Expected empty result, got {total_rows} rows"),
                })
            } else {
                None
            }
        }
        Assert::Empty(false) | Assert::NonEmpty(true) => {
            if total_rows == 0 {
                Some(Failure {
                    test: test.name.clone(),
                    severity: test.severity,
                    message: "Expected non-empty result, got 0 rows".into(),
                })
            } else {
                None
            }
        }
        Assert::NonEmpty(false) => None, // vacuously true
        Assert::CountEquals { field, value } => {
            for batch in batches {
                if let Some(col) = batch.column_by_name(field) {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        if arr.len() > 0 && arr.value(0) != *value {
                            return Some(Failure {
                                test: test.name.clone(),
                                severity: test.severity,
                                message: format!(
                                    "Expected {field}={value}, got {field}={}",
                                    arr.value(0)
                                ),
                            });
                        }
                    }
                }
            }
            None
        }
        Assert::AllMatch { field, pattern } => {
            let glob = match globset::Glob::new(pattern) {
                Ok(g) => g.compile_matcher(),
                Err(e) => {
                    return Some(Failure {
                        test: test.name.clone(),
                        severity: test.severity,
                        message: format!("Invalid glob pattern '{pattern}': {e}"),
                    });
                }
            };

            for batch in batches {
                if let Some(col) = batch.column_by_name(field) {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        for i in 0..arr.len() {
                            if !arr.is_null(i) && !glob.is_match(arr.value(i)) {
                                return Some(Failure {
                                    test: test.name.clone(),
                                    severity: test.severity,
                                    message: format!(
                                        "Row {i}: {field}='{}' does not match pattern '{pattern}'",
                                        arr.value(i)
                                    ),
                                });
                            }
                        }
                    }
                }
            }
            None
        }
    }
}
