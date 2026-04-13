//! Execution engine: runs test suites against lance-graph datasets.

use arrow_56::array::{Array, Int64Array, StringArray};
use arrow_56::record_batch::RecordBatch;
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
        failures.extend(run_test(test, datasets, config).await);
    }
    failures
}

async fn run_test(test: &TestCase, datasets: &LanceDatasets, config: &GraphConfig) -> Vec<Failure> {
    let query = match CypherQuery::new(&test.query) {
        Ok(q) => q.with_config(config.clone()),
        Err(e) => {
            return vec![Failure {
                test: test.name.clone(),
                severity: test.severity,
                message: format!("Cypher parse error: {e}"),
            }];
        }
    };

    let ds = datasets.clone();
    let batch = match query.execute(ds, None).await {
        Ok(b) => b,
        Err(e) => {
            return vec![Failure {
                test: test.name.clone(),
                severity: test.severity,
                message: format!("Query execution error: {e}"),
            }];
        }
    };

    // Print query + results for full visibility
    eprintln!("  ┌─ TEST: \"{}\"", test.name);
    eprintln!("  │ query: {}", test.query.trim().replace('\n', "\n  │        "));
    eprintln!("  │ result: {} rows", batch.num_rows());
    if batch.num_rows() > 0 {
        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        eprintln!("  │ ┌──{}──┐", col_names.join("──┬──"));
        let max_rows = batch.num_rows().min(20);
        for row in 0..max_rows {
            let vals: Vec<String> = (0..batch.num_columns())
                .map(|col| format_cell(batch.column(col).as_ref(), row))
                .collect();
            eprintln!("  │ │ {} │", vals.join(" │ "));
        }
        if batch.num_rows() > 20 {
            eprintln!("  │ │ ... +{} more rows │", batch.num_rows() - 20);
        }
        eprintln!("  │ └{}┘", "─".repeat(col_names.join("──┬──").len() + 4));
    }
    eprintln!("  └─");

    check_assertions(test, &batch)
}

fn format_cell(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    "<?>".to_string()
}

fn check_assertions(test: &TestCase, batch: &RecordBatch) -> Vec<Failure> {
    let mut failures = Vec::new();
    let total_rows = batch.num_rows();

    for assertion in &test.assert {
        if let Some(f) = check_one(test, assertion, batch, total_rows) {
            failures.push(f);
        }
    }

    failures
}

fn check_one(
    test: &TestCase,
    assertion: &Assert,
    batch: &RecordBatch,
    total_rows: usize,
) -> Option<Failure> {
    match assertion {
        Assert::Empty { empty } => {
            if *empty && total_rows > 0 {
                Some(Failure {
                    test: test.name.clone(),
                    severity: test.severity,
                    message: format!("Expected empty result, got {total_rows} rows"),
                })
            } else if !*empty && total_rows == 0 {
                Some(Failure {
                    test: test.name.clone(),
                    severity: test.severity,
                    message: "Expected non-empty result, got 0 rows".into(),
                })
            } else {
                None
            }
        }
        Assert::NonEmpty { non_empty } => {
            if *non_empty && total_rows == 0 {
                Some(Failure {
                    test: test.name.clone(),
                    severity: test.severity,
                    message: "Expected non-empty result, got 0 rows".into(),
                })
            } else {
                None
            }
        }
        Assert::CountEquals { count_equals } => {
            if let Some(col) = batch.column_by_name(&count_equals.field)
                && let Some(arr) = col.as_any().downcast_ref::<Int64Array>()
                && !arr.is_empty()
                && arr.value(0) != count_equals.value
            {
                return Some(Failure {
                    test: test.name.clone(),
                    severity: test.severity,
                    message: format!(
                        "Expected {}={}, got {}={}",
                        count_equals.field,
                        count_equals.value,
                        count_equals.field,
                        arr.value(0)
                    ),
                });
            }
            None
        }
        Assert::AllMatch { all_match } => {
            let glob = match globset::Glob::new(&all_match.pattern) {
                Ok(g) => g.compile_matcher(),
                Err(e) => {
                    return Some(Failure {
                        test: test.name.clone(),
                        severity: test.severity,
                        message: format!("Invalid glob pattern '{}': {e}", all_match.pattern),
                    });
                }
            };

            if let Some(col) = batch.column_by_name(&all_match.field)
                && let Some(arr) = col.as_any().downcast_ref::<StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) && !glob.is_match(arr.value(i)) {
                        return Some(Failure {
                            test: test.name.clone(),
                            severity: test.severity,
                            message: format!(
                                "Row {i}: {}='{}' does not match '{}'",
                                all_match.field,
                                arr.value(i),
                                all_match.pattern
                            ),
                        });
                    }
                }
            }
            None
        }
    }
}
