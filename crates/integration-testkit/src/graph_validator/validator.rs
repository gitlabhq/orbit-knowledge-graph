//! Execution engine: runs test suites against lance-graph datasets.

use arrow_56::array::{Array, Int64Array, StringArray};
use arrow_56::record_batch::RecordBatch;
use lance_graph::{CypherQuery, GraphConfig};

use super::assertions::{Assert, QueryBlock, Severity, TestCase, TestSuite};
use super::datasets::LanceDatasets;

#[derive(Debug)]
pub struct Failure {
    pub test: String,
    pub severity: Severity,
    pub message: String,
}

pub async fn run_suite(
    suite: &TestSuite,
    datasets: &LanceDatasets,
    config: &GraphConfig,
) -> Vec<Failure> {
    let mut failures = Vec::new();
    for test in &suite.tests {
        if test.skip {
            eprintln!("  [SKIP] \"{}\"", test.name);
            continue;
        }
        failures.extend(run_test(test, datasets, config).await);
    }
    failures
}

async fn run_test(test: &TestCase, datasets: &LanceDatasets, config: &GraphConfig) -> Vec<Failure> {
    let mut failures = Vec::new();

    for (i, block) in test.all_queries().iter().enumerate() {
        let label = if test.all_queries().len() == 1 {
            test.name.clone()
        } else {
            format!("{} [query {}]", test.name, i + 1)
        };

        failures.extend(run_query_block(&label, test.severity, block, datasets, config).await);
    }

    failures
}

async fn run_query_block(
    label: &str,
    severity: Severity,
    block: &QueryBlock,
    datasets: &LanceDatasets,
    config: &GraphConfig,
) -> Vec<Failure> {
    let query = match CypherQuery::new(&block.query) {
        Ok(q) => q.with_config(config.clone()),
        Err(e) => {
            return vec![Failure {
                test: label.to_string(),
                severity,
                message: format!("Cypher parse error: {e}"),
            }];
        }
    };

    let ds = datasets.clone();
    let batch = match query.execute(ds, None).await {
        Ok(b) => b,
        Err(e) => {
            return vec![Failure {
                test: label.to_string(),
                severity,
                message: format!("Query execution error: {e}"),
            }];
        }
    };

    print_result(label, &block.query, &batch);
    check_assertions(label, severity, &block.assert, &batch)
}

fn print_result(label: &str, query: &str, batch: &RecordBatch) {
    eprintln!("  ┌─ TEST: \"{label}\"");
    eprintln!("  │ query: {}", query.trim().replace('\n', "\n  │        "));
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

fn check_assertions(
    label: &str,
    severity: Severity,
    assertions: &[Assert],
    batch: &RecordBatch,
) -> Vec<Failure> {
    let mut failures = Vec::new();
    let total_rows = batch.num_rows();

    for assertion in assertions {
        if let Some(f) = check_one(label, severity, assertion, batch, total_rows) {
            failures.push(f);
        }
    }

    failures
}

fn check_one(
    label: &str,
    severity: Severity,
    assertion: &Assert,
    batch: &RecordBatch,
    total_rows: usize,
) -> Option<Failure> {
    match assertion {
        Assert::Empty { empty } => {
            if *empty && total_rows > 0 {
                Some(fail(
                    label,
                    severity,
                    format!("Expected empty result, got {total_rows} rows"),
                ))
            } else if !*empty && total_rows == 0 {
                Some(fail(
                    label,
                    severity,
                    "Expected non-empty result, got 0 rows".into(),
                ))
            } else {
                None
            }
        }
        Assert::NonEmpty { non_empty } => {
            if *non_empty && total_rows == 0 {
                Some(fail(
                    label,
                    severity,
                    "Expected non-empty result, got 0 rows".into(),
                ))
            } else {
                None
            }
        }
        Assert::RowCount { row_count } => {
            let expected = *row_count as usize;
            if total_rows != expected {
                Some(fail(
                    label,
                    severity,
                    format!("Expected {expected} rows, got {total_rows}"),
                ))
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
                return Some(fail(
                    label,
                    severity,
                    format!(
                        "Expected {}={}, got {}={}",
                        count_equals.field,
                        count_equals.value,
                        count_equals.field,
                        arr.value(0)
                    ),
                ));
            }
            None
        }
        Assert::CountGte { count_gte } => {
            if let Some(col) = batch.column_by_name(&count_gte.field)
                && let Some(arr) = col.as_any().downcast_ref::<Int64Array>()
                && !arr.is_empty()
                && arr.value(0) < count_gte.value
            {
                return Some(fail(
                    label,
                    severity,
                    format!(
                        "Expected {}>={}, got {}={}",
                        count_gte.field,
                        count_gte.value,
                        count_gte.field,
                        arr.value(0)
                    ),
                ));
            }
            None
        }
        Assert::AllMatch { all_match } => {
            let glob = match globset::Glob::new(&all_match.pattern) {
                Ok(g) => g.compile_matcher(),
                Err(e) => {
                    return Some(fail(
                        label,
                        severity,
                        format!("Invalid glob pattern '{}': {e}", all_match.pattern),
                    ));
                }
            };

            if let Some(col) = batch.column_by_name(&all_match.field)
                && let Some(arr) = col.as_any().downcast_ref::<StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) && !glob.is_match(arr.value(i)) {
                        return Some(fail(
                            label,
                            severity,
                            format!(
                                "Row {i}: {}='{}' does not match '{}'",
                                all_match.field,
                                arr.value(i),
                                all_match.pattern
                            ),
                        ));
                    }
                }
            }
            None
        }
        Assert::ContainsRow { contains_row } => {
            for row in 0..total_rows {
                let mut all_match = true;
                for (field, expected) in contains_row {
                    let actual = batch
                        .column_by_name(field)
                        .map(|col| format_cell(col.as_ref(), row));
                    if actual.as_deref() != Some(expected.as_str()) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    return None; // found the row
                }
            }
            let expected_str: Vec<String> = contains_row
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            Some(fail(
                label,
                severity,
                format!(
                    "No row matching {{{}}} in {total_rows} rows",
                    expected_str.join(", ")
                ),
            ))
        }
    }
}

fn fail(test: &str, severity: Severity, message: String) -> Failure {
    Failure {
        test: test.to_string(),
        severity,
        message,
    }
}
