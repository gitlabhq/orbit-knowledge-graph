use arrow_56::array::{Array, Int64Array, StringArray};
use arrow_56::record_batch::RecordBatch;
use lance_graph::{CypherQuery, GraphConfig};
use tabled::{Table, builder::Builder};

use super::assertions::{Assert, FieldValueArgs, QueryBlock, Severity, TestCase, TestSuite};
use super::datasets::LanceDatasets;

#[derive(Debug)]
pub(crate) struct Failure {
    pub test: String,
    pub severity: Severity,
    pub message: String,
}

pub(crate) async fn run_suite(
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
    let blocks = test.all_queries();
    let mut failures = Vec::new();

    for (i, block) in blocks.iter().enumerate() {
        let label = if blocks.len() == 1 {
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
        Err(e) => return vec![fail(label, severity, format!("Cypher parse error: {e}"))],
    };

    let batch = match query.execute(datasets.clone(), None).await {
        Ok(b) => b,
        Err(e) => return vec![fail(label, severity, format!("Query execution error: {e}"))],
    };

    let failures = check_assertions(label, severity, &block.assert, &batch);
    if !failures.is_empty() {
        print_result(label, &block.query, &batch);
    }
    failures
}

fn print_result(label: &str, query: &str, batch: &RecordBatch) {
    eprintln!("  TEST: \"{label}\"");
    eprintln!("  query: {}", query.trim());
    eprintln!("  result: {} rows", batch.num_rows());
    if batch.num_rows() > 0 {
        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let mut builder = Builder::new();
        builder.push_record(col_names);
        for row in 0..batch.num_rows().min(20) {
            let vals: Vec<String> = (0..batch.num_columns())
                .map(|col| format_cell(batch.column(col).as_ref(), row))
                .collect();
            builder.push_record(&vals);
        }
        let table = Table::from(builder).to_string();
        for line in table.lines() {
            eprintln!("  {line}");
        }
        if batch.num_rows() > 20 {
            eprintln!("  ... +{} more rows", batch.num_rows() - 20);
        }
    }
}

fn format_cell(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".into();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    "<?>".into()
}

fn check_assertions(
    label: &str,
    severity: Severity,
    assertions: &[Assert],
    batch: &RecordBatch,
) -> Vec<Failure> {
    let rows = batch.num_rows();
    assertions
        .iter()
        .filter_map(|a| check_one(label, severity, a, batch, rows))
        .collect()
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
            check_int_field(batch, count_equals, |v, e| v != e, "=", label, severity)
        }
        Assert::CountGte { count_gte } => {
            check_int_field(batch, count_gte, |v, e| v < e, ">=", label, severity)
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
                let all_match = contains_row.iter().all(|(field, expected)| {
                    batch
                        .column_by_name(field)
                        .map(|col| format_cell(col.as_ref(), row))
                        .as_deref()
                        == Some(expected.as_str())
                });
                if all_match {
                    return None;
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

fn check_int_field(
    batch: &RecordBatch,
    args: &FieldValueArgs,
    pred: impl Fn(i64, i64) -> bool,
    op: &str,
    label: &str,
    severity: Severity,
) -> Option<Failure> {
    let col = batch.column_by_name(&args.field)?;
    let arr = col.as_any().downcast_ref::<Int64Array>()?;
    if arr.is_empty() {
        return None;
    }
    let actual = arr.value(0);
    if pred(actual, args.value) {
        Some(fail(
            label,
            severity,
            format!(
                "Expected {}{op}{}, got {}={actual}",
                args.field, args.value, args.field
            ),
        ))
    } else {
        None
    }
}

fn fail(test: &str, severity: Severity, message: String) -> Failure {
    Failure {
        test: test.to_string(),
        severity,
        message,
    }
}
