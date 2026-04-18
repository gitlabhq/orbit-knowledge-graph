use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_56::array::{Array, Int64Array, Int64Builder, StringArray, StringBuilder};
use arrow_56::record_batch::RecordBatch;
use lance_graph::{CypherQuery, GraphConfig};
use tabled::{Table, builder::Builder};

use super::assertions::{
    Assert, AssertCheck, FieldValueArgs, QueryBlock, Severity, TestCase, TestSuite,
};
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

// -- where filter -----------------------------------------------------------

fn apply_filter(batch: &RecordBatch, where_clause: &HashMap<String, String>) -> RecordBatch {
    let matching: Vec<usize> = (0..batch.num_rows())
        .filter(|&row| {
            where_clause.iter().all(|(field, expected)| {
                batch
                    .column_by_name(field)
                    .map(|col| format_cell(col.as_ref(), row) == *expected)
                    .unwrap_or(false)
            })
        })
        .collect();

    let schema = batch.schema();
    let columns: Vec<Arc<dyn Array>> = (0..batch.num_columns())
        .map(|col_idx| {
            let col = batch.column(col_idx);
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                let mut b = StringBuilder::new();
                for &row in &matching {
                    if arr.is_null(row) {
                        b.append_null();
                    } else {
                        b.append_value(arr.value(row));
                    }
                }
                Arc::new(b.finish()) as Arc<dyn Array>
            } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                let mut b = Int64Builder::new();
                for &row in &matching {
                    if arr.is_null(row) {
                        b.append_null();
                    } else {
                        b.append_value(arr.value(row));
                    }
                }
                Arc::new(b.finish()) as Arc<dyn Array>
            } else {
                panic!(
                    "where filter: unsupported column type {:?}",
                    col.data_type()
                );
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap_or_else(|e| panic!("where filter failed: {e}"))
}

// -- assertion evaluation ----------------------------------------------------

fn check_assertions(
    label: &str,
    severity: Severity,
    assertions: &[Assert],
    batch: &RecordBatch,
) -> Vec<Failure> {
    assertions
        .iter()
        .filter_map(|a| {
            let (effective, scoped_label) = match &a.filter {
                Some(f) => {
                    let filtered = apply_filter(batch, f);
                    let desc: Vec<String> = f.iter().map(|(k, v)| format!("{k}={v}")).collect();
                    (filtered, format!("{label} [where {}]", desc.join(", ")))
                }
                None => (batch.clone(), label.to_string()),
            };
            let rows = effective.num_rows();
            let result = check_one(&scoped_label, severity, &a.check, &effective, rows);
            if a.negate {
                match result {
                    Some(_) => None,
                    None => Some(fail(
                        &scoped_label,
                        severity,
                        format!("Negated assertion passed (expected failure): {:?}", a.check),
                    )),
                }
            } else {
                result
            }
        })
        .collect()
}

fn check_one(
    label: &str,
    severity: Severity,
    assertion: &AssertCheck,
    batch: &RecordBatch,
    total_rows: usize,
) -> Option<Failure> {
    match assertion {
        AssertCheck::Empty { empty } => {
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
        AssertCheck::RowCount { row_count } => {
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
        AssertCheck::CountEquals { count_equals } => {
            check_int_field(batch, count_equals, |v, e| v != e, "=", label, severity)
        }
        AssertCheck::CountGte { count_gte } => {
            check_int_field(batch, count_gte, |v, e| v < e, ">=", label, severity)
        }
        AssertCheck::Match { match_args } => {
            let glob = match globset::Glob::new(&match_args.pattern) {
                Ok(g) => g.compile_matcher(),
                Err(e) => {
                    return Some(fail(
                        label,
                        severity,
                        format!("Invalid glob pattern '{}': {e}", match_args.pattern),
                    ));
                }
            };
            if let Some(col) = batch.column_by_name(&match_args.field)
                && let Some(arr) = col.as_any().downcast_ref::<StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) && !glob.is_match(arr.value(i)) {
                        return Some(fail(
                            label,
                            severity,
                            format!(
                                "Row {i}: {}='{}' does not match '{}'",
                                match_args.field,
                                arr.value(i),
                                match_args.pattern
                            ),
                        ));
                    }
                }
            }
            None
        }
        AssertCheck::Row { row } => check_row(batch, row, total_rows, label, severity),
        AssertCheck::NoNulls { no_nulls } => {
            let Some(col) = batch.column_by_name(no_nulls) else {
                return Some(fail(
                    label,
                    severity,
                    format!("Column '{no_nulls}' not found"),
                ));
            };
            let nulls = col.null_count();
            if nulls > 0 {
                Some(fail(
                    label,
                    severity,
                    format!("Column '{no_nulls}' has {nulls} NULL values"),
                ))
            } else {
                None
            }
        }
        AssertCheck::Unique { unique } => {
            let Some(col) = batch.column_by_name(unique) else {
                return Some(fail(
                    label,
                    severity,
                    format!("Column '{unique}' not found"),
                ));
            };
            let mut seen = HashSet::new();
            for i in 0..total_rows {
                if !col.is_null(i) {
                    let val = format_cell(col.as_ref(), i);
                    if !seen.insert(val.clone()) {
                        return Some(fail(
                            label,
                            severity,
                            format!("Duplicate value in column '{unique}': '{val}'"),
                        ));
                    }
                }
            }
            None
        }
        AssertCheck::ColumnValues { column_values } => {
            let Some(col) = batch.column_by_name(&column_values.field) else {
                return Some(fail(
                    label,
                    severity,
                    format!("Column '{}' not found", column_values.field),
                ));
            };
            let expected: HashSet<String> = column_values.values.iter().cloned().collect();
            let mut actual = HashSet::new();
            for i in 0..total_rows {
                if !col.is_null(i) {
                    actual.insert(format_cell(col.as_ref(), i));
                }
            }
            if actual == expected {
                None
            } else {
                let missing: Vec<_> = expected.difference(&actual).collect();
                let extra: Vec<_> = actual.difference(&expected).collect();
                let mut parts = Vec::new();
                if !missing.is_empty() {
                    parts.push(format!("missing: {missing:?}"));
                }
                if !extra.is_empty() {
                    parts.push(format!("unexpected: {extra:?}"));
                }
                Some(fail(
                    label,
                    severity,
                    format!(
                        "Column '{}' value set mismatch: {}",
                        column_values.field,
                        parts.join(", ")
                    ),
                ))
            }
        }
    }
}

fn check_row(
    batch: &RecordBatch,
    expected: &HashMap<String, String>,
    total_rows: usize,
    label: &str,
    severity: Severity,
) -> Option<Failure> {
    let found = (0..total_rows).any(|row| {
        expected.iter().all(|(field, exp)| {
            batch
                .column_by_name(field)
                .map(|col| format_cell(col.as_ref(), row))
                .as_deref()
                == Some(exp.as_str())
        })
    });

    if found {
        None
    } else {
        let desc: Vec<String> = expected.iter().map(|(k, v)| format!("{k}={v}")).collect();
        Some(fail(
            label,
            severity,
            format!(
                "No row matching {{{}}} in {total_rows} rows",
                desc.join(", ")
            ),
        ))
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
