use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::{Frontend, compile_local};
use duckdb_client::DuckDbClient;
use ontology::Ontology;
use orbit_utils::arrow::ArrowUtils;
use tabled::{Table, builder::Builder};

use super::assertions::{
    Assert, AssertCheck, FieldValueArgs, QueryBlock, Severity, TestCase, TestSuite,
};

#[derive(Debug)]
pub(crate) struct Failure {
    pub test: String,
    pub severity: Severity,
    pub message: String,
}

pub(crate) fn run_suite(
    suite: &TestSuite,
    client: &DuckDbClient,
    ontology: &Arc<Ontology>,
) -> Vec<Failure> {
    let mut failures = Vec::new();
    for test in &suite.tests {
        if test.skip {
            eprintln!("  [SKIP] \"{}\"", test.name);
            continue;
        }
        failures.extend(run_test(test, client, ontology));
    }
    failures
}

fn run_test(test: &TestCase, client: &DuckDbClient, ontology: &Arc<Ontology>) -> Vec<Failure> {
    if test.debug {
        dump_datasets(client, ontology);
    }

    let blocks = test.all_queries();
    let mut failures = Vec::new();

    for (i, block) in blocks.iter().enumerate() {
        let label = if blocks.len() == 1 {
            test.name.clone()
        } else {
            format!("{} [query {}]", test.name, i + 1)
        };
        failures.extend(run_query_block(
            &label,
            test.severity,
            block,
            client,
            ontology,
        ));
    }

    if !failures.is_empty() && !test.debug {
        dump_datasets(client, ontology);
    }

    failures
}

fn execute_cypher(
    cypher: &str,
    client: &DuckDbClient,
    ontology: &Arc<Ontology>,
) -> anyhow::Result<RecordBatch> {
    let (clean_query, aliases) = rewrite_query(cypher);
    let compiled = compile_local(&clean_query, Frontend::Gql, ontology)?;
    let sql = compiled.base.render();
    eprintln!("  SQL: {sql}");
    let batches = client.query_arrow(&sql)?;
    let batch = if batches.is_empty() {
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        RecordBatch::new_empty(schema)
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| anyhow::anyhow!("concat batches: {e}"))?
    };
    Ok(apply_aliases(batch, &compiled.input, &aliases))
}

/// Rewrite a fixture Cypher query for the Orbit GQL compiler:
/// - Strip `AS alias` from RETURN property items and collect the mapping
/// - Rewrite `ORDER BY alias` to `ORDER BY node.prop`
/// - Escape backslashes inside single-quoted string literals
fn rewrite_query(cypher: &str) -> (String, Vec<(String, String, String)>) {
    let alias_re = regex::Regex::new(r"(?i)\b(\w+)\.(\w+)\s+AS\s+(\w+)").unwrap();
    let mut aliases = Vec::new();
    let rewritten = alias_re.replace_all(cypher, |caps: &regex::Captures| {
        let node = caps[1].to_string();
        let prop = caps[2].to_string();
        let alias = caps[3].to_string();
        aliases.push((node.clone(), prop.clone(), alias));
        format!("{node}.{prop}")
    });
    let mut result = rewritten.into_owned();

    let order_re = regex::Regex::new(r"(?i)\bORDER\s+BY\s+(\w+)").unwrap();
    if let Some(caps) = order_re.captures(&result) {
        let sort_key = &caps[1];
        if let Some((node, prop, _)) = aliases.iter().find(|(_, _, a)| a == sort_key) {
            let replacement = format!("ORDER BY {node}.{prop}");
            result = order_re.replace(&result, replacement.as_str()).into_owned();
        }
    }

    let backslash_re = regex::Regex::new(r"'([^']*\\'[^']*)'|'([^']*\\[^']*)'").unwrap();
    if result.contains('\\') {
        let lit_re = regex::Regex::new(r"'([^']*)'").unwrap();
        result = lit_re
            .replace_all(&result, |caps: &regex::Captures| {
                let inner = &caps[1];
                if inner.contains('\\') {
                    format!("'{}'", inner.replace('\\', "\\\\"))
                } else {
                    caps[0].to_string()
                }
            })
            .into_owned();
    }

    (result, aliases)
}

/// Rename result columns: strip `{node_id}_` prefix, then apply user aliases.
fn apply_aliases(
    batch: RecordBatch,
    input: &compiler::Input,
    aliases: &[(String, String, String)],
) -> RecordBatch {
    let prefixes: Vec<String> = input.nodes.iter().map(|n| format!("{}_", n.id)).collect();
    let schema = batch.schema();
    let new_fields: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name();
            let mut stripped = name.clone();
            for prefix in &prefixes {
                if let Some(s) = name.strip_prefix(prefix.as_str()) {
                    stripped = s.to_string();
                    break;
                }
            }
            for (node, prop, alias) in aliases {
                let prefixed = format!("{node}_{prop}");
                if name == &prefixed {
                    return f.as_ref().clone().with_name(alias);
                }
            }
            f.as_ref().clone().with_name(stripped)
        })
        .collect();
    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .unwrap_or_else(|e| panic!("apply_aliases failed: {e}"))
}

fn dump_datasets(client: &DuckDbClient, ontology: &Arc<Ontology>) {
    let debug_queries = [
        (
            "Definitions",
            "MATCH (d:Definition) RETURN d.name, d.fqn, d.definition_type, d.file_path",
        ),
        ("Files", "MATCH (f:File) RETURN f.path, f.language"),
        (
            "Imports",
            "MATCH (i:ImportedSymbol) RETURN i.file_path, i.import_path, i.identifier_name, i.identifier_alias",
        ),
    ];

    eprintln!("\n  ╔══ DEBUG DUMP ══════════════════════════════════════");
    for (label, cypher) in debug_queries {
        match execute_cypher(cypher, client, ontology) {
            Ok(batch) => print_result(&format!("  {label}"), cypher, &batch),
            Err(e) => eprintln!("  {label}: query failed: {e}"),
        }
    }
    eprintln!("  ╚══════════════════════════════════════════════════\n");
}

fn run_query_block(
    label: &str,
    severity: Severity,
    block: &QueryBlock,
    client: &DuckDbClient,
    ontology: &Arc<Ontology>,
) -> Vec<Failure> {
    let batch = match execute_cypher(&block.query, client, ontology) {
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
        for row in 0..batch.num_rows() {
            let vals: Vec<String> = (0..batch.num_columns())
                .map(|col| format_cell(batch.column(col).as_ref(), row))
                .collect();
            builder.push_record(&vals);
        }
        let table = Table::from(builder).to_string();
        for line in table.lines() {
            eprintln!("  {line}");
        }
    }
}

fn format_cell(array: &dyn Array, row: usize) -> String {
    ArrowUtils::array_value_to_string(array, row).unwrap_or_else(|| "NULL".into())
}

fn expected_value_matches(array: &dyn Array, row: usize, expected: &serde_json::Value) -> bool {
    match expected {
        serde_json::Value::Null => array.is_null(row),
        serde_json::Value::Bool(value) => {
            !array.is_null(row) && format_cell(array, row) == value.to_string()
        }
        serde_json::Value::Number(value) => {
            !array.is_null(row) && format_cell(array, row) == value.to_string()
        }
        serde_json::Value::String(value) => {
            !array.is_null(row) && format_cell(array, row) == *value
        }
        _ => false,
    }
}

fn expected_value_display(expected: &serde_json::Value) -> String {
    match expected {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        _ => match orbit_utils::yaml::to_string(expected) {
            Ok(value) => value.trim().to_string(),
            Err(_) => "<unsupported>".to_string(),
        },
    }
}

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

    let indices =
        arrow::array::UInt32Array::from(matching.iter().map(|&i| i as u32).collect::<Vec<_>>());
    let columns: Vec<Arc<dyn Array>> = (0..batch.num_columns())
        .map(|col_idx| arrow::compute::take(batch.column(col_idx), &indices, None).unwrap())
        .collect();

    RecordBatch::try_new(batch.schema(), columns)
        .unwrap_or_else(|e| panic!("where filter failed: {e}"))
}

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
    expected: &HashMap<String, serde_json::Value>,
    total_rows: usize,
    label: &str,
    severity: Severity,
) -> Option<Failure> {
    let found = (0..total_rows).any(|row| {
        expected.iter().all(|(field, exp)| {
            batch
                .column_by_name(field)
                .map(|col| expected_value_matches(col.as_ref(), row, exp))
                .unwrap_or(false)
        })
    });

    if found {
        None
    } else {
        let desc: Vec<String> = expected
            .iter()
            .map(|(k, v)| format!("{k}={}", expected_value_display(v)))
            .collect();
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
