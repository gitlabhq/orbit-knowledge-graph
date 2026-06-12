use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use gkg_utils::arrow::{ArrowUtils, ColumnValue};

use super::format::{EdgeExpect, Expect, Matcher, NodeExpect, RowMatcher};
use super::seed::prefix_graph_table;
use crate::assertions::{assert_edge_tags_by_source, assert_edge_tags_by_target, edge_table};
use crate::context::TestContext;
use crate::t;

pub async fn check_expect(ctx: &TestContext, expect: &Expect, location: &str) {
    for (table, node_expect) in &expect.nodes {
        check_nodes(ctx, table, node_expect, location).await;
    }
    for edge_expect in &expect.edges {
        check_edges(ctx, edge_expect, location).await;
    }
    for (table, expected_total) in &expect.totals {
        check_total(ctx, table, *expected_total, location).await;
    }
}

async fn check_nodes(ctx: &TestContext, table: &str, expect: &NodeExpect, location: &str) {
    let batches = ctx
        .query(&format!("SELECT * FROM {} FINAL", t(table)))
        .await;
    let rows = extract_rows(&batches);

    if let Some(expected) = expect.expected_count() {
        assert_eq!(
            rows.len(),
            expected,
            "{location}: expected {expected} rows in {table}, got {}",
            rows.len()
        );
    }

    for matcher in &expect.rows {
        assert_exactly_one_match(&rows, matcher, &format!("{location}: {table}"));
    }
}

async fn check_edges(ctx: &TestContext, expect: &EdgeExpect, location: &str) {
    let kind = &expect.kind;
    let table = edge_table(kind);
    let mut conditions = vec![format!("relationship_kind = '{kind}'")];
    if let Some(from) = &expect.from {
        conditions.push(format!("source_kind = '{from}'"));
    }
    if let Some(to) = &expect.to {
        conditions.push(format!("target_kind = '{to}'"));
    }
    let batches = ctx
        .query(&format!(
            "SELECT * FROM {table} FINAL WHERE {}",
            conditions.join(" AND ")
        ))
        .await;
    let rows = extract_rows(&batches);
    let edge_location = format!("{location}: {kind} edges");

    if let Some(expected) = expect.count {
        assert_eq!(
            rows.len(),
            expected,
            "{edge_location}: expected {expected}, got {}",
            rows.len()
        );
    }

    if let Some(expected_path) = &expect.traversal_path {
        for (i, row) in rows.iter().enumerate() {
            let actual = &row["traversal_path"];
            assert_eq!(
                actual,
                &ColumnValue::String(expected_path.clone()),
                "{edge_location}: row {i} should have traversal_path '{expected_path}', got {actual:?}"
            );
        }
    }

    for matcher in &expect.rows {
        assert_exactly_one_match(&rows, matcher, &edge_location);
    }

    if !expect.source_tags.is_empty() {
        let (from, to) = tag_endpoint_kinds(expect, &edge_location);
        let expected = tag_pairs(&expect.source_tags);
        let expected_refs = tag_pair_refs(&expected);
        assert_edge_tags_by_source(ctx, kind, from, to, "source_tags", &expected_refs).await;
    }
    if !expect.target_tags.is_empty() {
        let (from, to) = tag_endpoint_kinds(expect, &edge_location);
        let expected = tag_pairs(&expect.target_tags);
        let expected_refs = tag_pair_refs(&expected);
        assert_edge_tags_by_target(ctx, kind, from, to, "target_tags", &expected_refs).await;
    }
}

fn tag_endpoint_kinds<'a>(expect: &'a EdgeExpect, edge_location: &str) -> (&'a str, &'a str) {
    match (&expect.from, &expect.to) {
        (Some(from), Some(to)) => (from.as_str(), to.as_str()),
        _ => panic!("{edge_location}: source_tags/target_tags require both from: and to:"),
    }
}

fn tag_pairs(tags: &std::collections::BTreeMap<i64, Vec<String>>) -> Vec<(i64, Vec<&str>)> {
    tags.iter()
        .map(|(id, t)| (*id, t.iter().map(String::as_str).collect()))
        .collect()
}

fn tag_pair_refs<'a>(pairs: &'a [(i64, Vec<&'a str>)]) -> Vec<(i64, &'a [&'a str])> {
    pairs.iter().map(|(id, t)| (*id, t.as_slice())).collect()
}

async fn check_total(ctx: &TestContext, table: &str, expected: usize, location: &str) {
    let physical = prefix_graph_table(table);
    let batches = ctx
        .query(&format!("SELECT count() AS c FROM {physical} FINAL"))
        .await;
    let actual = extract_rows(&batches)
        .first()
        .and_then(|row| row["c"].coerce::<i64>())
        .unwrap_or(0);
    assert_eq!(
        actual, expected as i64,
        "{location}: expected {expected} total rows in {table}, got {actual}"
    );
}

fn extract_rows(batches: &[RecordBatch]) -> Vec<HashMap<String, ColumnValue>> {
    batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|i| ArrowUtils::extract_row(batch, i)))
        .collect()
}

fn assert_exactly_one_match(
    rows: &[HashMap<String, ColumnValue>],
    matcher: &RowMatcher,
    location: &str,
) {
    // An absent column means every row silently fails to match; surface it as a
    // schema/typo error instead of a misleading "matched 0 rows". Skipped when the
    // result set is empty, where zero matches is the real failure to report.
    if !rows.is_empty() {
        for column in matcher.keys() {
            assert!(
                rows.iter().any(|row| row.contains_key(column)),
                "{location}: matcher references column '{column}' that is absent from every row; \
                 check the column name against the table schema"
            );
        }
    }
    let matching = rows.iter().filter(|row| row_matches(row, matcher)).count();
    if matching == 1 {
        return;
    }
    let described: Vec<String> = rows.iter().map(|row| describe_row(row, matcher)).collect();
    panic!(
        "{location}: matcher {matcher:?} matched {matching} rows (expected exactly 1); \
         actual rows (matched columns only):\n  {}",
        described.join("\n  ")
    );
}

fn row_matches(row: &HashMap<String, ColumnValue>, matcher: &RowMatcher) -> bool {
    matcher.iter().all(|(column, expected)| {
        row.get(column)
            .is_some_and(|actual| value_matches(expected, actual))
    })
}

fn value_matches(expected: &Matcher, actual: &ColumnValue) -> bool {
    match expected {
        Matcher::Contains(c) => actual.as_string().is_some_and(|s| s.contains(&c.contains)),
        Matcher::Value(value) => {
            let json = serde_json::to_value(value).expect("YAML value converts to JSON");
            ColumnValue::from(json) == *actual
        }
    }
}

fn describe_row(row: &HashMap<String, ColumnValue>, matcher: &RowMatcher) -> String {
    let fields: Vec<String> = matcher
        .keys()
        .map(|column| format!("{column}={:?}", row.get(column)))
        .collect();
    format!("{{ {} }}", fields.join(", "))
}
