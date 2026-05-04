use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;
use std::collections::BTreeSet;
use std::sync::LazyLock;

use crate::context::TestContext;
use crate::t;

static ONTOLOGY: LazyLock<ontology::Ontology> =
    LazyLock::new(|| ontology::Ontology::load_embedded().expect("embedded ontology should load"));

/// Returns the prefixed edge table name for the given relationship kind.
fn edge_table(relationship_kind: &str) -> String {
    t(ONTOLOGY.edge_table_for_relationship(relationship_kind))
}

pub async fn assert_node_count(ctx: &TestContext, table: &str, expected: usize) {
    let table = t(table);
    let result = ctx.query(&format!("SELECT 1 FROM {table} FINAL")).await;
    let actual = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        actual, expected,
        "expected {expected} rows in {table}, got {actual}"
    );
}

pub async fn assert_edge_count(
    ctx: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    expected: usize,
) {
    let edge_table = edge_table(relationship_kind);
    let query = format!(
        "SELECT source_id, target_id FROM {edge_table} FINAL \
         WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}'"
    );
    let result = ctx.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );
    assert_eq!(
        result[0].num_rows(),
        expected,
        "expected {expected} {relationship_kind} edges from {source_kind} to {target_kind}"
    );
}

pub async fn assert_edge_count_for_traversal_path(
    ctx: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    traversal_path: &str,
    expected: usize,
) {
    let edge_table = edge_table(relationship_kind);
    let query = format!(
        "SELECT 1 FROM {edge_table} FINAL \
         WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}' \
         AND traversal_path = '{traversal_path}'"
    );
    let result = ctx.query(&query).await;
    let actual = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        actual, expected,
        "expected {expected} {relationship_kind} edges ({source_kind} → {target_kind}) \
         with traversal_path '{traversal_path}', got {actual}"
    );
}

pub async fn assert_edges_have_traversal_path(
    ctx: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    expected_traversal_path: &str,
    expected_count: usize,
) {
    let edge_table = edge_table(relationship_kind);
    let query = format!(
        "SELECT traversal_path FROM {edge_table} FINAL \
         WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}'"
    );
    let result = ctx.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );
    let batch = &result[0];
    assert_eq!(
        batch.num_rows(),
        expected_count,
        "expected {expected_count} {relationship_kind} edges from {source_kind} to {target_kind}"
    );
    let paths = ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path")
        .expect("traversal_path column");
    for i in 0..batch.num_rows() {
        assert_eq!(
            paths.value(i),
            expected_traversal_path,
            "{relationship_kind} edge row {i} should have traversal_path '{expected_traversal_path}'"
        );
    }
}

/// Assert that every edge matching the given relationship/source/target kinds
/// contains the expected set of tags in the specified tag column
/// (`source_tags` or `target_tags`).
pub async fn assert_edge_tags(
    ctx: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    tag_column: &str,
    expected_tags: &[&str],
) {
    let edge_table = edge_table(relationship_kind);
    let query = format!(
        "SELECT {tag_column} FROM {edge_table} FINAL \
         WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}' \
         ORDER BY source_id, target_id"
    );
    let result = ctx.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );
    let expected: BTreeSet<&str> = expected_tags.iter().copied().collect();
    let batch = &result[0];
    for i in 0..batch.num_rows() {
        let tags: BTreeSet<String> = ArrowUtils::get_string_list(batch, tag_column, i)
            .into_iter()
            .collect();
        let tags_ref: BTreeSet<&str> = tags.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            tags_ref, expected,
            "{relationship_kind} edge row {i}: expected {tag_column} = {expected:?}, got {tags_ref:?}"
        );
    }
}

/// Assert that edges matching the given criteria have the expected tags per
/// source_id. `expected` maps source_id → expected tag set.
pub async fn assert_edge_tags_by_source(
    ctx: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    tag_column: &str,
    expected: &[(i64, &[&str])],
) {
    let edge_table = edge_table(relationship_kind);
    let query = format!(
        "SELECT source_id, {tag_column} FROM {edge_table} FINAL \
         WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}' \
         ORDER BY source_id"
    );
    let result = ctx.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );

    let batch = &result[0];
    let source_ids = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(batch, "source_id")
        .expect("source_id column");

    for &(sid, tags) in expected {
        let row = (0..batch.num_rows()).find(|&i| source_ids.value(i) == sid);
        let row = row
            .unwrap_or_else(|| panic!("{relationship_kind} edge with source_id={sid} not found"));
        let actual: BTreeSet<String> = ArrowUtils::get_string_list(batch, tag_column, row)
            .into_iter()
            .collect();
        let expected_set: BTreeSet<&str> = tags.iter().copied().collect();
        let actual_ref: BTreeSet<&str> = actual.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            actual_ref, expected_set,
            "{relationship_kind} edge source_id={sid}: expected {tag_column} = {expected_set:?}, got {actual_ref:?}"
        );
    }
}

/// Assert that edges matching the given criteria have the expected tags per
/// target_id. `expected` maps target_id → expected tag set.
pub async fn assert_edge_tags_by_target(
    ctx: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    tag_column: &str,
    expected: &[(i64, &[&str])],
) {
    let edge_table = edge_table(relationship_kind);
    let query = format!(
        "SELECT target_id, {tag_column} FROM {edge_table} FINAL \
         WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}' \
         ORDER BY target_id"
    );
    let result = ctx.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );

    let batch = &result[0];
    let target_ids = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(batch, "target_id")
        .expect("target_id column");

    for &(tid, tags) in expected {
        let row = (0..batch.num_rows()).find(|&i| target_ids.value(i) == tid);
        let row = row
            .unwrap_or_else(|| panic!("{relationship_kind} edge with target_id={tid} not found"));
        let actual: BTreeSet<String> = ArrowUtils::get_string_list(batch, tag_column, row)
            .into_iter()
            .collect();
        let expected_set: BTreeSet<&str> = tags.iter().copied().collect();
        let actual_ref: BTreeSet<&str> = actual.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            actual_ref, expected_set,
            "{relationship_kind} edge target_id={tid}: expected {tag_column} = {expected_set:?}, got {actual_ref:?}"
        );
    }
}
