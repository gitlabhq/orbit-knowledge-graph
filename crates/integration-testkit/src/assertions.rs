use crate::context::TestContext;
use crate::extract::get_string_column;

pub async fn assert_node_count(ctx: &TestContext, table: &str, expected: usize) {
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
    let query = format!(
        "SELECT source_id, target_id FROM gl_edge FINAL \
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
    let query = format!(
        "SELECT 1 FROM gl_edge FINAL \
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
    let query = format!(
        "SELECT traversal_path FROM gl_edge FINAL \
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
    let paths = get_string_column(batch, "traversal_path");
    for i in 0..batch.num_rows() {
        assert_eq!(
            paths.value(i),
            expected_traversal_path,
            "{relationship_kind} edge row {i} should have traversal_path '{expected_traversal_path}'"
        );
    }
}
