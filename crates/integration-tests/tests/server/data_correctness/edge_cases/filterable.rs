//! Verify that `traversal_path` (marked `filterable: false`) can still be
//! selected as a return column and returns correct values from ClickHouse.

use super::super::helpers::*;

pub(crate) async fn filterable_traversal_path_readable_as_column(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "g", "entity": "Group",
                     "columns": ["name", "traversal_path"],
                     "node_ids": [100]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group") && n.prop_str("traversal_path") == Some("1/100/")
    });
}

pub(crate) async fn filterable_traversal_path_readable_on_project(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project",
                     "columns": ["name", "traversal_path"],
                     "node_ids": [1000]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node("Project", 1000, |n| n.prop_str("traversal_path").is_some());
}

pub(crate) async fn filterable_other_filters_still_work_alongside_traversal_path_column(
    ctx: &TestContext,
) {
    // Select traversal_path as a column while filtering on a different field.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "g", "entity": "Group",
                     "columns": ["name", "traversal_path"],
                     "filters": {"name": "Public Group"}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group") && n.prop_str("traversal_path") == Some("1/100/")
    });
}
