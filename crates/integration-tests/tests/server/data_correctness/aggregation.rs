use super::helpers::*;

pub(super) async fn aggregation_count_returns_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_node_property_str("u", "User", 1, "username", "alice");
    resp.assert_group_row_value_i64("u", "User", 1, "mr_count", 2);
    resp.assert_group_node_property_str("u", "User", 2, "username", "bob");
    resp.assert_group_row_value_i64("u", "User", 2, "mr_count", 1);
    resp.assert_group_node_property_str("u", "User", 3, "username", "charlie");
    resp.assert_group_row_value_i64("u", "User", 3, "mr_count", 1);
}

pub(super) async fn aggregation_wildcard_user_to_mr_counts_inferred_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "*", "from": "u", "to": "mr"}],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "mr_edge_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_node_property_str("u", "User", 1, "username", "alice");
    resp.assert_group_row_value_i64("u", "User", 1, "mr_edge_count", 3);
    resp.assert_group_node_property_str("u", "User", 2, "username", "bob");
    resp.assert_group_row_value_i64("u", "User", 2, "mr_edge_count", 2);
    resp.assert_group_node_property_str("u", "User", 3, "username", "charlie");
    resp.assert_group_row_value_i64("u", "User", 3, "mr_edge_count", 2);
}

pub(super) async fn aggregation_count_group_contains_projects(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_node_property_str("g", "Group", 100, "name", "Public Group");
    resp.assert_group_row_value_i64("g", "Group", 100, "project_count", 2);
    resp.assert_group_node_property_str("g", "Group", 101, "name", "Private Group");
    resp.assert_group_row_value_i64("g", "Group", 101, "project_count", 2);
    resp.assert_group_node_property_str("g", "Group", 102, "name", "Internal Group");
    resp.assert_group_row_value_i64("g", "Group", 102, "project_count", 1);
}

pub(super) async fn aggregation_sort_orders_by_aggregate_value(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "member_count"}],
            "aggregation_sort": {"column": "member_count", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_node_order("g", "Group", &[101, 100, 102]);
    resp.assert_group_row_value_i64("g", "Group", 101, "member_count", 4);
    resp.assert_group_row_value_i64("g", "Group", 100, "member_count", 3);
    resp.assert_group_row_value_i64("g", "Group", 102, "member_count", 2);
}

pub(super) async fn aggregation_group_by_property_truncate_month(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [2000, 2001]},
                {"id": "n", "entity": "Note", "filters": {"created_at": {"op": "gte", "value": "2024-01-01T00:00:00Z"}}}
            ],
            "relationships": [{"type": "HAS_NOTE", "from": "mr", "to": "n"}],
            "group_by": [
                {"kind": "property", "node": "n", "property": "created_at", "transform": {"kind": "truncate", "unit": "month"}, "alias": "bucket"}
            ],
            "aggregations": [{"function": "count", "target": "n", "alias": "note_count"}],
            "aggregation_sort": {"column": "bucket", "direction": "ASC"},
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeIds);
    resp.skip_requirement(Requirement::Filter {
        field: "created_at".into(),
    });
    resp.skip_requirement(Requirement::AggregationSort);
    resp.skip_requirement(Requirement::Aggregation);

    let buckets: Vec<(String, i64)> = resp
        .rows()
        .iter()
        .map(|row| {
            let bucket = row
                .get("bucket")
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            let c = row.get("note_count").and_then(|v| v.as_i64()).unwrap_or(0);
            (bucket, c)
        })
        .collect();
    assert_eq!(
        buckets.len(),
        2,
        "expected two monthly buckets; got {buckets:?}"
    );
    let bucket_strs: Vec<&str> = buckets.iter().map(|(b, _)| b.as_str()).collect();
    assert!(
        bucket_strs.iter().any(|b| b.contains("2024-01")),
        "expected a January 2024 bucket; got {buckets:?}"
    );
    assert!(
        bucket_strs.iter().any(|b| b.contains("2024-02")),
        "expected a February 2024 bucket; got {buckets:?}"
    );
    for (b, c) in &buckets {
        assert_eq!(*c, 1, "bucket {b} should have one note, got {c}");
    }
    assert_eq!(
        bucket_strs,
        vec!["2024-01-01", "2024-02-01"],
        "buckets should be ASC-sorted Date strings"
    );
}

pub(super) async fn aggregation_sum_produces_correct_totals(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "sum", "target": "u", "property": "id", "alias": "id_sum"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_row_value_i64("g", "Group", 100, "id_sum", 1 + 2 + 6);
    resp.assert_group_row_value_i64("g", "Group", 101, "id_sum", 3 + 4 + 5 + 6);
    resp.assert_group_row_value_i64("g", "Group", 102, "id_sum", 1 + 4);
}

pub(super) async fn aggregation_redaction_excludes_unauthorized_from_counts(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2]);
    svc.allow("group", &[100, 101, 102, 200, 300]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "member_count"}],
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    // Aggregation counts are computed in ClickHouse SQL before redaction.
    // Redaction removes unauthorized *rows* (entity-level), not aggregated
    // values within surviving rows. Group 100 has 3 MEMBER_OF edges in the
    // DB so count stays 3 even though only users 1,2 are authorized.
    resp.assert_group_row_value_i64("g", "Group", 100, "member_count", 3);
}

pub(super) async fn aggregation_avg_produces_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "avg", "target": "u", "property": "id", "alias": "avg_id"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: users 1,2,6 → avg = 3.0
    // Group 101: users 3,4,5,6 → avg = 4.5
    // Group 102: users 1,4 → avg = 2.5
    resp.assert_group_row_value_f64("g", "Group", 100, "avg_id", 3.0);
    resp.assert_group_row_value_f64("g", "Group", 101, "avg_id", 4.5);
    resp.assert_group_row_value_f64("g", "Group", 102, "avg_id", 2.5);
}

pub(super) async fn aggregation_min_max_produce_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [
                {"function": "min", "target": "u", "property": "id", "alias": "min_id"},
                {"function": "max", "target": "u", "property": "id", "alias": "max_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: users 1,2,6 → min=1 max=6
    // Group 101: users 3,4,5,6 → min=3 max=6
    // Group 102: users 1,4 → min=1 max=4
    resp.assert_group_row_value_i64("g", "Group", 100, "min_id", 1);
    resp.assert_group_row_value_i64("g", "Group", 100, "max_id", 6);
    resp.assert_group_row_value_i64("g", "Group", 101, "min_id", 3);
    resp.assert_group_row_value_i64("g", "Group", 101, "max_id", 6);
    resp.assert_group_row_value_i64("g", "Group", 102, "min_id", 1);
    resp.assert_group_row_value_i64("g", "Group", 102, "max_id", 4);
}

pub(super) async fn aggregation_min_on_string_column(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "min", "target": "u", "property": "username", "alias": "first_username"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Lexicographic min: Group 100 (alice,bob,用户) → alice
    // Group 101 (charlie,diana,eve,用户) → charlie
    // Group 102 (alice,diana) → alice
    resp.assert_group_row_value_str("g", "Group", 100, "first_username", "alice");
    resp.assert_group_row_value_str("g", "Group", 101, "first_username", "charlie");
    resp.assert_group_row_value_str("g", "Group", 102, "first_username", "alice");
}

pub(super) async fn aggregation_multiple_functions_in_one_query(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [
                {"function": "count", "target": "u", "alias": "cnt"},
                {"function": "avg", "target": "u", "property": "id", "alias": "avg_id"},
                {"function": "min", "target": "u", "property": "id", "alias": "min_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: 3 members, avg=3.0, min=1
    resp.assert_group_row_value_i64("g", "Group", 100, "cnt", 3);
    resp.assert_group_row_value_f64("g", "Group", 100, "avg_id", 3.0);
    resp.assert_group_row_value_i64("g", "Group", 100, "min_id", 1);
    // Group 101: 4 members, avg=4.5, min=3
    resp.assert_group_row_value_i64("g", "Group", 101, "cnt", 4);
    resp.assert_group_row_value_f64("g", "Group", 101, "avg_id", 4.5);
    resp.assert_group_row_value_i64("g", "Group", 101, "min_id", 3);
}

pub(super) async fn aggregation_path_single_nested_group(ctx: &TestContext) {
    // Security context limited to 1/100/ — only Group 100 and its descendants
    // (Groups 200/300, Projects 1000/1002) are visible. Groups 101, 102 are
    // outside this path and must not appear in results.
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // MEMBER_OF edges under 1/100/: User 1→100, User 2→100, User 6→100 → count = 3
    resp.assert_group_node_property_str("g", "Group", 100, "name", "Public Group");
    resp.assert_group_row_value_i64("g", "Group", 100, "member_count", 3);
    resp.assert_group_node_absent("g", "Group", 101);
    resp.assert_group_node_absent("g", "Group", 102);

    // Also verify project visibility under the same restricted path.
    // Projects 1000 and 1002 are under 1/100/, so CONTAINS count = 2.
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    resp.assert_group_row_value_i64("g", "Group", 100, "project_count", 2);
    resp.assert_group_node_absent("g", "Group", 101);
    resp.assert_group_node_absent("g", "Group", 102);
}

pub(super) async fn aggregation_path_multiple_groups(ctx: &TestContext) {
    // Access to 1/100/ (nested, has subgroups) and 1/102/ (flat, no subgroups).
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into(), "1/102/".into()]).unwrap();

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 100 (under 1/100/): users 1, 2, 6 → count = 3
    resp.assert_group_row_value_i64("g", "Group", 100, "member_count", 3);
    // Group 102 (under 1/102/): users 1, 4 → count = 2
    resp.assert_group_row_value_i64("g", "Group", 102, "member_count", 2);
    // Group 101 (path 1/101/) not in security context
    resp.assert_group_node_absent("g", "Group", 101);
}

pub(super) async fn aggregation_sum_with_restricted_path(ctx: &TestContext) {
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "sum", "target": "u", "property": "id", "alias": "id_sum"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 100: users 1, 2, 6 (edges under 1/100/) → sum = 9
    resp.assert_group_row_value_i64("g", "Group", 100, "id_sum", 1 + 2 + 6);
    resp.assert_group_node_absent("g", "Group", 101);
    resp.assert_group_node_absent("g", "Group", 102);
}

pub(super) async fn aggregation_nested_path_includes_child_projects(ctx: &TestContext) {
    // Path 1/100/ includes Group 100's children: Projects 1000 (path 1/100/1000/)
    // and 1002 (path 1/100/1002/), plus subgroups 200, 300.
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 100 CONTAINS Projects 1000, 1002 (both under 1/100/) → count = 2
    resp.assert_group_row_value_i64("g", "Group", 100, "project_count", 2);
    resp.assert_group_node_absent("g", "Group", 101);
    resp.assert_group_node_absent("g", "Group", 102);
}

pub(super) async fn aggregation_no_group_by_with_filtered_other_node(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}},
                {"id": "u", "entity": "User", "node_ids": [1]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "total_mrs"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeIds);
    resp.assert_aggregation_value_i64("total_mrs", 2);
}

// When both nodes of the relationship are edge-only, `build_joins` starts
// from the edge scan directly. The `relationship_kind` filter must still
// reach the WHERE clause or the count leaks rows from every relationship
// type between the two endpoint kinds. Seed has 6 AUTHORED User to
// MergeRequest edges and 3 APPROVED edges on the same endpoint kinds.
pub(super) async fn aggregation_no_group_by_preserves_relationship_kind(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "total_authored"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_aggregation_value_i64("total_authored", 6);
}

pub(super) async fn aggregation_non_nested_path_only(ctx: &TestContext) {
    // Only 1/102/ — flat group with one project and two MEMBER_OF edges.
    let security_ctx = SecurityContext::new(1, vec!["1/102/".into()]).unwrap();

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 102: users 1, 4 (edges under 1/102/) → count = 2
    resp.assert_group_node_property_str("g", "Group", 102, "name", "Internal Group");
    resp.assert_group_row_value_i64("g", "Group", 102, "member_count", 2);
    resp.assert_group_node_absent("g", "Group", 100);
    resp.assert_group_node_absent("g", "Group", 101);
}

pub(super) async fn aggregation_group_by_non_default_redaction_id_column(ctx: &TestContext) {
    // MergeRequestDiff has redaction.id_column = merge_request_id. When it's
    // the group_by node, enforce.rs emits a separate `_gkg_d_pk` column
    // alongside `_gkg_d_id`. Both must land in GROUP BY. Regression guard
    // for the ClickHouse side of the compiler fix that added `_gkg_*_pk` to
    // the GROUP BY clause for aggregations.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}},
                {"id": "d", "entity": "MergeRequestDiff", "columns": ["state"]}
            ],
            "relationships": [{"type": "HAS_DIFF", "from": "mr", "to": "d"}],
            "group_by": [{"kind": "node", "node": "d"}],
            "aggregations": [
                {"function": "count", "target": "mr", "alias": "mr_count"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Seed: MR 2000 HAS_DIFF {5000, 5001}, MR 2001 HAS_DIFF {5002}.
    // Each diff has exactly one MR on the from side.
    for id in [5000, 5001, 5002] {
        resp.assert_group_node_property_str("d", "MergeRequestDiff", id, "state", "collected");
        resp.assert_group_row_value_i64("d", "MergeRequestDiff", id, "mr_count", 1);
    }
}

// 3-node aggregation where the intermediate node (MR) is cascade-optimized
// into a CTE. The cascade `IN (SELECT ...)` filter must stay in WHERE, not
// be folded into `countIf` — otherwise ClickHouse errors with
// "Unknown identifier `mr.id`" because the CTE alias isn't in FROM.
pub(super) async fn aggregation_three_node_with_cascade_intermediate(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "n", "entity": "Note", "id_range": {"start": 1, "end": 10000}}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "HAS_NOTE", "from": "mr", "to": "n"}
            ],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "n", "alias": "note_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // node_ids on User is used for cascade narrowing, not for verifying
    // specific IDs in the response — skip that requirement.
    resp.skip_requirement(Requirement::NodeIds);
    // Edges are consumed by the aggregation join, not returned as response edges.
    resp.skip_requirement(Requirement::Relationship {
        edge_type: "AUTHORED".into(),
    });
    resp.skip_requirement(Requirement::Relationship {
        edge_type: "HAS_NOTE".into(),
    });

    // User 1 authored MR 2000 (notes 3000, 3002, 3003) and MR 2001 (note 3001) → 4 notes
    resp.assert_group_node_property_str("u", "User", 1, "username", "alice");
    resp.assert_group_row_value_i64("u", "User", 1, "note_count", 4);
}

pub(super) async fn aggregation_empty_security_context_rejects_at_compile(ctx: &TestContext) {
    // Empty traversal_paths — the query engine refuses to compile rather than
    // silently returning empty results. The defense-in-depth check_ast pass
    // requires every gl_* alias to have a valid startsWith predicate, which
    // cannot be satisfied with zero paths.
    let _ = ctx;
    let security_ctx = SecurityContext::new(1, vec![]).unwrap();
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "member_count"}],
            "limit": 10
        }"#,
        &ontology,
        &security_ctx,
    );

    assert!(
        result.is_err(),
        "empty security context should reject at compile time, got: {result:?}"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("traversal_path"),
        "error should mention traversal_path filter, got: {err}"
    );
}

// When the query omits `alias`, the column `name` in the formatted response
// MUST equal the function name ("count", "sum", etc.). Regression guard for
// the v2 compiler bug where the default was "agg_result".
pub(super) async fn aggregation_no_alias_defaults_to_function_name(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "mr"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_node_property_str("u", "User", 1, "username", "alice");
    resp.assert_group_row_value_i64("u", "User", 1, "count", 2);
    resp.assert_group_row_value_i64("u", "User", 2, "count", 1);
    resp.assert_group_row_value_i64("u", "User", 3, "count", 1);
}

pub(super) async fn aggregation_no_alias_sum_defaults_to_function_name(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "sum", "target": "u", "property": "id"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_group_node_property_str("g", "Group", 100, "name", "Public Group");
    resp.assert_group_row_value_i64("g", "Group", 100, "sum", 1 + 2 + 6);
}
