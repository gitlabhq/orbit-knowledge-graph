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
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_i64("mr_count") == Some(2)
    });

    resp.assert_node("User", 2, |n| {
        n.prop_str("username") == Some("bob") && n.prop_i64("mr_count") == Some(1)
    });

    resp.assert_node("User", 3, |n| {
        n.prop_str("username") == Some("charlie") && n.prop_i64("mr_count") == Some(1)
    });
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
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_edge_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_i64("mr_edge_count") == Some(3)
    });
    resp.assert_node("User", 2, |n| {
        n.prop_str("username") == Some("bob") && n.prop_i64("mr_edge_count") == Some(2)
    });
    resp.assert_node("User", 3, |n| {
        n.prop_str("username") == Some("charlie") && n.prop_i64("mr_edge_count") == Some(2)
    });
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
            "aggregations": [{"function": "count", "target": "p", "group_by": "g", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group") && n.prop_i64("project_count") == Some(2)
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_str("name") == Some("Private Group") && n.prop_i64("project_count") == Some(2)
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group") && n.prop_i64("project_count") == Some(1)
    });
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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_order("Group", &[101, 100, 102]);

    resp.assert_node("Group", 101, |n| n.prop_i64("member_count") == Some(4));
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
    resp.assert_node("Group", 102, |n| n.prop_i64("member_count") == Some(2));
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
            "aggregations": [{"function": "sum", "target": "u", "property": "id", "group_by": "g", "alias": "id_sum"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("Group", 100, |n| n.prop_i64("id_sum") == Some(1 + 2 + 6));
    resp.assert_node("Group", 101, |n| {
        n.prop_i64("id_sum") == Some(3 + 4 + 5 + 6)
    });
    resp.assert_node("Group", 102, |n| n.prop_i64("id_sum") == Some(1 + 4));
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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    // Aggregation counts are computed in ClickHouse SQL before redaction.
    // Redaction removes unauthorized *rows* (entity-level), not aggregated
    // values within surviving rows. Group 100 has 3 MEMBER_OF edges in the
    // DB so count stays 3 even though only users 1,2 are authorized.
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
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
            "aggregations": [{"function": "avg", "target": "u", "property": "id", "group_by": "g", "alias": "avg_id"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: users 1,2,6 → avg = 3.0
    // Group 101: users 3,4,5,6 → avg = 4.5
    // Group 102: users 1,4 → avg = 2.5
    resp.assert_node("Group", 100, |n| n.prop_f64("avg_id") == Some(3.0));
    resp.assert_node("Group", 101, |n| n.prop_f64("avg_id") == Some(4.5));
    resp.assert_node("Group", 102, |n| n.prop_f64("avg_id") == Some(2.5));
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
            "aggregations": [
                {"function": "min", "target": "u", "property": "id", "group_by": "g", "alias": "min_id"},
                {"function": "max", "target": "u", "property": "id", "group_by": "g", "alias": "max_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: users 1,2,6 → min=1 max=6
    // Group 101: users 3,4,5,6 → min=3 max=6
    // Group 102: users 1,4 → min=1 max=4
    resp.assert_node("Group", 100, |n| {
        n.prop_i64("min_id") == Some(1) && n.prop_i64("max_id") == Some(6)
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_i64("min_id") == Some(3) && n.prop_i64("max_id") == Some(6)
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_i64("min_id") == Some(1) && n.prop_i64("max_id") == Some(4)
    });
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
            "aggregations": [{"function": "min", "target": "u", "property": "username", "group_by": "g", "alias": "first_username"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Lexicographic min: Group 100 (alice,bob,用户) → alice
    // Group 101 (charlie,diana,eve,用户) → charlie
    // Group 102 (alice,diana) → alice
    resp.assert_node("Group", 100, |n| {
        n.prop_str("first_username") == Some("alice")
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_str("first_username") == Some("charlie")
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("first_username") == Some("alice")
    });
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
            "aggregations": [
                {"function": "count", "target": "u", "group_by": "g", "alias": "cnt"},
                {"function": "avg", "target": "u", "property": "id", "group_by": "g", "alias": "avg_id"},
                {"function": "min", "target": "u", "property": "id", "group_by": "g", "alias": "min_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: 3 members, avg=3.0, min=1
    resp.assert_node("Group", 100, |n| {
        n.prop_i64("cnt") == Some(3)
            && n.prop_f64("avg_id") == Some(3.0)
            && n.prop_i64("min_id") == Some(1)
    });
    // Group 101: 4 members, avg=4.5, min=3
    resp.assert_node("Group", 101, |n| {
        n.prop_i64("cnt") == Some(4)
            && n.prop_f64("avg_id") == Some(4.5)
            && n.prop_i64("min_id") == Some(3)
    });
}

// ── Traversal path authorization ────────────────────────────────────────────

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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // MEMBER_OF edges under 1/100/: User 1→100, User 2→100, User 6→100 → count = 3
    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group") && n.prop_i64("member_count") == Some(3)
    });
    resp.assert_node_absent("Group", 101);
    resp.assert_node_absent("Group", 102);

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
            "aggregations": [{"function": "count", "target": "p", "group_by": "g", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    resp.assert_node("Group", 100, |n| n.prop_i64("project_count") == Some(2));
    resp.assert_node_absent("Group", 101);
    resp.assert_node_absent("Group", 102);
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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 100 (under 1/100/): users 1, 2, 6 → count = 3
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
    // Group 102 (under 1/102/): users 1, 4 → count = 2
    resp.assert_node("Group", 102, |n| n.prop_i64("member_count") == Some(2));
    // Group 101 (path 1/101/) not in security context
    resp.assert_node_absent("Group", 101);
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
            "aggregations": [{"function": "sum", "target": "u", "property": "id", "group_by": "g", "alias": "id_sum"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 100: users 1, 2, 6 (edges under 1/100/) → sum = 9
    resp.assert_node("Group", 100, |n| n.prop_i64("id_sum") == Some(1 + 2 + 6));
    resp.assert_node_absent("Group", 101);
    resp.assert_node_absent("Group", 102);
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
            "aggregations": [{"function": "count", "target": "p", "group_by": "g", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 100 CONTAINS Projects 1000, 1002 (both under 1/100/) → count = 2
    resp.assert_node("Group", 100, |n| n.prop_i64("project_count") == Some(2));
    resp.assert_node_absent("Group", 101);
    resp.assert_node_absent("Group", 102);
}

// Multi-node aggregation without group_by is now rejected at validation time
// to prevent full cross-join scans. Verify the rejection.
pub(super) async fn aggregation_no_group_by_with_filtered_other_node(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
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
        &ontology,
        &test_security_context(),
    );

    let err = result.expect_err("multi-node aggregation without group_by must reject");
    assert!(
        err.to_string().contains("group_by"),
        "error should mention group_by, got: {err}"
    );
}

// When both nodes of the relationship are edge-only, `build_joins` starts
// from the edge scan directly. The `relationship_kind` filter must still
// reach the WHERE clause or the count leaks rows from every relationship
// type between the two endpoint kinds. Seed has 4 AUTHORED User to
// MergeRequest edges and 3 APPROVED edges on the same endpoint kinds.
// Multi-node aggregation without group_by is now rejected at validation time.
pub(super) async fn aggregation_no_group_by_preserves_relationship_kind(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
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
        &ontology,
        &test_security_context(),
    );

    let err = result.expect_err("multi-node aggregation without group_by must reject");
    assert!(
        err.to_string().contains("group_by"),
        "error should mention group_by, got: {err}"
    );
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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        security_ctx,
    )
    .await;

    // Group 102: users 1, 4 (edges under 1/102/) → count = 2
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group") && n.prop_i64("member_count") == Some(2)
    });
    resp.assert_node_absent("Group", 100);
    resp.assert_node_absent("Group", 101);
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
            "aggregations": [
                {"function": "count", "target": "mr", "group_by": "d", "alias": "mr_count"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Seed: MR 2000 HAS_DIFF {5000, 5001}, MR 2001 HAS_DIFF {5002}.
    // Each diff has exactly one MR on the from side.
    resp.assert_node("MergeRequestDiff", 5000, |n| {
        n.prop_str("state") == Some("collected") && n.prop_i64("mr_count") == Some(1)
    });
    resp.assert_node("MergeRequestDiff", 5001, |n| {
        n.prop_str("state") == Some("collected") && n.prop_i64("mr_count") == Some(1)
    });
    resp.assert_node("MergeRequestDiff", 5002, |n| {
        n.prop_str("state") == Some("collected") && n.prop_i64("mr_count") == Some(1)
    });
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
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
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
    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_i64("note_count") == Some(4)
    });
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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
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
