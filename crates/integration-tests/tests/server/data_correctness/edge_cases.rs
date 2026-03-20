use super::helpers::*;

pub(super) async fn traversal_referential_integrity_on_complex_query(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "MEMBER_OF", "from": "u", "to": "g"},
                {"type": "CONTAINS", "from": "g", "to": "p"}
            ],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(14);
    resp.assert_referential_integrity();

    let member_of = resp.edges_of_type("MEMBER_OF");
    assert!(!member_of.is_empty(), "should have MEMBER_OF edges");
    let contains = resp.edges_of_type("CONTAINS");
    assert!(!contains.is_empty(), "should have CONTAINS edges");
}

pub(super) async fn giant_string_survives_pipeline(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note"], "node_ids": [3002]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Note", &[3002]);
    resp.assert_node("Note", 3002, |n| {
        n.prop_str("note")
            .is_some_and(|s| s.len() == 10_000 && s.chars().all(|c| c == 'x'))
    });
}

pub(super) async fn sql_injection_string_preserved(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note"], "node_ids": [3003]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Note", &[3003]);
    resp.assert_node("Note", 3003, |n| {
        n.prop_str("note").is_some_and(|s| s.contains("DROP TABLE"))
    });
}

/// SIP (Sideways Information Passing) pre-filter fires when the root node has
/// node_ids and there are relationships. Verify that the CTE uses the correct
/// id column from the root node's `id_property` and returns correct results.
pub(super) async fn sip_prefilter_with_node_ids_returns_correct_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [1, 3]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_node_ids("User", &[1, 3]);
    resp.assert_node_ids("Group", &[100, 101, 102]);
    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 102), (3, 101)]);
    resp.assert_referential_integrity();
}

/// SIP also fires when the root node has filters. Verify the CTE correctly
/// narrows the edge scan and returns only matching rows.
pub(super) async fn sip_prefilter_with_filter_returns_correct_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username", "user_type"],
                 "filters": {"user_type": "project_bot"}},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_filter("User", "user_type", |n| {
        n.prop_str("user_type") == Some("project_bot")
    });
    resp.assert_node_ids("User", &[4]);
    resp.assert_node_ids("Group", &[101, 102]);
    resp.assert_edge_set("MEMBER_OF", &[(4, 101), (4, 102)]);
    resp.assert_referential_integrity();
}

/// SIP with node_ids on a multi-hop variable-length traversal. The CTE should
/// push root IDs into the first edge scan of each UNION ALL arm.
pub(super) async fn sip_prefilter_multi_hop_returns_correct_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "parent", "entity": "Group", "columns": ["name"], "node_ids": [100]},
                {"id": "child", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "CONTAINS", "from": "parent", "to": "child", "min_hops": 1, "max_hops": 2}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("Group", &[100, 200, 300]);
    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
    resp.assert_edge_exists("Group", 100, "Group", 300, "CONTAINS");
    resp.assert_referential_integrity();
}

/// Target-side SIP for aggregation queries. When the aggregation target has
/// filters (e.g. `mr.state = 'opened'`), the optimizer materializes matching
/// target IDs in a CTE and narrows the edge scan from the target side.
/// Verify the aggregation results are numerically correct with SIP active.
pub(super) async fn sip_target_aggregation_with_filter_returns_correct_counts(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "open_mr_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Aggregation results don't include target node rows, so the filter on
    // mr.state is verified indirectly through the count values.
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });

    // alice authored MR 2000 (opened) and 2001 (opened) = 2
    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_i64("open_mr_count") == Some(2)
    });
    // bob authored MR 2002 (merged), not opened = should not appear
    resp.assert_node_absent("User", 2);
    // charlie authored MR 2003 (closed), not opened = should not appear
    resp.assert_node_absent("User", 3);
}

pub(super) async fn empty_result_has_valid_schema(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [99999]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeIds);
    resp.assert_node_count(0);
    assert_eq!(resp.edge_count(), 0);
}
