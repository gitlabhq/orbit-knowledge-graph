mod filterable;
mod like;

use super::helpers::*;

pub(crate) use filterable::*;
pub(crate) use like::*;

pub(super) async fn traversal_referential_integrity_on_complex_query(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
                {"id": "g", "entity": "Group"},
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}}
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
            "query_type": "traversal",
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
            "query_type": "traversal",
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "user_type"],
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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

/// Cross-namespace: User 2 is MEMBER_OF group 100 (ns `1/100/`) but authored
/// MR 2002 in ns `1/101/1001/`. When scoped to `1/101/`, User 2 must appear
/// as the MR author even though their membership edge is in a different namespace.
pub(super) async fn cross_namespace_user_authors_mr_in_different_group(ctx: &TestContext) {
    let ctx_101 = SecurityContext::new(1, vec!["1/101/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 20
        }"#,
        &allow_all(),
        ctx_101,
    )
    .await;

    resp.assert_node_count(2);

    // User 2 (bob) authored MR 2002 in ns 1/101/1001/ — must be visible
    resp.assert_node("User", 2, |n| n.prop_str("username") == Some("bob"));
    resp.assert_node("MergeRequest", 2002, |n| {
        n.prop_str("title") == Some("Refactor C")
    });
    resp.assert_edge_exists("User", 2, "MergeRequest", 2002, "AUTHORED");

    // User 1's AUTHORED edges are in ns 1/100/1000/ — must NOT appear
    resp.assert_node_absent("User", 1);
    resp.assert_node_absent("MergeRequest", 2000);
    resp.assert_node_absent("MergeRequest", 2001);

    resp.assert_referential_integrity();
}

/// Cross-namespace: Group 100 (ns `1/100/`) CONTAINS subgroup 200 (edge ns
/// `1/100/200/`) and subgroup 200 CONTAINS subgroup 300 (edge ns
/// `1/100/200/300/`). All containment edges must be visible when scoped to
/// the parent namespace `1/100/`.
pub(super) async fn cross_namespace_group_containment_across_depth(ctx: &TestContext) {
    let ctx_100 = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "child", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "child"}],
            "limit": 20
        }"#,
        &allow_all(),
        ctx_100,
    )
    .await;

    resp.assert_node_count(3);

    // Group 100 contains Group 200 (edge ns 1/100/200/)
    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
    // Group 200 contains Group 300 (edge ns 1/100/200/300/)
    resp.assert_edge_exists("Group", 200, "Group", 300, "CONTAINS");

    resp.assert_referential_integrity();
}

/// Cross-namespace isolation: scoped to `1/101/` should NOT see edges from
/// `1/100/` or `1/102/`. User 1's AUTHORED MRs in `1/100/1000/` and
/// User 3's MR in `1/102/1004/` must be invisible.
pub(super) async fn cross_namespace_isolation_no_leakage(ctx: &TestContext) {
    let ctx_101 = SecurityContext::new(1, vec!["1/101/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 50
        }"#,
        &allow_all(),
        ctx_101,
    )
    .await;

    resp.assert_node_count(2);

    // Only MR 2002 is in ns 1/101/ — authored by User 2
    resp.assert_node_ids("MergeRequest", &[2002]);
    resp.assert_edge_set("AUTHORED", &[(2, 2002)]);

    // MRs from other namespaces must not leak
    resp.assert_node_absent("MergeRequest", 2000); // ns 1/100/1000/
    resp.assert_node_absent("MergeRequest", 2001); // ns 1/100/1000/
    resp.assert_node_absent("MergeRequest", 2003); // ns 1/102/1004/

    resp.assert_referential_integrity();
}

/// Cross-namespace: narrow scope `1/100/1000/` sees AUTHORED edges in that
/// project's namespace. The source User has no traversal_path filter — they
/// come from any namespace. Only edges with matching traversal_path appear.
pub(super) async fn cross_namespace_narrow_scope_returns_all_authors(ctx: &TestContext) {
    let ctx_project = SecurityContext::new(1, vec!["1/100/1000/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 20
        }"#,
        &allow_all(),
        ctx_project,
    )
    .await;

    resp.assert_node_count(3);

    // Both MRs 2000 and 2001 are in 1/100/1000/, authored by User 1
    resp.assert_node_ids("MergeRequest", &[2000, 2001]);
    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_edge_set("AUTHORED", &[(1, 2000), (1, 2001)]);

    // User 2's MR 2002 is in 1/101/ — must not appear
    resp.assert_node_absent("MergeRequest", 2002);

    resp.assert_referential_integrity();
}

/// Cross-namespace aggregation: scoped to `1/100/`, count projects per group.
/// Group 100 CONTAINS projects 1000 and 1002 via edges in `1/100/` subtree.
/// Projects in `1/101/` and `1/102/` must not appear.
pub(super) async fn cross_namespace_aggregation_respects_scope(ctx: &TestContext) {
    let ctx_100 = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
            "aggregations": [{"function": "count", "target": "p", "group_by": "g", "alias": "project_count"}],
            "limit": 20
        }"#,
        &allow_all(),
        ctx_100,
    )
    .await;

    // Group 100 CONTAINS projects 1000 (edge ns 1/100/1000/) and 1002
    // (edge ns 1/100/1002/) — both in the 1/100/ subtree.
    // Group 200 CONTAINS Project 1010 (edge ns 1/100/200/1010/) — also in scope.
    resp.assert_node_count(2);
    resp.assert_node("Group", 100, |n| n.prop_i64("project_count") == Some(2));
    resp.assert_node("Group", 200, |n| n.prop_i64("project_count") == Some(1));

    // Groups 101 and 102 have CONTAINS edges outside 1/100/ — must not appear
    resp.assert_node_absent("Group", 101);
    resp.assert_node_absent("Group", 102);
}

/// Cross-namespace neighbors isolation: scoped to `1/101/`, neighbors of
/// Group 101 must only include entities connected via edges in the `1/101/`
/// subtree. Users with MEMBER_OF edges in other namespaces and projects
/// contained in other groups must not leak into the result.
pub(super) async fn neighbors_cross_namespace_no_false_positives(ctx: &TestContext) {
    let ctx_101 = SecurityContext::new(1, vec!["1/101/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [101]},
            "neighbors": {"node": "g", "direction": "both"}
        }"#,
        &allow_all(),
        ctx_101,
    )
    .await;

    // Group 101 has incoming MEMBER_OF from Users 3, 4, 5, 6 (edges in 1/101/),
    // outgoing CONTAINS to Projects 1001, 1003 (edges in 1/101/1001/ and 1/101/1003/),
    // and incoming IN_GROUP from WorkItem 4002 (edge in 1/101/).
    resp.assert_node_count(8);
    resp.assert_node_ids("Group", &[101]);
    resp.assert_node_ids("User", &[3, 4, 5, 6]);
    resp.assert_node_ids("Project", &[1001, 1003]);
    resp.assert_node_ids("WorkItem", &[4002]);

    // MEMBER_OF edges from 1/101/
    resp.assert_edge_exists("User", 3, "Group", 101, "MEMBER_OF");
    resp.assert_edge_exists("User", 4, "Group", 101, "MEMBER_OF");
    resp.assert_edge_exists("User", 5, "Group", 101, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 101, "MEMBER_OF");

    // CONTAINS edges from 1/101/ subtree
    resp.assert_edge_exists("Group", 101, "Project", 1001, "CONTAINS");
    resp.assert_edge_exists("Group", 101, "Project", 1003, "CONTAINS");

    // IN_GROUP edge from 1/101/
    resp.assert_edge_exists("WorkItem", 4002, "Group", 101, "IN_GROUP");

    // Users whose MEMBER_OF edges are only in other namespaces must not appear
    resp.assert_node_absent("User", 1); // MEMBER_OF Group 100 (1/100/) and Group 102 (1/102/)
    resp.assert_node_absent("User", 2); // MEMBER_OF Group 100 (1/100/)

    // Projects and groups from other namespaces must not leak
    resp.assert_node_absent("Group", 100);
    resp.assert_node_absent("Group", 200);
    resp.assert_node_absent("Project", 1000); // in 1/100/1000/
    resp.assert_node_absent("Project", 1002); // in 1/100/1002/
    resp.assert_node_absent("Project", 1004); // in 1/102/1004/

    resp.assert_referential_integrity();
}

pub(super) async fn empty_result_has_valid_schema(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
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

pub(super) async fn non_default_redaction_id_entity_traversal(ctx: &TestContext) {
    // MergeRequestDiff uses id_column=merge_request_id (not "id").
    // In edge-only mode, enforce.rs emits _gkg_d_pk via Expr::col(&node.id, "id")
    // which references a node table not in FROM. The fix pre-emits _gkg_d_pk
    // in lower using the edge column.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"], "node_ids": [2000]},
                {"id": "d", "entity": "MergeRequestDiff", "columns": ["state"]}
            ],
            "relationships": [{"type": "HAS_DIFF", "from": "mr", "to": "d"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_referential_integrity();
    resp.assert_node_ids("MergeRequest", &[2000]);
    resp.assert_node_ids("MergeRequestDiff", &[5000, 5001]);
    resp.assert_edge_set("HAS_DIFF", &[(2000, 5000), (2000, 5001)]);
}

pub(super) async fn non_default_redaction_id_denies_unauthorized(ctx: &TestContext) {
    // Redaction for MergeRequestDiff checks merge_request_id against
    // the merge_request resource type. Only allow MR 2001 — diffs
    // 5000/5001 have merge_request_id=2000 (denied), diff 5002 has
    // merge_request_id=2001 (allowed).
    let mut svc = MockRedactionService::new();
    svc.allow("merge_request", &[2001]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [2000, 2001]},
                {"id": "d", "entity": "MergeRequestDiff"}
            ],
            "relationships": [{"type": "HAS_DIFF", "from": "mr", "to": "d"}],
            "limit": 20
        }"#,
        &svc,
    )
    .await;

    // MR 2000 is denied, MR 2001 is allowed. Diff 5002 (MR 2001) is allowed.
    resp.assert_node_ids("MergeRequest", &[2001]);
    resp.assert_node_ids("MergeRequestDiff", &[5002]);
    resp.assert_edge_set("HAS_DIFF", &[(2001, 5002)]);
    resp.assert_node_count(2);
    resp.assert_node_absent("MergeRequest", 2000);
    resp.assert_node_absent("MergeRequestDiff", 5000);
    resp.assert_node_absent("MergeRequestDiff", 5001);
}

pub(super) async fn non_default_redaction_id_with_multiple_mrs(ctx: &TestContext) {
    // Allow both MR 2000 and 2001. All diffs should be authorized
    // via their merge_request_id.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [2000, 2001]},
                {"id": "d", "entity": "MergeRequestDiff"}
            ],
            "relationships": [{"type": "HAS_DIFF", "from": "mr", "to": "d"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_referential_integrity();
    resp.assert_node_ids("MergeRequest", &[2000, 2001]);
    resp.assert_node_ids("MergeRequestDiff", &[5000, 5001, 5002]);
    resp.assert_edge_set("HAS_DIFF", &[(2000, 5000), (2000, 5001), (2001, 5002)]);
}
