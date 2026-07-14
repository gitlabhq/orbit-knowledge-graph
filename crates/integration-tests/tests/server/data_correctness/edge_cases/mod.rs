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
            "nodes": [{"id": "n", "entity": "Note", "columns": ["note"], "node_ids": [3002]}],
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
            "nodes": [{"id": "n", "entity": "Note", "columns": ["note"], "node_ids": [3003]}],
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
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "open_mr_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });

    resp.assert_group_node_property_str("u", "User", 1, "username", "alice");
    resp.assert_group_row_value_i64("u", "User", 1, "open_mr_count", 2);
    resp.assert_group_node_absent("u", "User", 2);
    resp.assert_group_node_absent("u", "User", 3);
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

    resp.assert_node("User", 2, |n| n.prop_str("username") == Some("bob"));
    resp.assert_node("MergeRequest", 2002, |n| {
        n.prop_str("title") == Some("Refactor C")
    });
    resp.assert_edge_exists("User", 2, "MergeRequest", 2002, "AUTHORED");

    resp.assert_node_absent("User", 1);
    resp.assert_node_absent("MergeRequest", 2000);
    resp.assert_node_absent("MergeRequest", 2001);

    resp.assert_referential_integrity();
}

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

    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
    resp.assert_edge_exists("Group", 200, "Group", 300, "CONTAINS");

    resp.assert_referential_integrity();
}

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

    resp.assert_node_ids("MergeRequest", &[2002]);
    resp.assert_edge_set("AUTHORED", &[(2, 2002)]);

    resp.assert_node_absent("MergeRequest", 2000);
    resp.assert_node_absent("MergeRequest", 2001);
    resp.assert_node_absent("MergeRequest", 2003);

    resp.assert_referential_integrity();
}

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

    resp.assert_node_ids("MergeRequest", &[2000, 2001]);
    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_edge_set("AUTHORED", &[(1, 2000), (1, 2001)]);

    resp.assert_node_absent("MergeRequest", 2002);

    resp.assert_referential_integrity();
}

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
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}],
            "limit": 20
        }"#,
        &allow_all(),
        ctx_100,
    )
    .await;

    resp.assert_group_node_count("g", 2);
    resp.assert_group_row_value_i64("g", "Group", 100, "project_count", 2);
    resp.assert_group_row_value_i64("g", "Group", 200, "project_count", 1);

    resp.assert_group_node_absent("g", "Group", 101);
    resp.assert_group_node_absent("g", "Group", 102);
}

pub(super) async fn neighbors_cross_namespace_no_false_positives(ctx: &TestContext) {
    let ctx_101 = SecurityContext::new(1, vec!["1/101/".into()]).unwrap();
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [101]}],
            "neighbors": {"node": "g", "direction": "both"}
        }"#,
        &allow_all(),
        ctx_101,
    )
    .await;

    resp.assert_node_count(8);
    resp.assert_node_ids("Group", &[101]);
    resp.assert_node_ids("User", &[3, 4, 5, 6]);
    resp.assert_node_ids("Project", &[1001, 1003]);
    resp.assert_node_ids("WorkItem", &[4002]);

    resp.assert_edge_exists("User", 3, "Group", 101, "MEMBER_OF");
    resp.assert_edge_exists("User", 4, "Group", 101, "MEMBER_OF");
    resp.assert_edge_exists("User", 5, "Group", 101, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 101, "MEMBER_OF");

    resp.assert_edge_exists("Group", 101, "Project", 1001, "CONTAINS");
    resp.assert_edge_exists("Group", 101, "Project", 1003, "CONTAINS");

    resp.assert_edge_exists("WorkItem", 4002, "Group", 101, "IN_GROUP");

    resp.assert_node_absent("User", 1);
    resp.assert_node_absent("User", 2);

    resp.assert_node_absent("Group", 100);
    resp.assert_node_absent("Group", 200);
    resp.assert_node_absent("Project", 1000);
    resp.assert_node_absent("Project", 1002);
    resp.assert_node_absent("Project", 1004);

    resp.assert_referential_integrity();
}

pub(super) async fn empty_result_has_valid_schema(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "node_ids": [99999]}],
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
    // Redaction for MergeRequestDiff checks merge_request_id against the
    // merge_request resource type. Diffs 5000/5001 have merge_request_id=2000,
    // diff 5002 has merge_request_id=2001.
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

    resp.assert_node_ids("MergeRequest", &[2001]);
    resp.assert_node_ids("MergeRequestDiff", &[5002]);
    resp.assert_edge_set("HAS_DIFF", &[(2001, 5002)]);
    resp.assert_node_count(2);
    resp.assert_node_absent("MergeRequest", 2000);
    resp.assert_node_absent("MergeRequestDiff", 5000);
    resp.assert_node_absent("MergeRequestDiff", 5001);
}

pub(super) async fn non_default_redaction_id_with_multiple_mrs(ctx: &TestContext) {
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
