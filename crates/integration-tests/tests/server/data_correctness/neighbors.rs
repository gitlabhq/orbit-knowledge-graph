use super::helpers::*;

pub(super) async fn neighbors_outgoing_returns_correct_targets(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "neighbors": {"node": "u", "direction": "outgoing"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_referential_integrity();

    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_node_ids("MergeRequest", &[2000, 2001, 2002]);
    resp.assert_node_ids("Note", &[3000]);
    resp.assert_node_ids("WorkItem", &[4000, 4002]);

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "Group", 102, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
    resp.assert_edge_exists("User", 1, "MergeRequest", 2002, "APPROVED");
    resp.assert_edge_exists("User", 1, "Note", 3000, "AUTHORED");
    resp.assert_edge_exists("User", 1, "WorkItem", 4000, "AUTHORED");
    resp.assert_edge_exists("User", 1, "WorkItem", 4002, "AUTHORED");
    resp.assert_edge_exists("User", 1, "WorkItem", 4000, "ASSIGNED");

    resp.assert_node("Group", 100, |n| n.prop_str("name") == Some("Public Group"));
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group")
    });
}

pub(super) async fn neighbors_incoming_returns_correct_sources(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}],
            "neighbors": {"node": "g", "direction": "incoming", "rel_types": ["MEMBER_OF"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("Group", &[100]);
    resp.assert_node_ids("User", &[1, 2, 6]);

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 2, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 100, "MEMBER_OF");
}

pub(super) async fn neighbors_rel_types_filter_works(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}],
            "neighbors": {"node": "g", "direction": "outgoing", "rel_types": ["CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_node_ids("Project", &[1000, 1002]);
    resp.assert_edge_count("CONTAINS", 3);
}

pub(super) async fn neighbors_both_direction_returns_all_connected(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}],
            "neighbors": {"node": "g", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_node_ids("Project", &[1000, 1002]);
    resp.assert_node_ids("WorkItem", &[4000, 4001]);

    resp.assert_referential_integrity();
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
    resp.assert_edge_exists("WorkItem", 4000, "Group", 100, "IN_GROUP");
    resp.assert_edge_exists("WorkItem", 4001, "Group", 100, "IN_GROUP");
}

pub(super) async fn neighbors_mixed_entity_types(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [2000]}],
            "neighbors": {"node": "mr", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(10);
    resp.assert_referential_integrity();
    resp.assert_node_ids("User", &[1, 2, 3]);
    resp.assert_node_ids("Note", &[3000, 3002, 3003]);
    resp.assert_node_ids("MergeRequestDiff", &[5000, 5001]);
    resp.assert_node_ids("Project", &[1000]);

    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
    resp.assert_edge_exists("User", 2, "MergeRequest", 2000, "APPROVED");
    resp.assert_edge_exists("User", 3, "MergeRequest", 2000, "APPROVED");
    resp.assert_edge_exists("MergeRequest", 2000, "Note", 3000, "HAS_NOTE");
    resp.assert_edge_exists("MergeRequest", 2000, "Note", 3002, "HAS_NOTE");
    resp.assert_edge_exists("MergeRequest", 2000, "Note", 3003, "HAS_NOTE");
    resp.assert_edge_exists("MergeRequest", 2000, "MergeRequestDiff", 5000, "HAS_DIFF");
    resp.assert_edge_exists("MergeRequest", 2000, "MergeRequestDiff", 5001, "HAS_DIFF");
    resp.assert_edge_exists("MergeRequest", 2000, "Project", 1000, "IN_PROJECT");
}

pub(super) async fn neighbors_both_fused_scan_returns_complete_bidirectional_set(
    ctx: &TestContext,
) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}],
            "neighbors": {"node": "g", "direction": "both", "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_referential_integrity();
    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_node_ids("Project", &[1000, 1002]);

    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (2, 100), (6, 100)]);
    resp.assert_edge_set("CONTAINS", &[(100, 200), (100, 1000), (100, 1002)]);
}

pub(super) async fn neighbors_redaction_removes_unauthorized_targets(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[100]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["MEMBER_OF"]}
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("Group", &[100]);
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_absent("User", 1, "Group", 102, "MEMBER_OF");
}

pub(super) async fn neighbors_dynamic_columns_all_returns_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["MEMBER_OF"]},
            "options": {"dynamic_columns": "*"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 102)]);

    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group")
            && n.prop_str("visibility_level") == Some("public")
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group")
            && n.prop_str("visibility_level") == Some("internal")
    });
}

pub(super) async fn neighbors_center_node_properties_hydrated(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username", "name", "state"]}],
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["MEMBER_OF"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_referential_integrity();

    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice")
            && n.prop_str("name") == Some("Alice Admin")
            && n.prop_str("state") == Some("active")
    });

    resp.assert_node("Group", 100, |n| n.prop_str("name") == Some("Public Group"));
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group")
    });

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "Group", 102, "MEMBER_OF");
}

pub(super) async fn neighbors_non_default_pk_with_non_denorm_filter(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{
                "id": "d",
                "entity": "MergeRequestDiff",
                "filters": {"head_commit_sha": {"op": "starts_with", "value": "aaaa"}}
            }],
            "neighbors": {"node": "d", "direction": "incoming", "rel_types": ["HAS_DIFF"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::Filter {
        field: "head_commit_sha".into(),
    });
    resp.assert_referential_integrity();
    resp.assert_node_count(3);
    resp.assert_node_ids("MergeRequestDiff", &[5000, 5001]);
    resp.assert_node_ids("MergeRequest", &[2000]);
    resp.assert_edge_set("HAS_DIFF", &[(2000, 5000), (2000, 5001)]);
}

pub(super) async fn neighbors_non_default_pk_filter_excludes_non_matching(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{
                "id": "d",
                "entity": "MergeRequestDiff",
                "filters": {"head_commit_sha": {"op": "eq", "value": "no-such-sha"}}
            }],
            "neighbors": {"node": "d", "direction": "incoming", "rel_types": ["HAS_DIFF"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeIds);
    resp.skip_requirement(Requirement::Filter {
        field: "head_commit_sha".into(),
    });
    resp.skip_requirement(Requirement::Relationship {
        edge_type: "HAS_DIFF".into(),
    });
    resp.skip_requirement(Requirement::Neighbors);
    resp.assert_node_count(0);
    assert_eq!(resp.edge_count(), 0);
}

pub(super) async fn neighbors_non_default_pk_redaction_uses_merge_request_id(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("merge_request", &[2001]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{
                "id": "d",
                "entity": "MergeRequestDiff",
                "filters": {"head_commit_sha": {"op": "starts_with", "value": "bbbb"}}
            }],
            "neighbors": {"node": "d", "direction": "incoming", "rel_types": ["HAS_DIFF"]}
        }"#,
        &svc,
    )
    .await;

    resp.skip_requirement(Requirement::Filter {
        field: "head_commit_sha".into(),
    });
    resp.assert_node_count(2);
    resp.assert_node_ids("MergeRequestDiff", &[5002]);
    resp.assert_node_ids("MergeRequest", &[2001]);
    resp.assert_edge_set("HAS_DIFF", &[(2001, 5002)]);
    resp.assert_node_absent("MergeRequestDiff", 5000);
    resp.assert_node_absent("MergeRequestDiff", 5001);
    resp.assert_node_absent("MergeRequest", 2000);
}

pub(super) async fn neighbors_both_direction_preserves_edge_direction(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}],
            "neighbors": {"node": "g", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_node_ids("Project", &[1000, 1002]);
    resp.assert_node_ids("WorkItem", &[4000, 4001]);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 2, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 100, "MEMBER_OF");

    resp.assert_edge_exists("WorkItem", 4000, "Group", 100, "IN_GROUP");
    resp.assert_edge_exists("WorkItem", 4001, "Group", 100, "IN_GROUP");

    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
    resp.assert_edge_exists("Group", 100, "Project", 1000, "CONTAINS");
    resp.assert_edge_exists("Group", 100, "Project", 1002, "CONTAINS");

    let edges = resp.edges_of_type("MEMBER_OF");
    for edge in edges.iter() {
        assert_ne!(
            edge.from_id, 100,
            "Group 100 should not be source of MEMBER_OF (edges should not be reversed)"
        );
    }
}
