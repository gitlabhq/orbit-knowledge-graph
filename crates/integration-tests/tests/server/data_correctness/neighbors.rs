use super::helpers::*;

pub(super) async fn neighbors_outgoing_returns_correct_targets(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(8);
    resp.assert_referential_integrity();

    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_node_ids("MergeRequest", &[2000, 2001]);
    resp.assert_node_ids("Note", &[3000]);
    resp.assert_node_ids("WorkItem", &[4000, 4002]);

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "Group", 102, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
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
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
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
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
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
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
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
            "node": {"id": "mr", "entity": "MergeRequest", "node_ids": [2000]},
            "neighbors": {"node": "mr", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_referential_integrity();
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Note", &[3000, 3002, 3003]);
    resp.assert_node_ids("MergeRequestDiff", &[5000, 5001]);

    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
    resp.assert_edge_exists("MergeRequest", 2000, "Note", 3000, "HAS_NOTE");
    resp.assert_edge_exists("MergeRequest", 2000, "Note", 3002, "HAS_NOTE");
    resp.assert_edge_exists("MergeRequest", 2000, "Note", 3003, "HAS_NOTE");
    resp.assert_edge_exists("MergeRequest", 2000, "MergeRequestDiff", 5000, "HAS_DIFF");
    resp.assert_edge_exists("MergeRequest", 2000, "MergeRequestDiff", 5001, "HAS_DIFF");
}

pub(super) async fn neighbors_redaction_removes_unauthorized_targets(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[100]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
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
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
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

    // With dynamic_columns: "*", neighbor nodes should have all ontology columns.
    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group")
            && n.prop_str("visibility_level") == Some("public")
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group")
            && n.prop_str("visibility_level") == Some("internal")
    });
}

pub(super) async fn neighbors_both_direction_preserves_edge_direction(ctx: &TestContext) {
    // Group 100 has incoming MEMBER_OF from users and outgoing CONTAINS to
    // projects/subgroups. With direction: "both", edges should preserve their
    // actual direction (from→to) regardless of how they were discovered.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
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

    // Incoming MEMBER_OF: User→Group (not reversed)
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 2, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 100, "MEMBER_OF");

    // Incoming IN_GROUP: WorkItem→Group (not reversed)
    resp.assert_edge_exists("WorkItem", 4000, "Group", 100, "IN_GROUP");
    resp.assert_edge_exists("WorkItem", 4001, "Group", 100, "IN_GROUP");

    // Outgoing CONTAINS: Group→target (not reversed)
    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
    resp.assert_edge_exists("Group", 100, "Project", 1000, "CONTAINS");
    resp.assert_edge_exists("Group", 100, "Project", 1002, "CONTAINS");

    // No reversed edges: Group 100 should never appear as edge target for MEMBER_OF
    // or as edge source for an incoming relationship.
    let edges = resp.edges_of_type("MEMBER_OF");
    for edge in edges.iter() {
        assert_ne!(
            edge.from_id, 100,
            "Group 100 should not be source of MEMBER_OF (edges should not be reversed)"
        );
    }
}
