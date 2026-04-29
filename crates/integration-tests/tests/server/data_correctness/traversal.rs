use super::helpers::*;

pub(super) async fn traversal_user_group_returns_correct_pairs_and_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_edge_set(
        "MEMBER_OF",
        &[
            (1, 100),
            (1, 102),
            (2, 100),
            (3, 101),
            (4, 101),
            (4, 102),
            (5, 101),
            (6, 100),
            (6, 101),
        ],
    );

    resp.assert_referential_integrity();

    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_node("Group", 100, |n| n.prop_str("name") == Some("Public Group"));
    resp.assert_node("Group", 101, |n| {
        n.prop_str("name") == Some("Private Group")
    });
}

pub(super) async fn traversal_three_hop_returns_all_user_group_project_paths(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]}
            ],
            "relationships": [
                {"type": "MEMBER_OF", "from": "u", "to": "g"},
                {"type": "CONTAINS", "from": "g", "to": "p"}
            ],
            "limit": 30
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(14);
    resp.assert_referential_integrity();

    let member_of: HashSet<(i64, i64)> = resp
        .edges_of_type("MEMBER_OF")
        .iter()
        .map(|e| (e.from_id, e.to_id))
        .collect();
    let contains: HashSet<(i64, i64)> = resp
        .edges_of_type("CONTAINS")
        .iter()
        .map(|e| (e.from_id, e.to_id))
        .collect();

    assert!(member_of.contains(&(1, 100)));
    assert!(member_of.contains(&(1, 102)));
    assert!(member_of.contains(&(2, 100)));
    assert!(contains.contains(&(100, 1000)));
    assert!(contains.contains(&(100, 1002)));
    assert!(contains.contains(&(101, 1001)));
    assert!(contains.contains(&(102, 1004)));

    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project")
    });
}

pub(super) async fn traversal_user_authored_mr_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
    resp.assert_edge_exists("User", 1, "MergeRequest", 2001, "AUTHORED");
    resp.assert_edge_exists("User", 2, "MergeRequest", 2002, "AUTHORED");
    resp.assert_edge_exists("User", 3, "MergeRequest", 2003, "AUTHORED");

    resp.assert_node("MergeRequest", 2000, |n| {
        n.prop_str("title") == Some("Add feature A") && n.prop_str("state") == Some("opened")
    });
    resp.assert_node("MergeRequest", 2002, |n| {
        n.prop_str("title") == Some("Refactor C") && n.prop_str("state") == Some("merged")
    });
}

pub(super) async fn traversal_user_approved_mr_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "APPROVED", "from": "u", "to": "mr"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_referential_integrity();

    resp.assert_edge_set("APPROVED", &[(2, 2000), (3, 2000), (1, 2002)]);

    resp.assert_node("User", 2, |n| n.prop_str("username") == Some("bob"));
    resp.assert_node("MergeRequest", 2000, |n| {
        n.prop_str("title") == Some("Add feature A") && n.prop_str("state") == Some("opened")
    });
    resp.assert_node("MergeRequest", 2002, |n| {
        n.prop_str("title") == Some("Refactor C") && n.prop_str("state") == Some("merged")
    });
}

pub(super) async fn traversal_wildcard_user_to_mr_infers_relationship_kinds(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "*", "from": "u", "to": "mr"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("MergeRequest", &[2000, 2001, 2002]);
    resp.assert_edge_set("AUTHORED", &[(1, 2000), (1, 2001)]);
    resp.assert_edge_set("APPROVED", &[(1, 2002)]);
    resp.assert_edge_count("ASSIGNED", 0);
    resp.assert_edge_count("CLOSED", 0);
    resp.assert_edge_count("LAST_EDITED_BY", 0);
    resp.assert_edge_count("MERGED", 0);
    resp.assert_edge_count("REVIEWER", 0);
    resp.assert_edge_count("UPDATED_BY", 0);
    resp.assert_referential_integrity();
}

pub(super) async fn traversal_redaction_removes_unauthorized_data(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[100]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100]);
    resp.assert_node_absent("User", 2);
    resp.assert_node_absent("Group", 102);
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_absent("User", 1, "Group", 102, "MEMBER_OF");
}

pub(super) async fn traversal_with_order_by(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_node_order("User", &[6, 5, 4, 3, 2, 1]);
    resp.assert_edge_set(
        "MEMBER_OF",
        &[
            (1, 100),
            (1, 102),
            (2, 100),
            (3, 101),
            (4, 101),
            (4, 102),
            (5, 101),
            (6, 100),
            (6, 101),
        ],
    );
}

pub(super) async fn traversal_variable_length_reaches_depth_2(ctx: &TestContext) {
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
}

pub(super) async fn traversal_incoming_direction(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"], "node_ids": [100]},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "g", "to": "u", "direction": "incoming"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 2, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 100, "MEMBER_OF");
}

pub(super) async fn traversal_with_filter_narrows_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
                 "filters": {"state": "blocked"}},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("User", &[5]);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("blocked"));
    resp.assert_edge_exists("User", 5, "Group", 101, "MEMBER_OF");
}

pub(super) async fn traversal_variable_length_min_hops_skips_shallow(ctx: &TestContext) {
    // min_hops=2 should skip Group 200 (depth 1) and only return Group 300 (depth 2).
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "parent", "entity": "Group", "columns": ["name"], "node_ids": [100]},
                {"id": "child", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "CONTAINS", "from": "parent", "to": "child", "min_hops": 2, "max_hops": 3}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("Group", &[100, 300]);
    resp.assert_node_absent("Group", 200);
    resp.assert_edge_exists("Group", 100, "Group", 300, "CONTAINS");
}

pub(super) async fn traversal_variable_length_includes_depth_2_path_to_project(ctx: &TestContext) {
    // Reproducer for the variable-length cliff (MR !1069):
    // User 7 -> AUTHORED -> WI 4010 -> IN_PROJECT -> Project 1010, with
    // Project 1010 reachable via Group 100 -> Group 200 -> Project 1010
    // (depth-2 CONTAINS chain).
    //
    // Pre-fix bug: `inject_sip_first_edge` placed `e1.target_id IN _cascade_p`
    // on every UNION arm. At depth-2 the intermediate Group 200 ID is not in
    // _cascade_p (which holds Project IDs only), so depth-2 arms returned no
    // rows. The Group 100 -> Project 1010 path was silently dropped.
    //
    // Post-fix: the depth-2 path is included; both Group 100 and Group 200
    // appear with edges spanning all hops.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [7], "columns": ["username"]},
                {"id": "wi", "entity": "WorkItem", "columns": ["title"]},
                {"id": "p", "entity": "Project", "columns": ["name"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "wi"},
                {"type": "IN_PROJECT", "from": "wi", "to": "p"},
                {"type": "CONTAINS", "from": "g", "to": "p", "min_hops": 1, "max_hops": 2}
            ],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_referential_integrity();
    // 1 user + 1 wi + 1 project + 2 groups = 5 nodes.
    resp.assert_node_count(5);
    resp.assert_node_ids("User", &[7]);
    resp.assert_node_ids("WorkItem", &[4010]);
    resp.assert_node_ids("Project", &[1010]);
    // Both groups must appear: Group 200 (depth-1 to Project 1010) and
    // Group 100 (depth-2 via Group 200). Pre-fix dropped Group 100.
    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_edge_exists("User", 7, "WorkItem", 4010, "AUTHORED");
    resp.assert_edge_exists("WorkItem", 4010, "Project", 1010, "IN_PROJECT");
    // Variable-length CONTAINS arms collapse each path into a single
    // (start_group, end_project) edge in the response, with intermediate
    // hops carried in path_nodes. Both depth-1 (Group 200 → Project 1010)
    // and depth-2 (Group 100 → ... → Project 1010) arms must contribute.
    resp.assert_edge_exists("Group", 200, "Project", 1010, "CONTAINS");
    resp.assert_edge_exists("Group", 100, "Project", 1010, "CONTAINS");
}

pub(super) async fn aggregation_variable_length_counts_all_depths(ctx: &TestContext) {
    // Aggregation analog of the M1 cliff. group_by=u keeps the per-User row
    // so we can read the count from User 7's `n` property. Two matching
    // paths exist (Group 200, depth-1) and (Group 100, depth-2), so n=2 when
    // the depth-2 arm is correctly scanned. Pre-fix bug returned n=1.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [7]},
                {"id": "wi", "entity": "WorkItem"},
                {"id": "p", "entity": "Project"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "wi"},
                {"type": "IN_PROJECT", "from": "wi", "to": "p"},
                {"type": "CONTAINS", "from": "g", "to": "p", "min_hops": 1, "max_hops": 2}
            ],
            "aggregations": [{"function": "count", "target": "g", "group_by": "u", "alias": "n"}],
            "limit": 5
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("User", &[7]);
    resp.assert_node("User", 7, |n| n.prop_i64("n") == Some(2));
}

pub(super) async fn traversal_variable_length_with_redaction_at_depth(ctx: &TestContext) {
    // Redact Group 200 (the intermediate node in the 100→200→300 chain).
    //
    // Variable-length traversals carry intermediate node IDs in a `path_nodes`
    // array column (built by `build_hop_arm` in lower.rs). Redaction extracts
    // these into `dynamic_nodes` and checks each one. Since Group 200 appears
    // in `path_nodes` for every depth-2 row (as the intermediate hop), and in
    // the child column for every depth-1 row, ALL rows are denied:
    //   - Depth-1 (parent=100, child=200): denied — child 200 unauthorized
    //   - Depth-2 (parent=100, child=300): denied — path_nodes contains 200
    //
    // This is security-correct: without it, a multi-hop traversal could bypass
    // namespace authorization on intermediate nodes.
    let mut svc = MockRedactionService::new();
    svc.allow("group", &[100, 300]);

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
        &svc,
    )
    .await;

    resp.assert_node_absent("Group", 200);
    resp.skip_requirement(Requirement::NodeIds);
    resp.skip_requirement(Requirement::NodeCount);
    resp.skip_requirement(Requirement::Relationship {
        edge_type: "CONTAINS".into(),
    });
}

pub(super) async fn traversal_deduplicates_shared_nodes(ctx: &TestContext) {
    // Users 1 and 2 are both MEMBER_OF Group 100. The response should
    // contain Group 100 exactly once, not duplicated per fan-in path.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [1, 2]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    // User 1 → Group 100, 102. User 2 → Group 100. Group 100 appears once.
    resp.assert_node_count(4);
    resp.assert_node_ids("User", &[1, 2]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 102), (2, 100)]);
}

pub(super) async fn traversal_shared_target_fan_in(ctx: &TestContext) {
    // Multiple MRs on the same project each have notes. MR 2000 has notes
    // 3000, 3002, 3003. This tests fan-out from a single source correctly
    // produces unique target nodes.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"], "node_ids": [2000]},
                {"id": "n", "entity": "Note", "columns": ["note"]}
            ],
            "relationships": [{"type": "HAS_NOTE", "from": "mr", "to": "n"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("MergeRequest", &[2000]);
    resp.assert_node_ids("Note", &[3000, 3002, 3003]);
    resp.assert_edge_set("HAS_NOTE", &[(2000, 3000), (2000, 3002), (2000, 3003)]);
}

pub(super) async fn traversal_order_by_node_property(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "order_by": {"node": "mr", "property": "title", "direction": "ASC"},
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_referential_integrity();
    resp.assert_node_order("MergeRequest", &[2000, 2001, 2002, 2003]);
    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
}

pub(super) async fn traversal_order_by_source_node_property(ctx: &TestContext) {
    // order_by on the source (from) node's non-id property
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "order_by": {"node": "u", "property": "username", "direction": "ASC"},
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_referential_integrity();
    resp.assert_edge_count("MEMBER_OF", 9);
    // Usernames alphabetically: alice, bob, charlie, diana, eve, 用户...
    resp.assert_node_order("User", &[1, 2, 3, 4, 5, 6]);
}

pub(super) async fn traversal_order_by_with_node_ids_filter(ctx: &TestContext) {
    // order_by combined with node_ids filter
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [1, 2]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "order_by": {"node": "mr", "property": "title", "direction": "DESC"},
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_referential_integrity();
    // Only users 1 and 2 — alice authored MRs 2000,2001; bob authored MR 2002
    resp.assert_node_ids("User", &[1, 2]);
    // Titles DESC: Refactor C, Fix bug B, Add feature A
    resp.assert_node_order("MergeRequest", &[2002, 2001, 2000]);
    resp.assert_edge_set("AUTHORED", &[(1, 2000), (1, 2001), (2, 2002)]);
}

/// Code graph traversal WITHOUT node_ids — relies on auth-scope cascade.
/// Verifies that the optimizer's fallback cascade seed (from _nf_* CTEs)
/// produces correct results when no node is pinned by explicit IDs.
pub(super) async fn traversal_code_graph_calls_without_node_ids(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "caller", "entity": "Definition", "filters": [{"column": "project_id", "operator": "eq", "value": 1000}], "columns": ["name", "fqn"]},
                {"id": "callee", "entity": "Definition", "columns": ["name", "fqn"]}
            ],
            "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    // Seed data in project 1000: compile(12000) → helper(12001), helper(12001) → run_query(12002)
    // plus cross-project: helper(12001) → run_query(12102) in project 1001.
    // Caller is filtered to project 1000 (12000, 12001, 12002). Callee is unrestricted.
    resp.assert_node_count(4);
    resp.assert_referential_integrity();
    resp.assert_edge_set("CALLS", &[(12000, 12001), (12001, 12002), (12001, 12102)]);
}

/// Code graph traversal WITH node_ids — the existing cascade path.
/// Paired with the test above to verify both paths produce consistent results.
pub(super) async fn traversal_code_graph_calls_with_node_ids(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "caller", "entity": "Definition", "node_ids": [12000], "columns": ["name", "fqn"]},
                {"id": "callee", "entity": "Definition", "columns": ["name", "fqn"]}
            ],
            "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    // Pinned to compile(12000): only compile → helper edge
    resp.assert_node_count(2);
    resp.assert_referential_integrity();
    resp.assert_node_ids("Definition", &[12000, 12001]);
    resp.assert_edge_set("CALLS", &[(12000, 12001)]);
}
