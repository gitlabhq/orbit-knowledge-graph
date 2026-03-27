//! Deduplication correctness tests.
//!
//! Inserts intentional duplicate rows (same `id`, different `_version`)
//! into node tables then verifies the query pipeline returns only the
//! latest version and excludes soft-deleted rows.
//!
//! Both versions are inserted in a single INSERT so they land in the same
//! data part — ReplacingMergeTree never deduplicates within a part, only
//! across parts during background merges. This makes the tests deterministic.
//!
//! Uses IDs >= 9000 to avoid conflict with the main seed data.

use super::helpers::*;

fn dedup_svc() -> MockRedactionService {
    let mut svc = allow_all();
    svc.allow("user", &[9001, 9002, 9003, 9010, 9011]);
    svc.allow("merge_request", &[9100, 9101]);
    svc
}

/// Two versions of the same user in one INSERT. Search returns only the latest.
pub(super) async fn search_returns_latest_version(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type, _version, _deleted) VALUES
         (9001, 'stale_name', 'Stale Name', 'blocked', 'human', '2024-01-01 00:00:00', false),
         (9001, 'fresh_name', 'Fresh Name', 'active',  'human', '2024-06-01 00:00:00', false)",
    )
    .await;

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "name", "state"],
                     "node_ids": [9001]},
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[9001]);
    let node = resp.find_node("User", 9001).unwrap();
    node.assert_str("username", "fresh_name");
    node.assert_str("name", "Fresh Name");
    node.assert_str("state", "active");
}

/// Latest version has `_deleted = true`. Search should return nothing.
pub(super) async fn search_excludes_deleted_rows(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type, _version, _deleted) VALUES
         (9002, 'ghost', 'Ghost User', 'active', 'human', '2024-01-01 00:00:00', false),
         (9002, 'ghost', 'Ghost User', 'active', 'human', '2024-06-01 00:00:00', true)",
    )
    .await;

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "node_ids": [9002]},
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    // Deleted entity — no results. Skip the node_ids requirement since
    // the correct behavior is 0 rows (the ID exists but is soft-deleted).
    resp.skip_requirement(Requirement::NodeIds);
    resp.assert_node_count(0);
}

/// Duplicate MR rows (same id) in one INSERT. Aggregation counts it once.
pub(super) async fn aggregation_dedup_counts_unique_entities(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_merge_request (id, iid, title, state, traversal_path, _version, _deleted) VALUES
         (9100, 99, 'Dedup MR',         'merged', '1/100/1000/', '2024-01-01 00:00:00', false),
         (9100, 99, 'Dedup MR Updated', 'merged', '1/100/1000/', '2024-06-01 00:00:00', false)",
    )
    .await;
    ctx.execute(
        "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 9100, 'MergeRequest', 'IN_PROJECT', 1000, 'Project')",
    )
    .await;

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "p", "entity": "Project", "columns": ["name"], "node_ids": [1000]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[1000]);
    // MR is the aggregation target (counted), not a returned node — skip filter check.
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    // Duplicate MR 9100 should be counted once (opened MRs excluded by state filter).
    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project") && n.prop_i64("mr_count") == Some(1)
    });
}

/// Search with filter: latest version matches the filter. Should return the row.
pub(super) async fn search_filter_returns_latest_matching_version(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type, _version, _deleted) VALUES
         (9010, 'evolving', 'Evolving User', 'blocked', 'human', '2024-01-01 00:00:00', false),
         (9010, 'evolving', 'Evolving User', 'active',  'human', '2024-06-01 00:00:00', false)",
    )
    .await;

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "filters": {"state": "active"}},
            "limit": 100
        }"#,
        &dedup_svc(),
    )
    .await;

    // Unpinned search uses argMaxIfOrNull. Our test user 9010 has latest
    // version state='active', so it should appear. Other seed users with
    // state='active' also appear -- we just verify 9010 is among them.
    resp.skip_requirement(Requirement::NodeCount);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
    let node = resp.find_node("User", 9010).unwrap();
    node.assert_str("state", "active");
}

/// Search with filter: latest version does NOT match, but a stale version does.
/// Value filters are checked in both WHERE (prewhere pruning) and HAVING
/// (argMaxIfOrNull verifies the latest version). The row should be excluded
/// because the latest version's state is 'blocked', not 'active'.
pub(super) async fn search_filter_excludes_stale_match(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type, _version, _deleted) VALUES
         (9011, 'flipper', 'Flipper User', 'active',  'human', '2024-01-01 00:00:00', false),
         (9011, 'flipper', 'Flipper User', 'blocked', 'human', '2024-06-01 00:00:00', false)",
    )
    .await;

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "filters": {"state": "active"}},
            "limit": 100
        }"#,
        &dedup_svc(),
    )
    .await;

    // v1 (state='active') passes WHERE, but HAVING checks argMaxIfOrNull(state)
    // which returns 'blocked' (latest version). The HAVING filter rejects the group.
    // User 9011 should NOT appear in results.
    resp.skip_requirement(Requirement::NodeCount);
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    assert!(
        resp.find_node("User", 9011).is_none(),
        "user 9011 should be excluded (latest version is blocked, not active)"
    );
}

/// Duplicate user rows. Traversal should produce one edge, not two.
pub(super) async fn traversal_dedup_returns_single_edge(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type, _version, _deleted) VALUES
         (9003, 'dup_author',    'Old Author', 'active', 'human', '2024-01-01 00:00:00', false),
         (9003, 'dup_author_v2', 'New Author', 'active', 'human', '2024-06-01 00:00:00', false)",
    )
    .await;
    ctx.execute(
        "INSERT INTO gl_merge_request (id, iid, title, state, traversal_path, _version, _deleted) VALUES
         (9101, 98, 'MR by dup author', 'opened', '1/100/1000/', '2024-06-01 00:00:00', false)",
    )
    .await;
    ctx.execute(
        "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 9003, 'User', 'AUTHORED', 9101, 'MergeRequest')",
    )
    .await;

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [9003]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("User", &[9003]);
    resp.assert_edge_exists("User", 9003, "MergeRequest", 9101, "AUTHORED");
    resp.assert_edge_count("AUTHORED", 1);
}
