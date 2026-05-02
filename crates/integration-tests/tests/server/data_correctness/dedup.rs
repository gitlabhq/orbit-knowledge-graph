//! Deduplication correctness tests for `ReplacingMergeTree` node and edge tables.
//!
//! Validates that the query pipeline:
//! - Returns only the latest `_version` when duplicate rows exist
//! - Excludes soft-deleted rows (`_deleted = true`) from all query types
//! - Evaluates mutable filters (`state`, `status`) against the latest
//!   version, not stale ones
//! - Applies `_deleted = false` filtering to edge table scans
//!
//! Each test runs in a forked database (`run_subtests!`) so INSERT
//! operations are isolated. There are no cross-test data dependencies.
//!
//! Both versions are inserted in a single INSERT so they land in the same
//! data part -- ReplacingMergeTree never deduplicates within a part, only
//! across parts during background merges. This makes the tests deterministic.
//!
//! Uses IDs >= 9000 to avoid conflict with the main seed data.

use integration_testkit::t;

use super::helpers::*;

fn dedup_svc() -> MockRedactionService {
    let mut svc = allow_all();
    svc.allow("user", &[9001, 9002, 9003, 9010, 9011, 9300, 9301, 9600]);
    svc.allow(
        "merge_request",
        &[
            9100, 9101, 9200, 9201, 9310, 9311, 9400, 9401, 9500, 9501, 9700, 9701, 9800, 9801,
            9900, 9901,
        ],
    );
    svc
}

/// Two versions of the same user in one INSERT. Search returns only the latest.
pub(super) async fn search_returns_latest_version(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9001, 'stale_name', 'Stale Name', 'blocked', 'human', '2024-01-01 00:00:00', false),
         (9001, 'fresh_name', 'Fresh Name', 'active',  'human', '2024-06-01 00:00:00', false)",
        t("gl_user")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "name", "state"],
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
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9002, 'ghost', 'Ghost User', 'active', 'human', '2024-01-01 00:00:00', false),
         (9002, 'ghost', 'Ghost User', 'active', 'human', '2024-06-01 00:00:00', true)",
        t("gl_user")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"],
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
    ctx.execute(&format!(
         "INSERT INTO {} (id, iid, title, state, traversal_path, project_id, _version, _deleted) VALUES
          (9100, 99, 'Dedup MR',         'merged', '1/100/1000/', 1000, '2024-01-01 00:00:00', false),
          (9100, 99, 'Dedup MR Updated', 'merged', '1/100/1000/', 1000, '2024-06-01 00:00:00', false)",
         t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1000/', 9100, 'MergeRequest', 'IN_PROJECT', 1000, 'Project', ['state:merged'], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
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
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9010, 'evolving', 'Evolving User', 'blocked', 'human', '2024-01-01 00:00:00', false),
         (9010, 'evolving', 'Evolving User', 'active',  'human', '2024-06-01 00:00:00', false)",
        t("gl_user")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
                     "filters": {"state": "active"}},
            "limit": 100
        }"#,
        &dedup_svc(),
    )
    .await;

    // Search uses LIMIT 1 BY to pick the latest version per user. Our test
    // user 9010 has latest version state='active', so it should appear.
    // Other seed users with state='active' also appear -- we just verify
    // 9010 is among them.
    resp.skip_requirement(Requirement::NodeCount);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
    let node = resp.find_node("User", 9010).unwrap();
    node.assert_str("state", "active");
}

/// Search with filter: latest version does NOT match, but a stale version does.
/// LIMIT 1 BY picks the latest version per user, then the outer WHERE
/// checks mutable filters against the deduplicated row. The row should be
/// excluded because the latest version's state is 'blocked', not 'active'.
pub(super) async fn search_filter_excludes_stale_match(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9011, 'flipper', 'Flipper User', 'active',  'human', '2024-01-01 00:00:00', false),
         (9011, 'flipper', 'Flipper User', 'blocked', 'human', '2024-06-01 00:00:00', false)",
        t("gl_user")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
                     "filters": {"state": "active"}},
            "limit": 100
        }"#,
        &dedup_svc(),
    )
    .await;

    // LIMIT 1 BY picks the latest version (state='blocked'), then the outer
    // WHERE state='active' rejects it. User 9011 should NOT appear in results.
    resp.skip_requirement(Requirement::NodeCount);
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    assert!(
        resp.find_node("User", 9011).is_none(),
        "user 9011 should be excluded (latest version is blocked, not active)"
    );
}

/// Aggregation with mutable filter: v1 state='merged', v2 state='opened'.
/// The filter `state = 'merged'` should NOT match because the latest version
/// has state='opened'. Mutable filters must apply after deduplication.
pub(super) async fn aggregation_filter_excludes_stale_mutable_match(ctx: &TestContext) {
    // MR 9200: v1 merged, v2 opened -- latest is 'opened', should NOT match state='merged'
    // MR 9201: single version, state='merged' -- should match
    ctx.execute(&format!(
         "INSERT INTO {} (id, iid, title, state, traversal_path, project_id, _version, _deleted) VALUES
          (9200, 200, 'Flipped MR', 'merged', '1/100/1002/', 1002, '2024-01-01 00:00:00', false),
          (9200, 200, 'Flipped MR', 'opened', '1/100/1002/', 1002, '2024-06-01 00:00:00', false),
          (9201, 201, 'Stable MR',  'merged', '1/100/1002/', 1002, '2024-06-01 00:00:00', false)",
         t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1002/', 9200, 'MergeRequest', 'IN_PROJECT', 1002, 'Project', ['state:opened'], []),
         ('1/100/1002/', 9201, 'MergeRequest', 'IN_PROJECT', 1002, 'Project', ['state:merged'], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "p", "entity": "Project", "columns": ["name"], "node_ids": [1002]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[1002]);
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    // Only MR 9201 should be counted (state='merged').
    // MR 9200 must NOT be counted: its latest version has state='opened'.
    resp.assert_node("Project", 1002, |n| {
        n.prop_i64("mr_count") == Some(1) && n.prop_str("name") == Some("Internal Project")
    });
}

/// Duplicate user rows. Traversal should produce one edge, not two.
pub(super) async fn traversal_dedup_returns_single_edge(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9003, 'dup_author',    'Old Author', 'active', 'human', '2024-01-01 00:00:00', false),
         (9003, 'dup_author_v2', 'New Author', 'active', 'human', '2024-06-01 00:00:00', false)",
        t("gl_user")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, traversal_path, _version, _deleted) VALUES
         (9101, 98, 'MR by dup author', 'opened', '1/100/1000/', '2024-06-01 00:00:00', false)",
        t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1000/', 9003, 'User', 'AUTHORED', 9101, 'MergeRequest', [], ['state:opened'])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
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

/// Traversal with mutable filter: v1 state='merged', v2 state='opened'.
/// The filter should NOT match because the latest version has state='opened'.
pub(super) async fn traversal_filter_excludes_stale_version(ctx: &TestContext) {
    // MR 9400: v1 merged, v2 opened -- latest is 'opened'
    // MR 9401: single version, state='merged' -- should match
    ctx.execute(&format!(
         "INSERT INTO {} (id, iid, title, state, traversal_path, project_id, _version, _deleted) VALUES
          (9400, 400, 'Stale Traversal MR', 'merged', '1/100/1003/', 1003, '2024-01-01 00:00:00', false),
          (9400, 400, 'Stale Traversal MR', 'opened', '1/100/1003/', 1003, '2024-06-01 00:00:00', false),
          (9401, 401, 'Good Traversal MR',  'merged', '1/100/1003/', 1003, '2024-06-01 00:00:00', false)",
         t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1003/', 9400, 'MergeRequest', 'IN_PROJECT', 1003, 'Project', ['state:opened'], []),
         ('1/100/1003/', 9401, 'MergeRequest', 'IN_PROJECT', 1003, 'Project', ['state:merged'], []),
         ('1/100/1003/', 1, 'User', 'AUTHORED', 9400, 'MergeRequest', [], ['state:opened']),
         ('1/100/1003/', 1, 'User', 'AUTHORED', 9401, 'MergeRequest', [], ['state:merged'])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "p", "entity": "Project", "node_ids": [1003]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    // Only MR 9401 should appear (state='merged').
    // MR 9400 must NOT appear: latest version has state='opened'.
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    resp.assert_node_count(2);
    resp.assert_node_ids("Project", &[1003]);
    resp.assert_edge_exists("MergeRequest", 9401, "Project", 1003, "IN_PROJECT");
    resp.assert_edge_count("IN_PROJECT", 1);
}

/// Edge-only traversals cannot filter out deleted nodes at the query layer:
/// the node is soft-deleted but the edge row is not, and the node table is
/// not joined. In production this scenario does not arise because the SDLC
/// indexer soft-deletes FK edge rows in the same ETL batch as their parent
/// node (see `crates/indexer/src/modules/sdlc/pipeline.rs`). This test uses
/// a synthetic setup (deleted node + non-deleted edge) to document the
/// query-layer limitation.
pub(super) async fn traversal_deleted_node_visible_via_edge(ctx: &TestContext) {
    // MR 9500: v1 alive, v2 deleted
    // MR 9501: single version, alive
    ctx.execute(&format!(
         "INSERT INTO {} (id, iid, title, state, traversal_path, project_id, _version, _deleted) VALUES
          (9500, 500, 'Deleted MR', 'merged', '1/100/1004/', 1004, '2024-01-01 00:00:00', false),
          (9500, 500, 'Deleted MR', 'merged', '1/100/1004/', 1004, '2024-06-01 00:00:00', true),
          (9501, 501, 'Alive MR',   'merged', '1/100/1004/', 1004, '2024-06-01 00:00:00', false)",
         t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1004/', 9500, 'MergeRequest', 'IN_PROJECT', 1004, 'Project', ['state:merged'], []),
         ('1/100/1004/', 9501, 'MergeRequest', 'IN_PROJECT', 1004, 'Project', ['state:merged'], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}},
                {"id": "p", "entity": "Project", "node_ids": [1004]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    // The MR node's id_range generates a _nf_mr CTE that joins the node
    // table with dedup, filtering out deleted MR 9500. Only the alive MR
    // 9501's edge survives the IN subquery filter.
    resp.assert_node_count(2);
    resp.assert_node_ids("Project", &[1004]);
    resp.assert_edge_exists("MergeRequest", 9501, "Project", 1004, "IN_PROJECT");
    resp.assert_edge_count("IN_PROJECT", 1);
}

/// Neighbors dedup: duplicate user rows should not produce duplicate edges.
pub(super) async fn neighbors_dedup_returns_unique_edges(ctx: &TestContext) {
    // User 9300 has two versions, MR 9300 is the center node.
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9300, 'nbr_old', 'Neighbor Old', 'active', 'human', '2024-01-01 00:00:00', false),
         (9300, 'nbr_new', 'Neighbor New', 'active', 'human', '2024-06-01 00:00:00', false)",
        t("gl_user")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, traversal_path, _version, _deleted) VALUES
         (9310, 310, 'Neighbor center MR', 'opened', '1/100/1000/', '2024-06-01 00:00:00', false)",
        t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1000/', 9300, 'User', 'AUTHORED', 9310, 'MergeRequest', [], ['state:opened'])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "mr", "entity": "MergeRequest", "node_ids": [9310]},
            "neighbors": {"node": "mr", "direction": "both"}
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeCount);
    resp.assert_node_ids("MergeRequest", &[9310]);
    resp.assert_edge_exists("User", 9300, "MergeRequest", 9310, "AUTHORED");
}

/// Edge-only neighbors cannot filter out deleted nodes at the query layer:
/// the node is soft-deleted but the edge row is not, and neighbor queries
/// don't join non-center node tables. In production the indexer soft-deletes
/// FK edge rows alongside their parent node, so this scenario is synthetic.
pub(super) async fn neighbors_deleted_node_visible_via_edge(ctx: &TestContext) {
    // User 9301: v1 alive, v2 deleted. Still visible via edge (known limitation).
    // MR 9311 is the center node.
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9301, 'del_nbr', 'Deleted Neighbor', 'active', 'human', '2024-01-01 00:00:00', false),
         (9301, 'del_nbr', 'Deleted Neighbor', 'active', 'human', '2024-06-01 00:00:00', true)",
        t("gl_user")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, traversal_path, _version, _deleted) VALUES
         (9311, 311, 'Neighbor del center MR', 'opened', '1/100/1000/', '2024-06-01 00:00:00', false)",
        t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1000/', 9301, 'User', 'AUTHORED', 9311, 'MergeRequest', [], ['state:opened'])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "mr", "entity": "MergeRequest", "node_ids": [9311]},
            "neighbors": {"node": "mr", "direction": "both"}
        }"#,
        &dedup_svc(),
    )
    .await;

    // User 9301 is soft-deleted, but the edge row itself is not deleted.
    // Neighbors queries are edge-only -- they don't join node tables for
    // non-center nodes, so the deleted user's edge still appears. This
    // documents a known limitation: edge-only queries cannot filter out
    // deleted nodes.
    resp.skip_requirement(Requirement::NodeCount);
    resp.assert_node_ids("MergeRequest", &[9311]);
    resp.assert_edge_exists("User", 9301, "MergeRequest", 9311, "AUTHORED");
}

/// Hydration returns properties from the latest version, not a stale one.
/// User 9600 has v1 username='hydrate_old', v2 username='hydrate_new'.
/// The hydrated properties should show 'hydrate_new'.
pub(super) async fn hydration_returns_latest_properties(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, _version, _deleted) VALUES
         (9600, 'hydrate_old', 'Old Hydrated', 'active', 'human', '2024-01-01 00:00:00', false),
         (9600, 'hydrate_new', 'New Hydrated', 'active', 'human', '2024-06-01 00:00:00', false)",
        t("gl_user")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1000/', 9600, 'User', 'MEMBER_OF', 100, 'Group', [], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [9600], "columns": ["username", "name"]},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeCount);
    resp.assert_node_ids("User", &[9600]);
    // Hydration should return v2 properties, not v1.
    let node = resp.find_node("User", 9600).unwrap();
    node.assert_str("username", "hydrate_new");
    node.assert_str("name", "New Hydrated");
    resp.assert_edge_exists("User", 9600, "Group", 100, "MEMBER_OF");
}

/// Soft-deleted edge row excluded from traversal results.
/// The edge itself has _deleted=true (as the indexer would set it).
pub(super) async fn traversal_excludes_deleted_edge(ctx: &TestContext) {
    // MR 9700: alive, with a deleted edge to project 1000
    // MR 9701: alive, with a non-deleted edge to project 1000
    ctx.execute(&format!(
         "INSERT INTO {} (id, iid, title, state, traversal_path, project_id, _version, _deleted) VALUES
          (9700, 700, 'Alive MR deleted edge', 'merged', '1/100/1000/', 0, '2024-06-01 00:00:00', false),
          (9701, 701, 'Alive MR good edge',    'merged', '1/100/1000/', 1000, '2024-06-01 00:00:00', false)",
         t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags, _deleted) VALUES
         ('1/100/1000/', 9700, 'MergeRequest', 'IN_PROJECT', 1000, 'Project', ['state:merged'], [], true),
         ('1/100/1000/', 9701, 'MergeRequest', 'IN_PROJECT', 1000, 'Project', ['state:merged'], [], false)",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}},
                {"id": "p", "entity": "Project", "node_ids": [1000]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    // Only MR 9701's edge should appear. MR 9700's edge is soft-deleted.
    resp.skip_requirement(Requirement::NodeCount);
    resp.assert_node_ids("Project", &[1000]);
    resp.assert_edge_exists("MergeRequest", 9701, "Project", 1000, "IN_PROJECT");
    assert!(
        resp.find_edge("MergeRequest", 9700, "Project", 1000, "IN_PROJECT")
            .is_none(),
        "edge for MR 9700 should be excluded (_deleted=true)"
    );
}

/// Three versions of the same entity: dedup picks the latest (v3), not v2.
/// Confirms ORDER BY _version DESC picks the true latest across multiple versions.
pub(super) async fn search_three_versions_returns_latest(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, traversal_path, _version, _deleted) VALUES
         (9800, 800, 'MR v1', 'opened', '1/100/1000/', '2024-01-01 00:00:00', false),
         (9800, 800, 'MR v2', 'merged', '1/100/1000/', '2024-03-01 00:00:00', false),
         (9800, 800, 'MR v3', 'closed', '1/100/1000/', '2024-06-01 00:00:00', false),
         (9801, 801, 'Control MR', 'merged', '1/100/1000/', '2024-06-01 00:00:00', false)",
        t("gl_merge_request")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "columns": ["title", "state"],
                     "node_ids": [9800, 9801]},
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("MergeRequest", &[9800, 9801]);
    // MR 9800 should show v3 (state='closed'), not v2 (state='merged').
    let node = resp.find_node("MergeRequest", 9800).unwrap();
    node.assert_str("state", "closed");
    node.assert_str("title", "MR v3");
    // Control: MR 9801 has a single version.
    let control = resp.find_node("MergeRequest", 9801).unwrap();
    control.assert_str("state", "merged");
}

/// Aggregation excludes deleted entities from count via _nf_* CTE dedup.
/// MR 9900 has latest version _deleted=true. It should not be counted.
/// MR 9901 is alive and serves as the control (should be counted).
pub(super) async fn aggregation_excludes_deleted_from_count(ctx: &TestContext) {
    ctx.execute(&format!(
         "INSERT INTO {} (id, iid, title, state, traversal_path, project_id, _version, _deleted) VALUES
          (9900, 900, 'Counted then deleted', 'merged', '1/100/1002/', 1002, '2024-01-01 00:00:00', false),
          (9900, 900, 'Counted then deleted', 'merged', '1/100/1002/', 1002, '2024-06-01 00:00:00', true),
          (9901, 901, 'Alive MR',             'merged', '1/100/1002/', 1002, '2024-06-01 00:00:00', false)",
         t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1002/', 9900, 'MergeRequest', 'IN_PROJECT', 1002, 'Project', ['state:merged'], []),
         ('1/100/1002/', 9901, 'MergeRequest', 'IN_PROJECT', 1002, 'Project', ['state:merged'], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "p", "entity": "Project", "columns": ["name"], "node_ids": [1002]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &dedup_svc(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[1002]);
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    // MR 9900 is deleted, only MR 9901 should be counted.
    resp.assert_node("Project", 1002, |n| {
        n.prop_i64("mr_count") == Some(1) && n.prop_str("name") == Some("Internal Project")
    });
}
