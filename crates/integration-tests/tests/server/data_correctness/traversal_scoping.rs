//! Data-correctness tests for project/group traversal_path scoping (#601941).
//!
//! The path-resolution stage does not run in this harness, so the
//! resolved/flooded prefixes are supplied directly on the `SecurityContext`
//! (`with_scope_prefixes`) exactly as `PathResolutionStage` would in the server.

use std::collections::HashMap;

use integration_testkit::t;

use super::helpers::*;

fn scoped(authorized: &str, prefixes: &[(&str, &str)]) -> SecurityContext {
    let map: HashMap<String, String> = prefixes
        .iter()
        .map(|(alias, prefix)| (alias.to_string(), prefix.to_string()))
        .collect();
    SecurityContext::new(1, vec![authorized.into()])
        .unwrap()
        .with_scope_prefixes(map)
}

const MR_DIFF_FILE_CHAIN: &str = r#"{
    "query_type": "traversal",
    "nodes": [
        {"id": "mr", "entity": "MergeRequest", "columns": ["project_id"],
         "filters": {"project_id": {"op": "eq", "value": 1000}}},
        {"id": "diff", "entity": "MergeRequestDiff"},
        {"id": "df", "entity": "MergeRequestDiffFile"}
    ],
    "relationships": [
        {"type": "HAS_DIFF", "from": "mr", "to": "diff"},
        {"type": "HAS_FILE", "from": "diff", "to": "df"}
    ],
    "limit": 50
}"#;

fn assert_diff_file_chain(resp: &ResponseView) {
    resp.assert_node_count(4);
    resp.assert_node_ids("MergeRequest", &[2000]);
    resp.assert_node_ids("MergeRequestDiff", &[5000]);
    resp.assert_node_ids("MergeRequestDiffFile", &[9300, 9301]);
    resp.skip_requirement(Requirement::Filter {
        field: "project_id".into(),
    });
    resp.assert_edge_set("HAS_DIFF", &[(2000, 5000)]);
    resp.assert_edge_set("HAS_FILE", &[(5000, 9300), (5000, 9301)]);
    resp.assert_referential_integrity();
}

pub(super) async fn project_scoped_multi_edge_traversal_is_lossless(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, merge_request_id, merge_request_diff_id, project_id, new_path, traversal_path) VALUES
         (9300, 2000, 5000, 1000, 'src/a.rs', '1/100/1000/'),
         (9301, 2000, 5000, 1000, 'src/b.rs', '1/100/1000/')",
        t("gl_merge_request_diff_file")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/1000/', 5000, 'MergeRequestDiff', 'HAS_FILE', 9300, 'MergeRequestDiffFile', [], []),
         ('1/100/1000/', 5000, 'MergeRequestDiff', 'HAS_FILE', 9301, 'MergeRequestDiffFile', [], [])",
        t("gl_edge")
    ))
    .await;
    ctx.optimize_all().await;

    let broad = run_query_with_security(
        ctx,
        MR_DIFF_FILE_CHAIN,
        &allow_all(),
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;
    assert_diff_file_chain(&broad);

    let scoped_resp = run_query_with_security(
        ctx,
        MR_DIFF_FILE_CHAIN,
        &allow_all(),
        scoped(
            "1/",
            &[
                ("mr", "1/100/1000/"),
                ("diff", "1/100/1000/"),
                ("df", "1/100/1000/"),
            ],
        ),
    )
    .await;
    assert_diff_file_chain(&scoped_resp);
}

pub(super) async fn cross_namespace_closes_returns_cross_project_work_item(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/101/', 2000, 'MergeRequest', 'CLOSES', 4002, 'WorkItem', ['state:opened'], ['state:opened', 'wi_type:task'])",
        t("gl_edge")
    ))
    .await;
    ctx.optimize_all().await;

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "columns": ["project_id"],
                 "filters": {"project_id": {"op": "eq", "value": 1000}}},
                {"id": "wi", "entity": "WorkItem"}
            ],
            "relationships": [{"type": "CLOSES", "from": "mr", "to": "wi"}],
            "limit": 50
        }"#,
        &allow_all(),
        scoped("1/", &[("mr", "1/100/1000/")]),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("MergeRequest", &[2000]);
    resp.assert_node_ids("WorkItem", &[4002]);
    resp.skip_requirement(Requirement::Filter {
        field: "project_id".into(),
    });
    resp.assert_edge_set("CLOSES", &[(2000, 4002)]);
    resp.assert_referential_integrity();
}

pub(super) async fn multiple_anchors_apply_distinct_traversal_paths(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/101/', 2000, 'MergeRequest', 'CLOSES', 4002, 'WorkItem', ['state:opened'], ['state:opened', 'wi_type:task'])",
        t("gl_edge")
    ))
    .await;
    ctx.optimize_all().await;

    // Distinct per-anchor prefixes: swapping either onto the other node drops its
    // row. The cross-namespace CLOSES edge stays unscoped, so the pair still joins.
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "columns": ["project_id"],
                 "filters": {"project_id": {"op": "eq", "value": 1000}}},
                {"id": "wi", "entity": "WorkItem"}
            ],
            "relationships": [{"type": "CLOSES", "from": "mr", "to": "wi"}],
            "limit": 50
        }"#,
        &allow_all(),
        scoped("1/", &[("mr", "1/100/1000/"), ("wi", "1/101/")]),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("MergeRequest", &[2000]);
    resp.assert_node_ids("WorkItem", &[4002]);
    resp.skip_requirement(Requirement::Filter {
        field: "project_id".into(),
    });
    resp.assert_edge_set("CLOSES", &[(2000, 4002)]);
    resp.assert_referential_integrity();
}

// A scope-resolved Group->CONTAINS->Project anchor elides to a pure FK node-join
// star; the counts must match what the seeded data implies (no edge scans run).
pub(super) async fn scope_implied_container_elision_star_counts_authored_mrs(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, full_path, visibility_level, traversal_path) VALUES
         (700, 'g700', 'g700', 'public', '1/700/')",
        t("gl_group")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, full_path, visibility_level, traversal_path) VALUES
         (7000, 'p700', 'g700/p700', 'public', '1/700/7000/')",
        t("gl_project")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type, email) VALUES
         (7701, 'u7701', 'U 7701', 'active', 'human', 'u7701@x.com'),
         (7702, 'u7702', 'U 7702', 'active', 'human', 'u7702@x.com')",
        t("gl_user")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, merged_at, project_id, author_id, traversal_path) VALUES
         (7100, 1, 'a', 'opened', 'b', 'main', NULL, 7000, 7701, '1/700/7000/'),
         (7101, 2, 'b', 'opened', 'b', 'main', NULL, 7000, 7701, '1/700/7000/'),
         (7102, 3, 'c', 'opened', 'b', 'main', NULL, 7000, 7702, '1/700/7000/')",
        t("gl_merge_request")
    ))
    .await;
    ctx.optimize_all().await;

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "filters": {"full_path": "g700"}},
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [
                {"type": "CONTAINS", "from": "g", "to": "p"},
                {"type": "IN_PROJECT", "from": "mr", "to": "p"},
                {"type": "AUTHORED", "from": "u", "to": "mr"}
            ],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "c"}],
            "limit": 10
        }"#,
        &{
            let mut svc = allow_all();
            svc.allow("user", &[7701, 7702]);
            svc
        },
        scoped("1/", &[("g", "1/700/"), ("p", "1/700/"), ("mr", "1/700/")]),
    )
    .await;

    resp.assert_group_row_value_i64("u", "User", 7701, "c", 2);
    resp.assert_group_row_value_i64("u", "User", 7702, "c", 1);
}

// Same elision over a 4-hop FK chain (the A_ma3 diff-file shape): MR->latest diff
// ->files resolved by FK columns once the CONTAINS anchor is elided.
pub(super) async fn scope_implied_container_elision_chain_counts_diff_files(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, full_path, visibility_level, traversal_path) VALUES
         (701, 'g701', 'g701', 'public', '1/701/')",
        t("gl_group")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, full_path, visibility_level, traversal_path) VALUES
         (7010, 'p701', 'g701/p701', 'public', '1/701/7010/')",
        t("gl_project")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, merged_at, project_id, author_id, latest_merge_request_diff_id, traversal_path) VALUES
         (7110, 1, 'a', 'opened', 'b', 'main', NULL, 7010, 1, 7210, '1/701/7010/'),
         (7111, 2, 'b', 'opened', 'b', 'main', NULL, 7010, 1, 7211, '1/701/7010/')",
        t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, merge_request_id, state, head_commit_sha, traversal_path) VALUES
         (7210, 7110, 'collected', 'sha1', '1/701/7010/'),
         (7211, 7111, 'collected', 'sha2', '1/701/7010/')",
        t("gl_merge_request_diff")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, merge_request_id, merge_request_diff_id, project_id, new_path, traversal_path) VALUES
         (7310, 7110, 7210, 7010, 'a.rs', '1/701/7010/'),
         (7311, 7110, 7210, 7010, 'b.rs', '1/701/7010/'),
         (7312, 7111, 7211, 7010, 'c.rs', '1/701/7010/')",
        t("gl_merge_request_diff_file")
    ))
    .await;
    ctx.optimize_all().await;

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "filters": {"full_path": "g701"}},
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "d", "entity": "MergeRequestDiff"},
                {"id": "f", "entity": "MergeRequestDiffFile"}
            ],
            "relationships": [
                {"type": "CONTAINS", "from": "g", "to": "p"},
                {"type": "IN_PROJECT", "from": "mr", "to": "p"},
                {"type": "HAS_LATEST_DIFF", "from": "mr", "to": "d"},
                {"type": "HAS_FILE", "from": "d", "to": "f"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{"function": "count", "target": "f", "alias": "c"}],
            "limit": 10
        }"#,
        &{
            let mut svc = allow_all();
            svc.allow("project", &[7010]);
            svc
        },
        scoped(
            "1/",
            &[
                ("g", "1/701/"),
                ("p", "1/701/"),
                ("mr", "1/701/"),
                ("d", "1/701/"),
                ("f", "1/701/"),
            ],
        ),
    )
    .await;

    resp.assert_group_row_value_i64("p", "Project", 7010, "c", 3);
}

pub(super) async fn cross_namespace_has_label_returns_cross_group_label(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "wi", "entity": "WorkItem", "node_ids": [4000, 4001]},
                {"id": "lab", "entity": "Label"}
            ],
            "relationships": [{"type": "HAS_LABEL", "from": "wi", "to": "lab"}],
            "limit": 50
        }"#,
        &allow_all(),
        scoped("1/", &[("wi", "1/100/")]),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_node_ids("WorkItem", &[4000, 4001]);
    resp.assert_node_ids("Label", &[7000, 7001, 7002]);
    resp.assert_edge_set("HAS_LABEL", &[(4000, 7000), (4000, 7001), (4001, 7002)]);
    resp.assert_referential_integrity();
}
