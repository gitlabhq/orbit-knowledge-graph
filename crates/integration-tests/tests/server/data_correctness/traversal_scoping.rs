//! Data-correctness tests for project/group traversal_path scoping (#601941).
//!
//! These run real queries against seeded ClickHouse and assert on the returned
//! rows (not the compiled SQL). The path-resolution stage does not run in this
//! harness, so the resolved/flooded prefixes are supplied directly on the
//! `SecurityContext` (`with_scope_prefixes`) exactly as `PathResolutionStage`
//! would in the server. The flood itself is unit-tested in the ontology and
//! gkg-server crates; here we prove the *results* stay correct:
//! - a project-scoped multi-edge traversal returns the same rows as the broad
//!   query (scoping is lossless), and
//! - a cross-namespace traversal still returns its cross-project entities
//!   (the prefix never over-prunes a non-scope-preserving edge).

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
    resp.assert_filter("MergeRequest", "project_id", |n| {
        n.prop_i64("project_id") == Some(1000)
    });
    resp.assert_edge_set("HAS_DIFF", &[(2000, 5000)]);
    resp.assert_edge_set("HAS_FILE", &[(5000, 9300), (5000, 9301)]);
    resp.assert_referential_integrity();
}

/// The customer-zero chain (`MergeRequest --HAS_DIFF--> Diff --HAS_FILE-->
/// File`) scoped to its project must return exactly the same rows as the
/// unscoped query. Diff 5000 (MR 2000, project 1000) gets two changed files;
/// HAS_FILE lives in the project's traversal_path like HAS_DIFF.
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

    // Same query with the project prefix flooded onto the anchor MR and the
    // same-namespace diff/file payload nodes — identical result set.
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

/// Cross-namespace MergeRequest -> WorkItem: MR 2000 (project 1000,
/// `1/100/1000/`) closes WorkItem 4002, which lives in a different namespace
/// (`1/101/`). CLOSES is not scope-preserving, so the MR's project prefix must
/// not prune the cross-project work item.
pub(super) async fn cross_namespace_closes_returns_cross_project_work_item(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/101/', 2000, 'MergeRequest', 'CLOSES', 4002, 'WorkItem', ['state:opened'], ['state:opened', 'wi_type:task'])",
        t("gl_edge")
    ))
    .await;
    ctx.optimize_all().await;

    // The flood resolves only the MR anchor; CLOSES does not propagate to the WI.
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
    resp.assert_filter("MergeRequest", "project_id", |n| {
        n.prop_i64("project_id") == Some(1000)
    });
    resp.assert_edge_set("CLOSES", &[(2000, 4002)]);
    resp.assert_referential_integrity();
}

/// Multiple anchors in one query apply distinct traversal_paths. User 1
/// authors MRs in two different projects; the two MergeRequest nodes are pinned
/// to different projects, so each must be scoped to its own project's prefix.
/// Cross-contamination would drop one project's MRs.
pub(super) async fn multiple_anchors_apply_distinct_traversal_paths(ctx: &TestContext) {
    // User 1 already authors MRs 2000/2001 in project 1000 (1/100/1000/);
    // give them an authored MR in project 1001 (1/101/1001/) too.
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/101/1001/', 1, 'User', 'AUTHORED', 2002, 'MergeRequest', [], ['state:merged'])",
        t("gl_edge")
    ))
    .await;
    ctx.optimize_all().await;

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr_a", "entity": "MergeRequest", "columns": ["project_id"],
                 "filters": {"project_id": {"op": "eq", "value": 1000}}},
                {"id": "mr_b", "entity": "MergeRequest", "columns": ["project_id"],
                 "filters": {"project_id": {"op": "eq", "value": 1001}}}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr_a"},
                {"type": "AUTHORED", "from": "u", "to": "mr_b"}
            ],
            "limit": 50
        }"#,
        &allow_all(),
        scoped("1/", &[("mr_a", "1/100/1000/"), ("mr_b", "1/101/1001/")]),
    )
    .await;

    // mr_a is scoped to project 1000 (returns 2000, 2001); mr_b to project 1001
    // (returns 2002). Both prefixes apply independently — neither over-prunes.
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("MergeRequest", &[2000, 2001, 2002]);
    resp.assert_filter("MergeRequest", "project_id", |n| {
        matches!(n.prop_i64("project_id"), Some(1000) | Some(1001))
    });
    resp.assert_edge_set("AUTHORED", &[(1, 2000), (1, 2001), (1, 2002)]);
    resp.assert_referential_integrity();
}

/// Cross-namespace WorkItem -> Label: WorkItems 4000/4001 live in group 100
/// (`1/100/`); WorkItem 4001 carries Label 7002, which lives in a different
/// group (`1/101/`). HAS_LABEL is not scope-preserving, so scoping the work
/// items to their group must still return the cross-group label.
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
