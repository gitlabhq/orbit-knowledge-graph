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

pub(super) async fn fk_chain_recovers_files_without_edge_rows(ctx: &TestContext) {
    // No HAS_DIFF or HAS_FILE edge rows exist for this merge request; the whole
    // chain is answered from the merge_request_diff / _file foreign keys. This is
    // the FK-chain lowering returning rows the edge scan would miss when an edge
    // is tombstoned while its node stays live. The _deleted file is excluded.
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, project_id, traversal_path) VALUES
         (2099, 99, 'Isolated FK chain', 'opened', 1099, '1/100/1099/')",
        t("gl_merge_request")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, merge_request_id, state, traversal_path) VALUES
         (5099, 2099, 'collected', '1/100/1099/')",
        t("gl_merge_request_diff")
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, merge_request_id, merge_request_diff_id, project_id, new_path, traversal_path, _deleted) VALUES
         (9390, 2099, 5099, 1099, 'live_a.rs', '1/100/1099/', false),
         (9391, 2099, 5099, 1099, 'live_b.rs', '1/100/1099/', false),
         (9392, 2099, 5099, 1099, 'gone.rs',   '1/100/1099/', true)",
        t("gl_merge_request_diff_file")
    ))
    .await;
    ctx.optimize_all().await;

    let mut redaction = allow_all();
    redaction.allow("merge_request", &[2099]);
    redaction.allow("project", &[1099]);
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "columns": ["project_id"],
                 "filters": {"project_id": {"op": "eq", "value": 1099}}},
                {"id": "diff", "entity": "MergeRequestDiff"},
                {"id": "df", "entity": "MergeRequestDiffFile", "columns": ["new_path"]}
            ],
            "relationships": [
                {"type": "HAS_DIFF", "from": "mr", "to": "diff"},
                {"type": "HAS_FILE", "from": "diff", "to": "df"}
            ],
            "limit": 50
        }"#,
        &redaction,
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("MergeRequest", &[2099]);
    resp.assert_node_ids("MergeRequestDiff", &[5099]);
    resp.assert_node_ids("MergeRequestDiffFile", &[9390, 9391]);
    resp.assert_edge_set("HAS_DIFF", &[(2099, 5099)]);
    resp.assert_edge_set("HAS_FILE", &[(5099, 9390), (5099, 9391)]);
    resp.skip_requirement(Requirement::Filter {
        field: "project_id".into(),
    });
    resp.assert_referential_integrity();

    // Filtering the leaf makes reorder_by_selectivity reverse the chain; the
    // synthesized edges must keep the source -> target orientation (diff -> file,
    // mr -> diff), not the query's reversed traversal order.
    let reversed = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "diff", "entity": "MergeRequestDiff"},
                {"id": "df", "entity": "MergeRequestDiffFile", "columns": ["new_path"],
                 "filters": {"new_path": {"op": "ends_with", "value": ".rs"}}}
            ],
            "relationships": [
                {"type": "HAS_DIFF", "from": "mr", "to": "diff"},
                {"type": "HAS_FILE", "from": "diff", "to": "df"}
            ],
            "limit": 50
        }"#,
        &redaction,
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;

    reversed.assert_node_count(4);
    reversed.assert_node_ids("MergeRequestDiffFile", &[9390, 9391]);
    reversed.assert_edge_set("HAS_DIFF", &[(2099, 5099)]);
    reversed.assert_edge_set("HAS_FILE", &[(5099, 9390), (5099, 9391)]);
    reversed.skip_requirement(Requirement::Filter {
        field: "new_path".into(),
    });
    reversed.assert_referential_integrity();
}

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

    // Two anchors with distinct prefixes: mr only scans under 1/100/1000/ and wi
    // only under 1/101/, so swapping either prefix onto the other node would drop
    // its row. The cross-namespace CLOSES edge stays unscoped, so the pair joins.
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
