use super::helpers::*;

// ─────────────────────────────────────────────────────────────────────────────
// Search: traversal path scoping
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn search_scoped_path_excludes_other_namespaces(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("Project", &[1000, 1002]);
    resp.assert_node_absent("Project", 1001);
    resp.assert_node_absent("Project", 1003);
    resp.assert_node_absent("Project", 1004);
}

pub(super) async fn search_scoped_to_single_project_namespace(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/1000/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[1000]);
}

pub(super) async fn search_multi_path_returns_union_of_scopes(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into(), "1/102/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("Project", &[1000, 1002, 1004]);
    resp.assert_node_absent("Project", 1001);
    resp.assert_node_absent("Project", 1003);
}

pub(super) async fn search_scoped_mr_excludes_other_namespaces(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "mr", "entity": "MergeRequest", "columns": ["title"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/101/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("MergeRequest", &[2002]);
    resp.assert_node_absent("MergeRequest", 2000);
    resp.assert_node_absent("MergeRequest", 2001);
}

pub(super) async fn search_with_filter_respects_scope(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"],
                     "filters": {"visibility_level": "public"}},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[1000]);
    resp.assert_filter("Project", "visibility_level", |n| {
        n.prop_str("visibility_level") == Some("public")
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding: traversal path scoping
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn path_finding_scoped_excludes_paths_through_other_namespaces(
    ctx: &TestContext,
) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .await;

    resp.assert_referential_integrity();

    let pids = resp.path_ids();
    let destinations: HashSet<i64> = pids
        .iter()
        .filter_map(|&pid| resp.path(pid).last().map(|e| e.to_id))
        .collect();
    assert!(
        destinations.contains(&1000),
        "should find path to Project 1000 within scope 1/100/"
    );
    assert!(
        !destinations.contains(&1004),
        "should NOT find path to Project 1004 (in 1/102/, outside scope)"
    );
}

pub(super) async fn path_finding_multi_path_scope_finds_both(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into(), "1/102/".into()]).unwrap(),
    )
    .await;

    resp.assert_referential_integrity();

    let pids = resp.path_ids();
    let destinations: HashSet<i64> = pids
        .iter()
        .filter_map(|&pid| resp.path(pid).last().map(|e| e.to_id))
        .collect();
    assert!(
        destinations.contains(&1000) && destinations.contains(&1004),
        "multi-path scope should find paths to both 1000 and 1004, got: {destinations:?}"
    );
}

pub(super) async fn path_finding_narrow_scope_excludes_all_targets(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/101/".into()]).unwrap(),
    )
    .await;

    resp.assert_referential_integrity();

    let pids = resp.path_ids();
    assert!(
        pids.is_empty(),
        "scope 1/101/ should find no paths to Projects 1000 or 1004"
    );
}
