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

    // Only Public Project (1000) is public AND in 1/100/. Shared Project (1004) is public but in 1/102/.
    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[1000]);
    resp.assert_filter("Project", "visibility_level", |n| {
        n.prop_str("visibility_level") == Some("public")
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding: traversal path scoping
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn path_finding_scoped_excludes_paths_through_other_namespaces(ctx: &TestContext) {
    // User 1 → Group 100 → Project 1000 is within 1/100/.
    // User 1 → Group 102 → Project 1004 requires 1/102/.
    // Scoping to 1/100/ should only find the path through Group 100.
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
    // Only the path to 1000 (via Group 100) should survive.
    // Path to 1004 (via Group 102) should be excluded by traversal_path scoping.
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
    // Scope to 1/101/ — User 1 is in gl_user (no traversal_path filtering),
    // but Projects 1000 and 1004 are in 1/100/ and 1/102/ respectively.
    // Neither is reachable within 1/101/ scope.
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

// ─────────────────────────────────────────────────────────────────────────────
// Field-level admin_only restriction (RestrictPass)
//
// User.is_admin and User.is_auditor are declared admin_only in the ontology.
// Non-admin callers must never see their values in any response, and attempts
// to filter, order, or aggregate by those fields must fail at compile time.
// Admin callers (JWT claim admin=true) bypass every check.
// ─────────────────────────────────────────────────────────────────────────────

fn non_admin_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).unwrap()
}

fn admin_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()])
        .unwrap()
        .with_role(true, None)
}

pub(super) async fn admin_only_non_admin_filter_rejects_at_compile(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"is_admin": true}},
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("non-admin filter on is_admin must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("is_admin") && msg.contains("administrator"),
        "error should name the field and mention administrator access, got: {msg}"
    );
}

pub(super) async fn admin_only_non_admin_order_by_rejects_at_compile(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "is_admin", "direction": "DESC"},
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("non-admin order_by on is_admin must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("is_admin") && msg.contains("order_by") && msg.contains("administrator"),
        "error should name the field, order_by, and administrator access, got: {msg}"
    );
}

pub(super) async fn admin_only_non_admin_max_aggregation_rejects_at_compile(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"]}],
            "aggregations": [{
                "function": "max",
                "target": "u",
                "property": "is_admin",
                "alias": "has_admin"
            }],
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("non-admin MAX(is_admin) must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("is_admin") && msg.contains("aggregation") && msg.contains("administrator"),
        "error should name the field, aggregation, and administrator access, got: {msg}"
    );
}

pub(super) async fn admin_only_non_admin_count_aggregation_on_auditor_rejects_at_compile(
    ctx: &TestContext,
) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"]}],
            "aggregations": [{
                "function": "count",
                "target": "u",
                "property": "is_auditor",
                "alias": "auditor_count"
            }],
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("non-admin COUNT(is_auditor) must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("is_auditor") && msg.contains("aggregation"),
        "error should name the field and aggregation, got: {msg}"
    );
}

pub(super) async fn admin_only_non_admin_wildcard_columns_excludes_admin_fields(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": "*", "node_ids": [1]},
            "limit": 10
        }"#,
        &allow_all(),
        non_admin_ctx(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[1]);
    let alice = resp.find_node("User", 1).unwrap();
    alice.assert_str("username", "alice");
    assert!(
        alice.prop("is_admin").is_none(),
        "non-admin wildcard must not expose is_admin, got: {:?}",
        alice.prop("is_admin")
    );
    assert!(
        alice.prop("is_auditor").is_none(),
        "non-admin wildcard must not expose is_auditor, got: {:?}",
        alice.prop("is_auditor")
    );
}

pub(super) async fn admin_only_non_admin_explicit_columns_silently_stripped(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User",
                     "columns": ["username", "is_admin", "is_auditor"],
                     "node_ids": [1]},
            "limit": 10
        }"#,
        &allow_all(),
        non_admin_ctx(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[1]);
    let alice = resp.find_node("User", 1).unwrap();
    alice.assert_str("username", "alice");
    assert!(
        alice.prop("is_admin").is_none(),
        "non-admin explicit is_admin column must be silently stripped"
    );
    assert!(
        alice.prop("is_auditor").is_none(),
        "non-admin explicit is_auditor column must be silently stripped"
    );
}

pub(super) async fn admin_only_admin_filter_compiles(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "is_admin"],
                     "filters": {"is_admin": false}, "node_ids": [1]},
            "limit": 10
        }"#,
        &allow_all(),
        admin_ctx(),
    )
    .await;

    // Seed has no is_admin=true users, so filtering is_admin=false matches alice.
    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[1]);
    resp.assert_filter("User", "is_admin", |n| {
        n.prop_bool("is_admin") == Some(false)
    });
    let alice = resp.find_node("User", 1).unwrap();
    assert_eq!(
        alice.prop_bool("is_admin"),
        Some(false),
        "admin caller must see is_admin column value"
    );
}

pub(super) async fn admin_only_admin_order_by_compiles(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "is_admin"]},
            "order_by": {"node": "u", "property": "is_admin", "direction": "DESC"},
            "limit": 10
        }"#,
        &ontology,
        &admin_ctx(),
    )
    .expect("admin order_by on is_admin must compile");
}

pub(super) async fn admin_only_admin_aggregation_compiles(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{
                "function": "max",
                "target": "u",
                "property": "is_admin",
                "group_by": "g",
                "alias": "has_admin"
            }],
            "limit": 10
        }"#,
        &ontology,
        &admin_ctx(),
    )
    .expect("admin MAX(is_admin) grouped by Group must compile");
}

pub(super) async fn admin_only_admin_wildcard_columns_includes_admin_fields(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": "*", "node_ids": [1]},
            "limit": 10
        }"#,
        &allow_all(),
        admin_ctx(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[1]);
    let alice = resp.find_node("User", 1).unwrap();
    assert_eq!(
        alice.prop_bool("is_admin"),
        Some(false),
        "admin wildcard must expose is_admin column"
    );
    assert_eq!(
        alice.prop_bool("is_auditor"),
        Some(false),
        "admin wildcard must expose is_auditor column"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Field-level admin_only restriction on dynamic hydration
//
// Neighbors and PathFinding with `dynamic_columns: "*"` resolve entity
// columns from the ontology at compile time (not from `node.columns`),
// which historically bypassed `RestrictPass`. These tests ensure the
// hydration planner strips `admin_only` fields for non-admin callers on
// both discovered-neighbor nodes and center nodes.
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn admin_only_non_admin_neighbors_dynamic_wildcard_strips_admin_fields(
    ctx: &TestContext,
) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "incoming", "rel_types": ["MEMBER_OF"]},
            "options": {"dynamic_columns": "*"}
        }"#,
        &allow_all(),
        non_admin_ctx(),
    )
    .await;

    // Group 100 + Users 1, 2, 6 as incoming MEMBER_OF neighbors.
    resp.assert_node_count(4);
    resp.assert_node_ids("Group", &[100]);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (2, 100), (6, 100)]);

    for uid in [1, 2, 6] {
        let user = resp
            .find_node("User", uid)
            .unwrap_or_else(|| panic!("User {uid} should be hydrated"));
        assert!(
            user.prop("username").is_some(),
            "non-admin wildcard must expose username for User {uid}"
        );
        assert!(
            user.prop("is_admin").is_none(),
            "non-admin wildcard dynamic hydration must not expose is_admin on User {uid}, got: {:?}",
            user.prop("is_admin")
        );
        assert!(
            user.prop("is_auditor").is_none(),
            "non-admin wildcard dynamic hydration must not expose is_auditor on User {uid}, got: {:?}",
            user.prop("is_auditor")
        );
    }
}

pub(super) async fn admin_only_non_admin_neighbors_dynamic_center_node_strips_admin_fields(
    ctx: &TestContext,
) {
    // Center node (User 1) is a static node in the query. With
    // `dynamic_columns: "*"` and no explicit columns, the center node is
    // hydrated via the dynamic plan alongside neighbors. Its admin_only
    // fields must still be stripped for non-admins.
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["MEMBER_OF"]},
            "options": {"dynamic_columns": "*"}
        }"#,
        &allow_all(),
        non_admin_ctx(),
    )
    .await;

    // alice (User 1) + her two MEMBER_OF groups.
    resp.assert_node_count(3);
    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 102)]);

    let alice = resp.find_node("User", 1).expect("alice hydrated");
    assert!(
        alice.prop("is_admin").is_none(),
        "non-admin wildcard must not expose is_admin on center-node User, got: {:?}",
        alice.prop("is_admin")
    );
    assert!(
        alice.prop("is_auditor").is_none(),
        "non-admin wildcard must not expose is_auditor on center-node User, got: {:?}",
        alice.prop("is_auditor")
    );
}

pub(super) async fn admin_only_non_admin_path_finding_dynamic_wildcard_strips_admin_fields(
    ctx: &TestContext,
) {
    // PathFinding traverses User→Group→Project; dynamic hydration then
    // fetches properties for every node on the paths. Without the fix,
    // the User nodes in the path would expose is_admin / is_auditor.
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "options": {"dynamic_columns": "*"}
        }"#,
        &allow_all(),
        non_admin_ctx(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        1,
        "exactly one shortest path from User 1 to Project 1000"
    );
    for &pid in pids.iter() {
        assert_eq!(
            resp.path(pid).len(),
            2,
            "path {pid}: User→Group→Project = 2 edges"
        );
    }

    let alice = resp.find_node("User", 1).expect("alice on path");
    assert!(
        alice.prop("is_admin").is_none(),
        "non-admin path_finding wildcard must not expose is_admin, got: {:?}",
        alice.prop("is_admin")
    );
    assert!(
        alice.prop("is_auditor").is_none(),
        "non-admin path_finding wildcard must not expose is_auditor, got: {:?}",
        alice.prop("is_auditor")
    );
}

pub(super) async fn admin_only_admin_neighbors_dynamic_wildcard_includes_admin_fields(
    ctx: &TestContext,
) {
    // Mirror of the non-admin case: admin caller must still see the
    // admin_only values through dynamic hydration so the filter is
    // role-gated, not blanket removal.
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "incoming", "rel_types": ["MEMBER_OF"]},
            "options": {"dynamic_columns": "*"}
        }"#,
        &allow_all(),
        admin_ctx(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("Group", &[100]);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_edge_set("MEMBER_OF", &[(1, 100), (2, 100), (6, 100)]);

    let alice = resp.find_node("User", 1).expect("alice hydrated");
    assert_eq!(
        alice.prop_bool("is_admin"),
        Some(false),
        "admin wildcard must expose is_admin column in dynamic hydration"
    );
    assert_eq!(
        alice.prop_bool("is_auditor"),
        Some(false),
        "admin wildcard must expose is_auditor column in dynamic hydration"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-organization isolation
// ─────────────────────────────────────────────────────────────────────────────

/// Org 1 user searching for projects must not see org 2's project (id 9000).
pub(super) async fn cross_org_search_excludes_other_org(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name"]},
            "limit": 50
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_node_ids("Project", &[1000, 1001, 1002, 1003, 1004]);
    resp.assert_node_absent("Project", 9000);
}

/// Org 1 user traversing User->MR must not see org 2's MR (id 9100),
/// even though User 1 (alice) authored it in org 2.
pub(super) async fn cross_org_traversal_excludes_other_org(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 50
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;

    // Alice + her 2 org 1 MRs.
    resp.assert_node_count(3);
    resp.assert_node_ids("MergeRequest", &[2000, 2001]);
    resp.assert_edge_set("AUTHORED", &[(1, 2000), (1, 2001)]);
    resp.assert_node_absent("MergeRequest", 9100);
}

/// Org 1 aggregation counting groups must not include org 2's group (id 900).
pub(super) async fn cross_org_aggregation_excludes_other_org(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;

    // Org 1 groups should have counts; org 2 group must be absent.
    resp.assert_node("Group", 100, |n| {
        n.prop_i64("member_count").unwrap_or(0) > 0
    });
    resp.assert_node_absent("Group", 900);
}

/// Org 2 user can see their own data and nothing from org 1.
pub(super) async fn cross_org_inverse_isolation(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[900]);
    svc.allow("project", &[9000]);
    svc.allow("merge_request", &[9100]);

    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name"]},
            "limit": 50
        }"#,
        &svc,
        SecurityContext::new(2, vec!["2/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Project", &[9000]);
    resp.assert_node_absent("Project", 1000);
    resp.assert_node_absent("Project", 1001);
    resp.assert_node_absent("Project", 1002);
    resp.assert_node_absent("Project", 1003);
    resp.assert_node_absent("Project", 1004);
}
