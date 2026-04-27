use super::helpers::*;
use query_engine::compiler::TraversalPath;

// ─────────────────────────────────────────────────────────────────────────────
// Search: traversal path scoping
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn search_scoped_path_excludes_other_namespaces(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("Project", &[1000, 1002, 1010]);
    resp.assert_node_absent("Project", 1001);
    resp.assert_node_absent("Project", 1003);
    resp.assert_node_absent("Project", 1004);
}

pub(super) async fn search_scoped_to_single_project_namespace(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
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
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into(), "1/102/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("Project", &[1000, 1002, 1004, 1010]);
    resp.assert_node_absent("Project", 1001);
    resp.assert_node_absent("Project", 1003);
}

pub(super) async fn search_scoped_mr_excludes_other_namespaces(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}, "columns": ["title"]},
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
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "visibility_level"],
                     "filters": {"visibility_level": "public"}},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .await;

    // Public projects in 1/100/: Project 1000 directly and Project 1010
    // (under Group 200, path 1/100/200/1010/). Shared Project (1004) is
    // public but in 1/102/, excluded by scope.
    resp.assert_node_count(2);
    resp.assert_node_ids("Project", &[1000, 1010]);
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"],
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}},
                {"id": "u", "entity": "User", "columns": ["username"]}
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
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{
                "function": "count",
                "target": "u",
                "property": "is_auditor",
                "group_by": "g",
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
            "query_type": "traversal",
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "id_range": {"start": 1, "end": 10000},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "is_admin"],
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "is_admin"]},
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
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
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
            "limit": 50
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/".into()]).unwrap(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_node_ids("Project", &[1000, 1001, 1002, 1003, 1004, 1010]);
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
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
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
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
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

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation: compiled SQL security assertions
// ─────────────────────────────────────────────────────────────────────────────

/// Aggregation compiled SQL must contain startsWith(traversal_path, ?) for
/// every gl_* table alias. This asserts the SecurityPass output directly
/// rather than relying on CheckPass alone.
pub(super) async fn aggregation_sql_contains_traversal_path_filter(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();

    let compiled = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &ontology,
        &security_ctx,
    )
    .unwrap();

    let sql = &compiled.base.sql;
    assert!(
        sql.contains("startsWith"),
        "aggregation SQL must contain startsWith filter, got:\n{sql}"
    );
    assert!(
        sql.contains("traversal_path"),
        "aggregation SQL must filter on traversal_path, got:\n{sql}"
    );
    // The bound parameter for the path prefix must appear.
    let param_strs: Vec<_> = compiled
        .base
        .params
        .iter()
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect();
    assert!(
        sql.contains("1/100/") || param_strs.iter().any(|s| s.contains("1/100/")),
        "compiled SQL or params must contain '1/100/', got SQL:\n{sql}\nparams: {param_strs:?}"
    );
}

/// Multi-path SecurityContext with aggregation: compiled SQL must contain
/// startsWith predicates for both paths (via LCP + OR).
pub(super) async fn aggregation_multi_path_sql_contains_both_filters(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let security_ctx = SecurityContext::new(1, vec!["1/100/".into(), "1/102/".into()]).unwrap();

    let compiled = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &ontology,
        &security_ctx,
    )
    .unwrap();

    let sql = &compiled.base.sql;
    assert!(
        sql.contains("startsWith"),
        "multi-path aggregation SQL must contain startsWith, got:\n{sql}"
    );

    // Both path prefixes must appear in SQL or params.
    let all_text = format!("{sql} {:?}", compiled.base.params);
    assert!(
        all_text.contains("1/100/"),
        "compiled output must contain '1/100/', got:\n{all_text}"
    );
    assert!(
        all_text.contains("1/102/"),
        "compiled output must contain '1/102/', got:\n{all_text}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-entity role scoping: aggregation target nodes
//
// Vulnerability declares `required_role: security_manager` in its ontology,
// and the seed places 3 vulnerabilities -- one per project across three
// namespaces. These tests exercise the aggregation-query oracle pattern:
// a user with Reporter-only access on a path should not be able to
// observe Vulnerability rows by grouping on Project and counting on
// Vulnerability. The compiler drops Reporter paths from the Vulnerability
// startsWith predicate, so the count comes back as zero (or excludes the
// project entirely) no matter what filters the attacker attaches.
// ─────────────────────────────────────────────────────────────────────────────

fn reporter_path(path: &str) -> TraversalPath {
    TraversalPath::new(path, 20)
}

fn security_manager_path(path: &str) -> TraversalPath {
    TraversalPath::new(path, 25)
}

fn developer_path(path: &str) -> TraversalPath {
    TraversalPath::new(path, 30)
}

/// Reporter on 1/100/ but no Security Manager access anywhere. Counting
/// vulnerabilities per project must return no rows for Project 1000 — the
/// Vulnerability scan is filtered to zero traversal paths and produces
/// `Bool(false)`, so the aggregation sees an empty Vulnerability set.
pub(super) async fn aggregation_vulnerability_reporter_only_sees_zero_counts(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "v", "entity": "Vulnerability"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new_with_roles(1, vec![reporter_path("1/100/")]).unwrap(),
    )
    .await;

    // Reporter on 1/100/ cannot see any vulnerability — even though Project
    // 1000 lives in 1/100/1000/ (which is within the Reporter scope), the
    // Vulnerability scan is dropped to zero eligible paths because Reporter
    // is below Vulnerability's Security Manager requirement. INNER JOIN against an
    // empty Vulnerability set produces no rows at all.
    resp.assert_empty_aggregation();
    resp.assert_node_absent("Vulnerability", 8000);
    resp.assert_node_absent("Vulnerability", 8001);
    resp.assert_node_absent("Vulnerability", 8002);
}

/// Reporter on 1/100/, Developer on 1/101/. The compiler keeps only
/// 1/101/ in the Vulnerability predicate, so the aggregation surfaces
/// Project 1001 (vuln 8001) but not Project 1000 (vuln 8000).
pub(super) async fn aggregation_vulnerability_mixed_roles_only_surfaces_developer_paths(
    ctx: &TestContext,
) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "v", "entity": "Vulnerability"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new_with_roles(
            1,
            vec![reporter_path("1/100/"), developer_path("1/101/")],
        )
        .unwrap(),
    )
    .await;

    // Project 1001 (Developer path, which clears the Security Manager floor)
    // surfaces with count=1.
    resp.assert_node("Project", 1001, |n| n.prop_i64("vuln_count") == Some(1));
    // Project 1000 (Reporter-only path) must not appear — the Vulnerability
    // scan was filtered to paths where the user clears Security Manager, leaving
    // Project 1000 without any matching vuln row in the INNER JOIN.
    resp.assert_node_absent("Project", 1000);
    // Project 1004 is outside the user's scope entirely.
    resp.assert_node_absent("Project", 1004);
}

/// Security Manager (25) on a path hits the exact floor required by
/// Vulnerability's `required_role`. This guards the exact floor: an SM-only
/// user sees their own vuln counts without needing to escalate to Developer.
pub(super) async fn aggregation_vulnerability_security_manager_meets_the_required_floor(
    ctx: &TestContext,
) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "v", "entity": "Vulnerability"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new_with_roles(1, vec![security_manager_path("1/101/")]).unwrap(),
    )
    .await;

    // Project 1001 lives under 1/101/ and has vuln 8001. Security Manager
    // is the declared floor (25), so the Vulnerability alias keeps this
    // path in its startsWith predicate.
    resp.assert_node("Project", 1001, |n| n.prop_i64("vuln_count") == Some(1));
    // Projects outside 1/101/ are not in the user's scope.
    resp.assert_node_absent("Project", 1000);
    resp.assert_node_absent("Project", 1004);
}

/// Developer on all paths: classic aggregation baseline showing the
/// role-scoping change doesn't over-restrict legitimate access.
pub(super) async fn aggregation_vulnerability_developer_everywhere_sees_all_counts(
    ctx: &TestContext,
) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "v", "entity": "Vulnerability"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new_with_roles(
            1,
            vec![
                developer_path("1/100/"),
                developer_path("1/101/"),
                developer_path("1/102/"),
            ],
        )
        .unwrap(),
    )
    .await;

    // All three vuln-bearing projects appear, each with count=1.
    resp.assert_node("Project", 1000, |n| n.prop_i64("vuln_count") == Some(1));
    resp.assert_node("Project", 1001, |n| n.prop_i64("vuln_count") == Some(1));
    resp.assert_node("Project", 1004, |n| n.prop_i64("vuln_count") == Some(1));
}

/// Search on Vulnerability directly as a Reporter: the base-case read
/// path (not aggregation) must also drop rows when the required role is
/// not met, otherwise an attacker can bypass the fix by querying via
/// search instead of aggregation.
pub(super) async fn search_vulnerability_reporter_only_returns_empty(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "v", "entity": "Vulnerability", "id_range": {"start": 1, "end": 100000}, "columns": ["title", "severity"]},
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new_with_roles(1, vec![reporter_path("1/100/"), reporter_path("1/101/")])
            .unwrap(),
    )
    .await;

    resp.assert_node_count(0);
}

/// The filter-based oracle (severity filter + count on a single
/// project) must not leak: even a count-with-filter targeting a
/// specific vuln is neutralized because the Vulnerability scan is
/// filtered to zero paths before any WHERE clause evaluates.
pub(super) async fn aggregation_vulnerability_filter_oracle_is_neutralized(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "columns": ["name"], "node_ids": [1000]},
                {"id": "v", "entity": "Vulnerability", "filters": {"severity": "critical"}}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "c"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new_with_roles(1, vec![reporter_path("1/100/")]).unwrap(),
    )
    .await;

    // Without the fix, this would have returned count=1 (vuln 8000 is
    // critical in Project 1000) and the attacker could binary-search
    // severity values. With role scoping, Project 1000 drops out of the
    // result set entirely.
    resp.assert_empty_aggregation();
    resp.assert_node_absent("Project", 1000);
}

/// Compiled SQL must bind `Bool(false)` (no paths) for the Vulnerability
/// alias when the user has only Reporter access. Asserting the compiled
/// output directly guards against future passes that might re-introduce
/// the path list.
pub(super) async fn aggregation_vulnerability_sql_drops_reporter_paths(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let security_ctx = SecurityContext::new_with_roles(1, vec![reporter_path("1/100/")]).unwrap();

    let compiled = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "v", "entity": "Vulnerability"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}],
            "limit": 10
        }"#,
        &ontology,
        &security_ctx,
    )
    .unwrap();

    let sql = &compiled.base.sql;
    // Project alias still gets the `1/100/` prefix because Project's
    // required_role defaults to Reporter.
    let all_text = format!("{sql} {:?}", compiled.base.params);
    assert!(
        all_text.contains("1/100/"),
        "Project predicate must still reference 1/100/, got:\n{all_text}"
    );
    // At least one parameter bound to the literal boolean `false` — that's
    // what `build_path_filter(&[])` produces when the Vulnerability alias
    // has zero eligible paths.
    let has_false_bool = compiled.base.params.iter().any(|(_, p)| {
        matches!(
            (&p.ch_type, &p.value),
            (
                gkg_utils::clickhouse::ChType::Bool,
                serde_json::Value::Bool(false)
            )
        )
    });
    assert!(
        has_false_bool,
        "Vulnerability alias must compile to Bool(false) when no Security Manager paths exist, params: {:?}",
        compiled.base.params
    );
}

/// Multi-path aggregation returns correct scoped counts from both namespaces
/// and excludes groups outside the scope.
pub(super) async fn aggregation_multi_path_returns_union_of_scopes(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into(), "1/102/".into()]).unwrap(),
    )
    .await;

    // Group 100: members 1, 2, 6 (edges in 1/100/) = 3
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
    // Group 102: members 1, 4 (edges in 1/102/) = 2
    resp.assert_node("Group", 102, |n| n.prop_i64("member_count") == Some(2));
    // Group 101 is outside both paths.
    resp.assert_node_absent("Group", 101);
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation: globally-scoped entity guard (work_items/347)
//
// The User entity has no traversal_path column and is listed in
// skip_security_filter_for_entities, so direct aggregation on User alone
// would bypass both the traversal_path filter and the post-query Rails
// redaction layer (aggregation results are row-less). Reject at compile
// time unless the query contains at least one traversal_path-scoped node.
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn aggregation_user_only_rejects_at_compile(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]}],
            "aggregations": [{"function": "count", "target": "u", "alias": "cnt"}],
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("aggregation on User alone must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("traversal_path") && msg.contains("aggregation"),
        "error should reference traversal_path scoping and aggregation, got: {msg}"
    );
}

pub(super) async fn aggregation_user_only_with_pii_filter_rejects_at_compile(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{
                "id": "u", "entity": "User", "columns": ["username"],
                "filters": {"email": "target@example.com"}
            }],
            "aggregations": [{"function": "count", "target": "u", "alias": "hit"}],
            "limit": 1
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("User-only aggregation with email filter must reject");
    assert!(err.to_string().contains("traversal_path"));
}

pub(super) async fn aggregation_user_joined_to_scoped_group_compiles(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    )
    .expect("aggregation joined to Group (scoped) must compile for non-admin");
}

pub(super) async fn aggregation_user_only_admin_still_compiles(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]}],
            "aggregations": [{"function": "count", "target": "u", "alias": "cnt"}],
            "limit": 10
        }"#,
        &ontology,
        &admin_ctx(),
    )
    .expect("admin caller bypasses User-only aggregation guard");
}

pub(super) async fn aggregation_user_only_rejection_happens_before_sql_compile(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{
                "id": "u", "entity": "User",
                "filters": {"email": "victim@example.com"}
            }],
            "aggregations": [{"function": "count", "target": "u", "alias": "oracle"}],
            "limit": 1
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err = result.expect_err("must reject before producing SQL that leaks the email filter");
    let msg = err.to_string();
    assert!(
        !msg.contains("v2_gl_user") && !msg.contains("victim@example.com"),
        "rejection message must not echo the backing table or filter value, got: {msg}"
    );
}

pub(super) async fn aggregation_user_only_neighbors_query_is_not_blocked(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    compile(
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["MEMBER_OF"]},
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    )
    .expect("non-aggregation query on User must not be blocked by the aggregation guard");
}

pub(super) async fn aggregation_user_disconnected_scoped_node_rejects_at_compile(
    ctx: &TestContext,
) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    // User and Group are both declared, but no relationship connects them.
    // The declaration-based guard would accept this; the reachability guard
    // must reject because the User scan would be unbounded by any edge join.
    let result = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "filters": {"email": "target@example.com"}},
                {"id": "g", "entity": "Group"}
            ],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "hit"}],
            "limit": 1
        }"#,
        &ontology,
        &non_admin_ctx(),
    );

    let err =
        result.expect_err("aggregation with a declared-but-disconnected scoped node must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("globally-scoped") && msg.contains("relationships"),
        "error should reference the reachability requirement, got: {msg}"
    );
    assert!(
        !msg.contains("target@example.com"),
        "rejection must not echo the filter value: {msg}"
    );
}

pub(super) async fn aggregation_user_reachable_via_path_compiles(ctx: &TestContext) {
    let _ = ctx;
    let ontology = Arc::new(load_ontology());
    // Reachability is satisfied through the `path` config (path_finding-style
    // endpoints), not only through `relationships`.
    compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
                {"id": "p", "entity": "Project"}
            ],
            "path": {"type": "shortest", "from": "u", "to": "p", "max_depth": 3},
            "aggregations": [{"function": "count", "target": "u", "group_by": "p", "alias": "hit"}],
            "limit": 10
        }"#,
        &ontology,
        &non_admin_ctx(),
    )
    .expect("User reachable via path to a scoped Project must compile");
}

pub(super) async fn aggregation_user_joined_runtime_returns_expected_counts(ctx: &TestContext) {
    let resp = run_query_with_security(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &allow_all(),
        SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .await;

    // Group 100: 3 members under the allowlisted path.
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
    // Groups outside 1/100/ must not surface.
    resp.assert_node_absent("Group", 101);
    resp.assert_node_absent("Group", 102);
}
