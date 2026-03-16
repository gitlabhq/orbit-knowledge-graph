use super::helpers::*;

pub(super) async fn search_returns_correct_user_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "name", "state", "user_type"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_node_order("User", &[1, 2, 3, 4, 5, 6]);

    let alice = resp.find_node("User", 1).unwrap();
    alice.assert_str("username", "alice");
    alice.assert_str("name", "Alice Admin");
    alice.assert_str("state", "active");
    alice.assert_str("user_type", "human");

    let bob = resp.find_node("User", 2).unwrap();
    bob.assert_str("username", "bob");
    bob.assert_str("name", "Bob Builder");

    let eve = resp.find_node("User", 5).unwrap();
    eve.assert_str("username", "eve");
    eve.assert_str("state", "blocked");
    eve.assert_str("user_type", "service_account");
}

pub(super) async fn search_returns_correct_project_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);

    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project")
            && n.prop_str("visibility_level") == Some("public")
    });
    resp.assert_node("Project", 1003, |n| {
        n.prop_str("name") == Some("Secret Project")
            && n.prop_str("visibility_level") == Some("private")
    });
}

pub(super) async fn search_filter_eq_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "filters": {"state": "blocked"}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("blocked"));
    let eve = resp.find_node("User", 5).unwrap();
    eve.assert_str("username", "eve");
}

pub(super) async fn search_filter_in_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"],
                     "filters": {"visibility_level": {"op": "in", "value": ["public", "internal"]}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("Project", &[1000, 1002, 1004]);

    resp.assert_filter("Project", "visibility_level", |n| {
        let vis = n.prop_str("visibility_level").unwrap_or("");
        vis == "public" || vis == "internal"
    });
}

pub(super) async fn search_filter_starts_with_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "starts_with", "value": "a"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.starts_with("a"))
    });
    resp.find_node("User", 1)
        .unwrap()
        .assert_str("username", "alice");
}

pub(super) async fn search_node_ids_returns_only_specified(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "g", "entity": "Group", "columns": ["name"], "node_ids": [100, 102]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.find_node("Group", 100)
        .unwrap()
        .assert_str("name", "Public Group");
    resp.find_node("Group", 102)
        .unwrap()
        .assert_str("name", "Internal Group");
    resp.assert_node_absent("Group", 101);
}

pub(super) async fn search_filter_contains_returns_substring_matches(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "li"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("User", &[1, 3]);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.contains("li"))
    });
}

pub(super) async fn search_filter_is_null_matches_unset_columns(ctx: &TestContext) {
    // avatar_url is Nullable(String) in ClickHouse, so IS NULL matches
    // rows where no avatar has been set (our seed data never sets it).
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "avatar_url"],
                     "filters": {"avatar_url": {"op": "is_null", "value": true}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_filter("User", "avatar_url", |n| {
        n.prop_str("username").is_some() && n.prop("avatar_url").is_none()
    });
}

pub(super) async fn search_with_order_by_desc(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_node_order("User", &[6, 5, 4, 3, 2, 1]);
}

pub(super) async fn search_no_auth_returns_empty(ctx: &TestContext) {
    let svc = MockRedactionService::new();
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_count(0);
}

pub(super) async fn search_redaction_returns_only_allowed_ids(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("User", &[1, 3]);
    resp.assert_node_absent("User", 2);
    resp.assert_node_absent("User", 5);
}

pub(super) async fn search_unicode_properties_survive_pipeline(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "name"],
                     "node_ids": [6]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[6]);
    resp.assert_node("User", 6, |n| {
        n.prop_str("username") == Some("用户_émoji_🎉")
            && n.prop_str("name") == Some("Ünïcödé Üser")
    });
}

pub(super) async fn search_wildcard_columns_returns_all_ontology_fields(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": "*", "node_ids": [1]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[1]);

    let alice = resp.find_node("User", 1).unwrap();
    alice.assert_str("username", "alice");
    alice.assert_str("name", "Alice Admin");
    alice.assert_str("state", "active");
    alice.assert_str("user_type", "human");

    // Wildcard must not leak internal columns (traversal_path, _version, _deleted).
    assert!(
        alice.prop("traversal_path").is_none(),
        "traversal_path is internal and should not be exposed"
    );
    assert!(
        alice.prop("_version").is_none(),
        "_version is internal and should not be exposed"
    );
    assert!(
        alice.prop("_deleted").is_none(),
        "_deleted is internal and should not be exposed"
    );
}

pub(super) async fn search_boolean_columns_have_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note", "confidential", "internal"],
                     "node_ids": [3000, 3001]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("Note", &[3000, 3001]);

    resp.assert_node("Note", 3000, |n| {
        n.prop_bool("confidential") == Some(false) && n.prop_bool("internal") == Some(false)
    });
    resp.assert_node("Note", 3001, |n| {
        n.prop_bool("confidential") == Some(true) && n.prop_bool("internal") == Some(false)
    });
}

pub(super) async fn search_datetime_columns_serialize_as_strings(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note", "created_at"],
                     "node_ids": [3000, 3001]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_ids("Note", &[3000, 3001]);

    resp.assert_node("Note", 3000, |n| {
        n.prop_str("created_at")
            .is_some_and(|s| s.contains("2024") && s.contains("01") && s.contains("15"))
    });
    resp.assert_node("Note", 3001, |n| {
        n.prop_str("created_at")
            .is_some_and(|s| s.contains("2024") && s.contains("02") && s.contains("20"))
    });
}

pub(super) async fn search_nullable_datetime_returns_null_when_unset(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note", "created_at"],
                     "node_ids": [3002]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Note", &[3002]);
    resp.assert_node("Note", 3002, |n| {
        n.prop_str("note").is_some_and(|s| s.len() == 10_000) && n.prop("created_at").is_none()
    });
}

pub(super) async fn search_range_returns_paginated_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "range": {"start": 1, "end": 3}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("User", &[2, 3]);
}

pub(super) async fn search_limit_truncates_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 3
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_order("User", &[1, 2, 3]);
}

pub(super) async fn search_filter_no_match_returns_empty(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": "nonexistent_user"}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::Filter {
        field: "username".into(),
    });
    resp.assert_node_count(0);
}

pub(super) async fn search_combined_filter_node_ids_order_by(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "node_ids": [1, 2, 3, 5],
                     "filters": {"state": "active"}},
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_order("User", &[3, 2, 1]);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
    resp.assert_node_absent("User", 5);
}
