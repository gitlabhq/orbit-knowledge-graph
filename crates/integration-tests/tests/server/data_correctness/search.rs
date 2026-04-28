use super::helpers::*;

pub(super) async fn search_returns_correct_user_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "name", "state", "user_type"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_node_order("User", &[1, 2, 3, 4, 5, 6, 7]);

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
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "visibility_level"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);

    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project")
            && n.prop_str("visibility_level") == Some("public")
    });
    resp.assert_node("Project", 1003, |n| {
        n.prop_str("name") == Some("Secret Project")
            && n.prop_str("visibility_level") == Some("private")
    });
}

pub(super) async fn search_returns_correct_group_full_path(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "full_path"]},
            "order_by": {"node": "g", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_node_order("Group", &[100, 101, 102, 200, 300]);
    resp.assert_node("Group", 100, |n| {
        n.prop_str("full_path") == Some("public-group")
    });
    resp.assert_node("Group", 200, |n| {
        n.prop_str("full_path") == Some("public-group/deep-a")
    });
    resp.assert_node("Group", 300, |n| {
        n.prop_str("full_path") == Some("public-group/deep-a/deep-b")
    });
}

pub(super) async fn search_returns_correct_project_full_path(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "full_path"]},
            "order_by": {"node": "p", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_node_order("Project", &[1000, 1001, 1002, 1003, 1004, 1010]);
    resp.assert_node("Project", 1000, |n| {
        n.prop_str("full_path") == Some("public-group/public-project")
    });
    resp.assert_node("Project", 1004, |n| {
        n.prop_str("full_path") == Some("internal-group/shared-project")
    });
}

pub(super) async fn search_default_columns_include_full_path(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    let first = resp.find_node("Group", 100).unwrap();
    assert!(
        first.prop_str("full_path").is_some(),
        "full_path should be in default columns"
    );
}

pub(super) async fn search_filter_eq_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
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
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "visibility_level"],
                     "filters": {"visibility_level": {"op": "in", "value": ["public", "internal"]}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(4);
    resp.assert_node_ids("Project", &[1000, 1002, 1004, 1010]);

    resp.assert_filter("Project", "visibility_level", |n| {
        let vis = n.prop_str("visibility_level").unwrap_or("");
        vis == "public" || vis == "internal"
    });
}

pub(super) async fn search_filter_starts_with_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"],
                     "filters": {"username": {"op": "starts_with", "value": "ali"}}},
             "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.starts_with("ali"))
    });
    resp.find_node("User", 1)
        .unwrap()
        .assert_str("username", "alice");
}

pub(super) async fn search_node_ids_returns_only_specified(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "lic"}}},
             "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("User", &[1]);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.contains("lic"))
    });
}

pub(super) async fn search_filter_is_null_matches_unset_columns(ctx: &TestContext) {
    // avatar_url is Nullable(String) in ClickHouse, so IS NULL matches
    // rows where no avatar has been set (our seed data never sets it).
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "avatar_url"],
                     "filters": {"avatar_url": {"op": "is_null", "value": true}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_filter("User", "avatar_url", |n| {
        n.prop_str("username").is_some() && n.prop("avatar_url").is_none()
    });
}

pub(super) async fn search_with_order_by_desc(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_node_order("User", &[7, 6, 5, 4, 3, 2, 1]);
}

pub(super) async fn search_no_auth_returns_empty(ctx: &TestContext) {
    let svc = MockRedactionService::new();
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "name"],
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
            "query_type": "traversal",
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
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "id_range": {"start": 1, "end": 10000}, "columns": ["note", "confidential", "internal"],
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
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "id_range": {"start": 1, "end": 10000}, "columns": ["note", "created_at"],
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
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "id_range": {"start": 1, "end": 10000}, "columns": ["note", "created_at"],
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

pub(super) async fn search_limit_truncates_results(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"],
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
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

/// Regression: a DateTime64 filter literal must bind as the typed CH param,
/// otherwise CH rejects the implicit String cast with TYPE_MISMATCH (Code 53).
pub(super) async fn search_filter_gte_on_datetime_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "id_range": {"start": 1, "end": 10000},
                     "columns": ["title", "state", "merged_at"],
                     "filters": {
                         "state": {"op": "eq", "value": "merged"},
                         "merged_at": {"op": "gte", "value": "2024-06-01T00:00:00Z"}
                     }},
            "order_by": {"node": "mr", "property": "merged_at", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("MergeRequest", &[2005, 2004]);
    resp.assert_filter("MergeRequest", "state", |n| {
        n.prop_str("state") == Some("merged")
    });
    resp.assert_filter("MergeRequest", "merged_at", |n| {
        n.prop_str("merged_at").is_some_and(|s| s >= "2024-06-01")
    });
    resp.assert_node_absent("MergeRequest", 2002);
}

pub(super) async fn search_filter_lte_on_datetime_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "id_range": {"start": 1, "end": 10000},
                     "columns": ["title", "state", "merged_at"],
                     "filters": {
                         "state": {"op": "eq", "value": "merged"},
                         "merged_at": {"op": "lte", "value": "2024-07-01T00:00:00Z"}
                     }},
            "order_by": {"node": "mr", "property": "merged_at", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("MergeRequest", &[2004, 2002]);
    resp.assert_node_absent("MergeRequest", 2005);
    resp.assert_filter("MergeRequest", "state", |n| {
        n.prop_str("state") == Some("merged")
    });
    resp.assert_filter("MergeRequest", "merged_at", |n| {
        n.prop_str("merged_at").is_some_and(|s| s <= "2024-07-01")
    });
}

// 2004 is seeded at 2024-06-10 09:00:00, so strict `lt 2024-06-10` (parsed as
// midnight) excludes it: only 2002 (2024-03-15) qualifies.
pub(super) async fn search_filter_lt_on_datetime_excludes_same_day_after_midnight(
    ctx: &TestContext,
) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "id_range": {"start": 1, "end": 10000},
                     "columns": ["title", "state", "merged_at"],
                     "filters": {
                         "state": {"op": "eq", "value": "merged"},
                         "merged_at": {"op": "lt", "value": "2024-06-10T00:00:00Z"}
                     }},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_order("MergeRequest", &[2002]);
    resp.assert_node_absent("MergeRequest", 2004);
    resp.assert_node_absent("MergeRequest", 2005);
    resp.assert_filter("MergeRequest", "state", |n| {
        n.prop_str("state") == Some("merged")
    });
    resp.assert_filter("MergeRequest", "merged_at", |n| {
        n.prop_str("merged_at").is_some_and(|s| s < "2024-06-10")
    });
}

pub(super) async fn search_filter_is_not_null_on_datetime_returns_merged_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "id_range": {"start": 1, "end": 10000},
                     "columns": ["title", "state", "merged_at"],
                     "filters": {
                         "merged_at": {"op": "is_not_null"}
                     }},
            "order_by": {"node": "mr", "property": "merged_at", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_order("MergeRequest", &[2005, 2004, 2002]);
    resp.assert_filter("MergeRequest", "merged_at", |n| {
        n.prop_str("merged_at").is_some()
    });
}
