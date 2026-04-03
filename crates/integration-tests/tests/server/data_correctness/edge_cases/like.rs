//! LIKE operator data correctness: verify that LIKE filters with valid
//! patterns return the right rows from ClickHouse, and that metacharacter
//! escaping prevents wildcard expansion.

use super::super::helpers::*;

// ── contains ────────────────────────────────────────────────────────

pub(crate) async fn like_contains_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "lic"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.contains("lic"))
    });
}

pub(crate) async fn like_contains_matches_multiple(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "arl"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.contains("arl"))
    });
}

pub(crate) async fn like_contains_no_match_returns_empty(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "zzz"}}},
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

// ── starts_with ─────────────────────────────────────────────────────

pub(crate) async fn like_starts_with_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
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
}

pub(crate) async fn like_starts_with_no_match(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "starts_with", "value": "xyz"}}},
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

// ── ends_with ───────────────────────────────────────────────────────

pub(crate) async fn like_ends_with_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "ends_with", "value": "ice"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.ends_with("ice"))
    });
}

// ── metacharacter escaping ──────────────────────────────────────────

pub(crate) async fn like_percent_matched_literally(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "100%"}}},
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

pub(crate) async fn like_underscore_matched_literally(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "a_b"}}},
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

// ── equality on like_allowed:false fields ───────────────────────────

pub(crate) async fn like_equality_on_email_returns_correct_row(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "email"],
                     "filters": {"email": "alice@example.com"}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "email", |n| {
        n.prop_str("email") == Some("alice@example.com")
    });
}

pub(crate) async fn like_in_filter_on_email_works(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "email"],
                     "filters": {"email": {"op": "in", "value": ["alice@example.com", "bob@example.com"]}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_filter("User", "email", |n| {
        n.prop_str("email")
            .is_some_and(|e| e == "alice@example.com" || e == "bob@example.com")
    });
}
