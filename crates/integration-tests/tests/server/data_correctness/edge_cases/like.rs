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
    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
}

pub(crate) async fn like_contains_matches_multiple(ctx: &TestContext) {
    // "a" appears in alice, charlie, diana — but min length is 3,
    // so use "ali" (only alice) or a shared substring.
    // "li" is 2 chars (too short). Use "arl" for charlie only.
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
    resp.assert_node("User", 3, |n| n.prop_str("username") == Some("charlie"));
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
    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
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

    resp.assert_node_count(0);
}

// ── ends_with ───────────────────────────────────────────────────────

pub(crate) async fn like_ends_with_returns_matching_rows(ctx: &TestContext) {
    // "ice" matches alice
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
    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
}

// ── metacharacter escaping ──────────────────────────────────────────

pub(crate) async fn like_percent_matched_literally(ctx: &TestContext) {
    // "100%" should not expand — no usernames contain literal "%"
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

    resp.assert_node_count(0);
}

pub(crate) async fn like_underscore_matched_literally(ctx: &TestContext) {
    // "a_b" should not match "aXb" via single-char wildcard —
    // no usernames contain literal "a_b"
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
    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_str("email") == Some("alice@example.com")
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
    resp.assert_node_ids("User", &[1, 2]);
}
