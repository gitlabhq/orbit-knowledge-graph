//! Cursor pagination correctness tests.
//!
//! Verifies that cursor-based pagination slices the authorized
//! (post-redaction) result set correctly across query types.
//!
//! Seed data: 6 users (IDs 1-6), 5 active (1-4, 6), 1 blocked (5).

use super::helpers::*;

// ─────────────────────────────────────────────────────────────────────────────
// Search pagination
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_first_page(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("User", &[1, 2]);
}

pub(super) async fn cursor_second_page(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 2, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("User", &[3, 4]);
}

pub(super) async fn cursor_last_page_partial(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 4, "page_size": 10}
        }"#,
        &allow_all(),
    )
    .await;

    // 6 users total, offset=4 → users 5, 6
    resp.assert_node_count(2);
    resp.assert_node_order("User", &[5, 6]);
}

pub(super) async fn cursor_offset_beyond_data(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 100,
            "cursor": {"offset": 50, "page_size": 10}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Pagination with filters
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_with_filter(ctx: &TestContext) {
    // 5 active users (1-4, 6), 1 blocked (5). Cursor pages through active only.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "filters": {"state": "active"}},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("User", &[1, 2]);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
}

pub(super) async fn cursor_with_filter_second_page(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "filters": {"state": "active"}},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 2, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    // 5 active users total, offset=2 → users 3, 4
    resp.assert_node_count(2);
    resp.assert_node_order("User", &[3, 4]);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Pagination with redaction
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_with_redaction(ctx: &TestContext) {
    // Allow users 1, 3, 5 — deny 2, 4, 6. 3 authorized users total.
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3, 5]);
    svc.deny("user", &[2, 4, 6]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 2}
        }"#,
        &svc,
    )
    .await;

    // 3 authorized (1, 3, 5), first page = 1, 3
    resp.assert_node_count(2);
    resp.assert_node_order("User", &[1, 3]);
}

pub(super) async fn cursor_with_redaction_second_page(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3, 5]);
    svc.deny("user", &[2, 4, 6]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 2, "page_size": 10}
        }"#,
        &svc,
    )
    .await;

    // 3 authorized (1, 3, 5), offset=2 → only user 5
    resp.assert_node_count(1);
    resp.assert_node_order("User", &[5]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Page coverage: no overlap, no gaps
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_pages_cover_all_data(ctx: &TestContext) {
    // Page through all 6 users in pages of 2, collecting IDs from each page.
    let mut all_ids: Vec<i64> = Vec::new();

    for offset in (0u32..).step_by(2) {
        let json = format!(
            r#"{{
                "query_type": "search",
                "node": {{"id": "u", "entity": "User", "columns": ["username"]}},
                "order_by": {{"node": "u", "property": "id", "direction": "ASC"}},
                "limit": 100,
                "cursor": {{"offset": {offset}, "page_size": 2}}
            }}"#
        );

        let resp = run_query(ctx, &json, &allow_all()).await;
        let count = resp.node_count();

        if count == 0 {
            resp.assert_node_count(0);
            break;
        }

        let page_ids = resp.node_ids_ordered("User");

        // No overlap with previously seen IDs
        for id in &page_ids {
            assert!(!all_ids.contains(id), "ID {id} appeared in multiple pages");
        }
        all_ids.extend(page_ids);
        resp.skip_requirement(Requirement::Cursor);
        resp.skip_requirement(Requirement::NodeCount);
        resp.skip_requirement(Requirement::OrderBy);
    }

    assert_eq!(
        all_ids,
        vec![1, 2, 3, 4, 5, 6],
        "pages should cover all users in order"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal pagination
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_traversal(ctx: &TestContext) {
    // 9 MEMBER_OF edges total, page_size=3 → first 3 rows
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 3}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.skip_requirement(Requirement::Relationship {
        edge_type: "MEMBER_OF".into(),
    });
}
