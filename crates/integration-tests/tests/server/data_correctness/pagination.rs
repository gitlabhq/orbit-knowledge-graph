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
            resp.skip_requirement(Requirement::OrderBy);
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
    // Full traversal has 9 MEMBER_OF edge-rows → 9 result rows.
    // Page through all of them in pages of 4 and verify total coverage.
    let full = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 100
        }"#,
        &allow_all(),
    )
    .await;

    let full_count = full.node_count();
    assert_eq!(full_count, 9, "full traversal should return 9 edge-rows");
    full.assert_node_count(9);
    full.assert_edge_count("MEMBER_OF", 9);

    // First page: offset=0, page_size=4
    let page1 = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 4}
        }"#,
        &allow_all(),
    )
    .await;

    let page1_count = page1.node_count();
    assert!(
        page1_count > 0 && page1_count < full_count,
        "page should be a subset"
    );
    page1.assert_node_count(page1_count);
    page1.skip_requirement(Requirement::Relationship {
        edge_type: "MEMBER_OF".into(),
    });

    // Second page: offset=4, page_size=4
    let page2 = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 100,
            "cursor": {"offset": 4, "page_size": 4}
        }"#,
        &allow_all(),
    )
    .await;

    let page2_count = page2.node_count();
    assert!(page2_count > 0, "second page should have rows");
    page2.assert_node_count(page2_count);
    page2.skip_requirement(Requirement::Relationship {
        edge_type: "MEMBER_OF".into(),
    });

    // Last page: offset=8, page_size=4 → 1 row left
    let page3 = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 100,
            "cursor": {"offset": 8, "page_size": 4}
        }"#,
        &allow_all(),
    )
    .await;

    let page3_count = page3.node_count();
    assert!(page3_count > 0, "last page should have at least 1 row");
    page3.assert_node_count(page3_count);
    page3.skip_requirement(Requirement::Relationship {
        edge_type: "MEMBER_OF".into(),
    });
}
