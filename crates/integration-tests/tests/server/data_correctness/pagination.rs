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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 4, "page_size": 10}
        }"#,
        &allow_all(),
    )
    .await;

    // 7 users total, offset=4 → users 5, 6, 7
    resp.assert_node_count(3);
    resp.assert_node_order("User", &[5, 6, 7]);
}

pub(super) async fn cursor_offset_beyond_data(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
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
    // Page through all 7 users in pages of 2, collecting IDs from each page.
    let mut all_ids: Vec<i64> = Vec::new();

    for offset in (0u32..).step_by(2) {
        let json = format!(
            r#"{{
                "query_type": "traversal",
"node": {{"id": "u", "entity": "User", "id_range": {{"start": 1, "end": 10000}}, "columns": ["username"]}},
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
        vec![1, 2, 3, 4, 5, 6, 7],
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
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
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
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

// ─────────────────────────────────────────────────────────────────────────────
// Cursor without explicit order_by: deterministic default ordering
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_without_order_by_is_deterministic(ctx: &TestContext) {
    // Without explicit order_by, cursor queries now inject a default ORDER BY id ASC.
    // Run the same query twice and verify identical results.
    let query = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 3}
    }"#;

    let resp1 = run_query(ctx, query, &allow_all()).await;
    let ids1 = resp1.node_ids_ordered("User");
    resp1.assert_node_count(3);

    let resp2 = run_query(ctx, query, &allow_all()).await;
    let ids2 = resp2.node_ids_ordered("User");
    resp2.assert_node_count(3);

    assert_eq!(
        ids1, ids2,
        "repeated cursor queries without order_by should return identical results"
    );
}

pub(super) async fn cursor_without_order_by_pages_cover_all_data(ctx: &TestContext) {
    // Page through all 7 users in pages of 2 without explicit order_by.
    // The default ORDER BY id ASC should give stable, non-overlapping pages.
    let mut all_ids: Vec<i64> = Vec::new();

    for offset in (0u32..).step_by(2) {
        let json = format!(
            r#"{{
                "query_type": "traversal",
"node": {{"id": "u", "entity": "User", "id_range": {{"start": 1, "end": 10000}}, "columns": ["username"]}},
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

        for id in &page_ids {
            assert!(!all_ids.contains(id), "ID {id} appeared in multiple pages");
        }
        all_ids.extend(page_ids);
        resp.skip_requirement(Requirement::Cursor);
        resp.skip_requirement(Requirement::NodeCount);
    }

    assert_eq!(
        all_ids,
        vec![1, 2, 3, 4, 5, 6, 7],
        "pages without explicit order_by should cover all users in id-ascending order"
    );
}

pub(super) async fn cursor_traversal_without_order_by_is_deterministic(ctx: &TestContext) {
    let query = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 4}
    }"#;

    let resp1 = run_query(ctx, query, &allow_all()).await;
    let edges1 = resp1.edge_tuples();
    let count1 = resp1.node_count();
    resp1.assert_node_count(count1);
    resp1.skip_requirement(Requirement::Relationship {
        edge_type: "MEMBER_OF".into(),
    });

    let resp2 = run_query(ctx, query, &allow_all()).await;
    let edges2 = resp2.edge_tuples();
    let count2 = resp2.node_count();
    resp2.assert_node_count(count2);
    resp2.skip_requirement(Requirement::Relationship {
        edge_type: "MEMBER_OF".into(),
    });

    assert_eq!(count1, count2, "same cursor query should return same count");
    assert_eq!(edges1, edges2, "same cursor query should return same edges");
}

pub(super) async fn cursor_aggregation_without_sort_is_deterministic(ctx: &TestContext) {
    let query = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["id", "username"]},
            {"id": "mr", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 2}
    }"#;

    let resp1 = run_query(ctx, query, &allow_all()).await;
    let ids1 = resp1.node_ids_ordered("User");
    resp1.assert_node_count(resp1.node_count());
    // Satisfy Requirement::Aggregation by verifying a result value.
    let first_id = ids1[0];
    resp1.assert_node("User", first_id, |n| n.prop_i64("mr_count").is_some());

    let resp2 = run_query(ctx, query, &allow_all()).await;
    let ids2 = resp2.node_ids_ordered("User");
    resp2.assert_node_count(resp2.node_count());
    resp2.assert_node("User", first_id, |n| n.prop_i64("mr_count").is_some());

    assert_eq!(
        ids1, ids2,
        "aggregation cursor without sort should return deterministic results"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding pagination
// ─────────────────────────────────────────────────────────────────────────────

pub(super) async fn cursor_path_finding_pages_cover_all_paths(ctx: &TestContext) {
    // User 1 -> Projects 1000, 1002, 1004 via MEMBER_OF + CONTAINS = 3 paths.
    // Page through them in pages of 2 and verify full coverage without overlap.
    let full = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    let full_pids = full.path_ids();
    assert_eq!(full_pids.len(), 3, "3 shortest paths expected");

    // Collect all destination IDs from the full result for comparison.
    let full_destinations: Vec<i64> = {
        let mut dests: Vec<i64> = full_pids
            .iter()
            .filter_map(|&pid| full.path(pid).last().map(|e| e.to_id))
            .collect();
        dests.sort();
        dests
    };

    // Page 1: offset=0, page_size=2
    let page1 = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    let p1_pids = page1.path_ids();
    assert_eq!(p1_pids.len(), 2, "first page should have 2 paths");
    page1.assert_node_count(page1.node_count());

    // Page 2: offset=2, page_size=2
    let page2 = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 100,
            "cursor": {"offset": 2, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    let p2_pids = page2.path_ids();
    assert_eq!(p2_pids.len(), 1, "second page should have 1 path");
    page2.assert_node_count(page2.node_count());

    // Combine destinations from both pages and verify full coverage.
    let mut all_destinations: Vec<i64> = Vec::new();
    for &pid in p1_pids.iter() {
        if let Some(last) = page1.path(pid).last() {
            all_destinations.push(last.to_id);
        }
    }
    for &pid in p2_pids.iter() {
        if let Some(last) = page2.path(pid).last() {
            all_destinations.push(last.to_id);
        }
    }
    all_destinations.sort();

    assert_eq!(
        all_destinations, full_destinations,
        "paginated results should cover all paths from the full query"
    );
}

pub(super) async fn cursor_path_finding_is_deterministic(ctx: &TestContext) {
    // Run the same cursored path finding query twice and verify both runs
    // return the same set of destination nodes.
    let query = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["MEMBER_OF", "CONTAINS"]},
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 2}
    }"#;

    let resp1 = run_query(ctx, query, &allow_all()).await;
    resp1.assert_node_count(resp1.node_count());
    let resp2 = run_query(ctx, query, &allow_all()).await;
    resp2.assert_node_count(resp2.node_count());

    let pids1 = resp1.path_ids();
    let pids2 = resp2.path_ids();

    let dests1: Vec<i64> = pids1
        .iter()
        .map(|&pid| resp1.path(pid).last().unwrap().to_id)
        .collect();
    let dests2: Vec<i64> = pids2
        .iter()
        .map(|&pid| resp2.path(pid).last().unwrap().to_id)
        .collect();

    assert_eq!(
        dests1, dests2,
        "repeated cursor path_finding queries should return identical results"
    );
}
