//! Seed data: 7 users (IDs 1-7), active except user 5 (blocked).

use super::helpers::*;

fn next_cursor(resp: &ResponseView) -> Option<String> {
    resp.response
        .pagination
        .as_ref()
        .and_then(|p| p.next_cursor.clone())
}

fn has_more(resp: &ResponseView) -> bool {
    resp.response
        .pagination
        .as_ref()
        .expect("pagination is always present")
        .has_more
}

fn with_after(json: &str, after: &str) -> String {
    let mut v: serde_json::Value = serde_json::from_str(json).unwrap();
    v["cursor"]["after"] = serde_json::Value::String(after.to_string());
    v.to_string()
}

pub(super) async fn cursor_first_page(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "cursor": {"page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_node_order("User", &[1, 2]);
    assert!(has_more(&resp));
    assert!(next_cursor(&resp).is_some());
}

pub(super) async fn cursor_follows_next_cursor_to_exhaustion(ctx: &TestContext) {
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
        "order_by": {"node": "u", "property": "id", "direction": "ASC"},
        "cursor": {"page_size": 2}
    }"#;

    let mut all_ids: Vec<i64> = Vec::new();
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &allow_all()).await;
        let page_ids = resp.node_ids_ordered("User");
        for id in &page_ids {
            assert!(!all_ids.contains(id), "ID {id} appeared in multiple pages");
        }
        all_ids.extend(page_ids);
        resp.skip_requirement(Requirement::Cursor);
        resp.skip_requirement(Requirement::NodeCount);
        resp.skip_requirement(Requirement::OrderBy);
        match next_cursor(&resp) {
            Some(after) => {
                assert!(has_more(&resp));
                query = with_after(json, &after);
            }
            None => {
                assert!(
                    !has_more(&resp),
                    "exhausted stream must report has_more=false"
                );
                break;
            }
        }
    }

    assert_eq!(
        all_ids,
        vec![1, 2, 3, 4, 5, 6, 7],
        "pages should cover all users in order with no gaps or duplicates"
    );
}

pub(super) async fn cursor_with_filter(ctx: &TestContext) {
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"],
                 "filters": {"state": "active"}},
        "order_by": {"node": "u", "property": "id", "direction": "ASC"},
        "cursor": {"page_size": 2}
    }"#;

    let resp = run_query(ctx, json, &allow_all()).await;
    resp.assert_node_count(2);
    resp.assert_node_order("User", &[1, 2]);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
    let after = next_cursor(&resp).expect("more active users exist");

    let resp = run_query(ctx, &with_after(json, &after), &allow_all()).await;
    resp.assert_node_count(2);
    resp.assert_node_order("User", &[3, 4]);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("active"));
}

pub(super) async fn cursor_with_redaction(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3, 5]);
    svc.deny("user", &[2, 4, 6, 7]);

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
        "order_by": {"node": "u", "property": "id", "direction": "ASC"},
        "cursor": {"page_size": 2}
    }"#;

    let mut authorized: Vec<i64> = Vec::new();
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &svc).await;
        authorized.extend(resp.node_ids_ordered("User"));
        resp.skip_requirement(Requirement::Cursor);
        resp.skip_requirement(Requirement::NodeCount);
        resp.skip_requirement(Requirement::OrderBy);
        match next_cursor(&resp) {
            Some(after) => query = with_after(json, &after),
            None => break,
        }
    }

    assert_eq!(
        authorized,
        vec![1, 3, 5],
        "redaction shortens pages but pagination still reaches every authorized row"
    );
}

pub(super) async fn cursor_neighbors_pages_cover_all_edges(ctx: &TestContext) {
    let full = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "both"},
            "limit": 100
        }"#,
        &allow_all(),
    )
    .await;
    let mut full_edges: Vec<_> = full.edge_tuples().into_iter().collect();
    full_edges.sort();
    assert!(!full_edges.is_empty(), "group 100 should have neighbors");
    full.assert_node_count(full.node_count());
    full.skip_requirement(Requirement::Neighbors);
    full.skip_requirement(Requirement::NodeIds);

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "g", "entity": "Group", "node_ids": [100]},
        "neighbors": {"node": "g", "direction": "both"},
        "cursor": {"page_size": 2}
    }"#;

    let mut paged_edges = Vec::new();
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &allow_all()).await;
        paged_edges.extend(resp.edge_tuples());
        resp.assert_node_count(resp.node_count());
        resp.skip_requirement(Requirement::Neighbors);
        resp.skip_requirement(Requirement::NodeIds);
        match next_cursor(&resp) {
            Some(after) => query = with_after(json, &after),
            None => break,
        }
    }

    paged_edges.sort();
    paged_edges.dedup();
    assert_eq!(
        paged_edges, full_edges,
        "neighbor pages should cover the full neighbor set exactly"
    );
}

pub(super) async fn cursor_traversal(ctx: &TestContext) {
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

    assert_eq!(
        full.node_count(),
        9,
        "full traversal should return 9 edge-rows"
    );
    full.assert_node_count(9);
    full.assert_edge_count("MEMBER_OF", 9);
    let mut full_edges: Vec<_> = full.edge_tuples().into_iter().collect();
    full_edges.sort();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "cursor": {"page_size": 4}
    }"#;

    let mut paged_edges = Vec::new();
    let mut pages = 0;
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &allow_all()).await;
        pages += 1;
        paged_edges.extend(resp.edge_tuples());
        resp.assert_node_count(resp.node_count());
        resp.skip_requirement(Requirement::Relationship {
            edge_type: "MEMBER_OF".into(),
        });
        match next_cursor(&resp) {
            Some(after) => query = with_after(json, &after),
            None => break,
        }
    }

    assert_eq!(pages, 3, "9 edge-rows at page_size 4 should take 3 pages");
    paged_edges.sort();
    paged_edges.dedup();
    assert_eq!(
        paged_edges, full_edges,
        "paginated edges should equal the full traversal exactly"
    );
}

pub(super) async fn cursor_without_order_by_is_deterministic(ctx: &TestContext) {
    // No order_by: the cursor synthesizes PK tie-breakers, so pages are stable.
    let query = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
        "cursor": {"page_size": 3}
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
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
        "cursor": {"page_size": 2}
    }"#;

    let mut all_ids: Vec<i64> = Vec::new();
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &allow_all()).await;
        let page_ids = resp.node_ids_ordered("User");
        for id in &page_ids {
            assert!(!all_ids.contains(id), "ID {id} appeared in multiple pages");
        }
        all_ids.extend(page_ids);
        resp.skip_requirement(Requirement::Cursor);
        resp.skip_requirement(Requirement::NodeCount);
        match next_cursor(&resp) {
            Some(after) => query = with_after(json, &after),
            None => break,
        }
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
        "cursor": {"page_size": 4}
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
        "group_by": [{"kind": "node", "node": "u"}],
        "aggregations": [{"function": "count", "target": "mr", "alias": "mr_count"}],
        "cursor": {"page_size": 2}
    }"#;

    let resp1 = run_query(ctx, query, &allow_all()).await;
    let ids1 = resp1.group_node_ids_ordered("u", "User");
    resp1.assert_group_node_count("u", ids1.len());
    let first_id = ids1[0];
    let first_count = match first_id {
        1 => 2,
        2 | 3 => 1,
        other => panic!("unexpected first aggregation user id: {other}"),
    };
    resp1.assert_group_row_value_i64("u", "User", first_id, "mr_count", first_count);

    let resp2 = run_query(ctx, query, &allow_all()).await;
    let ids2 = resp2.group_node_ids_ordered("u", "User");
    resp2.assert_group_node_count("u", ids2.len());
    resp2.assert_group_row_value_i64("u", "User", first_id, "mr_count", first_count);

    assert_eq!(
        ids1, ids2,
        "aggregation cursor without sort should return deterministic results"
    );
}

pub(super) async fn cursor_aggregation_pages_cover_all_groups(ctx: &TestContext) {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["id", "username"]},
            {"id": "mr", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "group_by": [{"kind": "node", "node": "u"}],
        "aggregations": [{"function": "count", "target": "mr", "alias": "mr_count"}],
        "cursor": {"page_size": 2}
    }"#;

    let mut all_ids: Vec<i64> = Vec::new();
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &allow_all()).await;
        let page_ids = resp.group_node_ids_ordered("u", "User");
        for id in &page_ids {
            assert!(
                !all_ids.contains(id),
                "group {id} appeared in multiple pages"
            );
        }
        all_ids.extend(page_ids);
        resp.skip_requirement(Requirement::Cursor);
        resp.skip_requirement(Requirement::Aggregation);
        match next_cursor(&resp) {
            Some(after) => query = with_after(json, &after),
            None => break,
        }
    }

    all_ids.sort();
    assert_eq!(
        all_ids,
        vec![1, 2, 3],
        "aggregation pages should cover every group exactly once"
    );
}

pub(super) async fn cursor_path_finding_pages_cover_all_paths(ctx: &TestContext) {
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
    let full_destinations: Vec<i64> = {
        let mut dests: Vec<i64> = full_pids
            .iter()
            .filter_map(|&pid| full.path(pid).last().map(|e| e.to_id))
            .collect();
        dests.sort();
        dests
    };

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["MEMBER_OF", "CONTAINS"]},
        "cursor": {"page_size": 2}
    }"#;

    let mut all_destinations: Vec<i64> = Vec::new();
    let mut query = json.to_string();
    loop {
        let resp = run_query(ctx, &query, &allow_all()).await;
        for &pid in resp.path_ids().iter() {
            if let Some(last) = resp.path(pid).last() {
                all_destinations.push(last.to_id);
            }
        }
        resp.assert_node_count(resp.node_count());
        match next_cursor(&resp) {
            Some(after) => query = with_after(json, &after),
            None => break,
        }
    }
    all_destinations.sort();

    assert_eq!(
        all_destinations, full_destinations,
        "paginated results should cover all paths from the full query"
    );
}

pub(super) async fn cursor_path_finding_is_deterministic(ctx: &TestContext) {
    let query = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["MEMBER_OF", "CONTAINS"]},
        "cursor": {"page_size": 2}
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
