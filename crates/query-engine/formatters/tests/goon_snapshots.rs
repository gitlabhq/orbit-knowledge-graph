use std::sync::LazyLock;

use semver::Version;
use serde_json::{Map, Value, json};

use formatters::{ColumnDescriptor, GraphEdge, GraphNode, GraphResponse, PaginationResponse};

static FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| Version::new(1, 0, 0));

fn props(pairs: &[(&str, Value)]) -> Map<String, Value> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert((*k).into(), v.clone());
    }
    m
}

fn run(response: GraphResponse) -> String {
    formatters::goon_encode(&response, &FORMAT_VERSION)
}

fn empty_response(query_type: &str) -> GraphResponse {
    GraphResponse {
        format_version: "1.2.0".into(),
        query_type: query_type.into(),
        nodes: vec![],
        edges: vec![],
        columns: None,
        pagination: None,
    }
}

#[test]
fn snapshot_search() {
    let mut r = empty_response("traversal");
    r.nodes = vec![GraphNode {
        entity_type: "MergeRequest".into(),
        id: 482821625,
        properties: props(&[
            ("iid", json!(247)),
            ("state", json!("opened")),
            (
                "title",
                json!("Add per-activity reduction policy overrides"),
            ),
            ("created_at", json!("2026-05-08 14:47:05.123456")),
        ]),
    }];
    insta::assert_snapshot!(run(r));
}

#[test]
fn snapshot_traversal() {
    let mut r = empty_response("traversal");
    r.nodes = vec![
        GraphNode {
            entity_type: "User".into(),
            id: 5252563,
            properties: props(&[
                ("username", json!("jordan_ng")),
                ("name", json!("Jordan NG")),
            ]),
        },
        GraphNode {
            entity_type: "MergeRequest".into(),
            id: 482927048,
            properties: props(&[
                ("iid", json!(18)),
                ("state", json!("merged")),
                ("title", json!("chore: move skill to project scope")),
            ]),
        },
        GraphNode {
            entity_type: "Project".into(),
            id: 80212187,
            properties: props(&[
                ("name", json!("webapp-scaffold")),
                (
                    "full_path",
                    json!("gitlab-com/cx-engineering/webapp-scaffold"),
                ),
            ]),
        },
    ];
    r.edges = vec![
        GraphEdge {
            from: "User".into(),
            from_id: 5252563,
            to: "MergeRequest".into(),
            to_id: 482927048,
            edge_type: "AUTHORED".into(),
            depth: None,
            path_id: None,
            step: None,
        },
        GraphEdge {
            from: "MergeRequest".into(),
            from_id: 482927048,
            to: "Project".into(),
            to_id: 80212187,
            edge_type: "IN_PROJECT".into(),
            depth: None,
            path_id: None,
            step: None,
        },
    ];
    insta::assert_snapshot!(run(r));
}

#[test]
fn snapshot_aggregation_grouped() {
    let mut r = empty_response("aggregation");
    r.nodes = vec![
        GraphNode {
            entity_type: "User".into(),
            id: 1243277,
            properties: props(&[
                ("username", json!("ghost1")),
                ("merged_count", json!(65555)),
            ]),
        },
        GraphNode {
            entity_type: "User".into(),
            id: 35702613,
            properties: props(&[("username", json!("bot_a")), ("merged_count", json!(21277))]),
        },
        GraphNode {
            entity_type: "User".into(),
            id: 26832240,
            properties: props(&[("username", json!("bot_b")), ("merged_count", json!(20289))]),
        },
    ];
    r.columns = Some(vec![ColumnDescriptor {
        name: "merged_count".into(),
        function: "count".into(),
        target: None,
        property: None,
        value: None,
    }]);
    insta::assert_snapshot!(run(r));
}

#[test]
fn snapshot_aggregation_ungrouped() {
    let mut r = empty_response("aggregation");
    r.columns = Some(vec![ColumnDescriptor {
        name: "total".into(),
        function: "count".into(),
        target: None,
        property: None,
        value: Some(json!(2347)),
    }]);
    insta::assert_snapshot!(run(r));
}

#[test]
fn snapshot_path_finding() {
    let mut r = empty_response("path_finding");
    r.nodes = vec![
        GraphNode {
            entity_type: "User".into(),
            id: 64248,
            properties: props(&[("username", json!("stanhu"))]),
        },
        GraphNode {
            entity_type: "MergeRequest".into(),
            id: 482927048,
            properties: props(&[("iid", json!(18)), ("state", json!("merged"))]),
        },
        GraphNode {
            entity_type: "Project".into(),
            id: 278964,
            properties: props(&[("name", json!("GitLab"))]),
        },
    ];
    r.edges = vec![
        GraphEdge {
            from: "User".into(),
            from_id: 64248,
            to: "MergeRequest".into(),
            to_id: 482927048,
            edge_type: "AUTHORED".into(),
            depth: None,
            path_id: Some(0),
            step: Some(0),
        },
        GraphEdge {
            from: "MergeRequest".into(),
            from_id: 482927048,
            to: "Project".into(),
            to_id: 278964,
            edge_type: "IN_PROJECT".into(),
            depth: None,
            path_id: Some(0),
            step: Some(1),
        },
    ];
    insta::assert_snapshot!(run(r));
}

#[test]
fn snapshot_pagination() {
    let mut r = empty_response("traversal");
    r.nodes = vec![GraphNode {
        entity_type: "MR".into(),
        id: 1,
        properties: props(&[("iid", json!(42))]),
    }];
    r.pagination = Some(PaginationResponse {
        has_more: true,
        total_rows: 100,
    });
    insta::assert_snapshot!(run(r));
}
