use serde_json::{Map, Value, json};

use formatters::{
    ColumnDescriptor, GraphEdge, GraphNode, GraphResponse, GroupColumnDescriptor,
    PaginationResponse, toon_encode,
};

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn node(entity_type: &str, id: i64, properties: Value) -> GraphNode {
    GraphNode {
        entity_type: entity_type.into(),
        id,
        properties: object(properties),
    }
}

fn edge(from: (&str, i64), edge_type: &str, to: (&str, i64), step: Option<usize>) -> GraphEdge {
    GraphEdge {
        from: from.0.into(),
        from_id: from.1,
        to: to.0.into(),
        to_id: to.1,
        edge_type: edge_type.into(),
        depth: None,
        path_id: step.map(|_| 0),
        step,
    }
}

fn count_column(name: &str, target: &str) -> Option<Vec<ColumnDescriptor>> {
    Some(vec![ColumnDescriptor {
        name: name.into(),
        function: "count".into(),
        target: target.into(),
        property: None,
    }])
}

fn response(query_type: &str) -> GraphResponse {
    GraphResponse {
        format_version: String::new(),
        query_type: query_type.into(),
        nodes: vec![],
        edges: vec![],
        columns: None,
        group_columns: None,
        rows: None,
        pagination: None,
    }
}

#[test]
fn snapshot_search() {
    let mut r = response("traversal");
    r.nodes = vec![node(
        "MergeRequest",
        482821625,
        json!({
            "iid": 247,
            "state": "opened",
            "title": "Add per-activity reduction policy overrides",
            "created_at": "2026-05-08T14:47:05.123456Z",
        }),
    )];
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_traversal() {
    let mut r = response("traversal");
    r.nodes = vec![
        node(
            "User",
            5252563,
            json!({"username": "jordan_ng", "name": "Jordan NG"}),
        ),
        node(
            "MergeRequest",
            482927048,
            json!({"iid": 18, "state": "merged", "title": "chore: move skill to project scope"}),
        ),
        node(
            "Project",
            80212187,
            json!({"name": "webapp-scaffold", "full_path": "gitlab-com/cx-engineering/webapp-scaffold"}),
        ),
    ];
    r.edges = vec![
        edge(
            ("User", 5252563),
            "AUTHORED",
            ("MergeRequest", 482927048),
            None,
        ),
        edge(
            ("MergeRequest", 482927048),
            "IN_PROJECT",
            ("Project", 80212187),
            None,
        ),
    ];
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_aggregation_node_grouped() {
    let mut r = response("aggregation");
    r.columns = count_column("merged_count", "u");
    r.group_columns = Some(vec![GroupColumnDescriptor {
        name: "u".into(),
        kind: "node".into(),
        node: "u".into(),
        property: None,
        entity: Some("User".into()),
    }]);
    r.rows = Some(
        [
            (1243277, "ghost1", 65555),
            (35702613, "bot_a", 21277),
            (26832240, "bot_b", 20289),
        ]
        .into_iter()
        .map(|(id, username, count)| {
            object(json!({
                "u": {"type": "User", "id": id.to_string(), "properties": {"username": username}},
                "merged_count": count,
            }))
        })
        .collect(),
    );
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_aggregation_property_grouped() {
    let mut r = response("aggregation");
    r.columns = count_column("vulnerability_count", "v");
    r.group_columns = Some(vec![GroupColumnDescriptor {
        name: "severity".into(),
        kind: "property".into(),
        node: "v".into(),
        property: Some("severity".into()),
        entity: None,
    }]);
    r.rows = Some(
        [
            ("medium", 8421),
            ("high", 2350),
            ("low", 1542),
            ("critical", 120),
            ("info", 42),
        ]
        .into_iter()
        .map(|(severity, count)| {
            object(json!({"severity": severity, "vulnerability_count": count}))
        })
        .collect(),
    );
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_aggregation_ungrouped() {
    let mut r = response("aggregation");
    r.columns = count_column("total", "mr");
    r.group_columns = Some(vec![]);
    r.rows = Some(vec![object(json!({"total": 2347}))]);
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_path_finding() {
    let mut r = response("path_finding");
    r.nodes = vec![
        node("User", 64248, json!({"username": "stanhu"})),
        node(
            "MergeRequest",
            482927048,
            json!({"iid": 18, "state": "merged"}),
        ),
        node("Project", 278964, json!({"name": "GitLab"})),
    ];
    r.edges = vec![
        edge(
            ("User", 64248),
            "AUTHORED",
            ("MergeRequest", 482927048),
            Some(0),
        ),
        edge(
            ("MergeRequest", 482927048),
            "IN_PROJECT",
            ("Project", 278964),
            Some(1),
        ),
    ];
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_pagination() {
    let mut r = response("traversal");
    r.nodes = vec![node("MR", 1, json!({"iid": 42}))];
    r.pagination = Some(PaginationResponse {
        has_more: true,
        truncated: true,
        next_cursor: Some("eyJoIjoiYWQyMTczMWM5MTZm".into()),
    });
    insta::assert_snapshot!(toon_encode(&r));
}

#[test]
fn snapshot_long_description() {
    let description =
        ["Agents need the whole work item description to plan their changes."; 5].join(" ");
    let mut r = response("traversal");
    r.nodes = vec![node(
        "WorkItem",
        884,
        json!({"iid": 884, "title": "Stop truncating TOON values", "description": description}),
    )];
    let text = toon_encode(&r);
    assert!(text.contains(&description), "{text}");
    insta::assert_snapshot!(text);
}
