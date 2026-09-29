use semver::Version;
use serde_json::{Map, Value, json};

use formatters::{
    ColumnDescriptor, GraphEdge, GraphNode, GraphResponse, GroupColumnDescriptor,
    PaginationResponse,
};

fn run(response: &GraphResponse) -> String {
    formatters::gql_encode(response, &Version::new(1, 0, 0))
}

fn response(query_type: &str, nodes: Vec<GraphNode>, edges: Vec<GraphEdge>) -> GraphResponse {
    GraphResponse {
        format_version: "1.2.0".into(),
        query_type: query_type.into(),
        nodes,
        edges,
        columns: None,
        group_columns: None,
        rows: None,
        pagination: None,
    }
}

fn props(pairs: Value) -> Map<String, Value> {
    pairs.as_object().cloned().unwrap_or_default()
}

fn node(entity_type: &str, id: i64, properties: Value) -> GraphNode {
    GraphNode {
        entity_type: entity_type.into(),
        id,
        properties: props(properties),
    }
}

fn edge(edge_type: &str, from: (&str, i64), to: (&str, i64)) -> GraphEdge {
    GraphEdge {
        from: from.0.into(),
        from_id: from.1,
        to: to.0.into(),
        to_id: to.1,
        edge_type: edge_type.into(),
        depth: None,
        path_id: None,
        step: None,
    }
}

fn traversal() -> GraphResponse {
    let mut nested = edge("MEMBER_OF", ("User", 1), ("Group", 200));
    nested.depth = Some(2);
    response(
        "traversal",
        vec![
            node("User", 1, json!({"username": "alice", "name": "Alice"})),
            node("MergeRequest", 10, json!({"iid": 5, "state": "merged"})),
            node("MergeRequest", 11, json!({"iid": 6, "state": "opened"})),
            node("Group", 200, json!({"full_path": "gitlab-org/orbit"})),
            node("Project", 7, json!({"full_path": "gitlab-org/gitlab"})),
        ],
        vec![
            edge("AUTHORED", ("User", 1), ("MergeRequest", 11)),
            edge("AUTHORED", ("User", 1), ("MergeRequest", 10)),
            edge("AUTHORED", ("User", 1), ("MergeRequest", 10)),
            nested,
        ],
    )
}

#[test]
fn snapshot_traversal() {
    insta::assert_snapshot!(run(&traversal()));
}

#[test]
fn output_is_independent_of_input_order() {
    let mut reversed = traversal();
    reversed.nodes.reverse();
    reversed.edges.reverse();
    assert_eq!(run(&traversal()), run(&reversed));
}

#[test]
fn snapshot_search_literals_and_pagination() {
    let mut r = response(
        "search",
        vec![node(
            "Issue",
            1,
            json!({
                "confidential": false,
                "weight": 3,
                "score": 2.0,
                "state": "true",
                "title": "say \"hi\"\nnow",
                "description": "x".repeat(250),
                "milestone": null,
                "labels": "",
            }),
        )],
        vec![],
    );
    r.pagination = Some(PaginationResponse {
        has_more: true,
        truncated: true,
        next_cursor: Some("abc".into()),
    });
    insta::assert_snapshot!(run(&r));
}

#[test]
fn snapshot_path_finding() {
    let mut steps = [
        edge("IN_PROJECT", ("MergeRequest", 10), ("Project", 100)),
        edge("AUTHORED", ("User", 1), ("MergeRequest", 10)),
        edge("AUTHORED", ("User", 1), ("MergeRequest", 11)),
    ];
    for (edge, (path_id, step)) in steps.iter_mut().zip([(0, 1), (0, 0), (1, 0)]) {
        edge.path_id = Some(path_id);
        edge.step = Some(step);
    }
    let r = response(
        "path_finding",
        vec![node("Project", 100, json!({"name": "GitLab"}))],
        steps.to_vec(),
    );
    insta::assert_snapshot!(run(&r));
}

#[test]
fn snapshot_aggregation() {
    let mut r = response("aggregation", vec![], vec![]);
    r.group_columns = Some(vec![
        GroupColumnDescriptor {
            name: "p".into(),
            kind: "node".into(),
            node: "p".into(),
            property: None,
            entity: Some("Project".into()),
        },
        GroupColumnDescriptor {
            name: "bucket".into(),
            kind: "property".into(),
            node: "v".into(),
            property: Some("severity".into()),
            entity: None,
        },
    ]);
    r.columns = Some(vec![ColumnDescriptor {
        name: "latest".into(),
        function: "max".into(),
        target: "v".into(),
        property: Some("updated_at".into()),
    }]);
    let project = json!({"type": "Project", "id": "9", "properties": {"name": "gitlab"}});
    r.rows = Some(vec![
        props(json!({"p": project, "bucket": "high", "latest": 1.5})),
        props(json!({"p": project, "bucket": null, "latest": 3})),
    ]);
    insta::assert_snapshot!(run(&r));
}
