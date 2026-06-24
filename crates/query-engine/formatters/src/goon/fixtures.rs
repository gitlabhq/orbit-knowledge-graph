use serde_json::{Map, Value, json};

use crate::graph::{
    ColumnDescriptor, GraphEdge, GraphNode, GraphResponse, GroupColumnDescriptor,
    PaginationResponse,
};

pub fn response(query_type: &str, nodes: Vec<GraphNode>, edges: Vec<GraphEdge>) -> GraphResponse {
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

pub fn node(entity_type: &str, id: i64, props: &[(&str, Value)]) -> GraphNode {
    let mut properties = Map::new();
    for (k, v) in props {
        properties.insert((*k).into(), v.clone());
    }
    GraphNode {
        entity_type: entity_type.into(),
        id,
        properties,
    }
}

pub fn edge(edge_type: &str, from: &str, from_id: i64, to: &str, to_id: i64) -> GraphEdge {
    GraphEdge {
        from: from.into(),
        from_id,
        to: to.into(),
        to_id,
        edge_type: edge_type.into(),
        depth: None,
        path_id: None,
        step: None,
    }
}

pub fn path_edge(
    edge_type: &str,
    from: &str,
    from_id: i64,
    to: &str,
    to_id: i64,
    path_id: usize,
    step: usize,
) -> GraphEdge {
    GraphEdge {
        from: from.into(),
        from_id,
        to: to.into(),
        to_id,
        edge_type: edge_type.into(),
        depth: None,
        path_id: Some(path_id),
        step: Some(step),
    }
}

pub fn aggregation_column(name: &str, function: &str) -> ColumnDescriptor {
    ColumnDescriptor {
        name: name.into(),
        function: function.into(),
        target: None,
        property: None,
    }
}

pub fn property_group(name: &str, node: &str, property: &str) -> GroupColumnDescriptor {
    GroupColumnDescriptor {
        name: name.into(),
        kind: "property".into(),
        node: node.into(),
        property: Some(property.into()),
        entity: None,
    }
}

pub fn node_group(name: &str, node: &str, entity: &str) -> GroupColumnDescriptor {
    GroupColumnDescriptor {
        name: name.into(),
        kind: "node".into(),
        node: node.into(),
        property: None,
        entity: Some(entity.into()),
    }
}

pub fn agg_row(pairs: &[(&str, Value)]) -> Map<String, Value> {
    let mut row = Map::new();
    for (k, v) in pairs {
        row.insert((*k).into(), v.clone());
    }
    row
}

pub fn node_group_cell(entity_type: &str, id: i64, props: &[(&str, Value)]) -> Value {
    let mut properties = Map::new();
    for (k, v) in props {
        properties.insert((*k).into(), v.clone());
    }
    let mut obj = Map::new();
    obj.insert("type".into(), Value::String(entity_type.into()));
    obj.insert("id".into(), Value::String(id.to_string()));
    obj.insert("properties".into(), Value::Object(properties));
    Value::Object(obj)
}

pub fn pagination(has_more: bool, total_rows: usize) -> PaginationResponse {
    PaginationResponse {
        has_more,
        total_rows,
        truncated: has_more,
    }
}

pub fn traversal_response() -> GraphResponse {
    response(
        "traversal",
        vec![
            node(
                "User",
                5252563,
                &[
                    ("username", json!("jordan_ng")),
                    ("name", json!("Jordan NG")),
                ],
            ),
            node(
                "MergeRequest",
                482927048,
                &[
                    ("iid", json!(18)),
                    ("state", json!("merged")),
                    ("title", json!("chore: move skill to project scope")),
                    ("created_at", json!("2026-05-08 23:07:40.793493")),
                ],
            ),
            node(
                "Project",
                80212187,
                &[
                    ("name", json!("webapp-scaffold")),
                    (
                        "full_path",
                        json!("gitlab-com/cx-engineering/webapp-scaffold"),
                    ),
                ],
            ),
        ],
        vec![
            edge("AUTHORED", "User", 5252563, "MergeRequest", 482927048),
            edge("IN_PROJECT", "MergeRequest", 482927048, "Project", 80212187),
        ],
    )
}
