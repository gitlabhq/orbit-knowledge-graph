use compiler::input::{
    ColumnSelection, Direction, Input, InputNeighbors, InputNode, InputPath, PathType, QueryType,
    RelationshipSelection,
};
use compiler::passes::plan::HydrationCompileOptions;
use compiler::query_graph::api::{OperationKind, QueryGraph};
use std::sync::Arc;

#[test]
fn hydration_resolves_versions_before_filtering_deleted_rows() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "project".into(),
            entity: Some("Project".into()),
            node_ids: vec![7, 8],
            columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
            ..Default::default()
        }],
        limit: 10,
        ..Default::default()
    };
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph
        .hydration(&input, HydrationCompileOptions::default())
        .unwrap();
    let mut checked = 0;
    for query in graph.reachable(root).unwrap() {
        graph
            .rows(query)
            .unwrap()
            .walk(&mut |rows| {
                if let OperationKind::Filter { input, .. } = rows.kind()
                    && matches!(input.kind(), OperationKind::Latest { .. })
                {
                    checked += 1;
                }
                Ok::<_, compiler::query_graph::Error>(())
            })
            .unwrap();
    }
    assert_eq!(checked, 1);
    let (sql, params) = graph.lower().render(root).unwrap();
    assert!(sql.contains("LIMIT 1 BY") && sql.contains("toJSONString(map("));
    assert!(
        params
            .values()
            .any(|value| value.value == serde_json::json!([7, 8]))
    );
}

#[test]
fn both_direction_neighbors_use_one_expanding_edge_scan() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let input = Input {
        query_type: QueryType::Neighbors,
        nodes: vec![InputNode {
            id: "project".into(),
            entity: Some("Project".into()),
            node_ids: vec![7],
            ..Default::default()
        }],
        neighbors: Some(InputNeighbors {
            direction: Direction::Both,
            rel_types: RelationshipSelection::Kinds(vec!["IN_PROJECT".into()]),
        }),
        limit: 10,
        ..Default::default()
    };
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph.neighbors(&input).unwrap();
    let (sql, _) = graph.lower().render(root).unwrap();
    assert_eq!(sql.matches("arrayJoin(").count(), 1);
    assert_eq!(sql.matches("arrayFilter(").count(), 2);
    assert!(sql.contains("LIMIT 10"));
}

#[test]
fn bounded_paths_build_forward_and_backward_frontiers() {
    let model =
        compiler::data_model::clickhouse(Arc::new(ontology::Ontology::load_embedded().unwrap()))
            .unwrap();
    let input = Input {
        query_type: QueryType::PathFinding,
        nodes: [("start", 7), ("end", 8)]
            .into_iter()
            .map(|(alias, id)| InputNode {
                id: alias.into(),
                entity: Some("Project".into()),
                node_ids: vec![id],
                ..Default::default()
            })
            .collect(),
        path: Some(InputPath {
            path_type: PathType::Shortest,
            from: "start".into(),
            to: "end".into(),
            max_depth: 3,
            rel_types: RelationshipSelection::Kinds(vec!["IN_PROJECT".into()]),
        }),
        limit: 10,
        ..Default::default()
    };
    let mut graph = QueryGraph::new(model.as_ref());
    let root = graph.pathfinding(&input).unwrap();
    let (sql, _) = graph.lower().render(root).unwrap();
    assert!(sql.starts_with("WITH "));
    assert!(sql.contains("UNION ALL") && sql.contains("INNER JOIN"));
    assert!(sql.contains("arrayReverse(") && sql.contains("ORDER BY"));
}
