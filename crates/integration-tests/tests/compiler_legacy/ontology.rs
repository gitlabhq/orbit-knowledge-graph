use super::setup::{embedded_ontology, test_ctx};
use compiler::{
    ColumnSelection, Frontend, HydrationCompileOptions, Input, InputNode, QueryType, compile,
    compile_input,
};
use orbit_utils::traversal_path::TraversalPath;

#[test]
fn path_finding_uses_orbit_path_not_node_columns() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "node_ids": [100], "columns": ["name"]},
            {"id": "end", "entity": "Project", "node_ids": [200], "columns": ["name"]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["CONTAINS"]}
    }"#;

    let result = compile(json, Frontend::JsonDsl, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_path"));
    assert!(result.base.result_context.query_type == Some(QueryType::PathFinding));
}

#[test]
fn result_context_populated() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
        "limit": 10
    }"#;

    let result = compile(json, Frontend::JsonDsl, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert_eq!(result.base.result_context.len(), 2);

    let user = result.base.result_context.get("u").unwrap();
    assert_eq!(user.entity_type, "User");
    assert_eq!(user.id_column, "_orbit_u_id");
    assert_eq!(user.type_column, "_orbit_u_type");

    let project = result.base.result_context.get("p").unwrap();
    assert_eq!(project.entity_type, "Project");
    assert_eq!(project.id_column, "_orbit_p_id");
    assert_eq!(project.type_column, "_orbit_p_type");

    assert!(rendered.contains("_orbit_u_id"));
    assert!(rendered.contains("_orbit_u_type"));
    assert!(rendered.contains("_orbit_p_id"));
    assert!(rendered.contains("_orbit_p_type"));
}

#[test]
fn debug_json_round_trip() {
    let compiled = compile(
        r#"{
    "query_type": "traversal",
    "nodes": [
        {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
        {"id": "u", "entity": "User"}
    ],
    "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
    "limit": 10
        }"#,
        Frontend::JsonDsl,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();

    let rendered = compiled.base.render();
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );

    let debug_json = serde_json::json!({
        "base": compiled.base.sql,
        "base_rendered": rendered,
        "hydration": serde_json::json!([]),
    });
    let parsed: serde_json::Value = serde_json::from_str(&debug_json.to_string()).unwrap();
    assert!(
        parsed["base"].as_str().unwrap().contains("{p"),
        "base should have placeholders"
    );
    assert!(
        !parsed["base_rendered"].as_str().unwrap().contains("{p"),
        "rendered should not"
    );
    assert!(parsed["hydration"].is_array());
}

#[test]
fn hydration_widens_paths_to_segment_budget() {
    let deep = |count: usize| -> Vec<TraversalPath> {
        (0..count)
            .map(|i| TraversalPath::new_unchecked(format!("1/{i:0>40}/{:0>40}/", i + 10000)))
            .collect()
    };
    let node = |entity: &str, paths: Vec<TraversalPath>| InputNode {
        id: "hydrate".into(),
        entity: Some(entity.into()),
        columns: Some(ColumnSelection::List(vec!["id".into()])),
        node_ids: vec![1],
        traversal_paths: paths,
        ..InputNode::default()
    };
    let compile_hydration = |nodes: Vec<InputNode>, budget: Option<usize>| {
        let input = Input {
            query_type: QueryType::Hydration,
            nodes,
            limit: 10,
            ..Input::default()
        };
        compile_input(
            input,
            HydrationCompileOptions {
                dynamic: true,
                path_segment_budget: budget,
            },
            &embedded_ontology(),
            &test_ctx(),
        )
        .unwrap()
    };
    let bound_paths = |result: &compiler::CompiledQueryContext| -> Vec<TraversalPath> {
        result
            .base
            .params
            .values()
            .filter_map(|p| match &p.value {
                serde_json::Value::Array(items) => Some(items),
                _ => None,
            })
            .flat_map(|items| items.iter().filter_map(|v| v.as_str()))
            .map(TraversalPath::new_unchecked)
            .collect()
    };

    let exact = deep(500);
    let result = compile_hydration(
        vec![node("Note", exact.clone()), node("Project", exact.clone())],
        Some(2000),
    );
    let array_params = result
        .base
        .params
        .values()
        .filter(|p| matches!(p.value, serde_json::Value::Array(_)))
        .count();
    assert_eq!(array_params, 1, "arms share one path array param");
    let mut kept = bound_paths(&result);
    kept.sort_unstable();
    let mut expected = exact;
    expected.sort_unstable();
    assert_eq!(kept, expected, "under budget keeps exact leaf paths");

    let over = deep(900);
    let result = compile_hydration(vec![node("Note", over.clone())], Some(2000));
    let widened = bound_paths(&result);
    assert!(widened.iter().all(|w| !over.contains(w)));
    for path in &over {
        assert!(
            widened
                .iter()
                .any(|w| path.as_str().starts_with(w.as_str())),
            "{path} lost its ancestor prefix"
        );
    }

    let result = compile_hydration(vec![node("Note", over.clone())], None);
    assert_eq!(
        bound_paths(&result).len(),
        over.len(),
        "no budget, no widening"
    );
}

#[test]
fn hydration_uses_parameterized_ids() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
                "created_at".into(),
            ])),
            node_ids: vec![7777, 8888, 9999],
            ..InputNode::default()
        }],
        limit: 3,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let parameterized = &result.base.sql;

    assert!(
        parameterized.contains("Array(Int64)"),
        "IDs should be parameterized"
    );
    assert!(
        !parameterized.contains("7777"),
        "literal IDs should not appear in parameterized SQL"
    );

    let rendered = result.base.render();
    assert!(rendered.contains("7777") && rendered.contains("8888") && rendered.contains("9999"));
}

#[test]
fn hydration_empty_columns_produces_empty_json() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec![])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("map("),
        "empty props should use literal '{{}}', not map()"
    );
}
