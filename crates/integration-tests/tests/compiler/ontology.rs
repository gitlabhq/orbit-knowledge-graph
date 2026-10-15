use super::setup::{embedded_ontology, test_ctx};
use compiler::{
    ColumnSelection, HydrationCompileOptions, HydrationPlan, Input, InputNode, QueryType,
    compile_input,
};
use orbit_utils::traversal_path::TraversalPath;

#[test]
fn hydration_query_type_generates_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![
            InputNode {
                id: "hydrate".into(),
                entity: Some("Note".into()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "noteable_type".into(),
                ])),
                node_ids: vec![1, 2, 3],
                ..InputNode::default()
            },
            InputNode {
                id: "hydrate".into(),
                entity: Some("Project".into()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
                node_ids: vec![10, 20],
                ..InputNode::default()
            },
        ],
        limit: 10,
        ..Input::default()
    };

    let result = compile_input(
        input,
        HydrationCompileOptions::default(),
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    // Hydration SQL uses ClickHouse array literals (`IN [1,2,3]`) which
    // sqlparser doesn't support yet, so we check the raw SQL string.
    let raw = &result.base.render();

    assert!(raw.contains("UNION ALL"));
    assert!(raw.contains("toJSONString"));
    assert!(raw.contains("gl_note"));
    assert!(raw.contains("gl_project"));
    assert!(matches!(result.hydration, HydrationPlan::None));
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
fn hydration_single_entity_no_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec!["id".into(), "username".into()])),
            node_ids: vec![42],
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

    assert!(!rendered.contains("UNION ALL"));
    assert!(rendered.contains("toJSONString"));
    assert!(rendered.contains("gl_user"));
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
fn hydration_skips_security_context() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
            ])),
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
        !rendered.contains("arrayExists"),
        "hydration should skip security filters"
    );
    assert!(
        !rendered.contains("startsWith"),
        "hydration should not have startsWith"
    );
}

#[test]
fn hydration_id_only_columns_produces_map_with_id() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec!["id".into()])),
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
        rendered.contains("map(") && rendered.contains("'id'"),
        "PK should be included in map when requested"
    );
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

#[test]
fn hydration_id_column_included_in_map() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "username".into(),
                "state".into(),
            ])),
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

    assert!(rendered.contains("'username'") && rendered.contains("'state'"));
    let map_section = rendered
        .split("map(")
        .nth(1)
        .and_then(|s| s.split(')').next())
        .unwrap_or("");
    assert!(
        map_section.contains("'id'"),
        "map should contain 'id' key when requested"
    );
}
