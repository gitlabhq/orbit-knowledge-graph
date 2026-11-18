use super::setup::{embedded_ontology, test_ctx};
use compiler::{
    ColumnSelection, HydrationCompileOptions, Input, InputNode, QueryType, compile_input,
};
use orbit_utils::traversal_path::TraversalPath;

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
