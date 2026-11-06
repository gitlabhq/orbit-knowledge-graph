mod support;

use query_data_model::plan::logical::{ErrorClass, LogicalOperation};
use query_data_model::surface::{SurfaceError, json};

fn operations(query: serde_json::Value) -> Result<Vec<String>, SurfaceError> {
    let model = support::gitlab();
    let statement = json::parse(&query.to_string(), model.logical().as_admin())?;
    let plan = statement.plan;
    Ok(plan
        .nodes()
        .map(|node| match plan.operation(node) {
            LogicalOperation::Entities(_) => "entities",
            LogicalOperation::Filter(_) => "filter",
            LogicalOperation::Follow { .. } => "follow",
            LogicalOperation::Neighbors { .. } => "neighbors",
            LogicalOperation::Shortest { .. } => "shortest",
            LogicalOperation::Aggregate { .. } => "aggregate",
            LogicalOperation::Select(_) => "select",
            LogicalOperation::Order(_) => "order",
            LogicalOperation::Limit { .. } => "limit",
            LogicalOperation::Union | LogicalOperation::Read(_) => "other",
        })
        .map(str::to_string)
        .collect())
}

#[test]
fn a_traversal_builds_bindings_filters_and_a_projection() {
    let query = serde_json::json!({
        "query_type": "traversal",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}, "columns": ["title"]},
            {"id": "u", "entity": "User", "node_ids": [1, "2"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "order_by": "-mr.title",
        "limit": 5
    });
    assert_eq!(
        operations(query).unwrap(),
        [
            "entities", "filter", "follow", "filter", "order", "select", "limit"
        ]
    );
}

#[test]
fn an_aggregation_groups_measures_and_sorts() {
    let query = serde_json::json!({
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "n", "entity": "Note"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "group_by": ["u"],
        "aggregations": [{"count": "n", "distinct": true}],
        "aggregation_sort": "-count_distinct_n"
    });
    assert_eq!(
        operations(query).unwrap(),
        [
            "entities",
            "filter",
            "follow",
            "aggregate",
            "order",
            "select",
            "limit"
        ]
    );
}

#[test]
fn model_errors_come_from_construction() {
    let class = |query: serde_json::Value| match operations(query).unwrap_err() {
        SurfaceError::Plan(error) => Some(error.class()),
        _ => None,
    };
    let node = |filters: serde_json::Value| {
        serde_json::json!({
            "query_type": "traversal",
            "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": filters}]
        })
    };
    assert_eq!(
        class(node(serde_json::json!({"titel": "x"}))),
        Some(ErrorClass::Name)
    );
    assert_eq!(
        class(node(serde_json::json!({"state": "draft"}))),
        Some(ErrorClass::Type)
    );
    assert_eq!(
        class(node(serde_json::json!({"traversal_path": "1/"}))),
        Some(ErrorClass::Name)
    );
    let disconnected = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}, {"id": "u", "entity": "User"}],
        "relationships": [{"type": "AUTHORED", "from": "p", "to": "u"}]
    });
    assert_eq!(class(disconnected), Some(ErrorClass::Connectivity));
    let malformed = serde_json::json!({"query_type": "traversal", "nodes": []});
    assert!(matches!(
        operations(malformed),
        Err(SurfaceError::Schema(_))
    ));
}

#[test]
fn schema_errors_name_the_location_and_rule_but_not_the_value() {
    let message = |query: serde_json::Value| match operations(query) {
        Err(SurfaceError::Schema(message)) => message,
        other => panic!("expected a schema error, got {other:?}"),
    };
    let many: serde_json::Map<String, serde_json::Value> = (0..11)
        .map(|i| (format!("f{i}"), format!("private-count-literal-{i}").into()))
        .collect();
    let rejected = message(serde_json::json!({
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "not": "private-type-literal"},
            {"id": "w", "entity": "WorkItem", "not": many, "filters": {"state": {"bogus": "private-filter-literal"}}}
        ]
    }));
    assert!(
        rejected.contains("has more than 10 properties at /nodes/1/not"),
        "{rejected}"
    );
    for literal in [
        "private-type-literal",
        "private-count-literal",
        "private-filter-literal",
    ] {
        assert!(!rejected.contains(literal), "{rejected}");
    }
    let hydration = message(serde_json::json!({
        "query_type": "hydration",
        "nodes": [{"id": "h", "entity": "User", "node_ids": [1]}]
    }));
    assert!(hydration.contains("is not an allowed value"), "{hydration}");
    assert!(!hydration.contains("hydration"), "{hydration}");
}
