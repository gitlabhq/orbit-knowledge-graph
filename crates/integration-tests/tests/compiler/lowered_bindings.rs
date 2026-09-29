use std::collections::HashSet;

use compiler::ast::Expr;
use compiler::input::Input;
use compiler::passes::{lower, normalize, plan};
use query_data_model::ClickHouseDataModel;

use super::setup::embedded_ontology;

#[test]
fn lowering_rejects_unavailable_identity_sources() {
    let model = ClickHouseDataModel::derive(embedded_ontology()).unwrap();
    let input: Input = serde_json::from_value(serde_json::json!({
        "query_type": "traversal",
        "nodes": [{ "id": "p", "entity": "Project", "columns": ["id"], "node_ids": [1, 2] }]
    }))
    .unwrap();
    let input = normalize::normalize(input, &model).unwrap();
    let mut plan =
        plan::plan_clickhouse(&input, &model, Default::default(), &HashSet::new()).unwrap();
    let lowered = lower::emit(&plan, &input).unwrap();
    assert_eq!(lowered.metadata.nodes["p"].identity, Expr::col("p", "id"));
    assert_eq!(
        lowered.metadata.nodes["p"].table_alias.as_deref(),
        Some("p")
    );

    plan.node_edge_mappings
        .insert("p".into(), ("missing".into(), "id".into()));
    assert!(
        matches!(lower::emit(&plan, &input), Err(compiler::QueryError::Lowering(message)) if message.contains("no emitted identity"))
    );
}

#[test]
fn filtering_ctes_do_not_claim_visible_node_tables() {
    let model = ClickHouseDataModel::derive(embedded_ontology()).unwrap();
    let input: Input = serde_json::from_value(serde_json::json!({
        "query_type": "aggregation",
        "nodes": [
            { "id": "mr", "entity": "MergeRequest", "node_ids": [2000] },
            { "id": "u", "entity": "User", "filters": { "username": "alice" } }
        ],
        "relationships": [{ "type": "AUTHORED", "from": "u", "to": "mr" }],
        "aggregations": [{ "count": "mr", "as": "n" }]
    }))
    .unwrap();
    let input = normalize::normalize(input, &model).unwrap();
    let plan = plan::plan_clickhouse(&input, &model, Default::default(), &HashSet::new()).unwrap();
    let lowered = lower::emit(&plan, &input).unwrap();
    assert!(lowered.metadata.nodes["u"].table_alias.is_none());
    assert_eq!(
        lowered.metadata.nodes["u"].identity,
        Expr::col("mr", "author_id")
    );
}
