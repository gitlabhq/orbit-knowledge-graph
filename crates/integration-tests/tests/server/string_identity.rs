//! An entity whose identity is a string, from the demo Slack package, read from ClickHouse:
//! the query, the hydration read and the graph output all carry its text keys.

use std::sync::Arc;

use integration_testkit::{TestContext, graph_schema, t, versioned_data_model};
use orbit_arrow::ColumnValue;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::orbit_authorization::Key;
use query_engine::compiler::{
    HydrationArm, HydrationCompileOptions, HydrationRequest, compile, compile_hydration,
    uniform_grants,
};
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::shared::PipelineOutput;
use query_engine::shared::hydration::parse_hydration_batches;
use query_engine::types::QueryResult;

fn schema_sql(fragments: &query_data_model::spec::Fragments) -> Vec<String> {
    let prefix = integration_testkit::TABLE_PREFIX.as_str();
    graph_schema(fragments)
        .tables
        .iter()
        .map(|table| format!("{};", table.to_create_sql(prefix)))
        .collect()
}

#[tokio::test]
async fn graph_formatter_string_identity_e2e() {
    let fragments = query_data_model::testkit::with_demo().fragments().unwrap();
    let sql = schema_sql(&fragments);
    let statements: Vec<&str> = sql.iter().map(String::as_str).collect();
    let ctx = TestContext::new(&statements).await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, group_id, traversal_path) VALUES \
         ('T1', 'Orbit', 100, '1/100/'), ('T2', 'Other', 101, '1/101/')",
        t("gl_slack_workspace")
    ))
    .await;
    let catalog = versioned_data_model(fragments);
    let grants = uniform_grants(catalog.model(), false, &["1/100/"]);
    let limits = query_data_model::testkit::limits();

    let compiled = Arc::new(
        compile(
            "MATCH (w:SlackWorkspace) WHERE w.id IN ['T1', 'T2'] RETURN w",
            &catalog,
            &grants,
            limits,
        )
        .unwrap(),
    );
    let batches = ctx.query_parameterized(&compiled.base).await;
    let result = QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
    let node = compiled.base.result_context.get("w").unwrap();
    let ids: Vec<Key> = result
        .rows()
        .iter()
        .filter_map(|row| row.get_public_id(node))
        .collect();
    assert_eq!(ids, [Key::Text("T1".into())]);

    let arm = HydrationArm::new(
        catalog.model(),
        limits,
        "SlackWorkspace",
        &[Key::Text("T1".into()), Key::Text("T2".into())],
        &["name".to_string()],
        vec![TraversalPath::new_unchecked("1/100/".to_string())],
    )
    .unwrap();
    let request = HydrationRequest {
        arms: vec![arm],
        options: HydrationCompileOptions::default(),
    };
    let hydration = compile_hydration(request, &catalog, &grants, limits).unwrap();
    let rows = ctx.query_parameterized(&hydration).await;
    let properties = parse_hydration_batches(&rows, catalog.model()).unwrap();
    let key = ("SlackWorkspace".to_string(), Key::Text("T1".into()));
    assert_eq!(
        properties.get(&key).and_then(|row| row.get("name")),
        Some(&ColumnValue::String("Orbit".into()))
    );
    assert_eq!(properties.len(), 1, "{properties:?}");

    let output = PipelineOutput {
        row_count: result.authorized_count(),
        redacted_count: 0,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(&compiled),
        result_context: compiled.base.result_context.clone(),
        query_result: result,
        execution_log: vec![],
        pagination: None,
    };
    let graph = GraphFormatter.format(&output);
    let nodes = graph["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1, "{graph}");
    assert_eq!(nodes[0]["type"], "SlackWorkspace");
    assert_eq!(nodes[0]["id"], "T1");
    assert_eq!(nodes[0]["name"], "Orbit");
}
