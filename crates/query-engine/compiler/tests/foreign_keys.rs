use std::sync::Arc;

use compiler::constants::redaction_id_column;
use compiler::input::Input;
use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{codegen, enforce::ResultContext, normalize};
use compiler::planning::{backends::clickhouse, graph, optimize};
use query_data_model::ClickHouseDataModel;

#[test]
fn incoming_self_edge_realization_keeps_both_endpoints_and_their_filters() {
    let model = ClickHouseDataModel::derive(Arc::new(compiler::Ontology::load_embedded().unwrap()))
        .unwrap();
    let input: Input = serde_json::from_value(serde_json::json!({
        "query_type": "traversal",
        "nodes": [
            { "id": "canceling", "entity": "Pipeline", "columns": ["id"], "filters": { "status": "success" } },
            { "id": "canceled", "entity": "Pipeline", "columns": ["id"], "filters": { "status": "canceled" } }
        ],
        "relationships": [{ "type": "AUTO_CANCELED_BY", "from": "canceling", "to": "canceled", "direction": "incoming" }]
    })).unwrap();
    let input = normalize::normalize(input, &model).unwrap();
    let required = input
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                node.id_property.clone(),
                redaction_id_column(&node.id),
            )
        })
        .collect::<Vec<_>>();
    let edge_outputs = [[
        "source".into(),
        "target".into(),
        "source_kind".into(),
        "target_kind".into(),
        "kind".into(),
    ]];
    let mut bound = graph::traversal(&input, &model, &required, &edge_outputs).unwrap();
    let physical = bound
        .root
        .expand_sources(&mut |source| clickhouse::select(source, &model, &mut bound.values))
        .unwrap();
    let candidates =
        optimize::candidates(physical, bound.values, &[clickhouse::realize_foreign_key]).unwrap();
    assert_eq!(candidates.len(), 2);

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_pipeline(id BIGINT, status VARCHAR, auto_canceled_by_id BIGINT);
         CREATE TABLE gl_ci_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR, target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_pipeline VALUES
             (1, 'canceled', 2), (2, 'success', NULL),
             (3, 'canceled', 4), (4, 'failed', NULL),
             (5, 'success', 2), (6, 'canceled', 99);
         INSERT INTO gl_ci_edge VALUES
             (1, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY'),
             (3, 4, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY'),
             (5, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY'),
             (6, 99, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY');"
    ).unwrap();

    for (index, candidate) in candidates.iter().enumerate() {
        let query = lower_program(
            &candidate.program,
            &candidate.values,
            &mut Context::default(),
            &scalar::emit,
        )
        .unwrap()
        .into_query(&bound.outputs)
        .unwrap();
        let sql = codegen::duckdb::codegen(
            &compiler::Node::Query(Box::new(query)),
            ResultContext::new(),
        )
        .unwrap();
        let rendered = sql.render();
        assert_eq!(rendered.contains("gl_ci_edge"), index == 0, "{rendered}");
        let rows = connection
            .prepare(&rendered)
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, i64>("source")?, row.get::<_, i64>("target")?))
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, vec![(1, 2)]);

        let count = connection
            .query_row(&format!("SELECT count(*) FROM ({rendered})"), [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }
}
