use std::convert::Infallible;
use std::sync::Arc;

#[path = "support/clickhouse.rs"]
mod database;

use compiler::ast::Identifier;
use compiler::constants::redaction_id_column;
use compiler::input::Input;
use compiler::lowering::{Context, lower_program, scalar};
use compiler::passes::{check, codegen, enforce::ResultContext, normalize, security};
use compiler::planning::physical::Scalar;
use compiler::planning::{backends::clickhouse, graph, optimize};
use compiler::{Node, SecurityContext};
use query_data_model::ClickHouseDataModel;

type Candidate = optimize::Candidate<clickhouse::Scan, Scalar, Infallible>;

fn incoming_candidates() -> (ClickHouseDataModel, Vec<Identifier>, Vec<Candidate>) {
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
                redaction_id_column(&node.id).into(),
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

    (model, bound.outputs, candidates)
}

#[test]
fn incoming_self_edge_realization_keeps_both_endpoints_and_their_filters() {
    let (_, outputs, candidates) = incoming_candidates();

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
        .into_query(&outputs)
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

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn clickhouse_fk_candidates_keep_current_rows_and_both_endpoint_permissions() {
    let (model, outputs, candidates) = incoming_candidates();
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut sql = String::from(
        "CREATE TABLE gl_pipeline (
             id Int64, status String, auto_canceled_by_id Nullable(Int64),
             traversal_path String, _version UInt64, _deleted Bool
         ) ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         CREATE TABLE gl_ci_edge (
             source_id Int64, target_id Int64, source_kind String, target_kind String,
             relationship_kind String, traversal_path String, _version UInt64, _deleted Bool
         ) ENGINE = ReplacingMergeTree(_version)
           ORDER BY (traversal_path, source_id, target_id, source_kind, target_kind, relationship_kind);
         INSERT INTO gl_pipeline VALUES
             (1, 'canceled', 2, '1/100/', 1, false),
             (2, 'success', NULL, '1/100/', 1, false),
             (3, 'canceled', 2, '1/100/', 1, false),
             (3, 'success', 2, '1/100/', 2, false),
             (4, 'canceled', 2, '1/100/', 1, false),
             (4, 'canceled', 2, '1/100/', 2, true),
             (5, 'canceled', 6, '1/100/', 1, false),
             (6, 'success', NULL, '1/100/', 1, false),
             (6, 'success', NULL, '1/100/', 2, true),
             (7, 'canceled', 8, '1/100/', 1, false),
             (8, 'success', NULL, '1/200/', 1, false),
             (9, 'canceled', 2, '1/200/', 1, false),
             (10, 'canceled', NULL, '1/100/', 1, false),
             (11, 'canceled', 99, '1/100/', 1, false),
             (12, 'canceled', 13, '1/100/', 1, false),
             (13, 'success', NULL, '1/100/', 1, false),
             (13, 'failed', NULL, '1/100/', 2, false);
         INSERT INTO gl_ci_edge VALUES
             (1, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false),
             (1, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 2, false),
             (3, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false),
             (4, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false),
             (5, 6, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false),
             (7, 8, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false),
             (9, 2, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/200/', 1, false),
             (11, 99, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false),
             (12, 13, 'Pipeline', 'Pipeline', 'AUTO_CANCELED_BY', '1/100/', 1, false);\n",
    );

    for (index, candidate) in candidates.iter().enumerate() {
        let query = lower_program(
            &candidate.program,
            &candidate.values,
            &mut Context::default(),
            &scalar::emit,
        )
        .unwrap()
        .into_query(&outputs)
        .unwrap();
        let mut node = Node::Query(Box::new(query));
        security::apply_security_context(&mut node, &context, &model).unwrap();
        check::check_ast(&node, &context, &model).unwrap();
        let query =
            codegen::clickhouse::codegen(&node, ResultContext::new(), Default::default()).unwrap();
        let rendered = query.render();
        assert_eq!(rendered.contains("gl_ci_edge"), index == 0);
        sql.push_str(&format!(
            "SELECT source, target FROM ({rendered}) ORDER BY source, target FORMAT TSV;\n"
        ));
        sql.push_str(&format!("SELECT count(*) FROM ({rendered}) FORMAT TSV;\n"));
    }

    assert_eq!(database::execute(&sql).trim(), "1\t2\n1\n1\t2\n1");
}
