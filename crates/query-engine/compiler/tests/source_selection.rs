use std::sync::Arc;

use compiler::lowering::{lower, scalar};
use compiler::passes::{codegen, enforce::ResultContext};
use compiler::planning::bind::Source;
use compiler::planning::generic::{ValueType, Values};
use compiler::planning::physical::{CurrentRows, select_source};
use query_data_model::{ClickHouseDataModel, EdgeField, QueryDataModel};

#[test]
fn multi_table_sources_preserve_duplicates_and_filter_each_route() {
    let ontology = Arc::new(compiler::Ontology::load_embedded().unwrap());
    let model = ClickHouseDataModel::derive(ontology).unwrap();
    let calls = model.graph().relationship_id("CALLS").unwrap();
    let authored = model.graph().relationship_id("AUTHORED").unwrap();
    let calls_table = model.query_backend().relationship_table(calls).unwrap();
    let authored_table = model.query_backend().relationship_table(authored).unwrap();
    assert_ne!(calls_table, authored_table);

    let mut values = Values::default();
    let id = values.allocate(ValueType::Int64);
    let kind = values.allocate(ValueType::String);
    let source = Source::Edge {
        relationship: 0,
        relationships: vec![calls, authored],
        fields: vec![
            (id, EdgeField::SourceId),
            (kind, EdgeField::RelationshipKind),
        ],
    };
    let plan = select_source(source, &model, CurrentRows::Snapshot, &mut values).unwrap();
    assert_eq!(plan.output(&values).unwrap(), vec![id, kind]);

    let query = lower(&plan, &values, &scalar::emit)
        .unwrap()
        .into_query(&["id".into(), "kind".into()])
        .unwrap();
    let query = codegen::duckdb::codegen(
        &compiler::Node::Query(Box::new(query)),
        ResultContext::new(),
    )
    .unwrap();
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE {calls_table}(source_id BIGINT, relationship_kind VARCHAR);
         CREATE TABLE {authored_table}(source_id BIGINT, relationship_kind VARCHAR);
         INSERT INTO {calls_table} VALUES (1, 'CALLS'), (1, 'CALLS'), (99, 'AUTHORED');
         INSERT INTO {authored_table} VALUES (2, 'AUTHORED'), (98, 'CALLS');"
        ))
        .unwrap();

    let mut rows = connection
        .prepare(&query.render())
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            (1, "CALLS".into()),
            (1, "CALLS".into()),
            (2, "AUTHORED".into())
        ]
    );
}
