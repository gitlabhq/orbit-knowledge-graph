use std::sync::Arc;

use compiler::{Frontend, Ontology, SecurityContext, compile, compile_local};

#[path = "support/clickhouse.rs"]
mod clickhouse;

const QUERY: &str = "MATCH (a:Definition)-[:CALLS]->(b:Definition)
    WHERE a.id IN [1, 2, 3] AND a.name = b.name AND a.id < b.id
    RETURN a.id, b.id ORDER BY a.id";

#[test]
fn comparisons_keep_hidden_operands_and_sql_null_semantics() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_definition(id BIGINT, name VARCHAR);
         INSERT INTO gl_definition VALUES (1, 'same'), (2, 'same'), (3, NULL), (4, NULL), (5, 'other');
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR, target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (2, 1, 'Definition', 'Definition', 'CALLS'),
             (1, 5, 'Definition', 'Definition', 'CALLS'),
             (3, 4, 'Definition', 'Definition', 'CALLS');"
    ).unwrap();
    let compiled = compile_local(QUERY, Frontend::Gql, &ontology).unwrap();
    let mut statement = connection.prepare(&compiled.base.render()).unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>("a_id")?, row.get::<_, i64>("b_id")?))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 2)]);
    assert!(
        !statement
            .column_names()
            .iter()
            .any(|name| name.ends_with("_name"))
    );

    let compiled = compile_local(
        "MATCH (f:File) WHERE f.path = f.name RETURN f.id ORDER BY f.id",
        Frontend::Gql,
        &ontology,
    )
    .unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_file(id BIGINT, path VARCHAR, name VARCHAR);
         INSERT INTO gl_file VALUES (1, 'same', 'same'), (2, 'different', 'same'), (3, NULL, NULL);"
    ).unwrap();
    let ids = connection
        .prepare(&compiled.base.render())
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(ids, vec![1]);
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn comparisons_run_after_authorized_current_row_reads() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let compiled = compile(QUERY, Frontend::Gql, &ontology, &security).unwrap();
    let rendered = compiled.base.render();
    let sql = rendered.split(" SETTINGS ").next().unwrap();
    let result = clickhouse::execute(&format!(
        "CREATE TABLE gl_definition(id Int64, name Nullable(String), project_id Int64, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_definition VALUES
             (1, 'same', 100, '1/100/', 1, false), (2, 'same', 100, '1/100/', 1, false),
             (3, NULL, 100, '1/100/', 1, false), (4, NULL, 100, '1/100/', 1, false),
             (5, 'same', 200, '1/200/', 1, false);
         CREATE TABLE gl_code_edge(source_id Int64, target_id Int64, source_kind String, target_kind String, relationship_kind String,
             traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, source_id, target_id, source_kind, target_kind, relationship_kind);
         INSERT INTO gl_code_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (2, 1, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (1, 5, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (3, 4, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false);
         SELECT a_id, b_id FROM ({sql}) FORMAT TSV;"
    ));
    assert_eq!(result.trim(), "1\t2");
}
