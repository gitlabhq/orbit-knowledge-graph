use std::sync::Arc;

use compiler::{Frontend, Ontology, SecurityContext, compile, compile_local};

#[path = "support/clickhouse.rs"]
mod clickhouse;

#[test]
fn exact_and_variable_hops_preserve_walks_and_intermediate_types() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_definition(id BIGINT);
         INSERT INTO gl_definition VALUES (1), (2), (3), (4);
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR,
             target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (2, 3, 'Definition', 'Definition', 'CALLS'),
             (3, 4, 'Definition', 'Definition', 'CALLS'),
             (2, 4, 'File', 'Definition', 'CALLS'),
             (2, 4, 'Definition', 'Definition', 'CONTAINS');",
        )
        .unwrap();
    for (hops, expected) in [
        ([2, 2], vec![(3, 3)]),
        ([1, 3], vec![(2, 2), (3, 3), (4, 4)]),
    ] {
        let input = serde_json::json!({
            "query_type": "traversal",
            "nodes": [
                { "id": "a", "entity": "Definition", "columns": ["id"], "node_ids": [1] },
                { "id": "b", "entity": "Definition", "columns": ["id"] }
            ],
            "relationships": [{ "type": "CALLS", "from": "a", "to": "b", "hops": hops }]
        });
        let compiled = compile_local(&input.to_string(), Frontend::JsonDsl, &ontology).unwrap();
        let path = compiled.base.result_context.edges()[0]
            .path_column
            .as_ref()
            .unwrap();
        let sql = format!(
            "SELECT b_id, length({path}) FROM ({}) ORDER BY b_id",
            compiled.base.render()
        );
        let rows = connection
            .prepare(&sql)
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, expected);
    }
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn remote_hops_filter_every_edge_and_export_depth_and_path() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let compiled = compile(
        "MATCH (a:Definition {id: 1})-[:CALLS*1..3]->(b:Definition) RETURN a.id, b.id",
        Frontend::Gql,
        &ontology,
        &security,
    )
    .unwrap();
    let rendered = compiled.base.render();
    let sql = rendered.split(" SETTINGS ").next().unwrap();
    let output = clickhouse::execute(&format!(
        "CREATE TABLE gl_definition(id Int64, project_id Int64, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_definition VALUES (1, 100, '1/100/', 1, false), (2, 100, '1/100/', 1, false),
             (3, 100, '1/100/', 1, false), (4, 100, '1/100/', 1, false);
         CREATE TABLE gl_code_edge(source_id Int64, target_id Int64, source_kind String, target_kind String,
             relationship_kind String, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, source_id, target_id, source_kind, target_kind, relationship_kind);
         INSERT INTO gl_code_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (2, 3, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (3, 4, 'Definition', 'Definition', 'CALLS', '1/200/', 1, false);
         SELECT b_id, _gkg_edge_0_depth, length(_gkg_edge_0_path) FROM ({sql}) ORDER BY b_id FORMAT TSV;"
    ));
    assert_eq!(output.trim(), "2\t1\t2\n3\t2\t3");
}
