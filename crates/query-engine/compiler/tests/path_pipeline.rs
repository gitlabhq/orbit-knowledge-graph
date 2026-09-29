use std::sync::Arc;

use compiler::{Frontend, Ontology, SecurityContext, compile, compile_local};

#[path = "support/clickhouse.rs"]
mod clickhouse;

fn query(depth: u32) -> serde_json::Value {
    serde_json::json!({
        "query_type": "path_finding",
        "nodes": [
            { "id": "start", "entity": "Definition", "node_ids": [1], "columns": ["id"], "filters": { "name": "start" } },
            { "id": "end", "entity": "Definition", "node_ids": [4, 6], "columns": ["id"], "filters": { "name": "end" } }
        ],
        "path": { "type": "shortest", "from": "start", "to": "end", "max_depth": depth, "rel_types": ["CALLS"] },
        "cursor": { "page_size": 10 }
    })
}

const EDGES: &str = "(1, 2, 'Definition', 'Definition', 'CALLS'),
    (2, 4, 'Definition', 'Definition', 'CALLS'),
    (1, 3, 'Definition', 'Definition', 'CALLS'),
    (3, 4, 'Definition', 'Definition', 'CALLS'),
    (2, 3, 'Definition', 'Definition', 'CALLS'),
    (2, 1, 'Definition', 'Definition', 'CALLS'),
    (4, 6, 'Definition', 'Definition', 'CALLS'),
    (1, 4, 'File', 'Definition', 'CALLS'),
    (1, 4, 'Definition', 'Definition', 'CONTAINS')";

#[test]
fn bounded_paths_keep_shortest_ties_and_page_without_reintroducing_longer_paths() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE gl_definition(id BIGINT, name VARCHAR);
         INSERT INTO gl_definition VALUES (1, 'start'), (4, 'end'), (6, 'end');
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR,
             target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_edge VALUES {EDGES};"
        ))
        .unwrap();
    let execute = |input: &serde_json::Value| {
        let compiled = compile_local(&input.to_string(), Frontend::JsonDsl, &ontology).unwrap();
        let sql = format!(
            "SELECT depth, _gkg_cursor_0, _gkg_cursor_1, _gkg_cursor_2 FROM ({})",
            compiled.base.render()
        );
        connection
            .prepare(&sql)
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    vec![row.get::<_, Option<String>>(1)?, row.get(2)?, row.get(3)?],
                ))
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap()
    };
    assert!(execute(&query(1)).is_empty());
    assert_eq!(
        execute(&query(2))
            .iter()
            .map(|row| row.0)
            .collect::<Vec<_>>(),
        vec![2, 2]
    );
    let mut input = query(3);
    let rows = execute(&input);
    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![2, 2, 3, 3]
    );
    let gql = compile_local(
        "MATCH p = ANY SHORTEST (start:Definition {id: 1})-[:CALLS*1..3]->(`end`:Definition)
         WHERE start.name = 'start' AND `end`.name = 'end' AND `end`.id IN [4, 6]
         RETURN p PAGE 10",
        Frontend::Gql,
        &ontology,
    )
    .unwrap();
    let depths = connection
        .prepare(&format!("SELECT depth FROM ({})", gql.base.render()))
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(depths, vec![2, 2, 3, 3]);
    input["cursor"]["after"] = compiler::passes::cursor::encode(
        compiler::passes::cursor::canonical_hash(&input),
        &rows[1].1,
    )
    .into();
    assert_eq!(execute(&input), rows[2..]);
    let mut filtered = query(3);
    filtered["nodes"][1]["filters"]["name"] = "missing".into();
    assert!(execute(&filtered).is_empty());
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn path_security_filters_every_hop_before_selecting_shortest_paths() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let compiled = compile(
        &query(3).to_string(),
        Frontend::JsonDsl,
        &ontology,
        &security,
    )
    .unwrap();
    let sql = compiled.base.render();
    let sql = sql.split(" SETTINGS ").next().unwrap();
    let result = clickhouse::execute(&format!(
        "CREATE TABLE gl_definition(id Int64, name String, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_definition VALUES (1, 'start', '1/100/', 1, false), (4, 'end', '1/100/', 1, false), (6, 'end', '1/100/', 1, false);
         CREATE TABLE edges(source_id Int64, target_id Int64, source_kind String, target_kind String, relationship_kind String) ENGINE = Memory;
         INSERT INTO edges VALUES {EDGES};
         CREATE TABLE gl_code_edge(source_id Int64, target_id Int64, source_kind String, target_kind String,
             relationship_kind String, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, source_id, target_id, source_kind, target_kind, relationship_kind);
         INSERT INTO gl_code_edge SELECT *, '1/100/', 1, false FROM edges;
         INSERT INTO gl_code_edge VALUES (1, 4, 'Definition', 'Definition', 'CALLS', '1/200/', 1, false);
         SELECT depth, length(_gkg_path), length(_gkg_edge_kinds) FROM ({sql}) ORDER BY depth FORMAT TSV;"
    ));
    assert_eq!(result.trim(), "2\t3\t2\n2\t3\t2\n3\t4\t3\n3\t4\t3");
}
