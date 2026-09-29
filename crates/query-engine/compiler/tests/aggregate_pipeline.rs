use std::sync::Arc;

use compiler::{Frontend, Ontology, compile_local};

#[test]
fn both_frontends_aggregate_join_multiplicity_and_sort_groups() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_definition(id BIGINT, name VARCHAR, start_line BIGINT);
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR, target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_definition VALUES (1, 'caller', 1), (2, 'callee', 4), (3, 'other', NULL);
         INSERT INTO gl_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (1, 3, 'Definition', 'Definition', 'CALLS');"
    ).unwrap();

    let json = serde_json::json!({
        "query_type": "aggregation",
        "nodes": [
            { "id": "a", "entity": "Definition", "node_ids": [1] },
            { "id": "b", "entity": "Definition" }
        ],
        "relationships": [{ "type": "CALLS", "from": "a", "to": "b" }],
        "group_by": ["b.name"],
        "aggregations": [{ "count": "b", "as": "n" }, { "sum": "b.start_line", "as": "total" }],
        "aggregation_sort": "-n"
    })
    .to_string();
    let gql = "MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition)
               RETURN b.name, count(b) AS n, sum(b.start_line) AS total ORDER BY n DESC";

    for (query, frontend) in [(json.as_str(), Frontend::JsonDsl), (gql, Frontend::Gql)] {
        let compiled = compile_local(query, frontend, &ontology).unwrap();
        let rows = connection
            .prepare(&compiled.base.render())
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            rows,
            vec![("callee".into(), 2, Some(8)), ("other".into(), 1, None)]
        );
        assert!(compiled.base.result_context.edges().is_empty());
    }
}

#[test]
fn ungrouped_aggregation_returns_one_row_for_empty_input() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE gl_definition(id BIGINT, start_line BIGINT);")
        .unwrap();

    let compiled = compile_local(
        "MATCH (d:Definition) RETURN count(d) AS n, sum(d.start_line) AS total, avg(d.start_line) AS average",
        Frontend::Gql,
        &ontology,
    ).unwrap();
    let rows = connection
        .prepare(&compiled.base.render())
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
            ))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows, vec![(0, None, None)]);
}
