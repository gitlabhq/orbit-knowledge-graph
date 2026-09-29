use std::sync::Arc;

use compiler::passes::cursor;
use compiler::{Frontend, Ontology, compile_local};

#[test]
fn monthly_groups_preserve_nulls_and_aliases() {
    let ontology = Arc::new(Ontology::new().with_nodes(["Event"]).with_fields(
        "Event",
        [
            ("id", ontology::DataType::Int),
            ("created_at", ontology::DataType::DateTime),
        ],
    ));
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_event(id BIGINT, created_at TIMESTAMP);
         INSERT INTO gl_event VALUES
             (1, '2026-01-02 10:00:00'),
             (2, '2026-01-31 23:00:00'),
             (3, '2026-02-01 00:00:00'),
             (4, NULL);",
        )
        .unwrap();

    let query = serde_json::json!({
        "query_type": "aggregation",
        "nodes": [{ "id": "e", "entity": "Event", "node_ids": [1, 2, 3, 4] }],
        "group_by": [{ "key": "e.created_at", "truncate": "month", "as": "month" }],
        "aggregations": [{ "count": "e", "as": "n" }],
        "aggregation_sort": "-n"
    });
    let compiled = compile_local(&query.to_string(), Frontend::JsonDsl, &ontology).unwrap();
    let sql = format!(
        "SELECT CAST(month AS VARCHAR), n FROM ({})",
        compiled.base.render()
    );
    let mut rows = connection
        .prepare(&sql)
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            (None, 1),
            (Some("2026-01-01 00:00:00".into()), 2),
            (Some("2026-02-01 00:00:00".into()), 1),
        ]
    );
}

#[test]
fn aggregate_cursor_seeks_after_tied_counts_and_null_groups() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_definition(id BIGINT, name VARCHAR);
         INSERT INTO gl_definition VALUES
             (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, NULL), (6, NULL), (7, 'c');",
        )
        .unwrap();

    let mut query = serde_json::json!({
        "query_type": "aggregation",
        "nodes": [{ "id": "d", "entity": "Definition" }],
        "group_by": ["d.name"],
        "aggregations": [{ "count": "d", "as": "n" }],
        "aggregation_sort": "-n",
        "cursor": { "page_size": 1 }
    });
    let hash = cursor::canonical_hash(&query);

    for (keys, expected) in [
        (
            vec![Some("2".into()), Some("a".into())],
            vec![(Some("b".into()), 2), (None, 2)],
        ),
        (vec![Some("2".into()), None], vec![(Some("c".into()), 1)]),
    ] {
        query["cursor"]["after"] = cursor::encode(hash, &keys).into();
        let compiled = compile_local(&query.to_string(), Frontend::JsonDsl, &ontology).unwrap();
        let rows = connection
            .prepare(&compiled.base.render())
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, expected);
    }
}

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
