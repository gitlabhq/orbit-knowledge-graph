use std::sync::Arc;

use compiler::passes::cursor;
use compiler::{Frontend, Ontology, compile_local};

#[test]
fn nullable_sort_pagination_keeps_ties_and_advances_into_nulls() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_file(id BIGINT, path VARCHAR, language VARCHAR);
         INSERT INTO gl_file VALUES
             (1, 'a', 'go'),
             (2, 'b', 'go'),
             (3, 'c', 'rust'),
             (4, 'd', NULL),
             (5, 'e', NULL);",
        )
        .unwrap();

    let mut input = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{ "id": "f", "entity": "File", "columns": ["path"] }],
        "order_by": "f.language",
        "cursor": { "page_size": 2 }
    });
    let hash = cursor::canonical_hash(&input);

    for (keys, expected) in [
        (
            vec![Some("go".into()), Some("1".into())],
            vec!["b", "c", "d"],
        ),
        (vec![Some("rust".into()), Some("3".into())], vec!["d", "e"]),
        (vec![None, Some("4".into())], vec!["e"]),
    ] {
        input["cursor"]["after"] = cursor::encode(hash, &keys).into();
        let compiled = compile_local(&input.to_string(), Frontend::JsonDsl, &ontology).unwrap();
        let rows = connection
            .prepare(&compiled.base.render())
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, expected);
    }
}

#[test]
fn local_pipeline_returns_identity_and_pages_over_unprojected_sort_values() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_file(id BIGINT, path VARCHAR, language VARCHAR);
         INSERT INTO gl_file VALUES
             (1, 'a.rs', 'rust'),
             (2, 'b.rs', 'rust'),
             (3, 'c.go', 'go'),
             (4, 'd.rs', 'rust');",
        )
        .unwrap();

    let mut input = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "f",
            "entity": "File",
            "columns": ["path"],
            "filters": { "language": "rust" }
        }],
        "order_by": "f.id",
        "cursor": { "page_size": 2 }
    });
    let first = compile_local(&input.to_string(), Frontend::JsonDsl, &ontology).unwrap();
    let mut statement = connection.prepare(&first.base.render()).unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![("a.rs".into(), 1), ("b.rs".into(), 2), ("d.rs".into(), 4)]
    );
    assert!(first.base.result_context.get("f").is_some());

    let token = cursor::encode(cursor::canonical_hash(&input), &[Some("2".into())]);
    input["cursor"]["after"] = token.into();
    let next = compile_local(&input.to_string(), Frontend::JsonDsl, &ontology).unwrap();
    let rows = connection
        .prepare(&next.base.render())
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec!["d.rs"]);

    let gql = compile_local(
        "MATCH (f:File) WHERE f.language = 'rust' RETURN f.path ORDER BY f.id LIMIT 2",
        Frontend::Gql,
        &ontology,
    )
    .unwrap();
    let rows = connection
        .prepare(&gql.base.render())
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec!["a.rs", "b.rs", "d.rs"]);
}
