use std::sync::Arc;

use compiler::input::Input;
use compiler::lowering::{lower, scalar};
use compiler::passes::{codegen, enforce::ResultContext, frontend::gql};
use compiler::planning::{bind, physical::CurrentRows};
use query_data_model::DuckDbDataModel;

#[test]
fn both_frontends_filter_before_sort_and_limit_without_projecting_sort_keys() {
    let model =
        DuckDbDataModel::derive(Arc::new(compiler::Ontology::load_embedded().unwrap())).unwrap();

    let json: Input = serde_json::from_str(
        r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "f",
            "entity": "File",
            "columns": ["path"],
            "node_ids": [1, 2, 3, 4],
            "filters": {"language": "rust"}
        }],
        "order_by": "-f.id",
        "limit": 2
    }"#,
    )
    .unwrap();
    let (gql, _) = gql::parse_with_hash(
        "MATCH (f:File)
         WHERE f.id IN [1, 2, 3, 4] AND f.language = 'rust'
         RETURN f.path ORDER BY f.id DESC LIMIT 2",
    )
    .unwrap();

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_file(id BIGINT, path VARCHAR, language VARCHAR);
         INSERT INTO gl_file VALUES
             (1, 'a.rs', 'rust'),
             (2, 'b.rs', 'rust'),
             (3, 'c.rs', 'rust'),
             (4, 'd.go', 'go'),
             (5, 'e.rs', 'rust');",
        )
        .unwrap();

    for input in [json, gql] {
        let bound = bind::traversal(&input, &model, CurrentRows::Snapshot).unwrap();
        let query = lower(&bound.root, &bound.values, &scalar::emit)
            .unwrap()
            .into_query(&bound.outputs)
            .unwrap();
        let compiled = codegen::duckdb::codegen(
            &compiler::Node::Query(Box::new(query)),
            ResultContext::new(),
        )
        .unwrap();
        let mut statement = connection.prepare(&compiled.render()).unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, vec!["c.rs", "b.rs"]);
        assert_eq!(statement.column_names(), vec!["f_path"]);
    }
}

#[test]
fn binding_rejects_unavailable_fields_and_wrong_literal_types() {
    let model =
        DuckDbDataModel::derive(Arc::new(compiler::Ontology::load_embedded().unwrap())).unwrap();

    for query in [
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "f", "entity": "File", "columns": ["content"]}]
        }"#,
        r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "f",
                "entity": "File",
                "columns": ["id"],
                "filters": {"id": "wrong"}
            }]
        }"#,
    ] {
        let input = serde_json::from_str(query).unwrap();
        assert!(bind::traversal(&input, &model, CurrentRows::Snapshot).is_err());
    }
}

#[test]
fn range_membership_and_null_filters_preserve_sql_semantics() {
    let model =
        DuckDbDataModel::derive(Arc::new(compiler::Ontology::load_embedded().unwrap())).unwrap();
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_file(id BIGINT, language VARCHAR);
        INSERT INTO gl_file VALUES (1, NULL), (2, 'rust'), (3, 'go'), (4, NULL);",
        )
        .unwrap();

    for (filter, expected) in [
        (r#"{"in": ["rust", "go"]}"#, vec![2, 3]),
        (r#"{"in": []}"#, vec![]),
        (r#"{"is_null": true}"#, vec![4]),
        (r#"{"is_not_null": true}"#, vec![2, 3]),
    ] {
        let input = serde_json::from_value(serde_json::json!({
            "query_type": "traversal",
            "nodes": [{
                "id": "f",
                "entity": "File",
                "columns": ["id"],
                "id_range": { "start": 2, "end": 4 },
                "filters": {
                    "language": serde_json::from_str::<serde_json::Value>(filter).unwrap()
                }
            }],
            "order_by": "f.id"
        }))
        .unwrap();
        let bound = bind::traversal(&input, &model, CurrentRows::Snapshot).unwrap();
        let query = lower(&bound.root, &bound.values, &scalar::emit)
            .unwrap()
            .into_query(&bound.outputs)
            .unwrap();
        let compiled = codegen::duckdb::codegen(
            &compiler::Node::Query(Box::new(query)),
            ResultContext::new(),
        )
        .unwrap();
        let rows = connection
            .prepare(&compiled.render())
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(rows, expected, "{filter}");
    }
}
