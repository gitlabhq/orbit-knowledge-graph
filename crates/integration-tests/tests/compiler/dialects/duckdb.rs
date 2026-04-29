//! DuckDB dialect end-to-end tests.

use crate::compiler::setup::test_ontology;
use crate::compiler::utils::ParsedSql;
use compiler::compile_local;

fn parse_duckdb(json: &str) -> ParsedSql {
    let result = compile_local(json, &test_ontology()).unwrap();
    ParsedSql::from_query(&result.base)
}

#[test]
fn search_uses_positional_params() {
    let result = compile_local(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"],
                 "filters": {"username": "alice"}},
        "limit": 10
    }"#,
        &test_ontology(),
    )
    .unwrap();

    let rendered = result.base.render();
    assert!(
        rendered.contains("'alice'"),
        "expected inlined param: {rendered}"
    );

    let sql = ParsedSql::from_query(&result.base);
    assert!(sql.has_where());
    assert!(sql.has_column_ref("username"));
}

#[test]
fn no_clickhouse_functions_leak() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "node": {"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]},
        "limit": 10
    }"#,
    );

    assert!(!sql.has_function("startsWith"));
    assert!(!sql.has_function("has"));
    assert!(!sql.has_function("arrayConcat"));
}

#[test]
fn no_security_filter() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "node": {"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]},
        "limit": 10
    }"#,
    );

    assert!(sql.lacks_column_ref("traversal_path"));
}

#[test]
fn traversal() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "n", "entity": "Note", "columns": ["confidential"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "limit": 25
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_column_ref("relationship_kind"));
    assert_eq!(sql.limit_value(), Some(25));
}

#[test]
fn aggregation() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "n", "entity": "Note"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "aggregations": [
            {"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}
        ],
        "limit": 10
    }"#,
    );

    assert!(sql.has_function("COUNT"));
    assert!(sql.has_group_by());
}

#[test]
fn path_finding() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [100]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_order_by());
}

#[test]
fn neighbors() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"},
        "limit": 10
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    // Edge-only: no JOIN, edge scan with IN subquery.
    assert_eq!(sql.limit_value(), Some(10));
}

#[test]
fn node_ids_expand_params() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1, 2, 3]},
        "limit": 10
    }"#,
    );

    assert!(sql.has_operator("IN"));
    assert!(!sql.raw_contains("Array("));
}
