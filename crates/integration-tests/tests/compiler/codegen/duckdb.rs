//! Integration tests for the DuckDB codegen pipeline.
//!
//! These tests run `compile_local` end-to-end: JSON → validate → normalize →
//! lower → DuckDB codegen, and verify the emitted SQL uses DuckDB syntax
//! via structural assertions (sqlparser), not string matching.

use crate::compiler::setup::test_ontology;
use compiler::compile_local;
use query_engine_utils::ParsedSql;

fn compile_duckdb(json: &str) -> compiler::passes::codegen::ParameterizedQuery {
    compile_local(json, &test_ontology()).unwrap().base
}

fn parse_duckdb(json: &str) -> ParsedSql {
    ParsedSql::from_query(&compile_duckdb(json))
}

// ─── Parameter syntax ────────────────────────────────────────────────────────

#[test]
fn search_uses_positional_params() {
    let pq = compile_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"],
                 "filters": {"username": "alice"}},
        "limit": 10
    }"#,
    );

    // Rendered output should inline the param, proving it was a $N placeholder
    let rendered = pq.render();
    assert!(
        rendered.contains("'alice'"),
        "expected inlined param: {}",
        rendered
    );

    // Structural: parsed SQL should have a WHERE clause
    let sql = ParsedSql::from_query(&pq);
    assert!(sql.has_where());
    assert!(sql.has_column_ref("username"));
}

// ─── Function remapping ──────────────────────────────────────────────────────

#[test]
fn no_clickhouse_functions_leak() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "p", "entity": "Project", "columns": ["name"]},
        "limit": 10
    }"#,
    );

    assert!(!sql.has_function("startsWith"));
    assert!(!sql.has_function("has"));
    assert!(!sql.has_function("arrayConcat"));
}

// ─── No SET statements ──────────────────────────────────────────────────────

#[test]
fn no_set_statements_in_output() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
        "limit": 10
    }"#,
    );

    assert!(
        !sql.raw_contains("SET "),
        "DuckDB SQL should not contain SET: {}",
        sql.raw
    );
}

// ─── No security / enforce columns ──────────────────────────────────────────

#[test]
fn no_security_filter_in_output() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "p", "entity": "Project", "columns": ["name"]},
        "limit": 10
    }"#,
    );

    assert!(sql.lacks_column_ref("traversal_path"));
}

// ─── Traversal ──────────────────────────────────────────────────────────────

#[test]
fn traversal_compiles_to_duckdb() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
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

// ─── Aggregation ────────────────────────────────────────────────────────────

#[test]
fn aggregation_compiles_to_duckdb() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User"},
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

// ─── Path finding ───────────────────────────────────────────────────────────

#[test]
fn path_finding_compiles_to_duckdb() {
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

// ─── Neighbors ──────────────────────────────────────────────────────────────

#[test]
fn neighbors_compiles_to_duckdb() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"},
        "limit": 10
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_join());
    assert_eq!(sql.limit_value(), Some(10));
}

// ─── Array IN expansion ─────────────────────────────────────────────────────

#[test]
fn node_ids_filter_expands_params() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "node_ids": [1, 2, 3]},
        "limit": 10
    }"#,
    );

    assert!(sql.has_operator("IN"));
    assert!(
        !sql.raw_contains("Array("),
        "should not contain Array() type: {}",
        sql.raw
    );
}
