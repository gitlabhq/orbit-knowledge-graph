//! Integration tests for the DuckDB codegen pipeline.
//!
//! These tests run `compile_local` end-to-end: JSON → validate → normalize →
//! lower → DuckDB codegen, and verify the emitted SQL uses DuckDB syntax.

use super::setup::test_ontology;
use compiler::compile_local;

fn compile_duckdb(json: &str) -> compiler::passes::codegen::ParameterizedQuery {
    compile_local(json, &test_ontology()).unwrap().base
}

// ─── Parameter syntax ────────────────────────────────────────────────────────

#[test]
fn search_uses_positional_params() {
    let result = compile_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"],
                 "filters": {"username": "alice"}},
        "limit": 10
    }"#,
    );

    assert!(
        result.sql.contains('$'),
        "expected positional $N params: {}",
        result.sql
    );
    assert!(
        !result.sql.contains("{p"),
        "should not contain CH-style params: {}",
        result.sql
    );
}

// ─── Function remapping ──────────────────────────────────────────────────────

#[test]
fn traversal_path_filter_uses_starts_with() {
    let result = compile_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "p", "entity": "Project", "columns": ["name"]},
        "limit": 10
    }"#,
    );

    // Security filter is skipped in local pipeline, but if startsWith appears
    // from any other path it should be remapped. Verify no CH functions leak.
    assert!(
        !result.sql.contains("startsWith("),
        "should not contain CH function startsWith: {}",
        result.sql
    );
}

// ─── No SET statements ──────────────────────────────────────────────────────

#[test]
fn no_set_statements_in_output() {
    let result = compile_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
        "limit": 10
    }"#,
    );

    assert!(
        !result.sql.contains("SET "),
        "DuckDB SQL should not contain SET: {}",
        result.sql
    );
}

// ─── No security / enforce columns ──────────────────────────────────────────

#[test]
fn no_security_filter_in_output() {
    let result = compile_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "p", "entity": "Project", "columns": ["name"]},
        "limit": 10
    }"#,
    );

    assert!(
        !result.sql.contains("traversal_path"),
        "local pipeline should skip security filter: {}",
        result.sql
    );
}

// ─── Traversal ──────────────────────────────────────────────────────────────

#[test]
fn traversal_compiles_to_duckdb() {
    let result = compile_duckdb(
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

    assert!(
        result.sql.contains("gl_edge"),
        "traversal should reference edge table: {}",
        result.sql
    );
    assert!(
        result.sql.contains('$'),
        "should use positional params: {}",
        result.sql
    );
    assert!(
        !result.sql.contains("{p"),
        "should not contain CH params: {}",
        result.sql
    );
}

// ─── Aggregation ────────────────────────────────────────────────────────────

#[test]
fn aggregation_compiles_to_duckdb() {
    let result = compile_duckdb(
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

    assert!(
        result.sql.contains("COUNT"),
        "should contain COUNT: {}",
        result.sql
    );
    assert!(
        result.sql.contains("GROUP BY"),
        "should contain GROUP BY: {}",
        result.sql
    );
    assert!(
        result.sql.contains('$'),
        "should use positional params: {}",
        result.sql
    );
}

// ─── Path finding ───────────────────────────────────────────────────────────

#[test]
fn path_finding_compiles_to_duckdb() {
    let result = compile_duckdb(
        r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [100]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#,
    );

    assert!(
        result.sql.contains('$'),
        "should use positional params: {}",
        result.sql
    );
    assert!(
        !result.sql.contains("{p"),
        "should not contain CH params: {}",
        result.sql
    );
}

// ─── Neighbors ──────────────────────────────────────────────────────────────

#[test]
fn neighbors_compiles_to_duckdb() {
    let result = compile_duckdb(
        r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"},
        "limit": 10
    }"#,
    );

    assert!(
        result.sql.contains('$'),
        "should use positional params: {}",
        result.sql
    );
    assert!(
        result.sql.contains("gl_edge"),
        "should reference edge table: {}",
        result.sql
    );
}

// ─── Array IN expansion ─────────────────────────────────────────────────────

#[test]
fn node_ids_filter_expands_params() {
    let result = compile_duckdb(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "node_ids": [1, 2, 3]},
        "limit": 10
    }"#,
    );

    // Each node_id should be a separate positional param
    assert!(
        result.sql.contains("$1") && result.sql.contains("$2") && result.sql.contains("$3"),
        "expected 3 positional params for node_ids: {}",
        result.sql
    );
    assert!(
        !result.sql.contains("Array("),
        "should not contain Array() type: {}",
        result.sql
    );
}
