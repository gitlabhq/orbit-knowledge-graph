//! Integration tests for ClickHouse codegen output.
//!
//! Verifies that `compile()` produces structurally valid ClickHouse SQL
//! via sqlparser, not string matching.

use crate::compiler::setup::{test_ctx, test_ontology};
use compiler::compile;
use query_engine_utils::{ParsedSql, has_param_value};

fn parse_ch(json: &str) -> ParsedSql {
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    ParsedSql::from_query(&result.base)
}

#[test]
fn search_produces_valid_sql() {
    let sql = parse_ch(
        r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"]},
        "limit": 10
    }"#,
    );

    assert!(sql.has_table("gl_user"));
    assert!(sql.has_column_ref("username"));
    assert_eq!(sql.limit_value(), Some(10));
}

#[test]
fn traversal_produces_valid_sql() {
    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "n", "entity": "Note", "columns": ["confidential"], "filters": {"confidential": true}},
            {"id": "u", "entity": "User", "columns": ["username"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "limit": 25,
        "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
    }"#,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();

    let sql = ParsedSql::from_query(&result.base);
    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_column_ref("relationship_kind"));
    assert_eq!(sql.limit_value(), Some(25));
    assert!(sql.has_order_by());
    assert!(has_param_value(
        &result.base.params,
        &serde_json::json!("AUTHORED")
    ));
}

#[test]
fn aggregation_produces_valid_sql() {
    let sql = parse_ch(
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
    assert!(sql.has_table("gl_edge"));
}

#[test]
fn path_finding_produces_valid_sql() {
    let sql = parse_ch(
        r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [100]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#,
    );

    assert!(sql.has_cte("forward"));
    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_order_by());
}

#[test]
fn neighbors_produces_valid_sql() {
    let sql = parse_ch(
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

#[test]
fn security_filter_present() {
    let sql = parse_ch(
        r#"{
        "query_type": "search",
        "node": {"id": "p", "entity": "Project", "columns": ["name"]},
        "limit": 10
    }"#,
    );

    assert!(sql.has_column_ref("traversal_path"));
    assert!(sql.has_function("startsWith"));
}
