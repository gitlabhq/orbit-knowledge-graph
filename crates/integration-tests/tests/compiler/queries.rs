//! Compiler unit tests using a hand-built ontology.

use super::setup::{compile_to_ast, test_ctx, test_ontology};
use compiler::{compile, Node, QueryError};

#[test]
fn compile_to_ast_works() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"]},
        "limit": 10
    }"#;

    let node = compile_to_ast(json, &test_ontology()).unwrap();
    let Node::Query(ref q) = node;
    assert_eq!(q.limit, Some(10));
    assert!(!q.select.is_empty());
}

#[test]
fn traversal_query() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "n", "entity": "Note", "columns": ["confidential"], "filters": {"confidential": true}},
            {"id": "u", "entity": "User", "columns": ["username"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "limit": 25,
        "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();

    assert!(result.base.sql.contains("SELECT"));
    assert!(result.base.sql.contains("gl_edge"));
    assert!(
        result.base.sql.contains("relationship_kind"),
        "expected relationship_kind filter: {}",
        result.base.sql
    );
    assert!(result.base.sql.contains("LIMIT 25"));
    assert!(
        result
            .base
            .params
            .values()
            .any(|p| p.value == serde_json::json!("AUTHORED")),
        "expected AUTHORED in params: {:?}",
        result.base.params
    );
}

#[test]
fn bool_filter_value_is_preserved() {
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "n",
            "entity": "Note",
            "columns": ["confidential"],
            "filters": {
                "confidential": true
            }
        },
        "limit": 5
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(
        result
            .base
            .params
            .values()
            .any(|p| p.value == serde_json::Value::Bool(true)),
        "expected boolean filter to remain true in params: {:?}",
        result.base.params
    );
}

#[test]
fn aggregation_query() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "n", "entity": "Note", "columns": ["confidential"]}, {"id": "u", "entity": "User", "columns": ["username"]}],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(result.base.sql.contains("COUNT"));
    assert!(result.base.sql.contains("GROUP BY"));
}

#[test]
fn path_finding_query() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [100]},
            {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [200]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();

    assert!(
        result.base.sql.contains("forward AS"),
        "should have forward CTE"
    );
    assert!(
        result.base.sql.contains("backward AS"),
        "should have backward CTE"
    );
    assert!(result.base.sql.contains("UNION ALL"));
    assert!(
        result.base.sql.contains("arrayConcat"),
        "paths should be concatenated"
    );
    assert!(
        result.base.sql.contains("tuple"),
        "path nodes should be typed tuples"
    );
    assert!(
        result.base.sql.contains("f.end_id") && result.base.sql.contains("b.end_id"),
        "should join forward and backward on end_id"
    );
}

#[test]
fn path_finding_depth_control() {
    let shallow = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [1]},
            {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [2]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 1}
    }"#;

    let deep = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [1]},
            {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [2]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let shallow_result = compile(shallow, &test_ontology(), &test_ctx()).unwrap();
    let deep_result = compile(deep, &test_ontology(), &test_ctx()).unwrap();

    assert!(
        shallow_result.base.sql.contains("WITH forward AS"),
        "shallow should have forward CTE"
    );
    assert!(
        !shallow_result.base.sql.contains("backward AS"),
        "shallow (max_depth=1) should not have backward CTE"
    );
    assert!(
        deep_result.base.sql.contains("forward AS"),
        "deep should have forward CTE"
    );
    assert!(
        deep_result.base.sql.contains("backward AS"),
        "deep (max_depth=3) should have backward CTE"
    );
    assert!(
        deep_result.base.sql.len() > shallow_result.base.sql.len(),
        "deeper max_depth should produce more SQL"
    );
}

#[test]
fn neighbors_query() {
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [100]},
        "neighbors": {"node": "u", "direction": "both"}
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(result.base.sql.contains("SELECT"));
    assert!(result.base.sql.contains("_gkg_neighbor_id"));
    assert!(result.base.sql.contains("_gkg_neighbor_type"));
    assert!(result.base.sql.contains("_gkg_relationship_type"));
    assert!(
        result.base.sql.contains("_gkg_neighbor_is_outgoing"),
        "bidirectional neighbor query should include direction column: {}",
        result.base.sql
    );
    assert!(result.base.sql.contains("INNER JOIN"));
}

#[test]
fn filter_operators() {
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username", "state", "created_at"],
            "filters": {
                "created_at": {"op": "gte", "value": "2024-01-01"},
                "state": {"op": "in", "value": ["active", "blocked"]},
                "username": {"op": "contains", "value": "admin"}
            }
        },
        "limit": 30
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(result.base.sql.contains("WHERE"));
    assert!(result.base.sql.contains(">="));
    assert!(result.base.sql.contains("IN"));
    assert!(result.base.sql.contains("LIKE"));
}

#[test]
fn invalid_json_rejected() {
    assert!(compile("not valid json", &test_ontology(), &test_ctx()).is_err());
}

#[test]
fn missing_required_fields_rejected() {
    let result = compile(
        r#"{"query_type": "traversal"}"#,
        &test_ontology(),
        &test_ctx(),
    );
    assert!(result.is_err());
}

#[test]
fn sql_injection_in_node_id() {
    let json = r#"{"query_type": "traversal", "nodes": [{"id": "n; DROP TABLE users; --"}]}"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn sql_injection_in_relationship() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "a"}, {"id": "b"}],
        "relationships": [{"type": "REL", "from": "a' OR '1'='1", "to": "b"}]
    }"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn empty_node_id_rejected() {
    let json = r#"{"query_type": "traversal", "nodes": [{"id": ""}]}"#;
    assert!(compile(json, &test_ontology(), &test_ctx()).is_err());
}

#[test]
fn id_starting_with_number_rejected() {
    let json = r#"{"query_type": "traversal", "nodes": [{"id": "123abc"}]}"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn sql_injection_in_filter_property() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "filters": {"foo; DROP TABLE--": "value"}}]
    }"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn valid_identifiers_accepted() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "user_node", "entity": "User", "columns": ["username"]},
            {"id": "_private", "entity": "Note", "columns": ["confidential"]},
            {"id": "CamelCase", "entity": "Project", "columns": ["name"]},
            {"id": "node123", "entity": "Group", "columns": ["name"]}
        ],
        "relationships": [
            {"type": "AUTHORED", "from": "user_node", "to": "_private"},
            {"type": "CONTAINS", "from": "CamelCase", "to": "_private"},
            {"type": "MEMBER_OF", "from": "user_node", "to": "node123"}
        ]
    }"#;
    assert!(compile(json, &test_ontology(), &test_ctx()).is_ok());
}
