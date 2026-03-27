//! ClickHouse dialect end-to-end tests.

use crate::compiler::setup::{compile_to_ast, test_ctx, test_ontology};
use crate::compiler::utils::{ParsedSql, has_param_value};
use compiler::{Node, QueryError, compile};

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
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_column_ref("relationship_kind"));
    assert_eq!(sql.limit_value(), Some(25));
    assert!(has_param_value(
        &result.base.params,
        &serde_json::json!("AUTHORED")
    ));
}

#[test]
fn bool_filter_value_is_preserved() {
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "n",
            "entity": "Note",
            "columns": ["confidential"],
            "filters": { "confidential": true }
        },
        "limit": 5
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(has_param_value(
        &result.base.params,
        &serde_json::Value::Bool(true)
    ));
}

#[test]
fn aggregation_query() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "n", "entity": "Note", "columns": ["confidential"]},
            {"id": "u", "entity": "User", "columns": ["username"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_function("COUNT") || sql.has_function("countIf"));
    assert!(sql.has_group_by());
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
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_cte("forward"), "should have forward CTE");
    assert!(sql.has_cte("backward"), "should have backward CTE");
    assert!(sql.has_union_all());
    assert!(
        sql.has_function("arrayConcat"),
        "paths should be concatenated"
    );
    assert!(
        sql.has_function("tuple"),
        "path nodes should be typed tuples"
    );
    assert!(
        sql.has_column_ref("f.end_id") && sql.has_column_ref("b.end_id"),
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

    let shallow_sql = ParsedSql::from_query(
        &compile(shallow, &test_ontology(), &test_ctx())
            .unwrap()
            .base,
    );
    let deep_sql =
        ParsedSql::from_query(&compile(deep, &test_ontology(), &test_ctx()).unwrap().base);

    assert!(
        shallow_sql.has_cte("forward"),
        "shallow should have forward CTE"
    );
    assert!(
        !shallow_sql.has_cte("backward"),
        "shallow (max_depth=1) should not have backward CTE"
    );
    assert!(deep_sql.has_cte("forward"), "deep should have forward CTE");
    assert!(
        deep_sql.has_cte("backward"),
        "deep (max_depth=3) should have backward CTE"
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
    // Uses ClickHouse `IN [...]` array syntax for node_ids.
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_neighbor_id"));
    assert!(rendered.contains("_gkg_neighbor_type"));
    assert!(rendered.contains("_gkg_relationship_type"));
    assert!(
        rendered.contains("_gkg_neighbor_is_outgoing"),
        "bidirectional should include direction"
    );
    // Edge-only: no JOIN, edge scan with IN subquery for center node IDs.
    assert!(rendered.contains("gl_edge"));
    assert!(rendered.contains("UNION ALL"));
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
    // Uses ClickHouse `IN [...]` array syntax which sqlparser can't parse.
    let rendered = result.base.render();

    // Search uses argMax dedup: value filters move to HAVING,
    // namespace filters stay in WHERE (gl_user has none).
    assert!(rendered.contains("HAVING"));
    assert!(rendered.contains("argMax"));
    assert!(rendered.contains(">="));
    assert!(rendered.contains("IN"));
    assert!(rendered.contains("LIKE"));
}

#[test]
fn invalid_json_rejected() {
    assert!(compile("not valid json", &test_ontology(), &test_ctx()).is_err());
}

#[test]
fn missing_required_fields_rejected() {
    assert!(
        compile(
            r#"{"query_type": "traversal"}"#,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn sql_injection_in_node_id() {
    let err = compile(
        r#"{"query_type": "traversal", "nodes": [{"id": "n; DROP TABLE users; --"}]}"#,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn sql_injection_in_relationship() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [{"type": "REL", "from": "a' OR '1'='1", "to": "b"}]
        }"#,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn empty_node_id_rejected() {
    assert!(
        compile(
            r#"{"query_type": "traversal", "nodes": [{"id": ""}]}"#,
            &test_ontology(),
            &test_ctx(),
        )
        .is_err()
    );
}

#[test]
fn id_starting_with_number_rejected() {
    let err = compile(
        r#"{"query_type": "traversal", "nodes": [{"id": "123abc"}]}"#,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn sql_injection_in_filter_property() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"foo; DROP TABLE--": "value"}}]
        }"#,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)));
}

#[test]
fn valid_identifiers_produce_parseable_sql() {
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
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    ParsedSql::from_query(&result.base);
}
