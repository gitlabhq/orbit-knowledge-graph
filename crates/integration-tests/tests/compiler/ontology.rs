//! Compiler tests using the embedded (production) ontology.

use std::sync::Arc;

use super::setup::{admin_ctx, embedded_ontology, test_ctx};
use super::utils::ParsedSql;
use compiler::{
    ColumnSelection, HydrationPlan, Input, InputNode, QueryType, compile, compile_input,
};

// ─────────────────────────────────────────────────────────────────────────────
// Validation (error path — no SQL produced)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn valid_column_in_order_by() {
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
        "limit": 10,
        "order_by": {"node": "u", "property": "username", "direction": "ASC"}
    }"#;
    assert!(compile(json, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_order_by() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            "limit": 10,
            "order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn valid_column_in_filter() {
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": "admin"}},
        "limit": 10
    }"#;
    assert!(compile(json, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_filter() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"nonexistent_column": "value"}},
            "limit": 10
        }"#,
        &embedded_ontology(), &test_ctx(),
    ).unwrap_err();
    assert!(err.to_string().contains("nonexistent_column"));
}

#[test]
fn valid_column_in_aggregation() {
    assert!(compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
            "aggregations": [{"function": "count", "target": "p", "property": "name", "alias": "name_count"}],
            "limit": 10
        }"#,
        &embedded_ontology(), &test_ctx(),
    ).is_ok());
}

#[test]
fn invalid_column_in_aggregation() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
            "aggregations": [{"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}],
            "limit": 10
        }"#,
        &embedded_ontology(), &test_ctx(),
    ).unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn invalid_entity_type_rejected() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "n", "entity": "NonexistentType", "node_ids": [1], "columns": ["name"]},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("NonexistentType") && err.to_string().contains("is not one of")
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Full pipeline — SQL structure
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn full_pipeline() {
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

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_table("gl_edge"));
    assert_eq!(sql.limit_value(), Some(25));
}

#[test]
fn basic_search_query() {
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username"],
            "filters": { "username": {"op": "eq", "value": "admin"} }
        },
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    // Search uses LIMIT 1 BY dedup: mutable filters stay outside subquery.
    assert!(
        rendered.contains("LIMIT 1 BY"),
        "search should use LIMIT 1 BY for dedup"
    );
    assert!(
        rendered.contains("_deleted"),
        "search should filter deleted rows"
    );
    assert!(rendered.contains("username"));
    assert!(rendered.contains("LIMIT 10"));
    assert!(
        !rendered.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn complex_search_query() {
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username", "state", "created_at"],
            "filters": {
                "username": {"op": "starts_with", "value": "admin"},
                "state": {"op": "in", "value": ["active", "blocked"]},
                "created_at": {"op": "gte", "value": "2024-01-01"}
            }
        },
        "limit": 50,
        "order_by": {"node": "u", "property": "created_at", "direction": "DESC"}
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    // Uses ClickHouse `IN [...]` array syntax which sqlparser can't parse.
    let rendered = result.base.render();

    // Search uses LIMIT 1 BY dedup: mutable filters stay outside subquery.
    assert!(rendered.contains("LIMIT 1 BY"));
    assert!(rendered.contains("_deleted"));
    assert!(rendered.contains("username"));
    assert!(rendered.contains("state"));
    assert!(rendered.contains("created_at"));
    assert!(rendered.contains("ORDER BY"));
    assert!(rendered.contains("DESC"));
    assert!(rendered.contains("LIMIT 50"));
    assert!(
        !rendered.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn search_with_specific_columns() {
    let json = r#"{
        "query_type": "traversal",
        "node": { "id": "u", "entity": "User", "node_ids": [1], "columns": ["username", "state"] },
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_select_column("_gkg_u_id"));
    assert!(sql.has_select_column("_gkg_u_type"));
    assert!(sql.has_select_column("u_username"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn search_with_wildcard_columns() {
    let json = r#"{
        "query_type": "traversal",
        "node": { "id": "u", "entity": "User", "node_ids": [1], "columns": "*" },
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_select_column("_gkg_u_id"));
    assert!(sql.has_select_column("_gkg_u_type"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn traversal_with_columns() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_select_column("_gkg_u_id"));
    assert!(sql.has_select_column("_gkg_u_type"));
    assert!(sql.has_select_column("_gkg_p_id"));
    assert!(sql.has_select_column("_gkg_p_type"));
}

#[test]
fn aggregation_includes_mandatory_columns_for_group_by_node() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_select_column("_gkg_u_id"));
    assert!(sql.has_select_column("_gkg_u_type"));
    assert!(sql.lacks_select_column("_gkg_mr_id"));
    assert!(sql.lacks_select_column("_gkg_mr_type"));
    assert!(sql.has_function("COUNT") || sql.has_function("countIf"));
    assert!(sql.has_group_by());
}

#[test]
fn path_finding_uses_gkg_path_not_node_columns() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "node_ids": [100], "columns": ["name"]},
            {"id": "end", "entity": "Project", "node_ids": [200], "columns": ["name"]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_column_ref("_gkg_path"));
    assert!(result.base.result_context.query_type == Some(QueryType::PathFinding));
}

#[test]
fn result_context_populated() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert_eq!(result.base.result_context.len(), 2);

    let user = result.base.result_context.get("u").unwrap();
    assert_eq!(user.entity_type, "User");
    assert_eq!(user.id_column, "_gkg_u_id");
    assert_eq!(user.type_column, "_gkg_u_type");

    let project = result.base.result_context.get("p").unwrap();
    assert_eq!(project.entity_type, "Project");
    assert_eq!(project.id_column, "_gkg_p_id");
    assert_eq!(project.type_column, "_gkg_p_type");

    assert!(sql.has_select_column("_gkg_u_id"));
    assert!(sql.has_select_column("_gkg_u_type"));
    assert!(sql.has_select_column("_gkg_p_id"));
    assert!(sql.has_select_column("_gkg_p_type"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-hop
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn multi_hop_traversal_generates_union_subquery() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "min_hops": 1, "max_hops": 3}],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_union_all());
    assert!(sql.has_alias("hop_e0"));
    assert!(sql.has_alias("depth") || sql.has_column_ref("depth"));
}

#[test]
fn multi_hop_with_min_hops_filter() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "min_hops": 2, "max_hops": 3}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_column_ref("hop_e0.depth") || sql.has_column_ref("depth"));
}

#[test]
fn single_hop_does_not_generate_recursive_cte() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "n", "entity": "Note", "columns": ["confidential"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n", "min_hops": 1, "max_hops": 1}],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(
        !sql.raw_contains("WITH RECURSIVE"),
        "single hop should not generate recursive CTE"
    );
}

#[test]
fn multi_hop_aggregation() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "min_hops": 1, "max_hops": 2}],
        "aggregations": [{"function": "count", "target": "p", "group_by": "u", "alias": "project_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_union_all());
    assert!(sql.has_alias("hop_e0"));
    assert!(sql.has_function("COUNT") || sql.has_function("countIf"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Redaction columns
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn definition_uses_project_id_for_redaction() {
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "d", "entity": "Definition", "node_ids": [1], "columns": ["name", "project_id"]},
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_select_column("_gkg_d_id"));
    assert!(sql.has_select_column("_gkg_d_type"));
    // Search uses LIMIT 1 BY dedup, so project_id is a plain column reference.
    assert!(
        sql.raw_contains("d.project_id") && sql.raw_contains("_gkg_d_id"),
        "Definition should use project_id for redaction"
    );
}

#[test]
fn project_still_uses_id_for_redaction() {
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]},
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(sql.has_select_column("_gkg_p_id"));
    assert!(
        sql.raw_contains("p.id AS _gkg_p_id"),
        "Project should use id for redaction"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cursor pagination (compiler-level validation)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_pagination_validation() {
    use compiler::QueryError;

    let ontology = embedded_ontology();
    let ctx = test_ctx();

    // Valid cursor: offset + page_size <= limit
    let result = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 20}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "valid cursor should compile: {result:?}");

    // Cursor does not affect SQL — LIMIT comes from the limit field
    let result = result.unwrap();
    let sql = ParsedSql::from_query(&result.base);
    assert_eq!(sql.limit_value(), Some(100));

    // Cursor query emits SETTINGS for CH query cache
    assert!(
        result.base.sql.contains("use_query_cache = 1"),
        "cursor query should enable CH query cache: {}",
        result.base.sql
    );

    // offset + page_size > limit rejected
    let err = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "limit": 10,
        "cursor": {"offset": 5, "page_size": 10}
    }"#,
        &ontology,
        &ctx,
    )
    .unwrap_err();
    assert!(
        matches!(err, QueryError::PaginationError(_)),
        "offset + page_size > limit should be a pagination error, got: {err}"
    );

    // Cursor on traversal compiles fine (pagination is server-side)
    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p"}],
        "limit": 50,
        "cursor": {"offset": 10, "page_size": 20}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(
        result.is_ok(),
        "cursor on traversal should compile: {result:?}"
    );

    // offset + page_size == limit is valid (boundary)
    let result = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "limit": 10,
        "cursor": {"offset": 5, "page_size": 5}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(
        result.is_ok(),
        "offset + page_size == limit should be valid"
    );

    // offset == 0, page_size == limit is valid (full window)
    let result = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "limit": 30,
        "cursor": {"offset": 0, "page_size": 30}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "page_size == limit should be valid");

    // Missing required cursor fields rejected at deserialization
    let err = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "cursor": {"offset": 0}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "cursor missing page_size should fail");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "cursor": {"page_size": 10}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "cursor missing offset should fail");

    // Empty cursor object rejected
    let err = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "cursor": {}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "empty cursor should fail");

    // page_size = 0 rejected (schema minimum: 1)
    let err = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "limit": 10,
        "cursor": {"offset": 0, "page_size": 0}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "page_size = 0 should fail");

    // No cursor: default limit still works, no SETTINGS emitted
    let result = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [1]}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "no cursor should compile fine");
    let result = result.unwrap();
    let sql = ParsedSql::from_query(&result.base);
    assert_eq!(sql.limit_value(), Some(30), "default limit should be 30");
    assert!(
        !result.base.sql.contains("use_query_cache"),
        "non-cursor query should not enable query cache: {}",
        result.base.sql
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Render (parameterized → inlined)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn render_traversal_inlines_all_params() {
    let rendered = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
            {"id": "u", "entity": "User"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "limit": 10
    }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    let sql = ParsedSql::parse(&rendered);
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(sql.raw_contains("'opened'"));
    assert!(sql.raw_contains("'AUTHORED'"));
}

#[test]
fn render_in_filter_inlines_array() {
    let rendered = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "filters": {
            "user_type": {"op": "in", "value": ["project_bot", "service_account"]}
        }},
        "limit": 10
    }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    // Uses ClickHouse `IN [...]` array syntax.
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("'project_bot'") && rendered.contains("'service_account'"));
}

#[test]
fn render_node_ids_inlines_array() {
    let rendered = compile(
        r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "node_ids": [100, 200, 300]},
        "limit": 10
    }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    // Uses ClickHouse `IN [...]` array syntax for node_ids.
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("100") && rendered.contains("200") && rendered.contains("300"));
}

#[test]
fn debug_json_round_trip() {
    let compiled = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
            {"id": "u", "entity": "User"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "limit": 10
    }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();

    // Rendered (inlined) SQL should parse as valid ClickHouse SQL
    ParsedSql::from_query(&compiled.base);

    let debug_json = serde_json::json!({
        "base": compiled.base.sql,
        "base_rendered": compiled.base.render(),
        "hydration": serde_json::json!([]),
    });
    let parsed: serde_json::Value = serde_json::from_str(&debug_json.to_string()).unwrap();
    assert!(
        parsed["base"].as_str().unwrap().contains("{p"),
        "base should have placeholders"
    );
    assert!(
        !parsed["base_rendered"].as_str().unwrap().contains("{p"),
        "rendered should not"
    );
    assert!(parsed["hydration"].is_array());
}

// ─────────────────────────────────────────────────────────────────────────────
// Hydration
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn hydration_query_type_generates_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![
            InputNode {
                id: "hydrate".into(),
                entity: Some("Note".into()),
                table: Some("gl_note".into()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "noteable_type".into(),
                ])),
                node_ids: vec![1, 2, 3],
                ..InputNode::default()
            },
            InputNode {
                id: "hydrate".into(),
                entity: Some("Project".into()),
                table: Some("gl_project".into()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
                node_ids: vec![10, 20],
                ..InputNode::default()
            },
        ],
        limit: 10,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    // Hydration SQL uses ClickHouse array literals (`IN [1,2,3]`) which
    // sqlparser doesn't support yet, so we check the raw SQL string.
    let raw = &result.base.render();

    assert!(raw.contains("UNION ALL"));
    assert!(raw.contains("toJSONString"));
    assert!(raw.contains("gl_note"));
    assert!(raw.contains("gl_project"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn hydration_single_entity_no_union_all() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec!["id".into(), "username".into()])),
            node_ids: vec![42],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(!sql.has_union_all());
    assert!(sql.has_function("toJSONString"));
    assert!(sql.has_table("gl_user"));
}

#[test]
fn hydration_uses_parameterized_ids() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            table: Some("gl_note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
                "created_at".into(),
            ])),
            node_ids: vec![100, 200, 300],
            ..InputNode::default()
        }],
        limit: 3,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    // Hydration SQL uses ClickHouse array literals — check raw strings.
    let parameterized = &result.base.sql;

    assert!(
        parameterized.contains("Array(Int64)"),
        "IDs should be parameterized"
    );
    assert!(
        !parameterized.contains("100"),
        "literal IDs should not appear in parameterized SQL"
    );

    let rendered = result.base.render();
    assert!(rendered.contains("100") && rendered.contains("200") && rendered.contains("300"));
}

#[test]
fn hydration_skips_security_context() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("Note".into()),
            table: Some("gl_note".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "confidential".into(),
            ])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let sql = ParsedSql::from_query(&result.base);

    assert!(
        !sql.has_column_ref("traversal_path"),
        "hydration should skip security filters"
    );
    assert!(
        !sql.has_function("startsWith"),
        "hydration should not have startsWith"
    );
}

#[test]
fn hydration_empty_columns_produces_empty_json() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec!["id".into()])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("map("),
        "empty props should use literal '{{}}', not map()"
    );
}

#[test]
fn hydration_id_column_excluded_from_map() {
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".into(),
            entity: Some("User".into()),
            table: Some("gl_user".into()),
            columns: Some(ColumnSelection::List(vec![
                "id".into(),
                "username".into(),
                "state".into(),
            ])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("'username'") && rendered.contains("'state'"));
    let map_section = rendered
        .split("map(")
        .nth(1)
        .and_then(|s| s.split(')').next())
        .unwrap_or("");
    assert!(
        !map_section.contains("'id'"),
        "map should not contain 'id' key"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// LIKE hardening: rejection at compile time
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn like_rejects_short_contains_pattern() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"username": {"op": "contains", "value": "ab"}}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("search pattern must be at least 3"),
        "expected min length error, got: {err}"
    );
}

#[test]
fn like_rejects_single_char_starts_with() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"username": {"op": "starts_with", "value": "a"}}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("search pattern must be at least 3"),
        "expected min length error, got: {err}"
    );
}

#[test]
fn like_rejects_empty_ends_with() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"username": {"op": "ends_with", "value": ""}}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("search pattern must be at least 3"),
        "expected min length error, got: {err}"
    );
}

#[test]
fn like_rejects_contains_on_email() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"email": {"op": "contains", "value": "example"}}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("LIKE operators"),
        "expected like_allowed rejection, got: {err}"
    );
}

#[test]
fn like_rejects_starts_with_on_email() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"email": {"op": "starts_with", "value": "alice"}}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("LIKE operators"),
        "expected like_allowed rejection, got: {err}"
    );
}

#[test]
fn like_equality_on_email_compiles_for_admin() {
    // `like_allowed: false` blocks LIKE operators but not equality. Admin context
    // is used because User.email is also gated by `admin_only`, which the
    // RestrictPass enforces ahead of like_allowed.
    assert!(
        compile(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"email": "alice@example.com"}},
            "limit": 10
        }"#,
            &embedded_ontology(),
            &admin_ctx(),
        )
        .is_ok()
    );
}

#[test]
fn equality_on_email_rejected_for_non_admin() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"email": "alice@example.com"}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("email") && msg.contains("administrator"),
        "expected admin-only rejection on User.email, got: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// filterable: false — traversal_path blocked at compile time
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn filterable_rejects_traversal_path_starts_with() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/100"}}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("not an allowed value")
            || err.to_string().contains("is not one of")
            || err.to_string().contains("not filterable"),
        "expected traversal_path rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_equality() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project",
                     "filters": {"traversal_path": "1/100/1000/"}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("not an allowed value")
            || err.to_string().contains("is not one of")
            || err.to_string().contains("not filterable"),
        "expected traversal_path rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_on_mr() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "filters": {"traversal_path": "1/100/"}},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("not an allowed value")
            || err.to_string().contains("is not one of")
            || err.to_string().contains("not filterable"),
        "expected traversal_path rejection on MR, got: {err}"
    );
}

#[test]
fn filterable_allows_traversal_path_in_columns() {
    assert!(
        compile(
            r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "columns": ["name", "traversal_path"],
                     "node_ids": [100]},
            "limit": 10
        }"#,
            &embedded_ontology(),
            &test_ctx(),
        )
        .is_ok()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation filter pushdown (Bug 1 regression guard)
//
// Single-aggregate queries with a sort-key filter must push the filter into
// the LIMIT 1 BY dedup subquery so ClickHouse uses the primary-key index to
// skip granules. Without this, the dedup subquery scans the full authorized
// table before the outer countIf filters, costing ~15s on a 5,086-row count
// in production.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn aggregation_count_pushes_project_id_into_dedup_subquery() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "d", "entity": "Definition",
                   "filters": {"project_id": {"op": "eq", "value": 278964}}}],
        "aggregations": [{"function": "count", "target": "d", "alias": "total"}]
    }"#;
    let result = compile(json, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();

    // The countIf must contain the filter (folded as today).
    assert!(
        rendered.contains("countIf"),
        "should fold into countIf: {rendered}"
    );
    // The dedup subquery (the inner SELECT before LIMIT 1 BY) must also
    // carry the project_id filter so the granule index narrows the read.
    let inner = rendered
        .split("LIMIT 1 BY")
        .next()
        .expect("rendered SQL should contain LIMIT 1 BY");
    assert!(
        inner.contains("project_id"),
        "project_id must appear inside the LIMIT 1 BY dedup subquery: {rendered}"
    );
}

#[test]
fn pinned_traversal_narrows_joined_node_via_nf_cte() {
    // Bug 2: when one node has node_ids pinned and joins to another via an
    // edge, the joined-side node table must be narrowed to ids reachable
    // from the pinned source. Without the fix, the joined Definition table
    // dedups the full authorized scope (~tens of millions of rows on
    // production data) before the JOIN.
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "f", "entity": "File", "node_ids": ["12345"], "columns": ["path"]},
            {"id": "d", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "DEFINES", "from": "f", "to": "d"}],
        "limit": 50
    }"#;
    let result = compile(json, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();

    // Both _nf_f (existing) and _nf_d (new) CTEs must be present.
    assert!(
        rendered.contains("_nf_f"),
        "_nf_f CTE must be defined: {rendered}"
    );
    assert!(
        rendered.contains("_nf_d"),
        "_nf_d CTE must be derived from edge filtered by _nf_f: {rendered}"
    );
    // The Definition dedup subquery must filter by _nf_d.
    assert!(
        rendered.contains("d.id IN (SELECT id FROM _nf_d)"),
        "Definition subquery must be narrowed by _nf_d: {rendered}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Code-graph edges (CALLS, EXTENDS) — registered in schema.yaml
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn calls_traversal_compiles_against_embedded_ontology() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "caller", "entity": "Definition", "node_ids": [1], "columns": ["name"]},
            {"id": "callee", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "CALLS should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("'CALLS'"),
        "CALLS relationship_kind should appear in SQL: {rendered}"
    );
}

#[test]
fn aggregation_count_in_clause_pushes_project_id() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "d", "entity": "Definition",
                   "filters": {"project_id": {"op": "in", "value": [69095239, 278964, 74646916]}}}],
        "aggregations": [{"function": "count", "target": "d", "alias": "total"}]
    }"#;
    let result = compile(json, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();

    let inner = rendered
        .split("LIMIT 1 BY")
        .next()
        .expect("rendered SQL should contain LIMIT 1 BY");
    assert!(
        inner.contains("project_id"),
        "project_id IN must appear inside dedup subquery: {rendered}"
    );
}

#[test]
fn extends_traversal_compiles_against_embedded_ontology() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "child", "entity": "Definition", "node_ids": [1], "columns": ["name"]},
            {"id": "parent", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "EXTENDS", "from": "child", "to": "parent"}],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "EXTENDS should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("'EXTENDS'"),
        "EXTENDS relationship_kind should appear in SQL: {rendered}"
    );
}

#[test]
fn calls_to_imported_symbol_variant_compiles() {
    // Exercises the Definition→ImportedSymbol variant declared in calls.yaml
    // (used when a call site targets a symbol that has not been resolved to a
    // concrete definition).
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "caller", "entity": "Definition", "node_ids": [1], "columns": ["name"]},
            {"id": "sym", "entity": "ImportedSymbol", "columns": ["identifier_name"]}
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "sym"}],
        "limit": 10
    }"#;

    assert!(compile(json, &embedded_ontology(), &admin_ctx()).is_ok());
}

#[test]
fn calls_aggregation_compiles() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "caller", "entity": "Definition", "node_ids": [1]},
            {"id": "callee", "entity": "Definition"}
        ],
        "relationships": [{"type": "CALLS", "from": "caller", "to": "callee"}],
        "aggregations": [{"function": "count", "target": "caller", "group_by": "callee", "alias": "callers"}],
        "limit": 1
    }"#;

    assert!(compile(json, &embedded_ontology(), &admin_ctx()).is_ok());
}

#[test]
fn code_graph_edge_union_routes_to_code_table() {
    // CALLS, EXTENDS, DEFINES, IMPORTS all route to gl_code_edge — a UNION
    // ALL across them must touch only that table, never the SDLC gl_edge.
    // Definition is in scope for every edge in this set (CALLS Def→Def,
    // EXTENDS Def→Def, DEFINES Def→Def for nesting, IMPORTS reaches
    // Definition via ImportedSymbol → Definition; we approximate by
    // querying the Def→Def slice that all four edges support).
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "a", "entity": "Definition", "node_ids": [1]},
            {"id": "b", "entity": "Definition"}
        ],
        "relationships": [
            {"type": ["CALLS", "EXTENDS", "DEFINES"], "from": "a", "to": "b"}
        ],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &admin_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "code-graph edges should scan gl_code_edge: {rendered}"
    );
    // Match `gl_edge` only when it is a standalone identifier so the assertion
    // does not get fooled by `gl_code_edge` (which contains the substring
    // `_edge`) or future suffixed table names. `gl_edge` followed by an
    // alphanumeric or underscore is a different identifier and must not flag.
    let mentions_sdlc_edge = rendered.match_indices("gl_edge").any(|(idx, _)| {
        let after = rendered.as_bytes().get(idx + "gl_edge".len()).copied();
        let before = idx
            .checked_sub(1)
            .and_then(|i| rendered.as_bytes().get(i).copied());
        let next_is_ident = matches!(after, Some(b) if b.is_ascii_alphanumeric() || b == b'_');
        let prev_is_ident = matches!(before, Some(b) if b.is_ascii_alphanumeric() || b == b'_');
        !next_is_ident && !prev_is_ident
    });
    assert!(
        !mentions_sdlc_edge,
        "code-graph edges should not touch SDLC gl_edge: {rendered}"
    );
}
