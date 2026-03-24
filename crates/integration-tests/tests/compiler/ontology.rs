//! Compiler tests using the embedded (production) ontology.

use super::setup::{embedded_ontology, test_ctx};
use compiler::{
    ColumnSelection, HydrationPlan, Input, InputNode, QueryError, QueryType, compile, compile_input,
};

#[test]
fn valid_column_in_order_by() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"]},
        "limit": 10,
        "order_by": {"node": "u", "property": "username", "direction": "ASC"}
    }"#;
    assert!(compile(json, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_order_by() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"]},
        "limit": 10,
        "order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
    }"#;
    let err = compile(json, &embedded_ontology(), &test_ctx()).unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn valid_column_in_filter() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": "admin"}},
        "limit": 10
    }"#;
    assert!(compile(json, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_filter() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"nonexistent_column": "value"}},
        "limit": 10
    }"#;
    let err = compile(json, &embedded_ontology(), &test_ctx()).unwrap_err();
    assert!(
        err.to_string().contains("nonexistent_column"),
        "expected error mentioning invalid column name, got: {err}"
    );
}

#[test]
fn valid_column_in_aggregation() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}],
        "aggregations": [{"function": "count", "target": "p", "property": "name", "alias": "name_count"}],
        "limit": 10
    }"#;
    assert!(compile(json, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_aggregation() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}],
        "aggregations": [{"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}],
        "limit": 10
    }"#;
    let err = compile(json, &embedded_ontology(), &test_ctx()).unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn invalid_entity_type_rejected() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "n", "entity": "NonexistentType", "columns": ["name"]},
        "limit": 10
    }"#;
    let err = compile(json, &embedded_ontology(), &test_ctx()).unwrap_err();
    assert!(
        err.to_string().contains("NonexistentType") && err.to_string().contains("is not one of"),
        "expected validation error with valid options: {}",
        err
    );
}

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
    assert!(result.base.sql.contains("SELECT"));
    assert!(result.base.sql.contains("gl_edge"));
    assert!(result.base.sql.contains("LIMIT 25"));
}

#[test]
fn basic_search_query() {
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username"],
            "filters": {
                "username": {"op": "eq", "value": "admin"}
            }
        },
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(result.base.sql.contains("SELECT"));
    assert!(result.base.sql.contains("FROM"));
    assert!(result.base.sql.contains("WHERE"));
    assert!(result.base.sql.contains("username"));
    assert!(result.base.sql.contains("LIMIT 10"));
    assert!(
        !result.base.sql.contains("JOIN"),
        "search queries should not have joins"
    );
}

#[test]
fn complex_search_query() {
    let json = r#"{
        "query_type": "search",
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

    assert!(result.base.sql.contains("SELECT"));
    assert!(result.base.sql.contains("WHERE"));
    assert!(result.base.sql.contains("username"));
    assert!(result.base.sql.contains("state"));
    assert!(result.base.sql.contains("created_at"));
    assert!(result.base.sql.contains("ORDER BY"));
    assert!(result.base.sql.contains("DESC"));
    assert!(result.base.sql.contains("LIMIT 50"));
    assert!(
        !result.base.sql.contains("JOIN"),
        "search queries should not have joins"
    );
    assert!(result.base.sql.contains("AND"));
}

#[test]
fn search_with_specific_columns() {
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username", "state"]
        },
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(result.base.sql.contains("_gkg_u_id"));
    assert!(result.base.sql.contains("_gkg_u_type"));
    assert!(result.base.sql.contains("u_username"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn search_with_wildcard_columns() {
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": "*"
        },
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(result.base.sql.contains("_gkg_u_id"));
    assert!(result.base.sql.contains("_gkg_u_type"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn traversal_with_columns() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(result.base.sql.contains("_gkg_u_id"));
    assert!(result.base.sql.contains("_gkg_u_type"));
    assert!(result.base.sql.contains("_gkg_p_id"));
    assert!(result.base.sql.contains("_gkg_p_type"));
}

#[test]
fn aggregation_includes_mandatory_columns_for_group_by_node() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(result.base.sql.contains("_gkg_u_id"));
    assert!(result.base.sql.contains("_gkg_u_type"));
    assert!(!result.base.sql.contains("_gkg_mr_id"));
    assert!(!result.base.sql.contains("_gkg_mr_type"));
    assert!(result.base.sql.contains("COUNT"));
    assert!(result.base.sql.contains("GROUP BY"));
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

    assert!(result.base.sql.contains("_gkg_path"));
    assert!(result.base.result_context.query_type == Some(QueryType::PathFinding));
}

#[test]
fn result_context_populated() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert_eq!(result.base.result_context.len(), 2);

    let user = result.base.result_context.get("u").unwrap();
    assert_eq!(user.entity_type, "User");
    assert_eq!(user.id_column, "_gkg_u_id");
    assert_eq!(user.type_column, "_gkg_u_type");

    let project = result.base.result_context.get("p").unwrap();
    assert_eq!(project.entity_type, "Project");
    assert_eq!(project.id_column, "_gkg_p_id");
    assert_eq!(project.type_column, "_gkg_p_type");

    assert!(result.base.sql.contains("_gkg_u_id"));
    assert!(result.base.sql.contains("_gkg_u_type"));
    assert!(result.base.sql.contains("_gkg_p_id"));
    assert!(result.base.sql.contains("_gkg_p_type"));
}

#[test]
fn multi_hop_traversal_generates_union_subquery() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{
            "type": "MEMBER_OF",
            "from": "u",
            "to": "p",
            "min_hops": 1,
            "max_hops": 3
        }],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(
        result.base.sql.contains("UNION ALL"),
        "expected UNION ALL for unrolled multi-hop: {}",
        result.base.sql
    );
    assert!(
        result.base.sql.contains("AS hop_e0"),
        "expected hop_e0 subquery alias: {}",
        result.base.sql
    );
    assert!(
        result.base.sql.contains("AS depth"),
        "expected depth column: {}",
        result.base.sql
    );
}

#[test]
fn multi_hop_with_min_hops_filter() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{
            "type": "MEMBER_OF",
            "from": "u",
            "to": "p",
            "min_hops": 2,
            "max_hops": 3
        }],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(
        result.base.sql.contains("hop_e0.depth"),
        "expected depth reference: {}",
        result.base.sql
    );
}

#[test]
fn single_hop_does_not_generate_cte() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "n", "entity": "Note", "columns": ["confidential"]}
        ],
        "relationships": [{
            "type": "AUTHORED",
            "from": "u",
            "to": "n",
            "min_hops": 1,
            "max_hops": 1
        }],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(
        !result.base.sql.contains("WITH RECURSIVE"),
        "single hop should not generate CTE: {}",
        result.base.sql
    );
}

#[test]
fn multi_hop_aggregation() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "p", "entity": "Project", "columns": ["name"]}
        ],
        "relationships": [{
            "type": "MEMBER_OF",
            "from": "u",
            "to": "p",
            "min_hops": 1,
            "max_hops": 2
        }],
        "aggregations": [{"function": "count", "target": "p", "group_by": "u", "alias": "project_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(
        result.base.sql.contains("UNION ALL"),
        "aggregation should support multi-hop with union: {}",
        result.base.sql
    );
    assert!(
        result.base.sql.contains("AS hop_e0"),
        "expected hop_e0 subquery alias: {}",
        result.base.sql
    );
    assert!(
        result.base.sql.contains("COUNT"),
        "expected COUNT in query: {}",
        result.base.sql
    );
}

#[test]
fn definition_uses_project_id_for_redaction() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "d", "entity": "Definition", "columns": ["name", "project_id"]},
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(
        result.base.sql.contains("d.project_id AS _gkg_d_id"),
        "Definition should use project_id for redaction ID: {}",
        result.base.sql
    );
    assert!(result.base.sql.contains("_gkg_d_type"));
}

#[test]
fn project_still_uses_id_for_redaction() {
    let json = r#"{
        "query_type": "search",
        "node": {"id": "p", "entity": "Project", "columns": ["name"]},
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();

    assert!(
        result.base.sql.contains("p.id AS _gkg_p_id"),
        "Project should use id for redaction ID: {}",
        result.base.sql
    );
}

#[test]
fn range_pagination() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();

    let result = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "range": {"start": 40, "end": 50}
        }"#,
        &ontology,
        &ctx,
    )
    .unwrap();
    assert!(result.base.sql.contains("LIMIT 10"), "{}", result.base.sql);
    assert!(result.base.sql.contains("OFFSET 40"), "{}", result.base.sql);

    let result = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p"}],
            "range": {"start": 0, "end": 30},
            "order_by": {"node": "u", "property": "created_at", "direction": "DESC"}
        }"#,
        &ontology,
        &ctx,
    )
    .unwrap();
    assert!(result.base.sql.contains("LIMIT 30"), "{}", result.base.sql);
    assert!(result.base.sql.contains("OFFSET 0"), "{}", result.base.sql);
    assert!(result.base.sql.contains("ORDER BY"));
    assert!(result.base.sql.contains("DESC"));

    let err = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 10,
            "range": {"start": 0, "end": 5}
        }"#,
        &ontology,
        &ctx,
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::Validation(_)), "{err}");

    let err = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "range": {"start": 10, "end": 10}
        }"#,
        &ontology,
        &ctx,
    )
    .unwrap_err();
    assert!(err.to_string().contains("must be greater than"), "{err}");

    let err = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "range": {"start": 0, "end": 1001}
        }"#,
        &ontology,
        &ctx,
    )
    .unwrap_err();
    assert!(err.to_string().contains("must not exceed 1000"), "{err}");

    assert!(
        compile(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "range": {"start": 0, "end": 1000}
        }"#,
            &ontology,
            &ctx,
        )
        .is_ok()
    );
}

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

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders: {rendered}"
    );
    assert!(rendered.contains("'opened'"));
    assert!(rendered.contains("'AUTHORED'"));
}

#[test]
fn render_in_filter_inlines_array() {
    let rendered = compile(
        r#"{
            "query_type": "search",
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

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders: {rendered}"
    );
    assert!(
        rendered.contains("['project_bot', 'service_account']"),
        "should inline array: {rendered}"
    );
}

#[test]
fn render_node_ids_inlines_array() {
    let rendered = compile(
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "node_ids": [100, 200, 300]},
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();

    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders: {rendered}"
    );
    assert!(
        rendered.contains("[100, 200, 300]"),
        "should inline node_ids: {rendered}"
    );
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

    let debug_json = serde_json::json!({
        "base": compiled.base.sql,
        "base_rendered": compiled.base.render(),
        "hydration": serde_json::json!([]),
    });

    let serialized = debug_json.to_string();
    let parsed: serde_json::Value = serde_json::from_str(&serialized).expect("should round-trip");

    let base = parsed["base"].as_str().unwrap();
    let rendered = parsed["base_rendered"].as_str().unwrap();

    assert!(base.contains("{p"), "base should have placeholders");
    assert!(
        !rendered.contains("{p"),
        "rendered should have no placeholders"
    );
    assert!(parsed["hydration"].is_array());
}

#[test]
fn hydration_query_type_generates_union_all() {
    let ctx = test_ctx();

    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![
            InputNode {
                id: "hydrate".to_string(),
                entity: Some("Note".to_string()),
                table: Some("gl_note".to_string()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "noteable_type".into(),
                ])),
                node_ids: vec![1, 2, 3],
                ..InputNode::default()
            },
            InputNode {
                id: "hydrate".to_string(),
                entity: Some("Project".to_string()),
                table: Some("gl_project".to_string()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
                node_ids: vec![10, 20],
                ..InputNode::default()
            },
        ],
        limit: 10,
        ..Input::default()
    };

    let result = compile_input(input, &ctx).unwrap();
    let sql = &result.base.sql;

    assert!(sql.contains("UNION ALL"), "should contain UNION ALL");
    assert!(sql.contains("toJSONString"), "should contain toJSONString");
    assert!(sql.contains("gl_note"), "should reference gl_note");
    assert!(sql.contains("gl_project"), "should reference gl_project");
    assert!(
        matches!(result.hydration, HydrationPlan::None),
        "hydration query should not trigger further hydration"
    );
}

#[test]
fn hydration_single_entity_no_union_all() {
    let ctx = test_ctx();

    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".to_string(),
            entity: Some("User".to_string()),
            table: Some("gl_user".to_string()),
            columns: Some(ColumnSelection::List(vec!["id".into(), "username".into()])),
            node_ids: vec![42],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &ctx).unwrap();
    let sql = &result.base.sql;

    assert!(
        !sql.contains("UNION ALL"),
        "single entity should not UNION ALL"
    );
    assert!(
        sql.contains("toJSONString"),
        "should still use toJSONString"
    );
    assert!(sql.contains("gl_user"), "should reference gl_user");
}

#[test]
fn hydration_uses_parameterized_ids() {
    let ctx = test_ctx();

    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".to_string(),
            entity: Some("Note".to_string()),
            table: Some("gl_note".to_string()),
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

    let result = compile_input(input, &ctx).unwrap();
    let sql = &result.base.sql;

    assert!(
        sql.contains("Array(Int64)"),
        "IDs should be parameterized as Array(Int64), got: {sql}"
    );
    assert!(
        !sql.contains("100"),
        "literal IDs should not appear in parameterized SQL"
    );

    let rendered = result.base.render();
    assert!(
        rendered.contains("100") && rendered.contains("200") && rendered.contains("300"),
        "rendered SQL should inline the IDs"
    );
}

#[test]
fn hydration_skips_security_context() {
    let ctx = test_ctx();

    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".to_string(),
            entity: Some("Note".to_string()),
            table: Some("gl_note".to_string()),
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

    let result = compile_input(input, &ctx).unwrap();
    let sql = &result.base.sql;

    assert!(
        !sql.contains("traversal_path"),
        "hydration should skip security filters, got: {sql}"
    );
    assert!(
        !sql.contains("startsWith"),
        "hydration should not have startsWith filter"
    );
}

#[test]
fn hydration_empty_columns_produces_empty_json() {
    let ctx = test_ctx();

    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".to_string(),
            entity: Some("User".to_string()),
            table: Some("gl_user".to_string()),
            columns: Some(ColumnSelection::List(vec!["id".into()])),
            node_ids: vec![1],
            ..InputNode::default()
        }],
        limit: 1,
        ..Input::default()
    };

    let result = compile_input(input, &ctx).unwrap();
    let rendered = result.base.render();

    assert!(
        !rendered.contains("map("),
        "empty props should use literal '{{}}', not map(): {rendered}"
    );
}

#[test]
fn hydration_id_column_excluded_from_map() {
    let ctx = test_ctx();

    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![InputNode {
            id: "hydrate".to_string(),
            entity: Some("User".to_string()),
            table: Some("gl_user".to_string()),
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

    let result = compile_input(input, &ctx).unwrap();
    let rendered = result.base.render();

    assert!(
        rendered.contains("'username'") && rendered.contains("'state'"),
        "map should contain username and state"
    );

    let map_section = rendered
        .split("map(")
        .nth(1)
        .and_then(|s| s.split(')').next())
        .unwrap_or("");
    assert!(
        !map_section.contains("'id'"),
        "map should not contain 'id' key (it's the PK, selected separately)"
    );
}
