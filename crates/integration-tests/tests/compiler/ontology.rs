use std::sync::Arc;

use super::setup::{admin_ctx, embedded_ontology, test_ctx};
use compiler::{
    ColumnSelection, HydrationPlan, Input, InputNode, QueryType, TraversalPath, compile,
    compile_input,
};

#[test]
fn valid_column_in_order_by() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
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
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
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
        "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": "admin"}}],
        "limit": 10
    }"#;
    assert!(compile(json, &embedded_ontology(), &test_ctx()).is_ok());
}

#[test]
fn invalid_column_in_filter() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "filters": {"nonexistent_column": "value"}}],
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
            "nodes": [{"id": "n", "entity": "NonexistentType", "node_ids": [1], "columns": ["name"]}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("NonexistentType"), "got: {msg}");
    // The enrichment must surface valid candidates, not strip or truncate them.
    assert!(msg.contains("Valid values include:"), "got: {msg}");
    assert!(msg.contains("Branch"), "got: {msg}");
    assert!(msg.contains("get_graph_schema"), "got: {msg}");
}

#[test]
fn invalid_filter_key_lists_valid_candidates() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"project_full_path": "x"}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("project_full_path"), "got: {msg}");
    assert!(msg.contains("Valid values include:"), "got: {msg}");
    assert!(msg.contains("username"), "got: {msg}");
    assert!(msg.contains("get_graph_schema"), "got: {msg}");
    // The opaque "or N other candidates" truncation must not leak through.
    assert!(!msg.contains("other candidates"), "got: {msg}");
}

#[test]
fn invalid_group_by_property_lists_valid_fields() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": [{"kind": "property", "node": "p", "property": "reviewer_count"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "c"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("reviewer_count"), "got: {msg}");
    assert!(msg.contains("does not exist"), "got: {msg}");
    assert!(msg.contains("Valid fields"), "got: {msg}");
    assert!(msg.contains("name"), "got: {msg}");
}

#[test]
fn malformed_group_by_entry_shows_expected_shapes() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": [{"node": "p", "property": "name"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "c"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("/group_by/0"), "got: {msg}");
    assert!(msg.contains("\"kind\""), "got: {msg}");
    assert!(
        msg.contains("\"kind\": \"property\"") && msg.contains("\"kind\": \"node\""),
        "got: {msg}"
    );
}

#[test]
fn bare_string_group_by_entry_shows_expected_shapes() {
    let err = compile(
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
            "group_by": ["name"],
            "aggregations": [{"function": "count", "target": "p", "alias": "c"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("/group_by/0"), "got: {msg}");
    assert!(msg.contains("\"kind\""), "got: {msg}");
    assert!(
        msg.contains("\"kind\": \"property\"") && msg.contains("\"kind\": \"node\""),
        "got: {msg}"
    );
}

#[test]
fn invalid_column_lists_valid_candidates() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "columns": ["bogus_col"]}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("bogus_col"), "got: {msg}");
    assert!(msg.contains("/nodes/0/columns"), "got: {msg}");
    assert!(msg.contains("Valid values"), "got: {msg}");
    assert!(msg.contains("username"), "got: {msg}");
    // The opaque oneOf fallthrough must not leak through.
    assert!(!msg.contains("under any of the schemas"), "got: {msg}");
}

#[test]
fn invalid_relationship_type_lists_valid_candidates() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "BOGUS_REL", "from": "u", "to": "n"}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("BOGUS_REL"), "got: {msg}");
    assert!(msg.contains("/relationships/0/type"), "got: {msg}");
    assert!(msg.contains("Valid values"), "got: {msg}");
    assert!(msg.contains("AUTHORED"), "got: {msg}");
    assert!(!msg.contains("under any of the schemas"), "got: {msg}");
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
    let rendered = result.base.render();

    // AUTHORED is FK-elided via author_id — no edge table scan.
    assert!(rendered.contains("gl_note"));
    assert!(rendered.contains("gl_user"));
    assert!(rendered.contains("LIMIT 25"));
}

#[test]
fn package_built_by_pipeline_traversal() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "pkg", "entity": "Package", "columns": ["name", "version", "package_type"], "filters": {"package_type": "npm"}},
            {"id": "pl", "entity": "Pipeline", "columns": ["id", "status"]}
        ],
        "relationships": [{"type": "BUILT_BY", "from": "pkg", "to": "pl"}],
        "limit": 25
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("gl_package"));
    assert!(rendered.contains("gl_edge"));
    assert!(rendered.contains("'BUILT_BY'"));
    assert!(rendered.contains("(e0.source_kind = 'Package')"));
    assert!(rendered.contains("(e0.target_kind = 'Pipeline')"));
    assert!(rendered.contains("LIMIT 25"));
}

#[test]
fn basic_search_query() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "u",
            "entity": "User",
            "columns": ["username"],
            "filters": { "username": {"op": "eq", "value": "admin"} }
        }],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        rendered.contains(" FINAL"),
        "search should use FINAL for dedup"
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
        "nodes": [{
            "id": "u",
            "entity": "User",
            "columns": ["username", "state", "created_at"],
            "filters": {
                "username": {"op": "starts_with", "value": "admin"},
                "state": {"op": "in", "value": ["active", "blocked"]},
                "created_at": {"op": "gte", "value": "2024-01-01"}
            }
        }],
        "limit": 50,
        "order_by": {"node": "u", "property": "created_at", "direction": "DESC"}
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    // Uses ClickHouse `IN [...]` array syntax which sqlparser can't parse.
    let rendered = result.base.render();

    assert!(rendered.contains(" FINAL"));
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
        "nodes": [{ "id": "u", "entity": "User", "node_ids": [1], "columns": ["username", "state"] }],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(rendered.contains("u_username"));
    assert!(matches!(result.hydration, HydrationPlan::None));
}

#[test]
fn search_with_wildcard_columns() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{ "id": "u", "entity": "User", "node_ids": [1], "columns": "*" }],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
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
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(rendered.contains("_gkg_p_id"));
    assert!(rendered.contains("_gkg_p_type"));
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
        "group_by": [{"kind": "node", "node": "u"}],
        "aggregations": [{"function": "count", "target": "mr", "alias": "mr_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(!rendered.contains("_gkg_mr_id"));
    assert!(!rendered.contains("_gkg_mr_type"));
    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
    assert!(rendered.contains("GROUP BY"));
}

#[test]
fn path_finding_uses_gkg_path_not_node_columns() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "node_ids": [100], "columns": ["name"]},
            {"id": "end", "entity": "Project", "node_ids": [200], "columns": ["name"]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["CONTAINS"]}
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_path"));
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
    let rendered = result.base.render();

    assert_eq!(result.base.result_context.len(), 2);

    let user = result.base.result_context.get("u").unwrap();
    assert_eq!(user.entity_type, "User");
    assert_eq!(user.id_column, "_gkg_u_id");
    assert_eq!(user.type_column, "_gkg_u_type");

    let project = result.base.result_context.get("p").unwrap();
    assert_eq!(project.entity_type, "Project");
    assert_eq!(project.id_column, "_gkg_p_id");
    assert_eq!(project.type_column, "_gkg_p_type");

    assert!(rendered.contains("_gkg_u_id"));
    assert!(rendered.contains("_gkg_u_type"));
    assert!(rendered.contains("_gkg_p_id"));
    assert!(rendered.contains("_gkg_p_type"));
}

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
    let rendered = result.base.render();

    assert!(rendered.contains("UNION ALL"));
    assert!(rendered.contains("hop_e0_type"));
    assert!(rendered.contains("depth"));
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
    let rendered = result.base.render();

    assert!(rendered.contains("depth"));
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
    let rendered = result.base.render();

    assert!(
        !rendered.contains("WITH RECURSIVE"),
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
        "group_by": [{"kind": "node", "node": "u"}],
        "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("UNION ALL"));
    assert!(rendered.contains("e0"));
    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
}

#[test]
fn definition_uses_project_id_for_redaction() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "d", "entity": "Definition", "node_ids": [1], "columns": ["name", "project_id"]}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_d_id"));
    assert!(rendered.contains("_gkg_d_type"));
    assert!(
        rendered.contains("d.project_id") && rendered.contains("_gkg_d_id"),
        "Definition should use project_id for redaction"
    );
}

#[test]
fn project_still_uses_id_for_redaction() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
        "limit": 10
    }"#;

    let result = compile(json, &embedded_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_p_id"));
    assert!(
        rendered.contains("p.id AS _gkg_p_id"),
        "Project should use id for redaction"
    );
}

#[test]
fn cursor_pagination_validation() {
    use compiler::QueryError;

    let ontology = embedded_ontology();
    let ctx = test_ctx();

    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 20}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "valid cursor should compile: {result:?}");

    let result = result.unwrap();
    let rendered = result.base.render();
    assert!(rendered.contains("LIMIT 100"));

    assert!(
        result.base.sql.contains("use_query_cache = 1"),
        "cursor query should enable CH query cache: {}",
        result.base.sql
    );

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
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

    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
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

    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "limit": 30,
        "cursor": {"offset": 0, "page_size": 30}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "page_size == limit should be valid");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {"offset": 0}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "cursor missing page_size should fail");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {"page_size": 10}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "cursor missing offset should fail");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "cursor": {}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "empty cursor should fail");

    let err = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "limit": 10,
        "cursor": {"offset": 0, "page_size": 0}
    }"#,
        &ontology,
        &ctx,
    );
    assert!(err.is_err(), "page_size = 0 should fail");

    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}]
    }"#,
        &ontology,
        &ctx,
    );
    assert!(result.is_ok(), "no cursor should compile fine");
    let result = result.unwrap();
    let rendered = result.base.render();
    assert!(rendered.contains("LIMIT 30"), "default limit should be 30");
    assert!(
        !result.base.sql.contains("use_query_cache"),
        "non-cursor query should not enable query cache: {}",
        result.base.sql
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
        "rendered SQL should have no placeholders"
    );
    assert!(
        rendered.contains("'opened'") || rendered.contains("'state:opened'"),
        "rendered SQL should contain the state filter value"
    );
    assert!(rendered.contains("'AUTHORED'"));
}

#[test]
fn render_in_filter_inlines_array() {
    let rendered = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "filters": {
            "user_type": {"op": "in", "value": ["project_bot", "service_account"]}
        }}],
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
        "rendered SQL should have no placeholders"
    );
    assert!(rendered.contains("'project_bot'") && rendered.contains("'service_account'"));
}

#[test]
fn render_node_ids_inlines_array() {
    let rendered = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [100, 200, 300]}],
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

    let rendered = compiled.base.render();
    assert!(
        !rendered.contains("{p"),
        "rendered SQL should have no placeholders"
    );

    let debug_json = serde_json::json!({
        "base": compiled.base.sql,
        "base_rendered": rendered,
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
    let rendered = result.base.render();

    assert!(!rendered.contains("UNION ALL"));
    assert!(rendered.contains("toJSONString"));
    assert!(rendered.contains("gl_user"));
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
            node_ids: vec![7777, 8888, 9999],
            ..InputNode::default()
        }],
        limit: 3,
        ..Input::default()
    };

    let result = compile_input(input, &Arc::new(embedded_ontology()), &test_ctx()).unwrap();
    let parameterized = &result.base.sql;

    assert!(
        parameterized.contains("Array(Int64)"),
        "IDs should be parameterized"
    );
    assert!(
        !parameterized.contains("7777"),
        "literal IDs should not appear in parameterized SQL"
    );

    let rendered = result.base.render();
    assert!(rendered.contains("7777") && rendered.contains("8888") && rendered.contains("9999"));
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
    let rendered = result.base.render();

    assert!(
        !rendered.contains("arrayExists"),
        "hydration should skip security filters"
    );
    assert!(
        !rendered.contains("startsWith"),
        "hydration should not have startsWith"
    );
}

#[test]
fn hydration_id_only_columns_produces_map_with_id() {
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
        rendered.contains("map(") && rendered.contains("'id'"),
        "PK should be included in map when requested"
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
            columns: Some(ColumnSelection::List(vec![])),
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
fn hydration_id_column_included_in_map() {
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
        map_section.contains("'id'"),
        "map should contain 'id' key when requested"
    );
}

#[test]
fn like_rejects_short_contains_pattern() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"username": {"op": "contains", "value": "ab"}}}],
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
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"username": {"op": "starts_with", "value": "a"}}}],
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
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"username": {"op": "ends_with", "value": ""}}}],
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
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": {"op": "contains", "value": "example"}}}],
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
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": {"op": "starts_with", "value": "alice"}}}],
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
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": "alice@example.com"}}],
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
            "nodes": [{"id": "u", "entity": "User",
                     "filters": {"email": "alice@example.com"}}],
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

#[test]
fn filterable_allows_traversal_path_starts_with_inside_scope() {
    compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/100/"}}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .expect("traversal_path starts_with inside JWT scope should compile");
}

#[test]
fn filterable_allows_traversal_path_root_starts_with_inside_scope() {
    compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/"}}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .expect("traversal_path root starts_with inside JWT scope should compile");
}

#[test]
fn filterable_allows_traversal_path_equality_inside_scope() {
    compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": "1/100/1000/"}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .expect("traversal_path equality inside JWT scope should compile");
}

#[test]
fn filterable_rejects_traversal_path_outside_scope() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "mr", "entity": "MergeRequest",
                     "filters": {"traversal_path": "2/"}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("authorized traversal_path scope"),
        "expected traversal_path scope rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_above_scope() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": "1/"}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &compiler::SecurityContext::new(1, vec!["1/100/".into()]).unwrap(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("authorized traversal_path scope"),
        "expected traversal_path scope rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_without_trailing_slash() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/100"}}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid traversal_path format"),
        "expected traversal_path format rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_contains_operator() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "contains", "value": "100"}}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("only eq, in, and starts_with"),
        "expected traversal_path operator rejection, got: {err}"
    );
}

#[test]
fn filterable_rejects_traversal_path_below_entity_role_floor() {
    let err = compile(
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "v", "entity": "Vulnerability",
                     "filters": {"traversal_path": "1/100/1000/"}}],
            "limit": 10
        }"#,
        &embedded_ontology(),
        &compiler::SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/100/", 20)])
            .unwrap(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("authorized traversal_path scope"),
        "expected traversal_path role-scope rejection, got: {err}"
    );
}

#[test]
fn filterable_allows_traversal_path_in_columns() {
    assert!(
        compile(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "g", "entity": "Group",
                     "columns": ["name", "traversal_path"],
                     "node_ids": [100]}],
            "limit": 10
        }"#,
            &embedded_ontology(),
            &test_ctx(),
        )
        .is_ok()
    );
}

// Bug 1 regression guard: single-aggregate queries with a sort-key filter must
// keep the filter inside the FINAL scan so ClickHouse uses the primary-key index
// to skip granules. Without this, the latest-row scan reads the full authorized
// table before aggregation.
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

    assert!(
        rendered.contains("COUNT()") || rendered.contains("countIf"),
        "should contain COUNT() or countIf: {rendered}"
    );
    let inner = rendered
        .split(" FINAL")
        .nth(1)
        .expect("rendered SQL should contain FINAL");
    assert!(
        inner.contains("project_id"),
        "project_id must appear inside the FINAL scan: {rendered}"
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

    assert!(
        rendered.contains("gl_code_edge"),
        "DEFINES should scan gl_code_edge: {rendered}"
    );
    assert!(
        rendered.contains("12345"),
        "pinned File node_id must appear in WHERE clause: {rendered}"
    );
    assert!(
        rendered.contains("e0.source_id"),
        "edge-centric filter must reference source_id: {rendered}"
    );
}

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
        .split(" FINAL")
        .nth(1)
        .expect("rendered SQL should contain FINAL");
    assert!(
        inner.contains("project_id"),
        "project_id IN must appear inside FINAL scan: {rendered}"
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
        "group_by": [{"kind": "node", "node": "callee"}],
        "aggregations": [{"function": "count", "target": "caller", "alias": "callers"}],
        "limit": 1
    }"#;

    assert!(compile(json, &embedded_ontology(), &admin_ctx()).is_ok());
}

#[test]
fn code_graph_edge_union_routes_to_code_table() {
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
