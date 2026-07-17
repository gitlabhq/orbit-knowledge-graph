use crate::compiler::setup::{compile_to_ast, test_ctx, test_ontology};
use crate::compiler::utils::has_param_value;
use compiler::{Node, QueryError, compile};

#[test]
fn compile_to_ast_works() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
        "limit": 10
    }"#;

    let node = compile_to_ast(json, &test_ontology()).unwrap();
    let Node::Query(ref q) = node else {
        unreachable!()
    };
    assert_eq!(
        q.limit,
        Some(11),
        "fetch limit is the requested limit plus the has_more probe row"
    );
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
        "order_by": "-n.created_at"
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("gl_edge"));
    assert!(rendered.contains("relationship_kind"));
    assert!(rendered.contains("LIMIT 26"));
    assert!(has_param_value(
        &result.base.params,
        &serde_json::json!("AUTHORED")
    ));
}

#[test]
fn bool_filter_value_is_preserved() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "n",
            "entity": "Note",
            "columns": ["confidential"],
            "filters": { "confidential": true }
        }],
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
            {"id": "n", "entity": "Note", "node_ids": [1], "columns": ["confidential"]},
            {"id": "u", "entity": "User", "columns": ["username"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "group_by": ["u"],
        "aggregations": [{"function": "count", "target": "n", "alias": "note_count"}],
        "limit": 10
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
    assert!(rendered.contains("GROUP BY"));
}

#[test]
fn group_by_property_truncate_month_wraps_column() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "filters": {"confidential": {"eq": false}}}
        ],
        "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
        "group_by": [
            {"month": "u.created_at"}
        ],
        "limit": 50
    }"#;
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("toDate32(toStartOfMonth(u.created_at))"),
        "expected toDate32(toStartOfMonth(...)) wrapper; got:\n{rendered}"
    );
    assert!(
        rendered.contains("toDate32(toStartOfMonth(u.created_at)) AS created_at_month"),
        "expected default alias `created_at_month`; got:\n{rendered}"
    );
}

#[test]
fn group_by_property_truncate_all_units_compile() {
    for unit in ["minute", "hour", "day", "week", "month", "quarter", "year"] {
        let json = format!(
            r#"{{
                "query_type": "aggregation",
                "nodes": [
                    {{"id": "u", "entity": "Note", "node_ids": [1]}}
                ],
                "aggregations": [{{"function": "count", "target": "u", "alias": "n"}}],
                "group_by": [
                    {{"{unit}": "u.created_at"}}
                ],
                "limit": 10
            }}"#
        );
        let result = compile(&json, &test_ontology(), &test_ctx())
            .unwrap_or_else(|e| panic!("compile failed for unit {unit}: {e:?}"));
        let rendered = result.base.render();
        // Sub-daily units cast to DateTime64, daily+ to Date32, so the key
        // crosses Arrow as a typed date/timestamp rather than a bare integer.
        let expected = match unit {
            "minute" => "toDateTime64(toStartOfMinute(u.created_at), 0)",
            "hour" => "toDateTime64(toStartOfHour(u.created_at), 0)",
            "day" => "toDate32(toStartOfDay(u.created_at))",
            "week" => "toDate32(toStartOfWeek(u.created_at))",
            "month" => "toDate32(toStartOfMonth(u.created_at))",
            "quarter" => "toDate32(toStartOfQuarter(u.created_at))",
            "year" => "toDate32(toStartOfYear(u.created_at))",
            _ => unreachable!(),
        };
        assert!(
            rendered.contains(expected),
            "unit {unit}: expected {expected} in SQL; got:\n{rendered}"
        );
    }
}

#[test]
fn group_by_truncate_minute_without_selectivity_rejected() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note"}
        ],
        "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
        "group_by": [
            {"minute": "u.created_at"}
        ],
        "limit": 10
    }"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires either node_ids") && msg.contains("minute"),
        "expected cardinality-guard rejection; got: {msg}"
    );
}

#[test]
fn group_by_truncate_minute_with_node_ids_accepted() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1, 2]}
        ],
        "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
        "group_by": [
            {"minute": "u.created_at"}
        ],
        "limit": 10
    }"#;
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(
        result
            .base
            .render()
            .contains("toDateTime64(toStartOfMinute(u.created_at), 0)")
    );
}

#[test]
fn group_by_truncate_hour_with_property_filter_accepted() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "filters": {"created_at": {"gte": "2026-04-01T00:00:00Z"}}}
        ],
        "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
        "group_by": [
            {"hour": "u.created_at"}
        ],
        "limit": 50
    }"#;
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    assert!(
        result
            .base
            .render()
            .contains("toDateTime64(toStartOfHour(u.created_at), 0)")
    );
}

#[test]
fn group_by_truncate_on_non_date_property_rejected() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1]}
        ],
        "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
        "group_by": [
            {"month": "u.confidential"}
        ],
        "limit": 10
    }"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires a Date or DateTime property"),
        "expected data-type rejection; got: {msg}"
    );
}

#[test]
fn group_by_old_kind_object_rejected_with_hint() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1]}
        ],
        "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
        "group_by": [
            {"kind": "property", "node": "u", "property": "created_at", "transform": {"kind": "truncate", "unit": "month"}, "alias": "bucket"}
        ],
        "limit": 10
    }"#;
    let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("not a valid group_by entry") && msg.contains("month"),
        "expected group_by shape hint for the removed kind-tagged form; got: {msg}"
    );
}

#[test]
fn path_finding_query() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [100]},
            {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [200]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["CONTAINS"]}
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("forward AS"), "should have forward CTE");
    assert!(rendered.contains("backward AS"), "should have backward CTE");
    assert!(rendered.contains("UNION ALL"));
    assert!(
        rendered.contains("arrayConcat"),
        "paths should be concatenated"
    );
    assert!(
        rendered.contains("tuple("),
        "path nodes should be typed tuples"
    );
    assert!(
        rendered.contains("f.end_id") && rendered.contains("b.end_id"),
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
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 1, "rel_types": ["CONTAINS", "MEMBER_OF"]}
    }"#;

    let deep = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [1]},
            {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [2]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3, "rel_types": ["CONTAINS", "MEMBER_OF"]}
    }"#;

    let shallow_sql = compile(shallow, &test_ontology(), &test_ctx())
        .unwrap()
        .base
        .render();
    let deep_sql = compile(deep, &test_ontology(), &test_ctx())
        .unwrap()
        .base
        .render();

    assert!(
        shallow_sql.contains("forward AS"),
        "shallow should have forward CTE"
    );
    assert!(
        !shallow_sql.contains("backward AS"),
        "shallow (max_depth=1) should not have backward CTE"
    );
    assert!(
        deep_sql.contains("forward AS"),
        "deep should have forward CTE"
    );
    assert!(
        deep_sql.contains("backward AS"),
        "deep (max_depth=3) should have backward CTE"
    );
}

#[test]
fn neighbors_query() {
    let json = r#"{
        "query_type": "neighbors",
        "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "node_ids": [100]}],
        "neighbors": {"direction": "both"}
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_gkg_neighbor_id"));
    assert!(rendered.contains("_gkg_neighbor_type"));
    assert!(rendered.contains("_gkg_relationship_type"));
    assert!(
        rendered.contains("_gkg_neighbor_is_outgoing"),
        "bidirectional should include direction"
    );
    assert!(rendered.contains("gl_edge"));
    // A pinned default-PK center on a single edge table fuses both directions into
    // one scan: arrayJoin over the matched-arm tuples, no UNION ALL. The multi-table
    // and non-denorm-filter neighbors tests still exercise the UNION ALL path.
    assert!(
        rendered.contains("arrayJoin") && rendered.contains("arrayFilter"),
        "pinned default-PK both should fuse to a single arrayJoin scan"
    );
    assert!(!rendered.contains("UNION ALL"));
}

#[test]
fn filter_operators() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{
            "id": "u",
            "entity": "User",
            "columns": ["username", "state", "created_at"],
            "filters": {
                "created_at": {"gte": "2024-01-01"},
                "state": {"in": ["active", "blocked"]},
                "username": {"contains": "admin"}
            }
        }],
        "limit": 30
    }"#;

    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    // Search uses FINAL for latest-row dedup.
    assert!(rendered.contains(" FINAL"));
    assert!(rendered.contains("_deleted"));
    assert!(rendered.contains(">="));
    assert!(rendered.contains("IN"));
    assert!(rendered.contains("positionCaseInsensitive"));
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
fn valid_identifiers_produce_renderable_sql() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "user_node", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "_private", "entity": "Note", "columns": ["confidential"]},
            {"id": "CamelCase", "entity": "Project", "node_ids": [1], "columns": ["name"]},
            {"id": "node123", "entity": "Group", "columns": ["name"]}
        ],
        "relationships": [
            {"type": "AUTHORED", "from": "user_node", "to": "_private"},
            {"type": "CONTAINS", "from": "CamelCase", "to": "_private"},
            {"type": "MEMBER_OF", "from": "user_node", "to": "node123"}
        ]
    }"#;
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(!rendered.contains("{p"));
    assert!(rendered.contains("_gkg_user_node_id"));
    assert!(rendered.contains("_gkg__private_id"));
    assert!(rendered.contains("_gkg_CamelCase_id"));
    assert!(rendered.contains("_gkg_node123_id"));
}

fn multi_table_ontology() -> ontology::Ontology {
    use ontology::DataType;
    ontology::Ontology::new()
        .with_nodes(["User", "Project", "File", "Definition"])
        .with_edges(["AUTHORED", "CONTAINS", "DEFINES", "IMPORTS"])
        .with_edge_table("gl_code_edge")
        .with_edge_for_table("DEFINES", "gl_code_edge")
        .with_edge_for_table("IMPORTS", "gl_code_edge")
        .with_fields(
            "User",
            [("username", DataType::String), ("state", DataType::String)],
        )
        .with_default_columns("User", ["username"])
        .with_fields("Project", [("name", DataType::String)])
        .with_default_columns("Project", ["name"])
        .with_fields("File", [("path", DataType::String)])
        .with_default_columns("File", ["path"])
        .with_fields("Definition", [("name", DataType::String)])
        .with_default_columns("Definition", ["name"])
}

#[test]
fn multi_table_single_type_routes_to_default() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "p"}],
        "limit": 25
    }"#;
    let result = compile(json, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge"),
        "AUTHORED should scan gl_edge: {rendered}"
    );
    assert!(
        !rendered.contains("gl_code_edge"),
        "AUTHORED should not touch gl_code_edge: {rendered}"
    );
}

#[test]
fn multi_table_code_edge_routes_to_code_table() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "f", "entity": "File", "node_ids": [1]},
            {"id": "d", "entity": "Definition"}
        ],
        "relationships": [{"type": "DEFINES", "from": "f", "to": "d"}],
        "limit": 25
    }"#;
    let result = compile(json, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_code_edge"),
        "DEFINES should scan gl_code_edge: {rendered}"
    );
    assert!(
        !rendered.contains("gl_edge"),
        "DEFINES should not touch gl_edge: {rendered}"
    );
}

#[test]
fn multi_table_wildcard_scans_all_tables() {
    // v2 planner routes wildcard to the default edge table for a single hop.
    // It does not generate UNION ALL across edge tables per hop.
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "*", "from": "u", "to": "p"}],
        "limit": 25
    }"#;
    let result = compile(json, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge"),
        "wildcard should route to default gl_edge: {rendered}"
    );
}

#[test]
fn multi_table_mixed_types_scans_both_tables() {
    // v2 planner routes a single hop to one table (the first matched).
    // Mixed edge types in a single relationship entry go to one table.
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": ["AUTHORED", "DEFINES"], "from": "u", "to": "p"}],
        "limit": 25
    }"#;
    let result = compile(json, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge"),
        "mixed types should route to first matched table (gl_edge): {rendered}"
    );
    assert!(
        rendered.contains("AUTHORED") && rendered.contains("DEFINES"),
        "both relationship types should appear in the SQL: {rendered}"
    );
}

#[test]
fn single_table_ontology_no_union() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "p"}],
        "limit": 25
    }"#;
    let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("UNION ALL"),
        "single-table ontology should not produce UNION ALL: {rendered}"
    );
}

#[test]
fn multi_table_path_finding_scans_all_tables() {
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Definition", "node_ids": [100]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3, "rel_types": ["CONTAINS", "DEFINES"]}
    }"#;
    let result = compile(json, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge") && rendered.contains("gl_code_edge"),
        "wildcard path finding should scan both edge tables: {rendered}"
    );
}

#[test]
fn neighbors_non_default_pk_with_non_denorm_filter_no_alias_clash() {
    use ontology::DataType;
    let ontology = ontology::Ontology::new()
        .with_nodes(["File"])
        .with_edges(["DEFINES"])
        .with_fields("File", [("path", DataType::String)])
        .with_default_columns("File", ["path"])
        .with_redaction("File", "project", "project_id");

    let json = r#"{
        "query_type": "neighbors",
        "nodes": [{
            "id": "f",
            "entity": "File",
            "filters": {"path": {"contains": "labkit"}}
        }],
        "neighbors": {"direction": "both"}
    }"#;
    let result = compile(json, &ontology, &test_ctx()).unwrap();
    let rendered = result.base.render();

    let gl_file_refs = rendered.matches("gl_file").count();
    assert_eq!(
        gl_file_refs, 2,
        "expected one gl_file scan per direction arm; got {gl_file_refs}\nSQL:\n{rendered}"
    );
    assert!(
        rendered.contains("f.project_id AS project_id"),
        "dedup subquery must surface redaction id column: {rendered}"
    );
}

#[test]
fn multi_table_neighbors_scans_all_tables() {
    let json = r#"{
        "query_type": "neighbors",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
        "neighbors": {"direction": "both"}
    }"#;
    let result = compile(json, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge") && rendered.contains("gl_code_edge"),
        "wildcard neighbors should scan both edge tables: {rendered}"
    );
}

use crate::compiler::setup::{admin_ctx, embedded_ontology};

const SCOPED_PREFIX: &str = "1/24/23/";

fn scoped_ctx() -> compiler::SecurityContext {
    let mut prefixes = std::collections::HashMap::new();
    prefixes.insert("p".to_string(), SCOPED_PREFIX.to_string());
    admin_ctx().with_scope_prefixes(prefixes)
}

fn render_scoped(json: &str) -> String {
    compile(json, &embedded_ontology(), &scoped_ctx())
        .unwrap()
        .base
        .render()
}

#[test]
fn scoped_traversal_injects_tight_prefix() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "wi", "to": "p"}],
        "limit": 100
    }"#;
    assert!(render_scoped(json).contains(SCOPED_PREFIX));
}

#[test]
fn scoped_aggregation_injects_tight_prefix() {
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "wi", "to": "p"}],
        "group_by": ["p"],
        "aggregations": [{"function": "count", "target": "wi", "alias": "c"}],
        "limit": 100
    }"#;
    assert!(render_scoped(json).contains(SCOPED_PREFIX));
}

#[test]
fn cross_namespace_related_to_edge_stays_unscoped() {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}},
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "rel", "entity": "WorkItem", "columns": ["id", "title"]}
        ],
        "relationships": [
            {"type": "IN_PROJECT", "from": "wi", "to": "p"},
            {"type": "RELATED_TO", "from": "wi", "to": "rel"}
        ],
        "limit": 100
    }"#;
    let ontology = embedded_ontology();
    let compiled = compile(json, &ontology, &scoped_ctx()).unwrap();
    let sql = compiled.base.render();

    let expected = if ontology.partition().is_some() { 5 } else { 3 };
    assert_eq!(
        sql.matches(SCOPED_PREFIX).count(),
        expected,
        "startsWith on the anchor + two edge scans, plus a _partition_id per edge scan when partitioned"
    );

    let scoped_filter = sql.split("WHERE").nth(1).unwrap();
    let scoped_clause = scoped_filter.split("SELECT").next().unwrap();
    assert!(scoped_clause.contains(SCOPED_PREFIX));

    let after_related = sql.split("RELATED_TO").nth(1).unwrap();
    assert!(!after_related.contains(SCOPED_PREFIX));

    let compiler::HydrationPlan::Static(templates) = &compiled.hydration else {
        panic!("expected static hydration");
    };
    let rel = templates.iter().find(|t| t.node_alias == "rel").unwrap();
    assert!(rel.injected_columns.is_empty());
    assert_eq!(rel.destination_table, "gl_work_item");
}
