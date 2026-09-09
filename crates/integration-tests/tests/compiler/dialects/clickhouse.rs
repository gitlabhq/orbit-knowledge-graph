use crate::compiler::setup::{compile_pair, compile_to_ast, test_ctx, test_ontology};
use crate::compiler::utils::has_param_value;
use compiler::{Node, QueryError, compile};

#[test]
fn compile_to_ast_works() {
    let orbit_query = "MATCH (u:User {id: 1}) RETURN u.username LIMIT 10";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
        "limit": 10
    }"#;

    compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (n:Note {confidential: true})<-[:AUTHORED]-(u:User) RETURN n.confidential, u.username ORDER BY n.created_at DESC LIMIT 25";
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

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (n:Note {confidential: true}) RETURN n.confidential LIMIT 5";
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

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    assert!(has_param_value(
        &result.base.params,
        &serde_json::Value::Bool(true)
    ));
}

#[test]
fn aggregation_query() {
    let orbit_query = "MATCH (n:Note {id: 1})<-[:AUTHORED]-(u:User) RETURN u{.id, .username}, count(n) AS note_count LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "n", "entity": "Note", "node_ids": [1], "columns": ["confidential"]},
            {"id": "u", "entity": "User", "columns": ["id", "username"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "group_by": ["u"],
        "aggregations": [{"count": "n", "as": "note_count"}],
        "limit": 10
    }"#;

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
    assert!(rendered.contains("GROUP BY"));
}

#[test]
fn group_by_property_truncate_month_wraps_column() {
    let orbit_query = "MATCH (u:Note {confidential: false}) RETURN date_trunc('month', u.created_at), count(u) AS n LIMIT 50";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "filters": {"confidential": {"eq": false}}}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "month"}],
        "limit": 50
    }"#;
    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("toDate32(toStartOfMonth(u.created_at))"),
        "expected toDate32(toStartOfMonth(...)) wrapper; got:\n{rendered}"
    );
    assert!(
        rendered.contains("toDate32(toStartOfMonth(u.created_at)) AS u_created_at_month"),
        "expected derived column `u_created_at_month`; got:\n{rendered}"
    );
}

#[test]
fn group_by_property_truncate_all_units_compile() {
    for unit in ["minute", "hour", "day", "week", "month", "quarter", "year"] {
        let orbit_query = format!(
            "MATCH (u:Note {{id: 1}}) RETURN date_trunc('{unit}', u.created_at), count(u) AS n LIMIT 10"
        );
        let json = format!(
            r#"{{
                "query_type": "aggregation",
                "nodes": [
                    {{"id": "u", "entity": "Note", "node_ids": [1]}}
                ],
                "aggregations": [{{"count": "u", "as": "n"}}],
                "group_by": [{{"key": "u.created_at", "truncate": "{unit}"}}],
                "limit": 10
            }}"#
        );
        let result = compile_pair(&json, &orbit_query, &test_ontology(), &test_ctx())
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
    let orbit_query =
        "MATCH (u:Note) RETURN date_trunc('minute', u.created_at), count(u) AS n LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note"}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "minute"}],
        "limit": 10
    }"#;
    let err = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires either node_ids") && msg.contains("minute"),
        "expected cardinality-guard rejection; got: {msg}"
    );
}

#[test]
fn group_by_truncate_minute_with_node_ids_accepted() {
    let orbit_query = "MATCH (u:Note) WHERE u.id IN [1, 2] RETURN date_trunc('minute', u.created_at), count(u) AS n LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1, 2]}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "minute"}],
        "limit": 10
    }"#;
    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    assert!(
        result
            .base
            .render()
            .contains("toDateTime64(toStartOfMinute(u.created_at), 0)")
    );
}

#[test]
fn group_by_truncate_hour_with_property_filter_accepted() {
    let orbit_query = "MATCH (u:Note) WHERE u.created_at >= '2026-04-01T00:00:00Z' RETURN date_trunc('hour', u.created_at), count(u) AS n LIMIT 50";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "filters": {"created_at": {"gte": "2026-04-01T00:00:00Z"}}}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "hour"}],
        "limit": 50
    }"#;
    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    assert!(
        result
            .base
            .render()
            .contains("toDateTime64(toStartOfHour(u.created_at), 0)")
    );
}

#[test]
fn group_by_truncate_on_non_date_property_rejected() {
    let orbit_query =
        "MATCH (u:Note {id: 1}) RETURN date_trunc('month', u.confidential), count(u) AS n LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1]}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.confidential", "truncate": "month"}],
        "limit": 10
    }"#;
    let err = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires a Date or DateTime property"),
        "expected data-type rejection; got: {msg}"
    );
}

#[test]
fn group_by_truncate_custom_alias_preserved() {
    let orbit_query = "MATCH (u:Note {id: 1}) RETURN date_trunc('month', u.created_at) AS bucket, count(u) AS n LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1]}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "month", "as": "bucket"}],
        "limit": 10
    }"#;
    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("toDate32(toStartOfMonth(u.created_at)) AS bucket"),
        "expected alias `bucket`; got:\n{rendered}"
    );
}

#[test]
fn path_finding_query() {
    let orbit_query = "MATCH p = shortestPath((start:Project {id: 100})-[:CONTAINS*1..3]->(`end`:Project {id: 200})) RETURN p";
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [100]},
            {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [200]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["CONTAINS"]}
    }"#;

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
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

    let shallow_orbit_query = "MATCH p = shortestPath((start:Project {id: 1})-[:CONTAINS|MEMBER_OF*1]->(`end`:Project {id: 2})) RETURN p";
    let deep_orbit_query = "MATCH p = shortestPath((start:Project {id: 1})-[:CONTAINS|MEMBER_OF*1..3]->(`end`:Project {id: 2})) RETURN p";
    let shallow_sql = compile_pair(shallow, shallow_orbit_query, &test_ontology(), &test_ctx())
        .unwrap()
        .base
        .render();
    let deep_sql = compile_pair(deep, deep_orbit_query, &test_ontology(), &test_ctx())
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
    let orbit_query = "MATCH (u:User {id: 100})--(n) RETURN n";
    let json = r#"{
        "query_type": "neighbors",
        "nodes": [{"id": "u", "entity": "User", "columns": ["username"], "node_ids": [100]}],
        "neighbors": {"direction": "both"}
    }"#;

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (u:User) WHERE u.created_at >= '2024-01-01' AND u.state IN ['active', 'blocked'] AND u.username CONTAINS 'admin' RETURN u.username, u.state, u.created_at LIMIT 30";
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

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (u:User {id: 1})-[:AUTHORED]->(p:Project) RETURN u, p LIMIT 25";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "p"}],
        "limit": 25
    }"#;
    let result = compile_pair(json, orbit_query, &multi_table_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (f:File {id: 1})-[:DEFINES]->(d:Definition) RETURN f, d LIMIT 25";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "f", "entity": "File", "node_ids": [1]},
            {"id": "d", "entity": "Definition"}
        ],
        "relationships": [{"type": "DEFINES", "from": "f", "to": "d"}],
        "limit": 25
    }"#;
    let result = compile_pair(json, orbit_query, &multi_table_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (u:User {id: 1})-->(p:Project) RETURN u, p LIMIT 25";
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
    let result = compile_pair(json, orbit_query, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge"),
        "wildcard should route to default gl_edge: {rendered}"
    );
}

#[test]
fn multi_table_mixed_types_scans_both_tables() {
    let orbit_query =
        "MATCH (u:User {id: 1})-[:AUTHORED|DEFINES]->(p:Project) RETURN u, p LIMIT 25";
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
    let result = compile_pair(json, orbit_query, &multi_table_ontology(), &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (u:User {id: 1})-[:AUTHORED]->(p:Project) RETURN u, p LIMIT 25";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "p"}],
        "limit": 25
    }"#;
    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("UNION ALL"),
        "single-table ontology should not produce UNION ALL: {rendered}"
    );
}

#[test]
fn multi_table_path_finding_scans_all_tables() {
    let orbit_query = "MATCH path = shortestPath((start:User {id: 1})-[:CONTAINS|DEFINES*1..3]->(`end`:Definition {id: 100})) RETURN path";
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Definition", "node_ids": [100]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3, "rel_types": ["CONTAINS", "DEFINES"]}
    }"#;
    let result = compile_pair(json, orbit_query, &multi_table_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge") && rendered.contains("gl_code_edge"),
        "wildcard path finding should scan both edge tables: {rendered}"
    );
}

#[test]
fn neighbors_non_default_pk_with_non_denorm_filter_no_alias_clash() {
    let orbit_query = "MATCH (f:File)--(n) WHERE f.path CONTAINS 'labkit' RETURN n";
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
    let result = compile_pair(json, orbit_query, &ontology, &test_ctx()).unwrap();
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
    let orbit_query = "MATCH (p:Project {id: 1})--(n) RETURN n";
    let json = r#"{
        "query_type": "neighbors",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
        "neighbors": {"direction": "both"}
    }"#;
    let result = compile_pair(json, orbit_query, &multi_table_ontology(), &test_ctx()).unwrap();
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
    prefixes.insert(
        "p".to_string(),
        orbit_utils::traversal_path::TraversalPath::new_unchecked(SCOPED_PREFIX),
    );
    admin_ctx().with_scope_prefixes(prefixes)
}

fn render_scoped(json: &str, orbit_query: &str) -> String {
    compile_pair(json, orbit_query, &embedded_ontology(), &scoped_ctx())
        .unwrap()
        .base
        .render()
}

#[test]
fn scoped_traversal_injects_tight_prefix() {
    let orbit_query =
        "MATCH (wi:WorkItem)-[:IN_PROJECT]->(p:Project) WHERE p.id = 1 RETURN wi.id, p LIMIT 100";
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "wi", "to": "p"}],
        "limit": 100
    }"#;
    assert!(render_scoped(json, orbit_query).contains(SCOPED_PREFIX));
}

#[test]
fn scoped_aggregation_injects_tight_prefix() {
    let orbit_query = "MATCH (wi:WorkItem)-[:IN_PROJECT]->(p:Project) WHERE p.id = 1 RETURN p, count(wi) AS c LIMIT 100";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "wi", "entity": "WorkItem", "columns": ["id"]},
            {"id": "p", "entity": "Project", "filters": {"id": {"eq": 1}}}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "wi", "to": "p"}],
        "group_by": ["p"],
        "aggregations": [{"count": "wi", "as": "c"}],
        "limit": 100
    }"#;
    assert!(render_scoped(json, orbit_query).contains(SCOPED_PREFIX));
}

#[test]
fn cross_namespace_related_to_edge_stays_unscoped() {
    let orbit_query = "MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE p.id = 1 RETURN p, wi.id, rel.id, rel.title LIMIT 100";
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
    let compiled = compile_pair(json, orbit_query, &ontology, &scoped_ctx()).unwrap();
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

#[test]
fn orbit_query_bounded_hops_and_relationship_filters() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"User","node_ids":[1]},{"id":"b","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"a","to":"b","hops":[1,3]}]}"#,
            "MATCH (a:User {id: 1})-[:MEMBER_OF*1..3]->(b:Project) RETURN a, b",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"User","node_ids":[1]},{"id":"b","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"a","to":"b","hops":[2,2]}]}"#,
            "MATCH (a:User {id: 1})-[:MEMBER_OF*2]->(b:Project) RETURN a, b",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"User","node_ids":[1]},{"id":"b","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"a","to":"b","filters":{"target_id":{"gte":2}}}]}"#,
            "MATCH (a:User {id: 1})-[r:MEMBER_OF]->(b:Project) WHERE r.target_id >= 2 RETURN a, b",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"User","node_ids":[1]},{"id":"b","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"a","to":"b","filters":{"target_id":2}}]}"#,
            "MATCH (a:User {id: 1})-[:MEMBER_OF {target_id: 2}]->(b:Project) RETURN a, b",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"User","node_ids":[1]},{"id":"b","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"a","to":"b","filters":{"target_id":2}}]}"#,
            "MATCH (a:User {id: 1})-[:MEMBER_OF*1..1 {target_id: 2}]->(b:Project) RETURN a, b",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"a","entity":"User","node_ids":[1]},{"id":"b","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"a","to":"b","hops":[1,3]}]}"#,
            "MATCH (a:User {id: 1})-[:MEMBER_OF*1..3 {}]->(b:Project) RETURN a, b",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_id_selectors_preserve_additional_predicates() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"filters":{"id":{"in":[2,3]}}}]}"#,
            "MATCH (u:User {id: 1}) WHERE u.id IN [2, 3] RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"filters":{"id":{"eq":2}}}]}"#,
            "MATCH (u:User {id: 1}) WHERE u.id = 2 RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","id_range":{"start":2,"end":9}}]}"#,
            "MATCH (u:User) WHERE u.id > 1 AND u.id < 10 RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"id_range":{"start":1,"end":10000}}]}"#,
            "MATCH (u:User {id: 1}) WHERE u.id >= 1 AND u.id <= 10000 RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":[{"contains":"abc"},{"contains":"def"}]}}]}"#,
            "MATCH (u:User) WHERE (u.username CONTAINS 'abc' AND u.username CONTAINS 'def') RETURN u",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_aggregate_functions_property_groups_and_ordering() {
    let json = r#"{
        "query_type":"aggregation",
        "nodes":[{"id":"u","entity":"Note","node_ids":[1,2]}],
        "group_by":[{"key":"u.confidential","as":"confidential"}],
        "aggregations":[{"count":"u","as":"n"},{"sum":"u.id"},{"avg":"u.id"},{"min":"u.created_at"},{"max":"u.created_at"}],
        "aggregation_sort":"-n","limit":10
    }"#;
    let orbit_query = "MATCH (u:Note) WHERE u.id IN [1, 2] RETURN u.confidential AS confidential, count(u) AS n, sum(u.id), avg(u.id), min(u.created_at), max(u.created_at) ORDER BY n DESC LIMIT 10";
    compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_null_predicates_and_all_columns() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","columns":"*","filters":{"created_at":{"is_not_null":true},"state":{"is_null":true}}}]}"#;
    let orbit_query =
        "MATCH (u:User) WHERE u.created_at IS NOT NULL AND u.state IS NULL RETURN properties(u)";
    compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_parameters_are_bound_as_values() {
    use crate::compiler::setup::compile_pair_with_parameters;
    let attack = "'; DROP TABLE gl_user; //";
    let json = serde_json::json!({
        "query_type":"traversal",
        "nodes":[{"id":"u","entity":"User","filters":{"username":{"eq":attack}}}],
        "limit":5
    })
    .to_string();
    let orbit_query = "MATCH (u:User) WHERE u.username = $name RETURN u LIMIT $limit";
    let parameters = orbit_query::Parameters::from([
        ("name".into(), serde_json::Value::from(attack)),
        ("limit".into(), serde_json::Value::from(5)),
    ]);
    let result = compile_pair_with_parameters(
        &json,
        orbit_query,
        &parameters,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert!(!result.base.sql.contains(attack));
    assert!(has_param_value(
        &result.base.params,
        &serde_json::Value::from(attack)
    ));
}

#[test]
fn orbit_query_m23_literals_and_comments() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[16]}]}"#,
            "/* MATCH nothing */ match(u:User {id: 0x10}) return u; // done",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[8]}]}"#,
            "MATCH (u:User {id: 0o10}) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[-9223372036854775808]}]}"#,
            "MATCH (u:User {id: -9223372036854775808}) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":"a'b\\c\n😀"}}]}"#,
            r#"MATCH (`u`:User {username: 'a\'b\\c\n\uD83D\uDE00'}) RETURN u"#,
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"created_at":{"gte":"2024-01-01"}}}]}"#,
            "MATCH (u:User) WHERE u.created_at >= date('2024-01-01') RETURN u",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_rejects_inline_filters_on_variable_length_relationships() {
    for query in [
        "MATCH (a:Group {id: 1})-[:CONTAINS*2 {source_id: 1}]->(b:Group) RETURN a, b",
        "MATCH (a:Group {id: 1})-[:CONTAINS*1..2 {source_id: 1}]->(b:Group) RETURN a, b",
        "MATCH (a:Group {id: 1})<-[r:CONTAINS*2..3 {target_id: 1}]-(b:Group) RETURN a, b",
    ] {
        let error = orbit_query::parse(query, &orbit_query::Parameters::new()).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("property filters on variable-length relationships are unsupported"),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_rejects_neighbors_center_all_properties() {
    for query in [
        "MATCH (center:User {id: 1})--(n) RETURN properties(center), n",
        "MATCH (center:File {id: 1})-->(n) RETURN properties(center)",
        "MATCH (center:User {id: 1})<--(n) RETURN n, properties(center)",
    ] {
        let error = orbit_query::parse(query, &orbit_query::Parameters::new()).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("dynamic graph results cannot be renamed or projected as properties"),
            "{query}: {error}"
        );
    }
    let json = r#"{"query_type":"neighbors","nodes":[{"id":"center","entity":"User","node_ids":[1]}],"neighbors":{"direction":"both"}}"#;
    let query = "MATCH (center:User {id: 1})--(n) RETURN center, n";
    compile_pair(json, query, &embedded_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_virtual_filter_equality_hydration_parity() {
    let ontology = embedded_ontology();
    for (json, query) in [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"filters":{"content":"abc"}}]}"#,
            "MATCH (f:File {id: 1}) WHERE f.content = 'abc' RETURN f",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"filters":{"content":{"eq":"abc"}}}]}"#,
            "MATCH (f:File {id: 1, content: 'abc'}) RETURN f",
        ),
    ] {
        let compiled = compile_pair(json, query, &ontology, &test_ctx()).unwrap();
        let compiler::HydrationPlan::Static(templates) = compiled.hydration else {
            panic!("expected static hydration for {query}");
        };
        let filters = &templates
            .iter()
            .find(|t| t.node_alias == "f")
            .unwrap()
            .virtual_filters;
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "content");
        assert_eq!(filters[0].1.op, Some(compiler::input::FilterOp::Eq));
        assert_eq!(filters[0].1.value, Some(serde_json::Value::from("abc")));
    }
}

#[test]
fn orbit_query_rejects_unsupported_syntax_and_shapes() {
    let queries = [
        "CREATE (u:User)",
        "MATCH (u:User) DELETE u",
        "MATCH (u:User) SET u.username = 'x' RETURN u",
        "MATCH (u:User) RETURN u; MATCH (p:Project) RETURN p",
        "MATCH (u:User) RETURN u UNION MATCH (p:Project) RETURN p",
        "OPTIONAL MATCH (u:User) RETURN u",
        "MATCH (u:User) WITH u RETURN u",
        "UNWIND [1] AS u RETURN u",
        "MATCH (u:User), (p:Project) RETURN u, p",
        "MATCH () RETURN *",
        "MATCH (u:User|Project) RETURN u",
        "MATCH (u IS User) RETURN u",
        "MATCH (u:User) WHERE u.id = 1 OR u.id = 2 RETURN u",
        "MATCH (u:User) WHERE NOT u.id = 1 RETURN u",
        "MATCH (u:User) WHERE u.id <> 1 RETURN u",
        "MATCH (u:User) WHERE u.created_at = DATE '2024-01-01' RETURN u",
        "MATCH (u:User) RETURN DISTINCT u",
        "MATCH (u:User) RETURN count(*)",
        "MATCH (u:User) RETURN collect(u.username)",
        "MATCH (u:User) RETURN u SKIP 1",
        "MATCH (u:User) RETURN u ORDER BY u.id, u.username",
        "MATCH (u:User) RETURN u.username AS renamed",
        "MATCH (u:User) RETURN u{.username}, count(u)",
        "MATCH (u:User) RETURN u{.username}, u.state",
        "MATCH (u:User) RETURN u.username, u{.state}",
        "MATCH (u:User) RETURN date_trunc('month', u.created_at)",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*0..3]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*3..1]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..4]->(p:Project) RETURN u",
        "MATCH (u:User {id: 1})-->(n) WHERE n.id = 2 RETURN n",
        "MATCH (u:User {id: 1})-->(n) RETURN count(n)",
        "MATCH (u:User {id: 1})-[r:MEMBER_OF*1..2]->(p:Project) WHERE r.target_id = 2 RETURN u",
        "MATCH p = shortestPath((u:User {id: 1})<-[:MEMBER_OF*1..3]-(p:Project {id: 2})) RETURN p",
        "MATCH (u:User {id: 1})-->(u) RETURN u",
        "MATCH (u:User {id: 1, id: 2}) RETURN u",
        "MATCH (u:User) WHERE missing.username = 'abc' RETURN u",
        "MATCH (u:User) RETURN missing",
        "MATCH (u:User) WHERE u.username = $missing RETURN u",
        "MATCH (`u; DROP TABLE gl_user`:User) RETURN *",
        "MATCH (u:User {username: '\\q'}) RETURN u",
        "MATCH (u:User {username: '\\uD800'}) RETURN u",
    ];
    for query in queries {
        let error = orbit_query::compile(
            query,
            &orbit_query::Parameters::new(),
            &test_ontology(),
            &test_ctx(),
        )
        .expect_err(query);
        assert!(error.is_client_safe(), "{query}: {error}");
    }
}

#[test]
fn orbit_query_preserves_ontology_validation_and_security() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"NotAnEntity"}]}"#,
            "MATCH (u:NotAnEntity) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","columns":["missing"]}]}"#,
            "MATCH (u:User) RETURN u.missing",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"missing":"abc"}}]}"#,
            "MATCH (u:User {missing: 'abc'}) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":42}}]}"#,
            "MATCH (u:User {username: 42}) RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":{"contains":"ab"}}}]}"#,
            "MATCH (u:User) WHERE u.username CONTAINS 'ab' RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":{"token_match":"abc"}}}]}"#,
            "MATCH (u:User) WHERE token_match(u.username, 'abc') RETURN u",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"p","entity":"Project"}],"relationships":[{"type":"UNKNOWN_EDGE","from":"u","to":"p"}]}"#,
            "MATCH (u:User {id: 1})-[:UNKNOWN_EDGE]->(p:Project) RETURN u, p",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    }
    let json = r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","filters":{"traversal_path":{"starts_with":"1/"}}}]}"#;
    let orbit_query = "MATCH (p:Project) WHERE p.traversal_path STARTS WITH '1/' RETURN p";
    let context = compiler::SecurityContext::new(1, vec!["1/24/".into()]).unwrap();
    let error = compile_pair(json, orbit_query, &embedded_ontology(), &context).unwrap_err();
    assert!(matches!(error, QueryError::Authorization(_)));
}

#[test]
fn orbit_query_nesting_guard_sees_through_escaped_names() {
    let hidden = format!(
        "MATCH (u:User) WHERE u.`'` = 1 AND {}u.id = 1{} AND u.`'` = 2 RETURN u",
        "(".repeat(2_000),
        ")".repeat(2_000)
    );
    let error = orbit_query::parse(&hidden, &orbit_query::Parameters::new()).unwrap_err();
    assert!(error.to_string().contains("nesting is too deep"), "{error}");

    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"limit":5}"#;
    let orbit_query = "MATCH (`u`:`User` {`id`: 1}) RETURN `u` LIMIT 5";
    compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_charges_every_parameter_reference() {
    let ids: Vec<u64> = (0..2_000).collect();
    let parameters =
        orbit_query::Parameters::from([("ids".to_owned(), serde_json::Value::from(ids))]);
    let query = |references: usize| {
        let predicates = vec!["u.id IN $ids"; references].join(" AND ");
        format!("MATCH (u:User) WHERE {predicates} RETURN u LIMIT 5")
    };

    orbit_query::parse(&query(2), &parameters).unwrap();
    let error = orbit_query::parse(&query(200), &parameters).unwrap_err();
    assert!(matches!(error, QueryError::LimitExceeded(_)), "{error}");
}

#[test]
fn orbit_query_escaped_parameter_names_are_unescaped() {
    use crate::compiler::setup::compile_pair_with_parameters;
    let json = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[7]}],"limit":5}"#;
    let orbit_query = "MATCH (u:User {id: $`a``b`}) RETURN u LIMIT 5";
    let parameters = orbit_query::Parameters::from([
        ("a`b".to_owned(), serde_json::Value::from(7)),
        ("ab".to_owned(), serde_json::Value::from(99)),
    ]);
    let result = compile_pair_with_parameters(
        json,
        orbit_query,
        &parameters,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert!(has_param_value(
        &result.base.params,
        &serde_json::Value::from(7)
    ));

    let only_trimmed =
        orbit_query::Parameters::from([("ab".to_owned(), serde_json::Value::from(99))]);
    let error = orbit_query::parse(orbit_query, &only_trimmed).unwrap_err();
    assert!(
        error.to_string().contains("missing parameter $a`b"),
        "{error}"
    );
}

#[test]
fn orbit_query_bounds_input_before_recursive_parsing() {
    let nested = format!(
        "MATCH (u:User) WHERE {}u.id = 1{} RETURN u",
        "(".repeat(40),
        ")".repeat(40)
    );
    let oversized = format!(
        "MATCH (u:User {{username: '{}'}}) RETURN u",
        "x".repeat(40_000)
    );
    for query in [nested, oversized] {
        assert!(orbit_query::parse(&query, &orbit_query::Parameters::new()).is_err());
    }
    assert!(
        orbit_query::compile(
            "MATCH (u:User) WHERE u.id IN [] RETURN u",
            &orbit_query::Parameters::new(),
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn orbit_query_schema_caps_reject_with_the_json_category() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"limit":0}"#,
            "MATCH (u:User {id: 1}) RETURN u LIMIT 0",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"limit":1001}"#,
            "MATCH (u:User {id: 1}) RETURN u LIMIT 1001",
        ),
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"p","entity":"Project"}],"relationships":[{"type":"MEMBER_OF","from":"u","to":"p","hops":[1,4]}]}"#,
            "MATCH (u:User {id: 1})-[:MEMBER_OF*1..4]->(p:Project) RETURN u, p",
        ),
        (
            r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"group_by":["ghost"],"aggregations":[{"count":"u"}]}"#,
            "MATCH (u:User {id: 1}) RETURN ghost, count(u)",
        ),
    ];
    for (json, orbit_query) in cases {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    }

    let long_identifier = "a".repeat(65);
    let long_string = "x".repeat(1025);
    let bounded = [
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"{long_identifier}","entity":"User","node_ids":[1]}}]}}"#
            ),
            format!("MATCH ({long_identifier}:User {{id: 1}}) RETURN {long_identifier}"),
        ),
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","filters":{{"username":"{long_string}"}}}}]}}"#
            ),
            format!("MATCH (u:User {{username: '{long_string}'}}) RETURN u"),
        ),
    ];
    for (json, orbit_query) in &bounded {
        compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap_err();
    }
    compile_pair(
        r#"{"query_type":"traversal","nodes":[{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","entity":"User","node_ids":[1]}]}"#,
        "MATCH (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:User {id: 1}) RETURN aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
}

#[test]
fn orbit_query_structural_caps_reject_with_the_json_category() {
    let chain = |count: usize| {
        let nodes: Vec<String> = (0..count)
            .map(|i| format!(r#"{{"id":"n{i}","entity":"User"}}"#))
            .collect();
        let edges: Vec<String> = (1..count)
            .map(|i| format!(r#"{{"type":"MEMBER_OF","from":"n{}","to":"n{i}"}}"#, i - 1))
            .collect();
        let json = format!(
            r#"{{"query_type":"traversal","nodes":[{{"id":"n0","entity":"User","node_ids":[1]}},{}],"relationships":[{}]}}"#,
            nodes[1..].join(","),
            edges.join(",")
        );
        let pattern: Vec<String> = (1..count).map(|i| format!("(n{i}:User)")).collect();
        let text = format!(
            "MATCH (n0:User {{id: 1}})-[:MEMBER_OF]->{} RETURN n0",
            pattern.join("-[:MEMBER_OF]->")
        );
        (json, text)
    };
    let ids = |count: usize| {
        let list: Vec<String> = (1..=count).map(|i| i.to_string()).collect();
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","node_ids":[{}]}}]}}"#,
                list.join(",")
            ),
            format!(
                "MATCH (u:User) WHERE u.id IN [{}] RETURN u",
                list.join(", ")
            ),
        )
    };
    let repeated_predicates = |count: usize| {
        let json_entries: Vec<String> = (0..count)
            .map(|i| format!(r#"{{"contains":"abc{i}"}}"#))
            .collect();
        let text_entries: Vec<String> = (0..count)
            .map(|i| format!("u.username CONTAINS 'abc{i}'"))
            .collect();
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","filters":{{"username":[{}]}}}}]}}"#,
                json_entries.join(",")
            ),
            format!(
                "MATCH (u:User) WHERE {} RETURN u",
                text_entries.join(" AND ")
            ),
        )
    };

    let rel_types = |count: usize| {
        let kinds = &[
            "MEMBER_OF",
            "AUTHORED",
            "CONTAINS",
            "IN_PROJECT",
            "REVIEWER",
            "ASSIGNED",
            "HAS_HEAD_PIPELINE",
            "ON_BRANCH",
            "DEFINES",
            "CALLS",
            "EXTENDS",
        ][..count];
        let quoted: Vec<String> = kinds.iter().map(|k| format!("\"{k}\"")).collect();
        (
            format!(
                r#"{{"query_type":"traversal","nodes":[{{"id":"a","entity":"User","node_ids":[1]}},{{"id":"b","entity":"Project"}}],"relationships":[{{"type":[{}],"from":"a","to":"b"}}]}}"#,
                quoted.join(",")
            ),
            format!(
                "MATCH (a:User {{id: 1}})-[:{}]->(b:Project) RETURN a, b",
                kinds.join("|")
            ),
        )
    };

    for (json, text) in [chain(5), ids(500), repeated_predicates(10), rel_types(10)] {
        compile_pair(&json, &text, &embedded_ontology(), &test_ctx()).unwrap();
    }
    for (json, text) in [chain(6), ids(501), repeated_predicates(11), rel_types(11)] {
        let error = compile_pair(&json, &text, &embedded_ontology(), &test_ctx()).unwrap_err();
        assert!(
            matches!(error, QueryError::Validation(_)),
            "{text}: {error}"
        );
    }
}

#[test]
fn orbit_query_incoming_arrows_lower_to_the_outgoing_fk_plan() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"n","entity":"Note","node_ids":[1],"columns":["confidential"]},{"id":"u","entity":"User","columns":["username"]}],"relationships":[{"type":"AUTHORED","from":"u","to":"n"}]}"#;
    let orbit_query =
        "MATCH (n:Note {id: 1})<-[:AUTHORED]-(u:User) RETURN n.confidential, u.username";
    let compiled = compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
    assert!(
        compiled.base.sql.contains("_narrow_u"),
        "AUTHORED should resolve to the FK plan: {}",
        compiled.base.sql
    );
    assert!(
        compiled.base.sql.contains("n.author_id AS e0_src"),
        "edge source must be the User side: {}",
        compiled.base.sql
    );
}

#[test]
fn orbit_query_digit_string_ids_and_aggregated_identity_columns() {
    let cases = [
        (
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":["7"]}]}"#,
            "MATCH (u:User) WHERE u.id IN ['7'] RETURN u",
        ),
        (
            r#"{"query_type":"aggregation","nodes":[{"id":"f","entity":"File","node_ids":[1,2],"columns":["id","content"]}],"group_by":["f"],"aggregations":[{"count":"f","as":"total"}],"limit":5}"#,
            "MATCH (f:File) WHERE f.id IN [1, 2] RETURN f{.id, .content}, count(f) AS total LIMIT 5",
        ),
    ];
    for (json, orbit_query) in cases {
        let compiled = compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
        assert!(!compiled.base.sql.contains("COUNT()") || compiled.base.sql.contains("GROUP BY"));
    }
}
