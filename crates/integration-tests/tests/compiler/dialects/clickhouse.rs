use crate::compiler::setup::{compile_equivalent, test_ctx, test_ontology};
use crate::compiler::utils::has_param_value;
use compiler::gql::{PreparedStatement, prepare};
use compiler::input::DynamicColumnMode;
use compiler::{Frontend, QueryError, compile};
use ontology::introspection::{
    IntrospectionScope::{All, Local},
    build_compact_schema_response,
};

#[test]
fn compile_to_ast_works() {
    let orbit_query = "MATCH (u:User {id: 1}) RETURN u.username LIMIT 10";

    let compiled = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    assert!(compiled.base.sql.contains("LIMIT 11"));
    assert!(compiled.base.sql.contains("SELECT"));
}

#[test]
fn traversal_query() {
    let orbit_query = "MATCH (n:Note {confidential: true})<-[:AUTHORED]-(u:User) RETURN n.confidential, u.username ORDER BY n.created_at DESC LIMIT 25";

    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
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

    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    assert!(has_param_value(
        &result.base.params,
        &serde_json::Value::Bool(true)
    ));
}

#[test]
fn aggregation_query() {
    let orbit_query = "MATCH (n:Note {id: 1})<-[:AUTHORED]-(u:User) RETURN u{.id, .username}, count(n) AS note_count LIMIT 10";

    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("COUNT()") || rendered.contains("countIf"));
    assert!(rendered.contains("GROUP BY"));
}

#[test]
fn gql_count_distinct_renders_distinct_aggregates() {
    let orbit_query = "MATCH (n:Note {id: 1})<-[:AUTHORED]-(u:User) RETURN u{.id}, count(n) AS notes, count(DISTINCT n) AS distinct_notes, count(DISTINCT n.confidential) AS flags LIMIT 10";
    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert_eq!(rendered.matches("countDistinct").count(), 2, "{rendered}");
    assert!(rendered.contains("AS notes"), "{rendered}");
}

#[test]
fn json_distinct_count_matches_gql_count_distinct() {
    let orbit_query = "MATCH (n:Note {id: 1})<-[:AUTHORED]-(u:User) RETURN u{.id}, count(DISTINCT n.confidential) AS flags LIMIT 10";
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "n", "entity": "Note", "node_ids": [1]},
            {"id": "u", "entity": "User", "columns": ["id"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "group_by": ["u"],
        "aggregations": [{"count": "n.confidential", "distinct": true, "as": "flags"}],
        "limit": 10
    }"#;

    let result = compile_pair(json, orbit_query, &test_ontology(), &test_ctx()).unwrap();
    assert_eq!(result.base.render().matches("countDistinct").count(), 1);
}

#[test]
fn group_by_property_truncate_month_wraps_column() {
    let orbit_query = "MATCH (u:Note {confidential: false}) RETURN date_trunc('month', u.created_at), count(u) AS n LIMIT 50";
    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
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
        let result = compile(&orbit_query, Frontend::Gql, &test_ontology(), &test_ctx())
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
    let err = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires either node_ids") && msg.contains("minute"),
        "expected cardinality-guard rejection; got: {msg}"
    );
}

#[test]
fn group_by_truncate_minute_with_node_ids_accepted() {
    let orbit_query = "MATCH (u:Note) WHERE u.id IN [1, 2] RETURN date_trunc('minute', u.created_at), count(u) AS n LIMIT 10";
    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
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
    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
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
    let err = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("requires a Date or DateTime property"),
        "expected data-type rejection; got: {msg}"
    );
}

#[test]
fn group_by_truncate_custom_alias_preserved() {
    let orbit_query = "MATCH (u:Note {id: 1}) RETURN date_trunc('month', u.created_at) AS bucket, count(u) AS n LIMIT 10";
    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("toDate32(toStartOfMonth(u.created_at)) AS bucket"),
        "expected alias `bucket`; got:\n{rendered}"
    );
}

#[test]
fn path_finding_query() {
    let orbit_query = "MATCH p = ANY SHORTEST (start:Project {id: 100})-[:CONTAINS*1..3]->(`end`:Project {id: 200}) RETURN p";

    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(
        rendered.contains("_orbit_forward AS"),
        "should have forward CTE"
    );
    assert!(
        rendered.contains("_orbit_backward AS"),
        "should have backward CTE"
    );
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
        rendered.contains("_orbit_forward._orbit_end_node")
            && rendered.contains("_orbit_backward._orbit_end_node"),
        "should join forward and backward on end_id"
    );
}

#[test]
fn path_finding_depth_control() {
    let shallow_orbit_query = "MATCH p = ANY SHORTEST (start:Project {id: 1})-[:CONTAINS|MEMBER_OF*1]->(`end`:Project {id: 2}) RETURN p";
    let deep_orbit_query = "MATCH p = ANY SHORTEST (start:Project {id: 1})-[:CONTAINS|MEMBER_OF*1..3]->(`end`:Project {id: 2}) RETURN p";
    let shallow_sql = compile(
        shallow_orbit_query,
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap()
    .base
    .render();
    let deep_sql = compile(
        deep_orbit_query,
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
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

    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("_orbit_neighbor_node"));
    assert!(rendered.contains("_orbit_neighbor_kind"));
    assert!(rendered.contains("_orbit_relationship_kind"));
    assert!(
        rendered.contains("_orbit_neighbor_is_outgoing"),
        "bidirectional should include direction"
    );
    assert!(rendered.contains("gl_edge"));
    assert!(
        rendered.contains("UNION ALL") && !rendered.contains("arrayJoin"),
        "pinned both should scan each direction on its own id column; a fused \
         source_id OR target_id scan stops the primary key and both bloom indexes"
    );
    assert!(
        rendered.contains("(_orbit_neighbor_edge.source_id = 100)")
            && rendered.contains("(_orbit_neighbor_edge.target_id = 100)")
    );
}

#[test]
fn neighbors_project_the_edge_path_only_when_it_is_the_neighbors_own() {
    let ontology = crate::compiler::setup::embedded_ontology();
    let render = |orbit_query: &str| {
        compile(orbit_query, Frontend::Gql, &ontology, &test_ctx())
            .unwrap()
            .base
            .render()
    };

    let authored = render("MATCH (u:User {id: 1})-[:AUTHORED]-(n) RETURN u, n");
    assert!(
        authored
            .contains("((_orbit_neighbor_edge.relationship_kind = 'AUTHORED') AND (_orbit_neighbor_edge.target_kind = 'MergeRequest'))")
            && authored.contains("_orbit_neighbor_edge.traversal_path, '') AS _orbit_neighbor_traversal_path"),
        "{authored}"
    );

    let in_project = render("MATCH (p:Project {id: 1})-[:IN_PROJECT]-(n) RETURN p, n");
    assert!(
        in_project.contains("'' AS _orbit_neighbor_traversal_path"),
        "{in_project}"
    );
}

#[test]
fn filter_operators() {
    let orbit_query = "MATCH (u:User) WHERE u.created_at >= '2024-01-01' AND u.state IN ['active', 'blocked'] AND u.username CONTAINS 'admin' RETURN u.username, u.state, u.created_at LIMIT 30";

    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(rendered.contains("LIMIT 1 BY"));
    assert!(rendered.contains("_deleted"));
    assert!(rendered.contains(">="));
    assert!(rendered.contains("IN"));
    assert!(rendered.contains("multiSearchAny(lower("));
}

#[test]
fn invalid_query_text_rejected() {
    for query in [
        "not a query",
        "MATCH (u:User {id: 1})",
        "{\"query_type\": \"traversal\"}",
    ] {
        assert!(
            compile(query, Frontend::Gql, &test_ontology(), &test_ctx()).is_err(),
            "{query}"
        );
    }
}

#[test]
fn unsafe_identifiers_are_rejected() {
    for query in [
        "MATCH (`n; DROP TABLE users; --`:User {id: 1}) RETURN `n; DROP TABLE users; --`",
        "MATCH (a:User {id: 1})-[:`AUTHORED' OR '1'='1`]->(b:Note) RETURN a, b",
        "MATCH (``:User {id: 1}) RETURN ``",
        "MATCH (`123abc`:User {id: 1}) RETURN `123abc`",
        "MATCH (u:User {`foo; DROP TABLE--`: 'value'}) RETURN u",
    ] {
        let err = compile(query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap_err();
        assert!(
            matches!(
                err,
                QueryError::Validation(_) | QueryError::AllowlistRejected(_)
            ),
            "{query}: {err:?}"
        );
    }
}

#[test]
fn valid_identifiers_produce_renderable_sql() {
    let query = "MATCH (user_node:User {id: 1})-[:AUTHORED]->(_private:Note)<-[:CONTAINS]-(CamelCase:Project {id: 1}), \
                 (user_node)-[:MEMBER_OF]->(node123:Group) \
                 RETURN user_node{.username}, _private{.confidential}, CamelCase{.name}, node123{.name}";
    let result = compile(query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();

    assert!(!rendered.contains("{p"));
    assert!(rendered.contains("_orbit_user_node_id"));
    assert!(rendered.contains("_orbit__private_id"));
    assert!(rendered.contains("_orbit_CamelCase_id"));
    assert!(rendered.contains("_orbit_node123_id"));
}

fn multi_table_ontology() -> std::sync::Arc<ontology::Ontology> {
    use ontology::DataType;
    std::sync::Arc::new(
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
            .with_default_columns("Definition", ["name"]),
    )
}

#[test]
fn multi_table_single_type_routes_to_default() {
    let orbit_query = "MATCH (u:User {id: 1})-[:AUTHORED]->(p:Project) RETURN u, p LIMIT 25";
    let result = compile(
        orbit_query,
        Frontend::Gql,
        &multi_table_ontology(),
        &test_ctx(),
    )
    .unwrap();
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
    let result = compile(
        orbit_query,
        Frontend::Gql,
        &multi_table_ontology(),
        &test_ctx(),
    )
    .unwrap();
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
fn multi_table_wildcard_keeps_the_default_table_scan() {
    let orbit_query = "MATCH (u:User {id: 1})-->(p:Project) RETURN u, p LIMIT 25";
    let compiled = compile(
        orbit_query,
        Frontend::Gql,
        &multi_table_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert!(compiled.base.render().contains("FROM gl_edge"));
}

#[test]
fn multi_table_mixed_types_scan_every_route() {
    let orbit_query =
        "MATCH (u:User {id: 1})-[:AUTHORED|DEFINES]->(p:Project) RETURN u, p LIMIT 25";
    let result = compile(
        orbit_query,
        Frontend::Gql,
        &multi_table_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();
    assert!(rendered.contains("FROM gl_code_edge"), "{rendered}");
    assert!(rendered.contains("FROM gl_edge"), "{rendered}");
    assert!(rendered.contains("UNION ALL"), "{rendered}");
    assert_eq!(rendered.matches("LIMIT 1 BY").count(), 2, "{rendered}");
    assert!(!rendered.contains(" FINAL"), "{rendered}");
}

#[test]
fn single_table_ontology_no_union() {
    let orbit_query = "MATCH (u:User {id: 1})-[:AUTHORED]->(p:Project) RETURN u, p LIMIT 25";
    let result = compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    let rendered = result.base.render();
    assert!(
        !rendered.contains("UNION ALL"),
        "single-table ontology should not produce UNION ALL: {rendered}"
    );
}

#[test]
fn multi_table_path_finding_scans_all_tables() {
    let orbit_query = "MATCH path = ANY SHORTEST (start:User {id: 1})-[:CONTAINS|DEFINES*1..3]->(`end`:Definition {id: 100}) RETURN path";
    let result = compile(
        orbit_query,
        Frontend::Gql,
        &multi_table_ontology(),
        &test_ctx(),
    )
    .unwrap();
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
    let ontology = std::sync::Arc::new(
        ontology::Ontology::new()
            .with_nodes(["File"])
            .with_edges(["DEFINES"])
            .with_fields("File", [("path", DataType::String)])
            .with_default_columns("File", ["path"])
            .with_redaction("File", "project", "project_id"),
    );
    let result = compile(orbit_query, Frontend::Gql, &ontology, &test_ctx()).unwrap();
    let rendered = result.base.render();

    let gl_file_refs = rendered.matches("gl_file").count();
    assert_eq!(
        gl_file_refs, 2,
        "expected one gl_file scan per direction arm; got {gl_file_refs}\nSQL:\n{rendered}"
    );
    assert!(
        rendered.contains("f.project_id AS _orbit_f_id"),
        "neighbors must project the redaction id without an alias clash: {rendered}"
    );
}

#[test]
fn multi_table_neighbors_scans_all_tables() {
    let orbit_query = "MATCH (p:Project {id: 1})--(n) RETURN n";
    let result = compile(
        orbit_query,
        Frontend::Gql,
        &multi_table_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let rendered = result.base.render();
    assert!(
        rendered.contains("gl_edge") && rendered.contains("gl_code_edge"),
        "wildcard neighbors should scan both edge tables: {rendered}"
    );
}

use crate::compiler::setup::{admin_ctx, embedded_ontology};

const SCOPED_LOOKUP: &str = "FROM gl_project AS _orbit_scope WHERE";

fn render_scoped(orbit_query: &str) -> String {
    compile(
        orbit_query,
        Frontend::Gql,
        &embedded_ontology(),
        &admin_ctx(),
    )
    .unwrap()
    .base
    .render()
}

#[test]
fn scoped_traversal_injects_tight_prefix() {
    let orbit_query =
        "MATCH (wi:WorkItem)-[:IN_PROJECT]->(p:Project) WHERE p.id = 1 RETURN wi.id, p LIMIT 100";
    let sql = render_scoped(orbit_query);
    assert!(
        sql.contains(SCOPED_LOOKUP) && sql.contains("(_orbit_scope.id = 1)"),
        "{sql}"
    );
}

#[test]
fn scoped_authorization_join_injects_tight_prefix() {
    let orbit_query = "MATCH (c:Definition)-[:CALLS]->(d:Definition) WHERE d.project_id = 5 AND d.name = 'compile' RETURN c.fqn, d.fqn LIMIT 100";
    let sql = render_scoped(orbit_query);
    assert!(
        sql.contains("startsWith(c.traversal_path, (SELECT")
            && sql.contains("(_orbit_scope.id = 5)"),
        "the caller's authorization scan must carry the project prefix: {sql}"
    );
}

#[test]
fn scoped_aggregation_injects_tight_prefix() {
    let orbit_query = "MATCH (wi:WorkItem)-[:IN_PROJECT]->(p:Project) WHERE p.id = 1 RETURN p, count(wi) AS c LIMIT 100";
    let sql = render_scoped(orbit_query);
    assert!(
        sql.contains(SCOPED_LOOKUP) && sql.contains("(_orbit_scope.id = 1)"),
        "{sql}"
    );
}

#[test]
fn scoped_count_condition_excludes_the_scope_lookup() {
    let orbit_query =
        "MATCH (u:User)-[:MEMBER_OF]->(g:Group {id: 100}) RETURN g, count(u) AS n LIMIT 5";
    let sql = render_scoped(orbit_query);
    let count_arg = sql
        .split("countIf(")
        .nth(1)
        .unwrap()
        .split(" AS n")
        .next()
        .unwrap();
    assert!(!count_arg.contains("_orbit_scope"), "{sql}");
    assert!(sql.contains("FROM gl_group AS _orbit_scope WHERE"), "{sql}");
}

#[test]
fn cross_namespace_related_to_edge_scans_only_the_source_namespace() {
    let orbit_query = "MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem)-[:RELATED_TO]->(rel:WorkItem) WHERE p.id = 1 RETURN p, wi.id, rel.id, rel.title LIMIT 100";
    let ontology = embedded_ontology();
    let compiled = compile(orbit_query, Frontend::Gql, &ontology, &admin_ctx()).unwrap();
    let sql = compiled.base.render();

    let related_scan = sql.split("FROM gl_edge AS _orbit_edge_1").nth(1).unwrap();
    let related_scope = related_scan.split(" ON ").next().unwrap();
    assert!(
        related_scope.contains(SCOPED_LOOKUP)
            && !sql.contains("startsWith(rel.traversal_path, (SELECT"),
        "the RELATED_TO rows carry the source's path, the target may live anywhere: {sql}"
    );

    let compiler::HydrationPlan::Static(templates) = &compiled.hydration else {
        panic!("expected static hydration");
    };
    let rel = templates.iter().find(|t| t.node_alias == "rel").unwrap();
    assert!(rel.injected_columns.is_empty());
    assert_eq!(rel.destination_table, "gl_work_item");
}

#[test]
fn orbit_query_bounded_hops_and_relationship_filters() {
    for query in [
        "MATCH (a:User {id: 1})-[:MEMBER_OF*1..3]->(b:Project) RETURN a, b",
        "MATCH (a:User {id: 1})-[:MEMBER_OF*2]->(b:Project) RETURN a, b",
        "MATCH (a:User {id: 1})-[r:MEMBER_OF]->(b:Project) WHERE r.target_id >= 2 RETURN a, b",
    ] {
        compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    }
    for (expected, query) in [
        (
            "MATCH (a:User {id: 1})-[r:MEMBER_OF {target_id: 2}]->(b:Project) RETURN a, b",
            "MATCH (a:User {id: 1})-[:MEMBER_OF {target_id: 2}]->(b:Project) RETURN a, b",
        ),
        (
            "MATCH (a:User {id: 1})-[r:MEMBER_OF {target_id: 2}]->(b:Project) RETURN a, b",
            "MATCH (a:User {id: 1})-[:MEMBER_OF*1..1 {target_id: 2}]->(b:Project) RETURN a, b",
        ),
        (
            "MATCH (a:User {id: 1})-[:MEMBER_OF*1..3]->(b:Project) RETURN a, b",
            "MATCH (a:User {id: 1})-[:MEMBER_OF*1..3 {}]->(b:Project) RETURN a, b",
        ),
    ] {
        compile_equivalent(expected, query, &embedded_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_id_selectors_preserve_additional_predicates() {
    for query in [
        "MATCH (u:User) WHERE u.id IN [1, 2] AND u.id = 2 RETURN u",
        "MATCH (u:User) WHERE u.id IN [1, 2] AND u.id IN [2, 3] RETURN u",
        "MATCH (u:User) WHERE u.id >= 1 AND u.id IN [1, 2] AND u.id <= 3 AND u.id = 2 RETURN u",
        "MATCH (u:User {id: 1}) WHERE u.id IN [2, 3] AND u.id >= 1 AND u.id <= 3 RETURN u",
        "MATCH (u:User {id: 1}) WHERE u.id IN [2, 3] RETURN u",
        "MATCH (u:User {id: 1}) WHERE u.id = 2 RETURN u",
        "MATCH (u:User {id: 1}) WHERE u.id >= 1 AND u.id <= 10000 RETURN u",
    ] {
        compile(query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
    }
    for (expected, query) in [
        (
            "MATCH (u:User) WHERE u.id >= 2 AND u.id <= 9 RETURN u",
            "MATCH (u:User) WHERE u.id > 1 AND u.id < 10 RETURN u",
        ),
        (
            "MATCH (u:User) WHERE u.username CONTAINS 'abc' AND u.username CONTAINS 'def' RETURN u",
            "MATCH (u:User) WHERE (u.username CONTAINS 'abc' AND u.username CONTAINS 'def') RETURN u",
        ),
    ] {
        compile_equivalent(expected, query, &test_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_id_list_and_range_promote_independently() {
    let ontology = test_ontology();
    let context = test_ctx();
    for list in ["u.id IN [1, 2, 3, 5]", "u.id IN ['1', '2', '3', '5']"] {
        for bounds in [["u.id >= 1", "u.id <= 3"], ["u.id > 0", "u.id < 4"]] {
            let predicates = [list, bounds[0], bounds[1]];
            for order in [
                [0, 1, 2],
                [0, 2, 1],
                [1, 0, 2],
                [1, 2, 0],
                [2, 0, 1],
                [2, 1, 0],
            ] {
                let conditions = order.map(|i| predicates[i]).join(" AND ");
                let query =
                    format!("MATCH (u:User) WHERE {conditions} AND u.state = 'active' RETURN u");
                let input = compiler::passes::frontend::gql::parse(&query).unwrap();
                let node = &input.nodes[0];
                assert_eq!(node.node_ids, [1, 2, 3, 5], "{query}");
                let range = node.id_range.as_ref().unwrap();
                assert_eq!((range.start, range.end), (1, 3), "{query}");
                assert!(!node.filters.contains_key("id"), "{query}");
                compile(&query, Frontend::Gql, &ontology, &context).unwrap();
            }
        }
    }
}

#[test]
fn orbit_query_aggregate_functions_property_groups_and_ordering() {
    let orbit_query = "MATCH (u:Note) WHERE u.id IN [1, 2] RETURN u.confidential AS confidential, count(u) AS n, sum(u.id), avg(u.id), min(u.created_at), max(u.created_at) ORDER BY n DESC LIMIT 10";
    compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_null_predicates_and_all_columns() {
    let orbit_query =
        "MATCH (u:User) WHERE u.created_at IS NOT NULL AND u.state IS NULL RETURN properties(u)";
    compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_m23_literals_and_comments() {
    compile(
        "MATCH (u:User {id: -9223372036854775808}) RETURN u",
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    for (expected, query) in [
        (
            "MATCH (u:User {id: 16}) RETURN u",
            "/* MATCH nothing */ match(u:User {id: 0x10}) return u; // done",
        ),
        (
            "MATCH (u:User {id: 8}) RETURN u",
            "MATCH (u:User {id: 0o10}) RETURN u",
        ),
        (
            r#"MATCH (u:User {username: 'a\'b\\c\n😀'}) RETURN u"#,
            r#"MATCH (`u`:User {username: 'a\'b\\c\n\uD83D\uDE00'}) RETURN u"#,
        ),
        (
            "MATCH (u:User) WHERE u.created_at >= '2024-01-01' RETURN u",
            "MATCH (u:User) WHERE u.created_at >= date('2024-01-01') RETURN u",
        ),
    ] {
        compile_equivalent(expected, query, &test_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_rejects_inline_filters_on_variable_length_relationships() {
    for query in [
        "MATCH (a:Group {id: 1})-[:CONTAINS*2 {source_id: 1}]->(b:Group) RETURN a, b",
        "MATCH (a:Group {id: 1})-[:CONTAINS*1..2 {source_id: 1}]->(b:Group) RETURN a, b",
        "MATCH (a:Group {id: 1})<-[r:CONTAINS*2..3 {target_id: 1}]-(b:Group) RETURN a, b",
    ] {
        let error = compiler::passes::frontend::gql::parse(query).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("property filters on variable-length relationships are unsupported"),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_rejects_aggregation_over_shortest_paths() {
    let ontology = embedded_ontology();
    let context = test_ctx();
    for query in [
        "MATCH path = ANY SHORTEST (u:User)-[*1..3]->(p:Project) WHERE u.id >= 1 AND u.id <= 10000 RETURN p, count(u) AS hit LIMIT 10",
        "MATCH path = ANY SHORTEST (u:User {id: 1})-[*1..3]->(p:Project {id: 2}) WHERE u.id >= 2 AND u.id <= 2 RETURN p{.id}, count(u) AS hit LIMIT 1",
    ] {
        let error = compile(query, Frontend::Gql, &ontology, &context).expect_err(query);
        assert!(
            matches!(&error, QueryError::Validation(message)
                if message == "aggregation over shortest paths is not supported"),
            "{query}: {error}"
        );
    }
    for query in [
        "MATCH path = ANY SHORTEST (u:User {id: 1})-[*1..3]->(p:Project) RETURN path, count(u)",
        "MATCH path = ANY SHORTEST (u:User {id: 1})-[*1..3]->(p:Project) RETURN properties(path), count(u)",
    ] {
        let error = compiler::passes::frontend::gql::parse(query).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("aggregation cannot return the path variable"),
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
        let error = compiler::passes::frontend::gql::parse(query).expect_err(query);
        assert!(
            error
                .to_string()
                .contains("dynamic graph results cannot be renamed or projected as properties"),
            "{query}: {error}"
        );
    }
    let query = "MATCH (center:User {id: 1})--(n) RETURN center, n";
    compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    let query = "MATCH (center:User {id: 1})--(n) RETURN center.username, center.state, n";
    compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
}

#[test]
fn orbit_query_neighbors_accepts_the_endpoint_on_either_side() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    for (center_first, endpoint_first) in [
        (
            "MATCH (center:WorkItem {id: 1})<-[:AUTHORED]-(n) RETURN center, n",
            "MATCH (n)-[:AUTHORED]->(center:WorkItem {id: 1}) RETURN center, n",
        ),
        (
            "MATCH (center:WorkItem {id: 1})-->(n) RETURN center, n",
            "MATCH (n)<--(center:WorkItem {id: 1}) RETURN center, n",
        ),
        (
            "MATCH (center:WorkItem {id: 1})--(n) RETURN center, n",
            "MATCH (n)--(center:WorkItem {id: 1}) RETURN center, n",
        ),
    ] {
        compile_equivalent(center_first, endpoint_first, &ontology, &ctx)
            .unwrap_or_else(|error| panic!("{endpoint_first}: {error}"));
    }
}

#[test]
fn orbit_query_rejects_more_than_one_sort_key() {
    for query in [
        "MATCH (mr:MergeRequest {project_id: 1}) RETURN mr.iid ORDER BY mr.updated_at DESC, mr.iid DESC",
        "MATCH (mr:MergeRequest {project_id: 1}) RETURN mr.iid ORDER BY mr.updated_at,mr.iid LIMIT 5",
    ] {
        let error =
            compile(query, Frontend::Gql, &embedded_ontology(), &test_ctx()).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains("ORDER BY accepts one sort key")),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_virtual_filter_spellings_hydrate_identically() {
    let ontology = embedded_ontology();
    for (expected, query) in [
        (
            r#"MATCH (f:File {id: 1, content: 'abc'}) RETURN f"#,
            "MATCH (f:File {id: 1}) WHERE f.content = 'abc' RETURN f",
        ),
        (
            r#"MATCH (f:File {id: 1}) WHERE f.content = 'abc' RETURN f"#,
            "MATCH (f:File {id: 1, content: 'abc'}) RETURN f",
        ),
    ] {
        let compiled = compile_equivalent(expected, query, &ontology, &test_ctx()).unwrap();
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
fn orbit_query_consecutive_match_clauses_compile_like_one_pattern() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    let single = compile(
        "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}), (u:User)-[:AUTHORED]->(mr) WHERE mr.state = 'merged' AND u.username = 'a' RETURN mr.iid, u.username LIMIT 5",
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap();
    for query in [
        "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) MATCH (u:User)-[:AUTHORED]->(mr) WHERE mr.state = 'merged' AND u.username = 'a' RETURN mr.iid, u.username LIMIT 5",
        "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) WHERE mr.state = 'merged' MATCH (u:User)-[:AUTHORED]->(mr) WHERE u.username = 'a' RETURN mr.iid, u.username LIMIT 5",
    ] {
        let compiled = compile(query, Frontend::Gql, &ontology, &ctx).unwrap();
        assert_eq!(single.base.render(), compiled.base.render(), "{query}");
        assert_eq!(single.hydration, compiled.hydration, "{query}");
    }
    for (query, expected) in [
        (
            "MATCH (u:User {id: 1}) MATCH (p:Project {id: 2}) RETURN u, p",
            "all declared nodes must belong to one connected pattern",
        ),
        (
            "MATCH p = ANY SHORTEST (a:User {id: 1})-[*1..3]->(b:Project {id: 2}) MATCH (b)-[:IN_GROUP]->(g:Group) RETURN p",
            "a shortest path must be the only MATCH pattern",
        ),
        (
            "MATCH (u:User {id: 1}) OPTIONAL MATCH (u)-[:AUTHORED]->(mr:MergeRequest) RETURN u",
            "Orbit query syntax",
        ),
    ] {
        let error = compile(query, Frontend::Gql, &ontology, &ctx).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_rejects_relationships_the_ontology_cannot_match() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    for (query, expected) in [
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED]->(u:User) RETURN mr, u",
            "AUTHORED goes from User to MergeRequest; reverse the arrow: (u)-[:AUTHORED]->(mr)",
        ),
        (
            "MATCH (u:User)<-[a:AUTHORED]-(mr:MergeRequest {project_id: 1}) RETURN count(mr)",
            "AUTHORED goes from User to MergeRequest; reverse the arrow: (u)-[:AUTHORED]->(mr)",
        ),
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED]->(p:Project) RETURN mr, p",
            "AUTHORED does not connect MergeRequest and Project in either direction",
        ),
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED|CLOSES]->(u:User) RETURN mr, u",
            "AUTHORED goes from User to MergeRequest; reverse the arrow: (u)-[:AUTHORED]->(mr); CLOSES does not connect MergeRequest and User",
        ),
        (
            "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED|CLOSES]->(p:Project) RETURN mr, p",
            "AUTHORED|CLOSES does not connect MergeRequest and Project in either direction",
        ),
        (
            "MATCH (p:Project {id: 1})<-[IN_PROJECT]-(mr:MergeRequest) RETURN count(mr)",
            "[IN_PROJECT] declares a variable, not a relationship type; write [:IN_PROJECT]",
        ),
        (
            "MATCH (mr:MergeRequest {id: 1})-[IN_MILESTONE]->(m) RETURN m",
            "[IN_MILESTONE] declares a variable, not a relationship type; write [:IN_MILESTONE]",
        ),
    ] {
        let error = compile(query, Frontend::Gql, &ontology, &ctx).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
    for query in [
        "MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {project_id: 1}) RETURN mr, u",
        "MATCH (mr:MergeRequest {project_id: 1})<-[:AUTHORED]-(u:User) RETURN count(mr)",
        "MATCH (mr:MergeRequest {project_id: 1})-[:AUTHORED|IN_PROJECT]->(p:Project) RETURN mr, p",
        "MATCH (g:Group {id: 1})-[:CONTAINS*1..2]->(p:Project) RETURN p",
        "MATCH (p:Project {id: 1})<-[r]-(mr:MergeRequest) RETURN count(mr)",
        "MATCH (p:Project {id: 1})<-[R]-(mr:MergeRequest) RETURN count(mr)",
        "MATCH (mr:MergeRequest {id: 1})-[:IN_MILESTONE]->(m) RETURN m",
    ] {
        compile(query, Frontend::Gql, &ontology, &ctx)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
}

#[test]
fn orbit_query_undirected_relationships_require_neighbors() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    for query in [
        "MATCH (g:Group {id: 1})-[:MEMBER_OF]-(u:User) RETURN g, u",
        "MATCH (g:Group {id: 1})--(u:User) RETURN g, u",
        "MATCH (g:Group {id: 1})-[:MEMBER_OF*1..2]-(u:User) RETURN g, u",
        "MATCH (u:User {id: 1})-[:MEMBER_OF]->(g:Group)-[:CONTAINS]-(p:Project) RETURN u, g, p",
        "MATCH (g:Group {id: 1})-[:MEMBER_OF]-(u:User) RETURN count(u)",
    ] {
        let error = compile(query, Frontend::Gql, &ontology, &ctx).expect_err(query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains("undirected relationships are only supported for neighbors")),
            "{query}: {error}"
        );
    }
    for query in [
        "MATCH (u:User {id: 1})-[:MEMBER_OF]->(g:Group) RETURN u, g",
        "MATCH (g:Group {id: 1})<-[:MEMBER_OF]-(u:User) RETURN g, u",
    ] {
        compile(query, Frontend::Gql, &ontology, &ctx).unwrap();
    }
    for (expected, query) in [
        (
            "MATCH (g:Group {id: 1})-[:MEMBER_OF]-(neighbor) RETURN g, neighbor",
            "MATCH (g:Group {id: 1})-[:MEMBER_OF]-(n) RETURN n",
        ),
        (
            "MATCH (g:Group {id: 1})--(neighbor) RETURN g, neighbor",
            "MATCH (g:Group {id: 1})--(n) RETURN n",
        ),
    ] {
        compile_equivalent(expected, query, &ontology, &ctx).unwrap();
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
        "MATCH (u:User) WHERE u.created_at = DATE '2024-01-01' RETURN u",
        "MATCH (u:User) RETURN DISTINCT u",
        "MATCH (u:User) RETURN count(*)",
        "MATCH (u:User {id: 1}) RETURN sum(DISTINCT u.id) AS n",
        "MATCH (u:User) RETURN collect(u.username)",
        "MATCH (u:User {id: 1}) RETURN u SKIP 1 SKIP 1",
        "MATCH (u:User {id: 1}) RETURN u SKIP -1",
        "MATCH (u:User {id: 1}) RETURN u SKIP 10001",
        "MATCH (u:User) RETURN u DEBUG DEBUG",
        "MATCH (u:User) RETURN u DEBUG LIMIT 1",
        "MATCH (u:User) RETURN u LIMIT 1 ORDER BY u.id",
        "MATCH (u:User {id: 1}) RETURN u PAGE 1",
        "MATCH (u:User {id: 1}) RETURN u LIMIT 1 AFTER 'token'",
        "MATCH (u:User) RETURN u; DEBUG",
        "MATCH (u:User) RETURN count(u) AS n AS other",
        "MATCH (u:User) RETURN u ORDER BY u.id, u.username",
        "MATCH (u:User {id: 1}) RETURN u.username AS a, u.name AS a",
        "MATCH (u:User {id: 1}) RETURN u.username ORDER BY missing",
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
        "MATCH p = ANY SHORTEST (u:User {id: 1})<-[:MEMBER_OF*1..3]-(p:Project {id: 2}) RETURN p",
        "MATCH p = shortestPath((u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group)) RETURN p",
        "MATCH p = ALL SHORTEST (u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group) RETURN p",
        "MATCH p = SHORTEST 2 (u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group) RETURN p",
        "MATCH p = (u:User {id: 1})-[:MEMBER_OF*1..3]->(g:Group) RETURN p",
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
        let error = compiler::compile(
            query,
            compiler::Frontend::Gql,
            &test_ontology(),
            &test_ctx(),
        )
        .expect_err(query);
        assert!(error.is_client_safe(), "{query}: {error}");
    }
}

#[test]
fn orbit_query_preserves_ontology_validation_and_security() {
    for query in [
        "MATCH (u:NotAnEntity) RETURN u",
        "MATCH (u:User {missing: 'abc'}) RETURN u",
        "MATCH (u:User {username: 42}) RETURN u",
        "MATCH (u:User) WHERE u.username CONTAINS 'ab' RETURN u",
        "MATCH (u:User) WHERE token_match(u.username, 'abc') RETURN u",
        "MATCH (u:User {id: 1})-[:UNKNOWN_EDGE]->(p:Project) RETURN u, p",
    ] {
        compile(query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap_err();
    }
    compile_equivalent(
        "MATCH (u:User) RETURN u{.missing}",
        "MATCH (u:User) RETURN u.missing",
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap_err();
}

#[test]
fn orbit_query_nesting_guard_sees_through_escaped_names() {
    let hidden = format!(
        "MATCH (u:User) WHERE u.`'` = 1 AND {}u.id = 1{} AND u.`'` = 2 RETURN u",
        "(".repeat(2_000),
        ")".repeat(2_000)
    );
    let error = compiler::passes::frontend::gql::parse(&hidden).unwrap_err();
    assert!(error.to_string().contains("nesting is too deep"), "{error}");
    let orbit_query = "MATCH (`u`:`User` {`id`: 1}) RETURN `u` LIMIT 5";
    compile(orbit_query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
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
        assert!(compiler::passes::frontend::gql::parse(&query).is_err());
    }
    let query = "MATCH (u:User) WHERE u.id IN ['invalid'] AND u.id >= 1 AND u.id <= 3 RETURN u";
    assert!(
        compiler::compile(
            query,
            compiler::Frontend::Gql,
            &test_ontology(),
            &test_ctx(),
        )
        .is_err()
    );
}

#[test]
fn orbit_query_caps_reject_during_lowering() {
    let nodes: Vec<String> = (0..4_000).map(|i| format!("(a{i})")).collect();
    let properties: Vec<String> = (0..51).map(|i| format!("u.c{i}")).collect();
    for (query, first_rejected, message) in [
        (
            format!("MATCH {} RETURN a0", nodes.join(",")),
            "(a5)",
            "a query declares at most 5 nodes",
        ),
        (
            format!("MATCH (u:User) RETURN {}", properties.join(", ")),
            "u.c50",
            "a node projects at most 50 properties",
        ),
    ] {
        let column = query.find(first_rejected).unwrap() + 1;
        let expected = format!("line 1, column {column}: {message}");
        let error = compiler::passes::frontend::gql::parse(&query).unwrap_err();
        assert!(
            matches!(&error, QueryError::Validation(m) if *m == expected),
            "{error:?}"
        );
    }
}

#[test]
fn native_predicate_trees_are_bounded_before_planning() {
    use compiler::input::{BooleanExpression, Condition, FilterOp, InputFilter, PredicateTarget};
    let model =
        query_data_model::ClickHouseDataModel::derive(crate::compiler::setup::embedded_ontology())
            .unwrap();
    let validator = compiler::passes::validate::Validator::new(&model);
    let mut input =
        compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let leaf = |target| {
        BooleanExpression::Leaf(Condition {
            target,
            property: "id".into(),
            filter: InputFilter {
                op: Some(FilterOp::In),
                value: Some(serde_json::json!([1])),
                ..Default::default()
            },
        })
    };
    let mut deep = leaf(PredicateTarget::Node("u".into()));
    for _ in 0..1024 {
        deep = BooleanExpression::Not(Box::new(deep));
    }
    input.predicates = vec![deep];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("must not nest deeper than 32"),
        "{error}"
    );
    input.predicates = vec![BooleanExpression::Not(Box::new(leaf(
        PredicateTarget::Relationship(7),
    )))];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("undefined relationship"),
        "{error}"
    );
}

#[test]
fn native_conditions_need_a_column() {
    use compiler::input::{BooleanExpression, InputFilter};
    let base = compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let mut empty_group = base.clone();
    empty_group.predicates = vec![BooleanExpression::Not(Box::new(BooleanExpression::And(
        vec![],
    )))];
    let mut empty_property = base;
    empty_property.nodes[0].filters.insert(
        String::new(),
        vec![InputFilter {
            value: Some(serde_json::json!(1)),
            ..Default::default()
        }],
    );
    for input in [empty_group, empty_property] {
        let error = compiler::compile_input(
            input,
            Default::default(),
            &crate::compiler::setup::embedded_ontology(),
            &test_ctx(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("every condition must compare a property"),
            "{error}"
        );
    }
}

#[test]
fn orbit_query_schema_caps_reject() {
    let long_identifier = "a".repeat(65);
    let long_string = "x".repeat(1025);
    for query in [
        "MATCH (u:User {id: 1}) RETURN u LIMIT 0".to_string(),
        "MATCH (u:User {id: 1}) RETURN u LIMIT 1001".to_string(),
        "MATCH (u:User {id: 1})-[:MEMBER_OF*1..4]->(p:Project) RETURN u, p".to_string(),
        "MATCH (u:User {id: 1}) RETURN ghost, count(u)".to_string(),
        format!("MATCH ({long_identifier}:User {{id: 1}}) RETURN {long_identifier}"),
        format!("MATCH (u:User {{username: '{long_string}'}}) RETURN u"),
    ] {
        compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap_err();
    }
    let longest = "a".repeat(64);
    compile(
        &format!("MATCH ({longest}:User {{id: 1}}) RETURN {longest}"),
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
}

#[test]
fn orbit_query_path_depth_caps() {
    let ontology = test_ontology();
    let ctx = test_ctx();
    for depth in [0, 1, 3, 4, u32::MAX] {
        let query = format!(
            "MATCH p = ANY SHORTEST (a:Project {{id: 1}})-[:CONTAINS*1..{depth}]->(b:Project {{id: 2}}) RETURN p"
        );
        let result = compile(&query, Frontend::Gql, &ontology, &ctx);
        if (1..=3).contains(&depth) {
            result.unwrap();
        } else {
            assert!(matches!(result.unwrap_err(), QueryError::Validation(_)));
        }
    }
}

#[test]
fn orbit_query_neighbors_relationship_type_caps() {
    let ontology = test_ontology();
    let ctx = test_ctx();
    for (left, right) in [("-", "->"), ("<-", "-"), ("-", "-")] {
        for count in [1, 10, 11] {
            let kinds = vec!["AUTHORED"; count];
            let query = format!(
                "MATCH (u:User {{id: 1}}){left}[:{}]{right}(n) RETURN n",
                kinds.join("|")
            );
            let result = compile(&query, Frontend::Gql, &ontology, &ctx);
            if count <= 10 {
                result.unwrap();
            } else {
                assert!(matches!(result.unwrap_err(), QueryError::Validation(_)));
            }
        }
    }
}

#[test]
fn orbit_query_structural_caps_reject() {
    let chain = |count: usize| {
        let pattern: Vec<String> = (1..count).map(|i| format!("(n{i}:Group)")).collect();
        format!(
            "MATCH (n0:Group {{id: 1}})-[:CONTAINS]->{} RETURN n0",
            pattern.join("-[:CONTAINS]->")
        )
    };
    let ids = |count: usize| {
        let list: Vec<String> = (1..=count).map(|i| i.to_string()).collect();
        format!(
            "MATCH (u:User) WHERE u.id IN [{}] RETURN u",
            list.join(", ")
        )
    };
    let repeated_predicates = |count: usize| {
        let entries: Vec<String> = (0..count)
            .map(|i| format!("u.username CONTAINS 'abc{i}'"))
            .collect();
        format!("MATCH (u:User) WHERE {} RETURN u", entries.join(" AND "))
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
        format!(
            "MATCH (a:User {{id: 1}})-[:{}]->(b:Project) RETURN a, b",
            kinds.join("|")
        )
    };

    for text in [chain(5), ids(500), repeated_predicates(10), rel_types(10)] {
        compile(&text, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    }
    for text in [chain(6), ids(501), repeated_predicates(11), rel_types(11)] {
        let error = compile(&text, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap_err();
        assert!(
            matches!(error, QueryError::Validation(_)),
            "{text}: {error}"
        );
    }
}

#[test]
fn orbit_query_traversal_aliases_are_ignored_and_sortable() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    let base = "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1})";
    for (plain, aliased) in [
        (
            "RETURN mr.iid, p ORDER BY mr.iid LIMIT 5",
            "RETURN mr.iid AS number, p AS project ORDER BY number LIMIT 5",
        ),
        (
            "RETURN mr.iid, p ORDER BY mr.iid LIMIT 5",
            "RETURN mr.iid AS number, p AS project ORDER BY mr.iid LIMIT 5",
        ),
        (
            "RETURN mr.iid, p{.full_path} ORDER BY mr.iid DESC LIMIT 5",
            "RETURN mr.iid AS number, p{.full_path} AS project ORDER BY number DESC LIMIT 5",
        ),
        (
            "RETURN mr.iid ORDER BY mr.iid SKIP 5 LIMIT 5",
            "RETURN mr.iid AS number ORDER BY number SKIP 5 LIMIT 5",
        ),
    ] {
        let plain = compile(&format!("{base} {plain}"), Frontend::Gql, &ontology, &ctx).unwrap();
        let query = format!("{base} {aliased}");
        let aliased = compile(&query, Frontend::Gql, &ontology, &ctx).unwrap();
        assert_eq!(plain.base.render(), aliased.base.render(), "{query}");
        assert_eq!(plain.hydration, aliased.hydration, "{query}");
    }

    let plain = compile(
        "MATCH (center:WorkItem {id: 1})-->(n) RETURN center, n",
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap();
    let aliased = compile(
        "MATCH (center:WorkItem {id: 1})-->(n) RETURN center AS item, n",
        Frontend::Gql,
        &ontology,
        &ctx,
    )
    .unwrap();
    assert_eq!(plain.base.render(), aliased.base.render());

    for (query, expected) in [
        (
            "RETURN mr.iid, p AS project ORDER BY project",
            "not a node alias",
        ),
        ("RETURN mr.iid AS x, p AS x", "duplicate alias"),
        ("RETURN mr.iid AS x, p.full_path AS x", "duplicate alias"),
        ("RETURN mr.iid ORDER BY missing", "or a property alias"),
    ] {
        let query = format!("{base} {query}");
        let error = compile(&query, Frontend::Gql, &ontology, &ctx).expect_err(&query);
        assert!(
            matches!(error, QueryError::Validation(ref message) if message.contains(expected)),
            "{query}: {error}"
        );
    }
}

#[test]
fn orbit_query_repeated_node_projections_ignore_property_order() {
    for properties in [".username, .name", ".name, .username"] {
        let query = format!(
            "MATCH (u:User {{id: 1}})-[:AUTHORED]->(mr:MergeRequest) RETURN u{{.username, .name}} AS author, u{{{properties}}} AS owner, count(mr) AS mr_count"
        );
        compile(&query, Frontend::Gql, &embedded_ontology(), &test_ctx()).unwrap();
    }
}

#[test]
fn orbit_query_digit_string_ids_and_aggregated_identity_columns() {
    let ontology = embedded_ontology();
    compile_equivalent(
        "MATCH (u:User {id: 7}) RETURN u",
        "MATCH (u:User) WHERE u.id IN ['7'] RETURN u",
        &ontology,
        &test_ctx(),
    )
    .unwrap();
    let compiled = compile(
        "MATCH (f:File) WHERE f.id IN [1, 2] RETURN f{.id, .content}, count(f) AS total LIMIT 5",
        Frontend::Gql,
        &ontology,
        &test_ctx(),
    )
    .unwrap();
    assert!(!compiled.base.sql.contains("COUNT()") || compiled.base.sql.contains("GROUP BY"));
}

#[test]
fn orbit_query_debug_and_dynamic_properties_set_options() {
    let compiled = compile(
        "MATCH (u:User {id: 1}) RETURN u DEBUG",
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert!(compiled.input.options.include_debug_sql);
    let compiled = compile(
        "MATCH (g:Group {id: 100})--(n) RETURN g, properties(n)",
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
    assert_eq!(
        compiled.input.options.dynamic_columns,
        DynamicColumnMode::All
    );
    let compiled = compile("MATCH p = ANY SHORTEST (start:Project {id: 100})-[:CONTAINS*1..3]->(`end`:Project {id: 200}) RETURN properties(p)", Frontend::Gql, &test_ontology(), &test_ctx())
    .unwrap();
    assert_eq!(
        compiled.input.options.dynamic_columns,
        DynamicColumnMode::All
    );
}

#[test]
fn orbit_query_optional_clauses_preserve_options() {
    for order in ["", " ORDER BY u.id"] {
        for (limit, rows, skip) in [
            ("", None, None),
            (" LIMIT 7", Some(7), None),
            (" SKIP 7", None, Some(7)),
            (" SKIP 7 LIMIT 3", Some(3), Some(7)),
        ] {
            for debug in ["", " DEBUG"] {
                let query = format!("MATCH (u:User {{id: 1}}) RETURN u{order}{limit}{debug}");
                let compiled =
                    compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
                let input = compiled.input;
                assert_eq!(
                    input.options.include_debug_sql,
                    !debug.is_empty(),
                    "{query}"
                );
                assert_eq!(input.order_by.is_some(), !order.is_empty(), "{query}");
                assert_eq!(input.skip, skip, "{query}");
                if let Some(rows) = rows {
                    assert_eq!(input.limit, rows, "{query}");
                }
            }
        }
    }
}

#[test]
fn orbit_query_debug_in_literals_and_comments_stays_data() {
    for (literal, value) in [
        (r#"'DEBUG'"#, "DEBUG"),
        (r#"'x\' RETURN u DEBUG //'"#, "x' RETURN u DEBUG //"),
        (r#"'\u0027 RETURN u DEBUG //'"#, "' RETURN u DEBUG //"),
        (
            r#""'; DROP TABLE gl_user; --""#,
            "'; DROP TABLE gl_user; --",
        ),
    ] {
        for comment in ["", " /* DEBUG */", " // DEBUG"] {
            let query = format!("MATCH (u:User {{username: {literal}}}) RETURN u{comment}");
            let compiled = compile(&query, Frontend::Gql, &test_ontology(), &test_ctx()).unwrap();
            assert!(!compiled.input.options.include_debug_sql, "{query}");
            assert!(
                has_param_value(&compiled.base.params, &serde_json::json!(value)),
                "{query}"
            );
            assert!(!compiled.base.sql.contains(value), "{query}");
        }
    }
}

#[test]
fn orbit_query_reserved_skip_words_need_backticks() {
    assert!(
        compile(
            "MATCH (offset:User {id: 1}) RETURN offset",
            Frontend::Gql,
            &test_ontology(),
            &test_ctx(),
        )
        .is_err()
    );
    compile(
        "MATCH (`offset`:User {id: 1}) RETURN `offset`",
        Frontend::Gql,
        &test_ontology(),
        &test_ctx(),
    )
    .unwrap();
}

#[test]
fn gql_prepares_queries_and_scoped_schema() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    assert!(matches!(
        prepare("MATCH (n:User {id: 1}) RETURN n", &ontology, &ctx, All),
        Ok(PreparedStatement::Query(_))
    ));
    for (scope, call, node) in [
        (All, "CALL db.schema()", None),
        (Local, "CALL db.schema()", None),
        (All, "CALL db.schema('MergeRequest')", Some("MergeRequest")),
        (Local, "CALL db.schema('File')", Some("File")),
    ] {
        let PreparedStatement::Schema(response) = prepare(call, &ontology, &ctx, scope).unwrap()
        else {
            panic!("expected schema: {call}");
        };
        if let Some(name) = node {
            assert_eq!(response.domains.len(), 1);
            assert_eq!(response.domains[0].nodes.len(), 1);
            let node = serde_json::to_value(&response.domains[0].nodes[0]).unwrap();
            assert_eq!(node["name"], name);
            assert!(!node["props"].as_array().unwrap().is_empty());
            assert!(response.edges.is_empty());
        } else {
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(build_compact_schema_response(&ontology, scope, None))
                    .unwrap(),
            );
        }
    }
    for (scope, call) in [
        (All, "CALL db.schema('Missing')"),
        (All, "CALL db.schema('*')"),
        (All, "CALL db.schema('File', 'User')"),
        (All, "CALL db.schema() YIELD name"),
        (Local, "CALL db.schema('MergeRequest')"),
    ] {
        assert!(
            prepare(call, &ontology, &ctx, scope)
                .unwrap_err()
                .is_client_safe()
        );
    }
    assert!(compile("CALL db.schema()", Frontend::Gql, &ontology, &ctx).is_err());
}

#[test]
fn cte_statements_carry_the_ctes_they_read() {
    let compiled = compile(
        "MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(mr:MergeRequest)-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile) WHERE g.full_path = 'g701' RETURN p, count(f) AS c LIMIT 10",
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let statements = compiler::passes::codegen::clickhouse::cte_statements(&compiled.ast).unwrap();
    assert!(
        statements
            .iter()
            .any(|(_, sql, _)| sql.starts_with("WITH ")),
        "expected a CTE body that reads another CTE"
    );
    for (own, sql, _) in &statements {
        for (position, _) in sql.match_indices("_orbit_relation_") {
            let digits: String = sql[position + "_orbit_relation_".len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            let name = format!("_orbit_relation_{digits}");
            assert!(
                &name == own || sql.contains(&format!("{name} AS ")),
                "{name} is read but not defined in: {sql}"
            );
        }
    }
}
