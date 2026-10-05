//! DuckDB dialect end-to-end tests.

use crate::compiler::setup::test_ontology;
use crate::compiler::utils::ParsedSql;
use compiler::{Frontend, compile_local};

fn compile(json: &str) -> compiler::passes::codegen::CompiledQueryContext {
    compile_local(json, Frontend::JsonDsl, &test_ontology()).unwrap()
}

fn parse_duckdb(json: &str) -> ParsedSql {
    ParsedSql::from_query(&compile(json).base)
}

#[test]
fn temporal_parameters_bind_and_render_without_clickhouse_syntax() {
    use compiler::ast::{Expr, Node, Query, SelectExpr, SqlType, TableRef, TimeZone};

    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("parameters.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE events(created_at TIMESTAMPTZ); INSERT INTO events VALUES ('2026-10-05T01:02:03.123456Z');").unwrap();
    let ast = Node::Query(Box::new(Query {
        select: vec![SelectExpr::col("e", "created_at")],
        from: TableRef::scan("events", "e"),
        where_clause: Some(Expr::eq(
            Expr::col("e", "created_at"),
            Expr::param(
                SqlType::Timestamp {
                    precision: 6,
                    timezone: Some(TimeZone::Utc),
                },
                "2026-10-05T01:02:03.123456Z",
            ),
        )),
        ..Default::default()
    }));
    let compiled = compiler::passes::codegen::duckdb::codegen(&ast, Default::default()).unwrap();
    assert!(!compiled.render().contains("toDateTime"));
    let parameter = compiled.params.get("p1").unwrap();
    let parameters = duckdb_client::to_sql_params(&[parameter]);
    let bound = database
        .query_arrow_params(&compiled.sql, &parameters)
        .unwrap();
    let rendered = database.query_arrow(&compiled.render()).unwrap();
    assert_eq!(bound, rendered);
    assert_eq!(bound[0].num_rows(), 1);
}

#[test]
fn nested_cte_codegen_executes_with_its_local_definition() {
    use compiler::ast::{Cte, Expr, Node, Query, SelectExpr, TableRef};

    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("nested.duckdb")).unwrap();
    database
        .initialize_schema("CREATE TABLE nodes(id BIGINT); INSERT INTO nodes VALUES (7);")
        .unwrap();
    let ast = Node::Query(Box::new(Query {
        ctes: vec![Cte::new(
            "result",
            Query {
                ctes: vec![Cte::new(
                    "seed",
                    Query {
                        select: vec![SelectExpr::col("n", "id")],
                        from: TableRef::scan("nodes", "n"),
                        ..Default::default()
                    },
                )],
                select: vec![SelectExpr::col("s", "id")],
                from: TableRef::scan("seed", "s"),
                ..Default::default()
            },
        )],
        select: vec![SelectExpr::new(Expr::col("r", "id"), "id")],
        from: TableRef::scan("result", "r"),
        ..Default::default()
    }));
    let query = compiler::passes::codegen::duckdb::codegen(&ast, Default::default()).unwrap();
    let result = database.query_arrow(&query.render()).unwrap();
    assert_eq!(
        arrow::util::display::array_value_to_string(result[0].column(0), 0).unwrap(),
        "7"
    );
}

#[test]
fn semantic_aggregate_codegen_preserves_null_and_empty_input_results() {
    use compiler::ast::{Expr, Node, Query, SelectExpr, TableRef};
    use compiler::input::AggFunction;

    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("aggregate.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE measurements(value BIGINT, keep BOOLEAN); INSERT INTO measurements VALUES (2, true), (2, true), (NULL, true), (9, false);").unwrap();
    for (function, argument, distinct, filtered, expected) in [
        (AggFunction::Count, false, false, false, "4"),
        (AggFunction::Count, true, false, false, "3"),
        (AggFunction::Count, false, false, true, "3"),
        (AggFunction::Count, true, false, true, "2"),
        (AggFunction::Count, true, true, true, "1"),
        (AggFunction::Sum, true, false, true, "4"),
        (AggFunction::Avg, true, false, true, "2.0"),
        (AggFunction::Min, true, false, true, "2"),
        (AggFunction::Max, true, false, true, "2"),
    ] {
        for empty in [false, true] {
            let ast = Node::Query(Box::new(Query {
                select: vec![SelectExpr::new(
                    Expr::Aggregate {
                        function,
                        argument: argument.then(|| Box::new(Expr::col("m", "value"))),
                        distinct,
                        condition: filtered.then(|| Box::new(Expr::col("m", "keep"))),
                    },
                    "result",
                )],
                from: TableRef::scan("measurements", "m"),
                where_clause: empty.then(|| Expr::lit(false)),
                ..Default::default()
            }));
            let query =
                compiler::passes::codegen::duckdb::codegen(&ast, Default::default()).unwrap();
            let results = database.query_arrow(&query.render()).unwrap();
            let actual =
                arrow::util::display::array_value_to_string(results[0].column(0), 0).unwrap();
            let expected = if empty {
                if function == AggFunction::Count {
                    "0"
                } else {
                    ""
                }
            } else {
                expected
            };
            assert_eq!(actual, expected, "{}", query.sql);
        }
    }
}

#[test]
fn search_uses_positional_params() {
    let result = compile(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"],
                   "filters": {"username": "alice"}}],
        "limit": 10
    }"#,
    );

    let sql = &result.base.sql;
    assert!(sql.contains("$"), "expected positional params: {sql}");
    assert!(sql.contains("username"), "expected username filter: {sql}");
}

#[test]
fn no_clickhouse_functions_leak() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
        "limit": 10
    }"#,
    );

    assert!(!sql.has_function("startsWith"));
    assert!(!sql.has_function("has"));
    assert!(!sql.has_function("arrayConcat"));
}

#[test]
fn no_security_filter() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1], "columns": ["name"]}],
        "limit": 10
    }"#,
    );

    assert!(!sql.has_function("startsWith"));
}

#[test]
fn traversal() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "n", "entity": "Note", "columns": ["confidential"]}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "limit": 25
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_column_ref("relationship_kind"));
    assert_eq!(sql.limit_value(), Some(26), "limit + 1 probe row");
}

#[test]
fn aggregation() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "n", "entity": "Note"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
        "group_by": ["u"],
        "aggregations": [{"count": "n", "as": "note_count"}],
        "limit": 10
    }"#,
    );

    assert!(
        sql.has_function("COUNT")
            || sql.has_function("count")
            || sql.has_function("countIf")
            || sql.has_function("count_if"),
        "expected count function"
    );
    assert!(sql.has_group_by());
}

#[test]
fn path_finding() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [100]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                 "rel_types": ["MEMBER_OF"]}
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    assert!(sql.has_order_by());
}

#[test]
fn neighbors() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "neighbors",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
        "neighbors": {"direction": "outgoing"},
        "limit": 10
    }"#,
    );

    assert!(sql.has_table("gl_edge"));
    assert_eq!(sql.limit_value(), Some(11), "limit + 1 probe row");
}

#[test]
fn group_by_truncate_emits_duckdb_date_trunc() {
    let result = compile(
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "Note", "node_ids": [1]}
        ],
        "aggregations": [{"count": "u", "as": "n"}],
        "group_by": [{"key": "u.created_at", "truncate": "month", "as": "bucket"}],
        "limit": 10
    }"#,
    );
    let rendered = result.base.render();
    assert!(
        rendered.contains("date_trunc('month', u.created_at)"),
        "expected DuckDB date_trunc('month', ...); got:\n{rendered}"
    );
    assert!(
        !rendered.contains("toStartOfMonth"),
        "ClickHouse-only toStartOfMonth must not leak into DuckDB SQL:\n{rendered}"
    );
}

#[test]
fn group_by_truncate_all_units_emit_duckdb_date_trunc() {
    for unit in ["minute", "hour", "day", "week", "month", "quarter", "year"] {
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
        let result = compile_local(&json, Frontend::JsonDsl, &test_ontology())
            .unwrap_or_else(|e| panic!("compile_local failed for unit {unit}: {e:?}"));
        let rendered = result.base.render();
        let expected = format!("date_trunc('{unit}', u.created_at)");
        assert!(
            rendered.contains(&expected),
            "unit {unit}: expected `{expected}` in DuckDB SQL; got:\n{rendered}"
        );
    }
}

#[test]
fn node_ids_expand_params() {
    let sql = parse_duckdb(
        r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1, 2, 3]}],
        "limit": 10
    }"#,
    );

    assert!(sql.has_operator("IN"));
    assert!(!sql.raw_contains("Array("));
}

fn compile_gql(cypher: &str) -> Result<compiler::passes::codegen::CompiledQueryContext, String> {
    compile_local(cypher, Frontend::Gql, &test_ontology()).map_err(|e| e.to_string())
}

#[test]
fn gql_untyped_edge_pattern() {
    let r = compile_gql("MATCH (u:User {id: 1})-[e]->(n:Note) RETURN n.confidential");
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[test]
fn gql_both_nodes_projected() {
    let r = compile_gql(
        "MATCH (u:User {id: 1})-[e:AUTHORED]->(n:Note) RETURN u.username, n.confidential",
    );
    assert!(r.is_ok(), "{}", r.unwrap_err());
    let sql = r.unwrap().base.render();
    assert!(sql.contains("u_username"), "missing u_username: {sql}");
    assert!(
        sql.contains("n_confidential"),
        "missing n_confidential: {sql}"
    );
}

#[test]
fn gql_open_ended_scan() {
    let r = compile_gql("MATCH (u:User) RETURN u.username");
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[test]
fn gql_count_without_node_ids() {
    let r = compile_gql("MATCH (u:User) RETURN count(u) AS n");
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[test]
fn gql_typed_edge_traversal() {
    let r = compile_gql("MATCH (u:User {id: 1})-[e:AUTHORED]->(n:Note) RETURN n.confidential");
    assert!(r.is_ok(), "{}", r.unwrap_err());
    let sql = r.unwrap().base.render();
    assert!(
        sql.contains("relationship_kind"),
        "edge type filter missing: {sql}"
    );
}

#[test]
fn gql_order_by_across_traversal() {
    let r = compile_gql(
        "MATCH (u:User {id: 1})-[e:AUTHORED]->(n:Note) \
         RETURN n.confidential ORDER BY n.confidential",
    );
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[test]
fn gql_property_to_property_comparison() {
    let r = compile_gql(
        "MATCH (u:User {id: 1})-[e:AUTHORED]->(n:Note) \
         WHERE u.username <> u.state RETURN u.username",
    );
    assert!(r.is_ok(), "{}", r.unwrap_err());
    let sql = r.unwrap().base.render();
    assert!(
        sql.contains("u.username != u.state"),
        "property-to-property comparison missing: {sql}"
    );
}
