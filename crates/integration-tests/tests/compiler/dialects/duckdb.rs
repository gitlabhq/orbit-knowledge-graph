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
fn local_starts_with_folds_unicode_case() {
    let ontology = std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap());
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("folding.duckdb")).unwrap();
    database
        .initialize_schema(&compiler::generate_local_ddl(&ontology, ""))
        .unwrap();
    database
        .execute(
            "INSERT INTO gl_file (id, traversal_path, project_id, branch, name, extension, language, path) VALUES (1, '1/', 1, 'main', 'f', '', '', 'ärger'), (2, '1/', 1, 'main', 'f', '', '', 'other')",
            &[],
        )
        .unwrap();
    let compiled = compile_local(
        r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","columns":["path"],"filters":{"path":{"starts_with":"ÄRG"}}}],"limit":10}"#,
        Frontend::JsonDsl,
        &ontology,
    )
    .unwrap();

    let result = database
        .query_arrow(&compiled.base.render())
        .unwrap_or_else(|error| panic!("{error}: {}", compiled.base.render()));

    assert_eq!(duckdb_client::string_column(&result, "f_path"), ["ärger"]);
}

#[test]
fn fused_neighbors_execute_both_directions_including_self_loops() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("neighbors.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE gl_edge(source_id BIGINT, source_kind VARCHAR, target_id BIGINT, target_kind VARCHAR, relationship_kind VARCHAR, traversal_path VARCHAR); INSERT INTO gl_edge VALUES (1,'Project',2,'Project','CONTAINS','1/'), (3,'Project',1,'Project','CONTAINS','1/'), (1,'Project',1,'Project','CONTAINS','1/');").unwrap();
    let compiled = compile(
        r#"{"query_type":"neighbors","nodes":[{"id":"p","entity":"Project","node_ids":[1]}],"neighbors":{"direction":"both","rel_types":["CONTAINS"]}}"#,
    );
    let result = database
        .query_arrow(&compiled.base.render())
        .unwrap_or_else(|error| panic!("{error}: {}", compiled.base.render()));
    assert_eq!(
        result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        4
    );
}

#[test]
fn canonical_functions_execute_in_duckdb() {
    use compiler::ast::{Expr, Function, Node, Op, Query, SelectExpr, TableRef};

    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("functions.duckdb")).unwrap();
    database
        .initialize_schema("CREATE TABLE source(id BIGINT); INSERT INTO source VALUES (1);")
        .unwrap();
    let array = || Expr::func(Function::Array, vec![Expr::int(1), Expr::int(2)]);
    let positive = || Expr::lambda("x", Expr::binary(Op::Gt, Expr::ident("x"), Expr::int(1)));
    for (expression, expected) in [
        (
            Expr::EmptyTupleArray(vec![
                compiler::ast::SqlType::Int64,
                compiler::ast::SqlType::String,
            ]),
            "[]",
        ),
        (
            Expr::func(Function::ByteLength, vec![Expr::string("é")]),
            "2",
        ),
        (
            Expr::func(
                Function::Substring,
                vec![Expr::string("éclair"), Expr::int(1), Expr::int(2)],
            ),
            "éc",
        ),
        (
            Expr::func(Function::ArrayFilter, vec![positive(), array()]),
            "[2]",
        ),
        (
            Expr::func(
                Function::ArrayMap,
                vec![
                    Expr::lambda("x", Expr::binary(Op::Add, Expr::ident("x"), Expr::int(1))),
                    array(),
                ],
            ),
            "[2, 3]",
        ),
        (
            Expr::func(Function::ArrayExists, vec![positive(), array()]),
            "true",
        ),
        (
            Expr::func(
                Function::TupleElement,
                vec![
                    Expr::func(Function::Tuple, vec![Expr::int(7), Expr::string("x")]),
                    Expr::int(1),
                ],
            ),
            "7",
        ),
        (
            Expr::func(
                Function::CountSubstrings,
                vec![Expr::string("1/2/"), Expr::string("/")],
            ),
            "2",
        ),
        (
            Expr::func(
                Function::ToJson,
                vec![Expr::func(
                    Function::Object,
                    vec![Expr::string("k"), Expr::string("v")],
                )],
            ),
            "{\"k\":\"v\"}",
        ),
    ] {
        let ast = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(expression, "value")],
            from: TableRef::scan("source", "s"),
            ..Default::default()
        }));
        let query = compiler::passes::codegen::duckdb::codegen(&ast, Default::default()).unwrap();
        let result = database.query_arrow(&query.render()).unwrap();
        assert_eq!(
            arrow::util::display::array_value_to_string(result[0].column(0), 0).unwrap(),
            expected,
            "{}",
            query.render()
        );
    }
}

#[test]
fn week_buckets_start_sunday_and_return_dates() {
    use compiler::ast::{Expr, Node, Query, SelectExpr, TableRef};
    use compiler::input::TruncateUnit;

    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("weeks.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE events(created_at TIMESTAMP); INSERT INTO events VALUES ('2026-10-03 23:59:59'), ('2026-10-04 00:00:00'), ('2026-10-05 12:00:00');").unwrap();
    let ast = Node::Query(Box::new(Query {
        select: vec![SelectExpr::new(
            Expr::TimeBucket {
                unit: TruncateUnit::Week,
                value: Box::new(Expr::col("e", "created_at")),
            },
            "bucket",
        )],
        from: TableRef::scan("events", "e"),
        order_by: vec![compiler::ast::OrderExpr::asc(Expr::col("e", "created_at"))],
        ..Default::default()
    }));
    let query = compiler::passes::codegen::duckdb::codegen(&ast, Default::default()).unwrap();
    let result = database.query_arrow(&query.render()).unwrap();
    assert_eq!(
        result[0].column(0).data_type(),
        &arrow::datatypes::DataType::Date32
    );
    let values: Vec<_> = (0..3)
        .map(|i| arrow::util::display::array_value_to_string(result[0].column(0), i).unwrap())
        .collect();
    assert_eq!(values, ["2026-09-27", "2026-10-04", "2026-10-04"]);
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
    let Node::Query(arm) = ast else {
        unreachable!()
    };
    let union = Node::Query(Box::new(Query {
        select: vec![SelectExpr::col("n", "id")],
        from: TableRef::scan("nodes", "n"),
        union_all: vec![*arm],
        ..Default::default()
    }));
    let query = compiler::passes::codegen::duckdb::codegen(&union, Default::default()).unwrap();
    let result = database.query_arrow(&query.render()).unwrap();
    assert_eq!(
        result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
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
        (AggFunction::Collect, true, false, true, "[2, 2]"),
        (AggFunction::Collect, true, true, true, "[2]"),
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
                } else if function == AggFunction::Collect {
                    "[]"
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
        let expected = if unit == "week" {
            "date_trunc('week', u.created_at + INTERVAL 1 DAY) - INTERVAL 1 DAY".into()
        } else {
            format!("date_trunc('{unit}', u.created_at)")
        };
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
fn gql_boolean_predicates_execute_with_nulls_groups_and_cross_aliases() {
    let ontology = std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap());
    let compile_query = |query: &str| compile_local(query, Frontend::Gql, &ontology).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("predicates.duckdb")).unwrap();
    database.initialize_schema(
         "CREATE TABLE gl_definition(id BIGINT, name VARCHAR, definition_type VARCHAR, project_id BIGINT DEFAULT 1, traversal_path VARCHAR DEFAULT '1/', _deleted BOOLEAN DEFAULT false, _version BIGINT DEFAULT 1);
          INSERT INTO gl_definition(id, name, definition_type) VALUES (1, 'alice', 'active'), (2, 'bob', 'blocked'), (3, NULL, 'active'), (4, 'carol', NULL), (10, 'first', 'target'), (20, 'second', 'target');
         CREATE TABLE gl_edge(source_id BIGINT, source_kind VARCHAR, target_id BIGINT, target_kind VARCHAR, relationship_kind VARCHAR, traversal_path VARCHAR, _deleted BOOLEAN DEFAULT false);
          INSERT INTO gl_edge(source_id, source_kind, target_id, target_kind, relationship_kind, traversal_path) VALUES (1, 'Definition', 10, 'Definition', 'CALLS', '1/'), (2, 'Definition', 20, 'Definition', 'CALLS', '1/'), (3, 'Definition', 10, 'Definition', 'CALLS', '1/'), (4, 'Definition', 20, 'Definition', 'CALLS', '1/');"
    ).unwrap();
    let ids = |query: &str| {
        let sql = compile_query(query).base.render();
        let batches = database
            .query_arrow(&sql)
            .unwrap_or_else(|error| panic!("{error}: {sql}"));
        batches
            .iter()
            .flat_map(|batch| {
                let ids = batch.column_by_name("u_id").unwrap();
                (0..batch.num_rows())
                    .map(move |row| arrow::util::display::array_value_to_string(ids, row).unwrap())
            })
            .collect::<Vec<_>>()
    };
    for (predicate, expected) in [
        ("NOT u.name IN ['alice', 'bob']", vec!["4"]),
        ("NOT u.name IN ['alice']", vec!["2", "4"]),
        ("NOT u.name IN []", vec!["1", "2", "3", "4"]),
        ("u.name IN []", vec![]),
        (
            "u.name = 'alice' OR u.name = 'bob' AND NOT u.definition_type = 'blocked'",
            vec!["1"],
        ),
        (
            "(u.name = 'alice' OR u.name = 'bob') AND NOT u.definition_type = 'blocked'",
            vec!["1"],
        ),
        (
            "u.name = 'alice' OR u.name = 'bob' AND u.definition_type = 'blocked'",
            vec!["1", "2"],
        ),
        (
            "(u.name = 'alice' OR u.name = 'bob') AND u.definition_type = 'blocked'",
            vec!["2"],
        ),
        (
            "NOT (u.name = 'alice' OR u.definition_type = 'blocked')",
            vec![],
        ),
        (
            "u.name IS NULL OR NOT u.definition_type IS NOT NULL",
            vec!["3", "4"],
        ),
        ("NOT NOT u.name = 'alice'", vec!["1"]),
        ("NOT u.name CONTAINS 'ali'", vec!["2", "4"]),
        ("NOT u.id IN [1, 2] AND u.id < 4", vec!["3"]),
    ] {
        let query = format!(
            "MATCH (u:Definition) WHERE u.id <= 4 AND ({predicate}) RETURN u.id ORDER BY u.id"
        );
        assert_eq!(ids(&query), expected, "{query}");
    }
    for (predicate, expected) in [
        ("u.name = 'alice' OR p.name = 'second'", vec!["1", "2", "4"]),
        (
            "NOT u.name IN ['alice'] OR edge.target_id = 10",
            vec!["1", "2", "3", "4"],
        ),
        ("NOT (u.name = 'alice' OR edge.target_id = 20)", vec![]),
    ] {
        let query = format!(
            "MATCH (u:Definition)-[edge:CALLS]->(p:Definition) WHERE {predicate} RETURN u.id, p.id ORDER BY u.id"
        );
        assert_eq!(ids(&query), expected, "{query}");
    }
    let sql = compile_query("MATCH (u:Definition)-[edge:CALLS]->(p:Definition) WHERE NOT u.name IN ['alice'] OR edge.target_id = 10 RETURN count(u) AS total").base.render();
    let batches = database
        .query_arrow(&sql)
        .unwrap_or_else(|error| panic!("{error}: {sql}"));
    assert_eq!(
        arrow::util::display::array_value_to_string(batches[0].column_by_name("total").unwrap(), 0)
            .unwrap(),
        "4"
    );
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
