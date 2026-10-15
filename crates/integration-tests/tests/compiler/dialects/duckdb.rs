//! DuckDB dialect end-to-end tests.

use crate::compiler::setup::test_ontology;
use compiler::{Frontend, compile_local};

fn compile(json: &str) -> compiler::passes::codegen::CompiledQueryContext {
    compile_local(json, Frontend::JsonDsl, &test_ontology()).unwrap()
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
