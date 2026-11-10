//! DuckDB dialect end-to-end tests.

use crate::compiler::setup::embedded_local_model;
use crate::compiler::utils::ParsedSql;
use compiler::{Frontend, compile_local};
use orbit_utils::query_types::{SqlType, TimeZone};
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::implementations::relational::plan::{
    AggregateFunction, BinaryOp, Expr, Function, Measure, Order, ReadMode, Relation,
    RelationalBuilder, RelationalError, TimeUnit,
};
use query_data_model::plan::tracking::Declare;

fn compile(json: &str) -> compiler::passes::codegen::CompiledQueryContext {
    compile_local(json, Frontend::JsonDsl, &embedded_local_model()).unwrap()
}

fn parse_duckdb(json: &str) -> ParsedSql {
    ParsedSql::from_query(&compile(json).base)
}

fn local(
    construct: impl FnOnce(&mut Builder<'_>) -> Result<Relation, RelationalError>,
) -> compiler::ParameterizedQuery {
    let model = integration_testkit::load_local_data_model();
    let plan = model.build(construct).unwrap();
    compiler::passes::codegen::render(&plan, &*model, Default::default(), Default::default())
}

type Builder<'c> = RelationalBuilder<'c, DuckDb>;

fn scan(q: &mut Builder<'_>, table: &str, alias: &str) -> Result<Relation, RelationalError> {
    let table = q.catalog().table(table)?;
    q.scan(table, ReadMode::Current, alias)
}

fn named(
    q: &mut Builder<'_>,
    rows: Relation,
    items: Vec<(&str, Expr)>,
) -> Result<Relation, RelationalError> {
    let items = items
        .into_iter()
        .map(|(name, value)| Declare::named(name, value))
        .collect();
    let (rows, columns) = q.compute(rows, items)?;
    q.select(rows, columns)
}

fn int(value: i64) -> Expr {
    Expr::Parameter {
        data_type: SqlType::Int64,
        value: value.into(),
    }
}

fn text(value: &str) -> Expr {
    Expr::Parameter {
        data_type: SqlType::String,
        value: value.into(),
    }
}

fn call(function: Function, args: Vec<Expr>) -> Expr {
    Expr::Call { function, args }
}

fn lambda(body: Expr) -> Expr {
    Expr::Lambda {
        param: "x".into(),
        body: Box::new(body),
    }
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn variable() -> Expr {
    Expr::Variable("x".into())
}

#[test]
fn cycles_are_rejected() {
    let local = embedded_local_model();
    let cycle_query = serde_json::json!({"query_type":"traversal", "nodes":[{"id":"a","entity":"Definition","node_ids":[1],"columns":["id"]},{"id":"b","entity":"Definition","columns":["id"]},{"id":"c","entity":"Definition","columns":["id"]}], "relationships":[{"type":"CALLS","from":"a","to":"b"},{"type":"CALLS","from":"b","to":"c"},{"type":"CALLS","from":"c","to":"a"}]});
    let error = compile_local(&cycle_query.to_string(), Frontend::JsonDsl, &local).unwrap_err();
    assert!(error.to_string().contains("cycle"), "{error}");
    let error = compile_local("MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition)-[:CALLS]->(c:Definition)-[:CALLS]->(a) RETURN a.id, b.id, c.id", Frontend::Gql, &local).unwrap_err();
    assert!(error.to_string().contains("cycles are unsupported"));
}

#[test]
fn local_starts_with_folds_unicode_case() {
    let local = embedded_local_model();
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("folding.duckdb")).unwrap();
    database
        .initialize_schema(&compiler::generate_local_ddl(&local, ""))
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
        &local,
    )
    .unwrap();

    let result = database
        .query_arrow(&compiled.base.render())
        .unwrap_or_else(|error| panic!("{error}: {}", compiled.base.render()));

    assert_eq!(duckdb_client::string_column(&result, "f_path"), ["ärger"]);
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
#[test]
fn fused_neighbors_execute_both_directions_including_self_loops() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("neighbors.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE gl_project(id BIGINT, traversal_path VARCHAR); INSERT INTO gl_project VALUES (1, '1/'); CREATE TABLE gl_edge(source_id BIGINT, source_kind VARCHAR, target_id BIGINT, target_kind VARCHAR, relationship_kind VARCHAR, traversal_path VARCHAR); INSERT INTO gl_edge VALUES (1,'Project',2,'Project','CONTAINS','1/'), (3,'Project',1,'Project','CONTAINS','1/'), (1,'Project',1,'Project','CONTAINS','1/');").unwrap();
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
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("functions.duckdb")).unwrap();
    database
        .initialize_schema("CREATE TABLE gl_file(id BIGINT); INSERT INTO gl_file VALUES (1);")
        .unwrap();
    let array = || call(Function::Array, vec![int(1), int(2)]);
    let positive = || lambda(binary(BinaryOp::Gt, variable(), int(1)));
    for (expression, expected) in [
        (
            Expr::EmptyArray(vec![SqlType::Int64, SqlType::String]),
            "[]",
        ),
        (call(Function::ByteLength, vec![text("é")]), "2"),
        (
            call(Function::Substring, vec![text("éclair"), int(1), int(2)]),
            "éc",
        ),
        (
            call(Function::ArrayFilter, vec![positive(), array()]),
            "[2]",
        ),
        (
            call(
                Function::ArrayMap,
                vec![lambda(binary(BinaryOp::Add, variable(), int(1))), array()],
            ),
            "[2, 3]",
        ),
        (
            call(Function::ArrayExists, vec![positive(), array()]),
            "true",
        ),
        (
            call(
                Function::TupleElement,
                vec![call(Function::Tuple, vec![int(7), text("x")]), int(1)],
            ),
            "7",
        ),
        (
            call(Function::CountSubstrings, vec![text("1/2/"), text("/")]),
            "2",
        ),
        (
            call(
                Function::ToJson,
                vec![call(Function::Object, vec![text("k"), text("v")])],
            ),
            "{\"k\":\"v\"}",
        ),
    ] {
        let query = local(|q| {
            let rows = scan(q, "gl_file", "s")?;
            named(q, rows, vec![("value", expression)])
        });
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
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("weeks.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE gl_file(name TIMESTAMP); INSERT INTO gl_file VALUES ('2026-10-03 23:59:59'), ('2026-10-04 00:00:00'), ('2026-10-05 12:00:00');").unwrap();
    let query = local(|q| {
        let rows = scan(q, "gl_file", "e")?;
        let name = rows.qualified("e", "name")?.expr();
        let bucket = Expr::TimeBucket {
            unit: TimeUnit::Week,
            value: Box::new(name.clone()),
        };
        let (rows, columns) = q.compute(rows, vec![Declare::named("bucket", bucket)])?;
        let order = Order {
            value: name,
            descending: false,
        };
        let rows = q.order(rows, vec![order])?;
        q.select(rows, columns)
    });
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
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("parameters.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE gl_file(name TIMESTAMPTZ); INSERT INTO gl_file VALUES ('2026-10-05T01:02:03.123456Z');").unwrap();
    let compiled = local(|q| {
        let rows = scan(q, "gl_file", "e")?;
        let name = rows.qualified("e", "name")?.expr();
        let instant = Expr::Parameter {
            data_type: SqlType::Timestamp {
                precision: 6,
                timezone: Some(TimeZone::Utc),
            },
            value: "2026-10-05T01:02:03.123456Z".into(),
        };
        let rows = q.filter(rows, binary(BinaryOp::Eq, name.clone(), instant))?;
        named(q, rows, vec![("name", name)])
    });
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

fn nested(q: &mut Builder<'_>, union: bool) -> Result<Relation, RelationalError> {
    let result = q.cte("result", false, |q| {
        let seed = q.cte("seed", false, |q| {
            let rows = scan(q, "gl_file", "n")?;
            let id = rows.qualified("n", "id")?.expr();
            named(q, rows, vec![("id", id)])
        })?;
        let rows = q.read(&seed, Some("s"))?;
        let id = rows.qualified("s", "id")?.expr();
        named(q, rows, vec![("id", id)])
    })?;
    let rows = q.read(&result, Some("r"))?;
    let id = rows.qualified("r", "id")?.expr();
    let rows = named(q, rows, vec![("id", id)])?;
    if !union {
        return Ok(rows);
    }
    let other = scan(q, "gl_file", "n")?;
    let id = other.qualified("n", "id")?.expr();
    let other = named(q, other, vec![("id", id)])?;
    q.union(vec![rows, other])
}

#[test]
fn nested_cte_codegen_executes_with_its_local_definition() {
    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("nested.duckdb")).unwrap();
    database
        .initialize_schema("CREATE TABLE gl_file(id BIGINT); INSERT INTO gl_file VALUES (7);")
        .unwrap();
    let query = local(|q| nested(q, false));
    let result = database.query_arrow(&query.render()).unwrap();
    assert_eq!(
        arrow::util::display::array_value_to_string(result[0].column(0), 0).unwrap(),
        "7"
    );
    let query = local(|q| nested(q, true));
    let result = database.query_arrow(&query.render()).unwrap();
    assert_eq!(
        result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
}

#[test]
fn semantic_aggregate_codegen_preserves_null_and_empty_input_results() {
    use AggregateFunction as F;

    let directory = tempfile::tempdir().unwrap();
    let database =
        duckdb_client::DuckDbClient::open(&directory.path().join("aggregate.duckdb")).unwrap();
    database.initialize_schema("CREATE TABLE gl_definition(start_line BIGINT, end_line BIGINT); INSERT INTO gl_definition VALUES (2, 1), (2, 1), (NULL, 1), (9, 0);").unwrap();
    for (function, argument, distinct, filtered, expected) in [
        (F::Count, false, false, false, "4"),
        (F::Count, true, false, false, "3"),
        (F::Count, false, false, true, "3"),
        (F::Count, true, false, true, "2"),
        (F::Count, true, true, true, "1"),
        (F::Sum, true, false, true, "4"),
        (F::Avg, true, false, true, "2.0"),
        (F::Min, true, false, true, "2"),
        (F::Max, true, false, true, "2"),
        (F::Collect, true, false, true, "[2, 2]"),
        (F::Collect, true, true, true, "[2]"),
    ] {
        for empty in [false, true] {
            let query = local(|q| {
                let rows = scan(q, "gl_definition", "m")?;
                let rows = match empty {
                    true => {
                        let none = Expr::Parameter {
                            data_type: SqlType::Bool,
                            value: false.into(),
                        };
                        q.filter(rows, none)?
                    }
                    false => rows,
                };
                let start = rows.qualified("m", "start_line")?.expr();
                let end = rows.qualified("m", "end_line")?.expr();
                let measure = Measure {
                    function,
                    args: argument.then_some(start).into_iter().collect(),
                    distinct,
                    condition: filtered.then(|| binary(BinaryOp::Eq, end, int(1))),
                };
                let aggregated = q.aggregate(rows, Vec::new(), vec![measure])?;
                let value = aggregated.measures[0].expr();
                named(q, aggregated.rows, vec![("result", value)])
            });
            let results = database.query_arrow(&query.render()).unwrap();
            let actual =
                arrow::util::display::array_value_to_string(results[0].column(0), 0).unwrap();
            let expected = match (empty, function) {
                (false, _) => expected,
                (true, F::Count) => "0",
                (true, F::Collect) => "[]",
                (true, _) => "",
            };
            assert_eq!(actual, expected, "{}", query.sql);
        }
    }
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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
        let result = compile_local(&json, Frontend::JsonDsl, &embedded_local_model())
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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
    compile_local(cypher, Frontend::Gql, &embedded_local_model()).map_err(|e| e.to_string())
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
#[test]
fn gql_untyped_edge_pattern() {
    let r = compile_gql("MATCH (u:User {id: 1})-[e]->(n:Note) RETURN n.confidential");
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
#[test]
fn gql_open_ended_scan() {
    let r = compile_gql("MATCH (u:User) RETURN u.username");
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
#[test]
fn gql_count_without_node_ids() {
    let r = compile_gql("MATCH (u:User) RETURN count(u) AS n");
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
#[test]
fn gql_order_by_across_traversal() {
    let r = compile_gql(
        "MATCH (u:User {id: 1})-[e:AUTHORED]->(n:Note) \
         RETURN n.confidential ORDER BY n.confidential",
    );
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

#[ignore = "asserts the shape of the removed fixture ontology, not the embedded model"]
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
