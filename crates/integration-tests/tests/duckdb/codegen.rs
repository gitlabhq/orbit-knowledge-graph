use compiler::ast::{Expr, Function, Node, Op, Query, SelectExpr, TableRef};
use duckdb_client::{DuckDbClient, OpenMode};

#[test]
fn renderings_no_duckdb_pipeline_emits_still_execute() {
    let database = DuckDbClient::open(OpenMode::InMemory).unwrap();
    database
        .initialize_schema("CREATE TABLE source(id BIGINT); INSERT INTO source VALUES (1);")
        .unwrap();
    let array = || Expr::func(Function::Array, vec![Expr::int(1), Expr::int(2)]);
    let positive = || Expr::lambda("x", Expr::binary(Op::Gt, Expr::ident("x"), Expr::int(1)));
    for (expression, expected) in [
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
            Expr::func(
                Function::CountSubstrings,
                vec![Expr::string("1/2/"), Expr::string("/")],
            ),
            "2",
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
