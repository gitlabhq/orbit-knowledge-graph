use compiler::ast::{Cte, Expr, Node, Query, SelectExpr, TableRef};
use compiler::input::AggFunction;
use compiler::passes::codegen::codegen;
use integration_testkit::TestContext;

#[tokio::test]
async fn semantic_aggregate_and_nested_cte_codegen_execute() {
    let ctx = TestContext::new(&[]).await;
    ctx.execute("CREATE TABLE measurements(value Nullable(Int64), keep Bool) ENGINE=Memory")
        .await;
    ctx.execute("INSERT INTO measurements VALUES (2,true),(2,true),(NULL,true),(9,false)")
        .await;
    for (argument, distinct, expected) in
        [(false, false, "3"), (true, false, "2"), (true, true, "1")]
    {
        let ast = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "result",
                Query {
                    ctes: vec![Cte::new(
                        "seed",
                        Query {
                            select: vec![SelectExpr::star()],
                            from: TableRef::scan("measurements", "m"),
                            ..Default::default()
                        },
                    )],
                    select: vec![SelectExpr::new(
                        Expr::Aggregate {
                            function: AggFunction::Count,
                            argument: argument.then(|| Box::new(Expr::col("s", "value"))),
                            distinct,
                            condition: Some(Box::new(Expr::col("s", "keep"))),
                        },
                        "n",
                    )],
                    from: TableRef::scan("seed", "s"),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::col("r", "n")],
            from: TableRef::scan("result", "r"),
            ..Default::default()
        }));
        let compiled = codegen(&ast, Default::default(), Default::default()).unwrap();
        let result = ctx.query(&compiled.render()).await;
        assert_eq!(
            arrow::util::display::array_value_to_string(result[0].column(0), 0).unwrap(),
            expected
        );
        let Node::Query(arm) = ast else {
            unreachable!()
        };
        let union = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::int(0), "n")],
            from: TableRef::scan("system.one", "one"),
            union_all: vec![*arm],
            ..Default::default()
        }));
        let compiled = codegen(&union, Default::default(), Default::default()).unwrap();
        let result = ctx.query(&compiled.render()).await;
        assert_eq!(
            result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }
}
