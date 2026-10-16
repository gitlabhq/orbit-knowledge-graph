use compiler::ast::{Cte, Expr, Node, Query, SelectExpr, TableRef};
use compiler::input::AggFunction;
use compiler::passes::{codegen::codegen, cursor};
use integration_testkit::TestContext;
use integration_testkit::visitor::NodeExt;
use serde_json::Value;

use super::helpers::run_query;
use crate::common::compile_and_execute;

pub(super) async fn cursor_after_token_with_sql_metacharacters_is_parameterized(ctx: &TestContext) {
    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]}],
        "order_by": "-u.username",
        "cursor": {"page_size": 2}
    }"#;

    let mut query: Value = serde_json::from_str(json).unwrap();
    let token = cursor::encode(
        cursor::canonical_hash(&query),
        &[Some("'); DROP TABLE gl_user; --".into()), Some("0".into())],
    );
    query["cursor"]["after"] = token.into();

    let resp = run_query(ctx, &query.to_string()).await;
    resp.assert_node_order("User", &[]);
    resp.assert_node_count(0);

    let intact = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]}]
        }"#,
    )
    .await;
    intact.assert_node_count(7);
}

pub(super) async fn long_node_text_is_excerpted_only_on_wide_pages(ctx: &TestContext) {
    let full_text = format!("{}tail", "🙂".repeat(10_000));
    let query_with_limit = |limit: u32| {
        format!(
            r#"{{
            "query_type": "traversal",
            "nodes": [{{"id": "n", "entity": "Note", "node_ids": [3002],
                       "columns": ["note", "discussion_id"],
                       "filters": {{"note": {{"ends_with": "tail"}}}}}}],
            "limit": {limit}
        }}"#
        )
    };

    let single_row = run_query(ctx, &query_with_limit(1)).await;
    single_row.assert_node_count(1);
    single_row.assert_node_ids("Note", &[3002]);
    single_row.assert_filter("Note", "note", |note| {
        note.prop_str("note") == Some(&full_text)
    });
    single_row
        .find_node("Note", 3002)
        .unwrap()
        .assert_prop("discussion_id", &"🙂".repeat(2048).into());

    let wide_page = run_query(ctx, &query_with_limit(1000)).await;
    wide_page.assert_node_count(1);
    wide_page.assert_node_ids("Note", &[3002]);
    wide_page.assert_filter("Note", "note", |note| {
        note.prop_str("note").is_some_and(|excerpt| {
            excerpt.ends_with(" [truncated]") && excerpt.chars().count() < full_text.chars().count()
        })
    });
}

pub(super) async fn path_finding_code_filtered_endpoints_stay_on_same_traversal_path(
    ctx: &TestContext,
) {
    let (_compiled, result) = compile_and_execute(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Definition", "filters": {"name": {"eq": "compile"}}},
                {"id": "end", "entity": "Definition", "filters": {"name": {"eq": "run_query"}}}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["CALLS"]},
            "limit": 10
        }"#,
    )
    .await;

    let paths: Vec<Vec<i64>> = result
        .rows()
        .iter()
        .map(|row| row.path_nodes().iter().map(|node| node.id).collect())
        .collect();

    assert_eq!(
        paths,
        vec![vec![12000, 12001, 12002]],
        "filtered Definition path finding must not join CALLS hops across traversal_path"
    );
    assert!(
        paths.iter().all(|path| path.last() != Some(&12102)),
        "cross-project decoy run_query must not be reachable: {paths:?}"
    );
}

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
