use compiler::passes::codegen::render;
use integration_testkit::visitor::NodeExt;
use integration_testkit::{TestContext, load_data_model, t};
use orbit_utils::query_types::SqlType;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    AggregateFunction, Expr, Measure, ReadMode, Relation, RelationalBuilder, RelationalError,
};
use query_data_model::plan::tracking::Declare;

use super::helpers::run_query;
use crate::common::compile_and_execute;

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

fn semantic(
    q: &mut RelationalBuilder<'_, ClickHouse>,
    argument: bool,
    distinct: bool,
    union: bool,
) -> Result<Relation, RelationalError> {
    let result = q.cte("result", false, |q| {
        let seed = q.cte("seed", false, |q| {
            let table = q.catalog().table(&t("gl_merge_request"))?;
            let rows = q.scan(table, ReadMode::Raw, "m")?;
            q.select_all(rows)
        })?;
        let rows = q.read(&seed, Some("s"))?;
        let iid = rows.qualified("s", "iid")?.expr();
        let draft = rows.qualified("s", "draft")?.expr();
        let measure = Measure {
            function: AggregateFunction::Count,
            args: argument.then_some(iid).into_iter().collect(),
            distinct,
            condition: Some(draft),
        };
        let aggregated = q.aggregate(rows, Vec::new(), vec![measure])?;
        let count = aggregated.measures[0].expr();
        let (rows, columns) = q.compute(aggregated.rows, vec![Declare::named("n", count)])?;
        q.select(rows, columns)
    })?;
    let rows = q.read(&result, Some("r"))?;
    let n = rows.qualified("r", "n")?.expr();
    let (rows, columns) = q.compute(rows, vec![Declare::named("n", n)])?;
    let rows = q.select(rows, columns)?;
    if !union {
        return Ok(rows);
    }
    let table = q.catalog().table(&t("gl_merge_request"))?;
    let one = q.scan(table, ReadMode::Raw, "one")?;
    let zero = Expr::Parameter {
        data_type: SqlType::Int64,
        value: 0.into(),
    };
    let (one, columns) = q.compute(one, vec![Declare::named("n", zero)])?;
    let one = q.limit(one, Some(1), None)?;
    let one = q.select(one, columns)?;
    q.union(vec![rows, one])
}

#[tokio::test]
async fn semantic_aggregate_and_nested_cte_codegen_execute() {
    let model = load_data_model();
    let ctx = TestContext::new(&[]).await;
    let table = t("gl_merge_request");
    ctx.execute(&format!(
        "CREATE TABLE {table}(iid Nullable(Int64), draft Bool) ENGINE=Memory"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {table} VALUES (2,true),(2,true),(NULL,true),(9,false)"
    ))
    .await;
    let render = |union, argument, distinct| {
        let plan = model
            .catalog()
            .build(|q| semantic(q, argument, distinct, union))
            .unwrap();
        render(&plan, &*model, Default::default(), Default::default()).render()
    };
    for (argument, distinct, expected) in
        [(false, false, "3"), (true, false, "2"), (true, true, "1")]
    {
        let result = ctx.query(&render(false, argument, distinct)).await;
        assert_eq!(
            arrow::util::display::array_value_to_string(result[0].column(0), 0).unwrap(),
            expected
        );
        let result = ctx.query(&render(true, argument, distinct)).await;
        assert_eq!(
            result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }
}
