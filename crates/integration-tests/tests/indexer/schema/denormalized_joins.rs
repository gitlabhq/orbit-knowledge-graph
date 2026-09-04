//! Each declared denormalized join must hold exactly the rows a live join of its source chain yields.

use clickhouse_client::FromArrowColumn;
use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology, load_seed,
};
use ontology::denormalized::{DenormalizedJoin, alias};

#[tokio::test]
async fn denormalized_tables_match_their_source_join() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    load_seed(&ctx, "data_correctness").await;
    ctx.optimize_all().await;

    for join in load_ontology().denormalized_joins() {
        let materialized = count(
            &ctx,
            &format!("FROM {} FINAL WHERE _deleted = false", join.table),
        )
        .await;
        let expected = count(&ctx, &live_source_join(join)).await;
        assert_eq!(
            materialized, expected,
            "{}: materialized {materialized} rows, source join yields {expected}",
            join.table
        );
        assert!(
            expected > 0,
            "{}: the seed exercises no rows for this join",
            join.table
        );
    }
}

async fn count(ctx: &TestContext, from: &str) -> i64 {
    let batches = ctx.query(&format!("SELECT toInt64(count()) {from}")).await;
    i64::extract_column(&batches, 0).unwrap()[0]
}

/// The chain joined live, every table `FINAL` and not deleted.
fn live_source_join(join: &DenormalizedJoin) -> String {
    let mut sql = format!("FROM {} AS {} FINAL", join.tables[0].table, alias(0));
    for (i, table) in join.tables.iter().enumerate().skip(1) {
        let link = table.join.as_ref().expect("chained table");
        let mut on = vec![format!(
            "{}.{} = {}.{}",
            alias(i - 1),
            link.prev_column,
            alias(i),
            link.this_column
        )];
        on.extend(
            table
                .filter
                .iter()
                .map(|(col, value)| format!("{}.{col} = '{value}'", alias(i))),
        );
        sql.push_str(&format!(
            " INNER JOIN {} AS {} FINAL ON {}",
            table.table,
            alias(i),
            on.join(" AND ")
        ));
    }
    let live: Vec<String> = (0..join.tables.len())
        .map(|i| format!("{}._deleted = false", alias(i)))
        .collect();
    sql + " WHERE " + &live.join(" AND ")
}
