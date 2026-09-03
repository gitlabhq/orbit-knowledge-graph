//! Each declared denormalized join must hold exactly the rows a live join of its source chain yields.

use clickhouse_client::FromArrowColumn;
use ontology::denormalized::DenormalizedJoin;

use super::helpers::*;

pub(super) async fn denormalized_tables_match_their_source_join(ctx: &TestContext) {
    let ontology = load_ontology();
    for join in ontology.denormalized_joins() {
        let (materialized, expected) = counts(ctx, join).await;
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

/// `(join table rows, live source join rows)`, both `FINAL` and not deleted.
async fn counts(ctx: &TestContext, join: &DenormalizedJoin) -> (i64, i64) {
    let alias = |i: usize| format!("t{i}");
    let mut sql = format!(
        "SELECT toInt64(count()) FROM {} AS t0 FINAL",
        join.tables[0].table
    );
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
    sql.push_str(&format!(" WHERE {}", live.join(" AND ")));

    let expected = ctx.query(&sql).await;
    let materialized = ctx
        .query(&format!(
            "SELECT toInt64(count()) FROM {} FINAL WHERE _deleted = false",
            join.table
        ))
        .await;
    (
        i64::extract_column(&materialized, 0).unwrap()[0],
        i64::extract_column(&expected, 0).unwrap()[0],
    )
}
