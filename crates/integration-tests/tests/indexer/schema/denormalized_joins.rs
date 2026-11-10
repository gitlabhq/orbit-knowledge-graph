use clickhouse_client::FromArrowColumn;
use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_data_model, load_fragments, load_seed,
};
use query_data_model::implementations::clickhouse::spec::{ClickHouseOptions, JoinSpec};
use query_data_model::spec::{Fragment, decode_options};
use query_data_model::{ClickHouse, StorageBackend};

#[tokio::test]
async fn denormalized_tables_match_their_source_join() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    load_seed(&ctx, "data_correctness").await;
    ctx.optimize_all().await;
    let model = load_data_model();

    for (table, join) in joins() {
        let materialized = count(&ctx, &format!("FROM {table} FINAL WHERE _deleted = false")).await;
        let expected = count(&ctx, &live_source_join(&join)).await;
        assert_eq!(
            materialized, expected,
            "{table}: materialized {materialized} rows, source join yields {expected}"
        );
        assert!(
            expected > 0,
            "{table}: the seed exercises no rows for this join"
        );
        let binding = model
            .joins()
            .iter()
            .find(|binding| binding.table == table)
            .unwrap();
        let stored_ids = binding
            .nodes
            .iter()
            .map(|node| node.identity_column.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let source_ids = binding
            .nodes
            .iter()
            .map(|node| format!("{}.id", alias(node.source_occurrence)))
            .collect::<Vec<_>>()
            .join(", ");
        let stored = format!(
            "SELECT {stored_ids} FROM {} FINAL WHERE _deleted = false",
            binding.table
        );
        let source = format!("SELECT {source_ids} {}", live_source_join(&join));
        for (left, right) in [(&stored, &source), (&source, &stored)] {
            assert_eq!(
                count(&ctx, &format!("FROM ({left} EXCEPT ALL {right})")).await,
                0
            );
        }
    }
}

async fn count(ctx: &TestContext, from: &str) -> i64 {
    let batches = ctx.query(&format!("SELECT toInt64(count()) {from}")).await;
    i64::extract_column(&batches, 0).unwrap()[0]
}

fn joins() -> Vec<(String, JoinSpec)> {
    load_fragments()
        .into_iter()
        .filter_map(|fragment| match fragment {
            Fragment::Storage(storage) => storage.engine,
            _ => None,
        })
        .filter(|engine| engine.kind == ClickHouse::KIND)
        .flat_map(|engine| {
            decode_options::<ClickHouseOptions>(&engine.backend)
                .unwrap()
                .joins
        })
        .collect()
}

fn alias(occurrence: usize) -> String {
    format!("t{occurrence}")
}

fn live_source_join(join: &JoinSpec) -> String {
    let mut sql = format!("FROM {} AS {} FINAL", join.sources[0].table, alias(0));
    for (i, source) in join.sources.iter().enumerate().skip(1) {
        let link = source.on.as_ref().expect("chained table");
        let mut on = vec![format!(
            "{}.{} = {}.{}",
            alias(i - 1),
            link.previous,
            alias(i),
            link.column
        )];
        on.extend(
            source
                .filter
                .iter()
                .map(|(col, value)| format!("{}.{col} = '{value}'", alias(i))),
        );
        sql.push_str(&format!(
            " INNER JOIN {} AS {} FINAL ON {}",
            source.table,
            alias(i),
            on.join(" AND ")
        ));
    }
    let live: Vec<String> = (0..join.sources.len())
        .map(|i| format!("{}._deleted = false", alias(i)))
        .collect();
    sql + " WHERE " + &live.join(" AND ")
}
