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
    let ontology = load_ontology();
    let model = integration_testkit::derive_clickhouse_data_model(&ontology);

    for join in ontology.denormalized_joins() {
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
        let binding = model
            .backend()
            .materialized_joins()
            .iter()
            .find(|binding| binding.table == join.table)
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
        let source = format!("SELECT {source_ids} {}", live_source_join(join));
        for (left, right) in [(&stored, &source), (&source, &stored)] {
            assert_eq!(
                count(&ctx, &format!("FROM ({left} EXCEPT ALL {right})")).await,
                0
            );
        }
    }
    for copy in model.backend().reordered_layouts() {
        let identity = model
            .backend()
            .table(&copy.source)
            .unwrap()
            .replacement_identity()
            .join(", ");
        ctx.execute(&format!("INSERT INTO {} ({identity}, _version, _deleted) SELECT {identity}, toDateTime64('2099-01-01 00:00:00', 6), true FROM {} FINAL LIMIT 1", copy.source, copy.source)).await;
        for (left, right) in [(&copy.source, &copy.table), (&copy.table, &copy.source)] {
            let difference = count(
                &ctx,
                &format!(
                    "FROM (SELECT * FROM {left} FINAL EXCEPT ALL SELECT * FROM {right} FINAL)"
                ),
            )
            .await;
            assert_eq!(difference, 0, "{} differs from {}", copy.table, copy.source);
        }
        assert!(count(&ctx, &format!("FROM {} FINAL", copy.table)).await > 0);
    }
}

async fn count(ctx: &TestContext, from: &str) -> i64 {
    let batches = ctx.query(&format!("SELECT toInt64(count()) {from}")).await;
    i64::extract_column(&batches, 0).unwrap()[0]
}

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
