use integration_testkit::query_scenario;
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_seed};

#[tokio::test]
async fn query_scenarios() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    load_seed(&ctx, "data_correctness").await;
    ctx.optimize_all().await;
    query_scenario::run_dir(
        &ctx,
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/server/query_scenarios/fixtures"
        ),
    )
    .await;
}
