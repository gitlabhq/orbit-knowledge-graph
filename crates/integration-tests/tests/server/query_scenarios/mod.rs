use integration_testkit::query_scenario;
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/query_scenarios/fixtures"
);
const PRESETS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/query_scenarios/presets"
);

#[tokio::test]
async fn query_scenarios() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    query_scenario::load_yaml_seed(&ctx, PRESETS, "data_correctness").await;
    ctx.optimize_all().await;
    query_scenario::run_dir(&ctx, FIXTURES, PRESETS).await;
}
