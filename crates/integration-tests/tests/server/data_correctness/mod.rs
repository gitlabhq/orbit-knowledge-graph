mod retained;

use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, query_scenario};

const SCENARIOS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/scenarios"
);
const QUERY_SETTINGS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/query_settings"
);
const PRESETS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/presets"
);
const COMPILER_FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compiler/fixtures");

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_correctness_scenarios() {
    let ctx = data_correctness_context().await;
    query_scenario::run_dir(&ctx, SCENARIOS, PRESETS).await;
}

#[tokio::test]
async fn query_settings_scenarios() {
    super::status::fixtures::use_production_query_settings();
    let ctx = data_correctness_context().await;
    query_scenario::run_dir(&ctx, QUERY_SETTINGS, PRESETS).await;
}

#[tokio::test]
async fn compiler_fixture_index_explains() {
    let ctx = data_correctness_context().await;
    integration_testkit::compiler::run_index_explains(
        &ctx,
        std::path::Path::new(COMPILER_FIXTURES),
    )
    .await;
}

async fn data_correctness_context() -> TestContext {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    query_scenario::load_yaml_seed(&context, PRESETS, "data_correctness").await;
    context.optimize_if_needed().await;
    context
}
