mod compiler_legacy;
mod duckdb;

#[path = "server/querying_pipeline/mod.rs"]
mod querying_pipeline;

const SCENARIOS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/scenarios"
);
const PRESETS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/presets"
);

#[test]
fn query_scenarios_run_on_duckdb() {
    integration_testkit::query_scenario::run_duckdb_dir(SCENARIOS, PRESETS, "data_correctness");
}

#[tokio::test(flavor = "multi_thread")]
async fn compile_only_scenarios() {
    integration_testkit::query_scenario::run_compile_only_dir(SCENARIOS, PRESETS).await;
}

#[test]
fn query_scenario_fixtures_parse() {
    integration_testkit::query_scenario::validate_parse(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/server/data_correctness/scenarios"
    ));
}
