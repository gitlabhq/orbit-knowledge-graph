mod compiler;

#[path = "server/querying_pipeline/mod.rs"]
mod querying_pipeline;

#[test]
fn query_scenarios_run_on_duckdb() {
    integration_testkit::query_scenario::run_duckdb_dir(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/server/data_correctness/scenarios"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/server/data_correctness/presets"
        ),
        "data_correctness",
    );
}

#[test]
fn query_scenario_fixtures_parse() {
    integration_testkit::query_scenario::validate_parse(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/server/data_correctness/scenarios"
    ));
}
