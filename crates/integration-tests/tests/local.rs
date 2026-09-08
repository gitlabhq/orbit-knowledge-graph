mod compiler;

#[path = "server/querying_pipeline/mod.rs"]
mod querying_pipeline;

#[test]
fn query_scenario_fixtures_parse() {
    integration_testkit::query_scenario::validate_parse(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/server/query_scenarios/fixtures"
    ));
}
