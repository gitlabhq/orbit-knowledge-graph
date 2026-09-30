use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, query_scenario};

const SCENARIOS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/performance/scenarios"
);
const PRESETS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/performance/presets"
);

// Perf queries are authored as compile-only scenarios: run_dir compiles each
// against the live ontology so DSL drift fails CI, but runs nothing, so no
// seeded data is required. The perf load job reads the same YAML files.
#[tokio::test]
async fn performance_queries() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    query_scenario::run_dir(&ctx, SCENARIOS, PRESETS).await;
}
