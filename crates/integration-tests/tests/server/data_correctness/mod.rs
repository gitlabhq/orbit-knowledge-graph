mod helpers;
mod retained;

use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, query_scenario, run_subtests_shared,
};

const SCENARIOS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/scenarios"
);
const PRESETS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/server/data_correctness/presets"
);

#[tokio::test]
async fn data_correctness() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    query_scenario::load_yaml_seed(&ctx, PRESETS, "data_correctness").await;
    ctx.optimize_all().await;

    run_subtests_shared!(
        &ctx,
        retained::cursor_after_token_with_sql_metacharacters_is_parameterized,
        retained::long_node_text_is_excerpted_only_on_wide_pages,
        retained::path_finding_code_filtered_endpoints_stay_on_same_traversal_path,
    );
}

#[tokio::test]
async fn data_correctness_scenarios() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    query_scenario::load_yaml_seed(&ctx, PRESETS, "data_correctness").await;
    ctx.optimize_all().await;
    query_scenario::run_dir(&ctx, SCENARIOS, PRESETS).await;
}
