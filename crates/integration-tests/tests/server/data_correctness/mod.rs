mod helpers;
mod retained;

use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, query_scenario, run_subtests_shared};

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

#[tokio::test]
async fn data_correctness() {
    let ctx = data_correctness_context().await;

    run_subtests_shared!(
        &ctx,
        retained::cursor_after_token_with_sql_metacharacters_is_parameterized,
        retained::long_node_text_is_excerpted_only_on_wide_pages,
        retained::path_finding_code_filtered_endpoints_stay_on_same_traversal_path,
    );
}

#[tokio::test]
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

async fn data_correctness_context() -> TestContext {
    TestContext::new(&[*GRAPH_SCHEMA_SQL])
        .await
        .cached_fork("data_correctness", |context| async move {
            query_scenario::load_yaml_seed(&context, PRESETS, "data_correctness").await;
            context.optimize_all().await;
        })
        .await
}
