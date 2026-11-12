mod dag;
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
        retained::long_node_text_is_excerpted_only_on_wide_pages,
        retained::path_finding_code_filtered_endpoints_stay_on_same_traversal_path,
    );
}

#[tokio::test]
async fn data_correctness_dag() {
    let ctx = data_correctness_context().await;
    dag::query_4_searches_the_heads_clickhouse_returns(&ctx).await;
    dag::merge_requests_join_the_slack_messages_that_discuss_them(&ctx).await;
    dag::a_caller_who_reads_no_workspace_gets_no_messages(&ctx).await;
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
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    query_scenario::load_yaml_seed(&context, PRESETS, "data_correctness").await;
    context.optimize_if_needed().await;
    context
}
