mod dag;
mod helpers;
mod peers;
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
    dag::seed(&ctx).await;
    dag::query_4_drops_the_files_their_read_code_check_denies(&ctx).await;
    dag::merge_requests_join_the_slack_messages_that_discuss_them(&ctx).await;
    dag::the_joined_rows_carry_the_edges_they_cross(&ctx).await;
    dag::a_code_search_reads_only_the_repositories_the_caller_can_read(&ctx).await;
    dag::merge_requests_hand_slack_only_the_keys_their_check_allows(&ctx).await;
    dag::a_head_under_none_keeps_the_files_its_via_chain_admits(&ctx).await;
    peers::seed(&ctx).await;
    peers::rollouts_on_one_clickhouse_join_releases_on_another_by_uuid(&ctx).await;
    peers::releases_on_a_second_clickhouse_join_the_merge_requests_they_ship(&ctx).await;
    peers::merge_requests_on_clickhouse_join_their_reviews_on_duckdb(&ctx).await;
    peers::peer_joins_carry_the_edges_they_cross(&ctx).await;
    peers::seed_guilds(&ctx, &integration_testkit::dag_fakes::peer_packages()).await;
    peers::guild_posts_reach_the_live_stage_only_for_the_guilds_the_caller_reads(&ctx).await;
    peers::clan_posts_reach_the_live_stage_only_for_the_clans_the_caller_reads(&ctx).await;
    peers::guild_posts_join_the_guilds_the_pattern_reads(&ctx).await;
    dag::a_deep_page_fails_past_the_stage_cap_instead_of_losing_rows(&ctx).await;
}

#[tokio::test]
#[ignore = "engine: the loader rejects a composite identity on a table store (catalog/layout.rs)"]
async fn data_correctness_ranks() {
    let ctx = data_correctness_context().await;
    peers::seed_guilds(&ctx, &integration_testkit::dag_fakes::rank_packages()).await;
    peers::rank_posts_reach_the_live_stage_only_for_the_ranks_the_caller_reads(&ctx).await;
    peers::rank_columns_decode_through_a_stage_boundary(&ctx).await;
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
