use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    create_project, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_environments(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_environments
            (id, project_id, name, slug, state, tier, environment_type, external_url, merge_request_id, traversal_path, _siphon_replicated_at)
        VALUES
            (7001, 1000, 'production', 'production', 'available', 0, NULL,     'https://example.com',         NULL, '1/100/1000/', '2024-01-20 12:00:00'),
            (7002, 1000, 'staging',    'staging',    'available', 1, NULL,     'https://staging.example.com', NULL, '1/100/1000/', '2024-01-20 12:00:00'),
            (7003, 1000, 'review/feature-x', 'review-feature-x', 'stopping', 4, 'review', NULL, NULL, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_environment", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT name, state, tier FROM {} FINAL ORDER BY id",
            t("gl_environment")
        ))
        .await;
    let name =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "name").expect("name column");
    let state =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "state").expect("state column");
    let tier =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "tier").expect("tier column");
    assert_eq!(name.value(0), "production");
    assert_eq!(state.value(0), "available");
    assert_eq!(tier.value(0), "production");
    assert_eq!(name.value(2), "review/feature-x");
    assert_eq!(state.value(2), "stopping");
    assert_eq!(tier.value(2), "other");

    assert_edges_have_traversal_path(
        ctx,
        "IN_PROJECT",
        "Environment",
        "Project",
        "1/100/1000/",
        3,
    )
    .await;
}

pub async fn processes_mr_pipeline_created_environments(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    // Two environments created by MR pipelines (review apps) plus one
    // independent production environment. Only the first two should
    // produce CREATED_FOR_MR edges; production carries no merge_request_id.
    ctx.execute(
        "INSERT INTO siphon_environments
            (id, project_id, name, slug, state, tier, environment_type, merge_request_id, traversal_path, _siphon_replicated_at)
        VALUES
            (7101, 1000, 'review/feature-a', 'review-feature-a', 'available', 4, 'review', 5001, '1/100/1000/', '2024-01-20 12:00:00'),
            (7102, 1000, 'review/feature-b', 'review-feature-b', 'available', 4, 'review', 5002, '1/100/1000/', '2024-01-20 12:00:00'),
            (7103, 1000, 'production',       'production',       'available', 0, NULL,     NULL, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(
        ctx,
        "CREATED_FOR_MR",
        "Environment",
        "MergeRequest",
        "1/100/1000/",
        2,
    )
    .await;
}
