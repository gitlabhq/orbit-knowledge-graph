use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    create_project, create_user, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_deployments(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;

    ctx.execute(
        "INSERT INTO siphon_deployments
            (id, iid, project_id, environment_id, ref, tag, sha, user_id, deployable_type, status, created_at, finished_at, archived, traversal_path, _siphon_replicated_at)
        VALUES
            (9001, 1, 1000, 7001, 'main',    false, 'abc123', 1,    'CommitStatus', 2, '2024-01-15 10:00:00', '2024-01-15 10:05:00', false, '1/100/1000/', '2024-01-20 12:00:00'),
            (9002, 2, 1000, 7001, 'main',    false, 'def456', 1,    'CommitStatus', 3, '2024-01-16 10:00:00', '2024-01-16 10:05:00', false, '1/100/1000/', '2024-01-20 12:00:00'),
            (9003, 3, 1000, 7002, 'staging', false, 'ghi789', NULL, 'CommitStatus', 1, '2024-01-17 10:00:00', NULL,                  false, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_deployment", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT status FROM {} FINAL ORDER BY id",
            t("gl_deployment")
        ))
        .await;
    let status =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "status").expect("status column");
    assert_eq!(status.value(0), "success");
    assert_eq!(status.value(1), "failed");
    assert_eq!(status.value(2), "running");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Deployment", "Project", "1/100/1000/", 3)
        .await;
    // Third row has NULL user_id — should be skipped by the IsNotNull filter.
    assert_edges_have_traversal_path(ctx, "DEPLOYED_BY", "User", "Deployment", "1/100/1000/", 2)
        .await;
}

pub async fn processes_deployment_environment_link(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;

    ctx.execute(
        "INSERT INTO siphon_environments
            (id, project_id, name, slug, state, tier, traversal_path, _siphon_replicated_at)
        VALUES (7201, 1000, 'production', 'production', 'available', 0, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_deployments
            (id, iid, project_id, environment_id, ref, tag, sha, user_id, deployable_type, status, traversal_path, _siphon_replicated_at)
        VALUES
            (9201, 1, 1000, 7201, 'main', false, 'aaa111', 1, 'CommitStatus', 2, '1/100/1000/', '2024-01-20 12:00:00'),
            (9202, 2, 1000, 7201, 'main', false, 'bbb222', 1, 'CommitStatus', 2, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(
        ctx,
        "IN_ENVIRONMENT",
        "Deployment",
        "Environment",
        "1/100/1000/",
        2,
    )
    .await;
}

pub async fn processes_deployment_merge_request_links(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_deployment_merge_requests
            (deployment_id, merge_request_id, environment_id, project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (9301, 5001, 7301, 1000, '1/100/1000/', '2024-01-20 12:00:00'),
            (9301, 5002, 7301, 1000, '1/100/1000/', '2024-01-20 12:00:00'),
            (9302, 5003, 7301, 1000, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(
        ctx,
        "DEPLOYED_TO",
        "MergeRequest",
        "Deployment",
        "1/100/1000/",
        3,
    )
    .await;
}
