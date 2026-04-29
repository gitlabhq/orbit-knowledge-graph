use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edge_tags, assert_edge_tags_by_target, assert_edges_have_traversal_path,
    assert_node_count, create_namespace, create_project, create_runner, create_runner_namespace,
    create_runner_project, create_user, global_envelope, global_handler, handler_context,
    namespace_envelope, namespace_handler,
};

pub async fn processes_pipelines(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, user_id, iid, sha, ref, status, source, tag, duration, created_at, started_at, finished_at, traversal_path, _siphon_replicated_at)
        VALUES
        (5001, 1, 1000, 1, 1, 'abc123def456', 'main', 'success', 1, false, 120, '2024-01-15 10:00:00', '2024-01-15 10:01:00', '2024-01-15 10:03:00', '1/100/1000/', '2024-01-20 12:00:00'),
        (5002, 1, 1000, 1, 2, 'def456abc789', 'feature-branch', 'failed', 1, false, 60, '2024-01-16 10:00:00', '2024-01-16 10:01:00', '2024-01-16 10:02:00', '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_pipeline", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT status FROM {} FINAL ORDER BY id",
            t("gl_pipeline")
        ))
        .await;
    let status =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "status").expect("status column");
    assert_eq!(status.value(0), "success");
    assert_eq!(status.value(1), "failed");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Pipeline", "Project", "1/100/1000/", 2)
        .await;
    assert_edges_have_traversal_path(ctx, "TRIGGERED", "User", "Pipeline", "1/100/1000/", 2).await;

    // FK edge: Pipeline source_tags carry status from the MemTable (no JOIN needed).
    assert_edge_tags_by_source(
        ctx,
        "IN_PROJECT",
        "Pipeline",
        "Project",
        "source_tags",
        &[(5001, &["status:success"]), (5002, &["status:failed"])],
    )
    .await;
}

pub async fn processes_stages(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, iid, sha, ref, status, source, tag, traversal_path, _siphon_replicated_at)
        VALUES (5001, 1, 1000, 1, 'abc123', 'main', 'success', 1, false, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, created_at, updated_at, traversal_path, _siphon_replicated_at)
        VALUES
        (6001, 1, 5001, 1000, 'build', 3, 0, '2024-01-15 10:00:00', '2024-01-15 10:01:00', '1/100/1000/', '2024-01-20 12:00:00'),
        (6002, 1, 5001, 1000, 'test', 3, 1, '2024-01-15 10:01:00', '2024-01-15 10:02:00', '1/100/1000/', '2024-01-20 12:00:00'),
        (6003, 1, 5001, 1000, 'deploy', 3, 2, '2024-01-15 10:02:00', '2024-01-15 10:03:00', '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_stage", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT name FROM {} FINAL ORDER BY id",
            t("gl_stage")
        ))
        .await;
    let name =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "name").expect("name column");
    assert_eq!(name.value(0), "build");
    assert_eq!(name.value(1), "test");
    assert_eq!(name.value(2), "deploy");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Stage", "Project", "1/100/1000/", 3).await;
    assert_edges_have_traversal_path(ctx, "HAS_STAGE", "Pipeline", "Stage", "1/100/1000/", 3).await;
}

pub async fn processes_jobs(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at)
        VALUES (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, project_id, user_id, name, status, ref, tag, allow_failure, environment, `when`, retried, created_at, started_at, finished_at, queued_at, traversal_path, _siphon_replicated_at)
        VALUES
        (7001, 1, 6001, 1000, 1, 'compile', 'success', 'main', false, false, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00'),
        (7002, 1, 6001, 1000, 1, 'lint', 'success', 'main', false, true, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_job", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT name FROM {} FINAL ORDER BY id",
            t("gl_job")
        ))
        .await;
    let name =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "name").expect("name column");
    assert_eq!(name.value(0), "compile");
    assert_eq!(name.value(1), "lint");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Job", "Project", "1/100/1000/", 2).await;
    assert_edges_have_traversal_path(ctx, "HAS_JOB", "Stage", "Job", "1/100/1000/", 2).await;
    assert_edges_have_traversal_path(ctx, "TRIGGERED", "User", "Job", "1/100/1000/", 2).await;
}

pub async fn processes_ci_hierarchy(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, user_id, iid, sha, ref, status, source, tag, traversal_path, _siphon_replicated_at)
        VALUES (5001, 1, 1000, 1, 1, 'abc123', 'main', 'success', 1, false, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at)
        VALUES
        (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00'),
        (6002, 1, 5001, 1000, 'test', 3, 1, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, project_id, user_id, name, status, ref, allow_failure, traversal_path, _siphon_replicated_at)
        VALUES
        (7001, 1, 6001, 1000, 1, 'compile', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00'),
        (7002, 1, 6001, 1000, 1, 'docker-build', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00'),
        (7003, 1, 6002, 1000, 1, 'unit-tests', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00'),
        (7004, 1, 6002, 1000, 1, 'integration-tests', 'success', 'main', true, '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_pipeline", 1).await;
    assert_node_count(ctx, "gl_stage", 2).await;
    assert_node_count(ctx, "gl_job", 4).await;

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Pipeline", "Project", "1/100/1000/", 1)
        .await;
    assert_edges_have_traversal_path(ctx, "HAS_STAGE", "Pipeline", "Stage", "1/100/1000/", 2).await;
    assert_edges_have_traversal_path(ctx, "HAS_JOB", "Stage", "Job", "1/100/1000/", 4).await;
    assert_edges_have_traversal_path(ctx, "TRIGGERED", "User", "Pipeline", "1/100/1000/", 1).await;
    assert_edges_have_traversal_path(ctx, "TRIGGERED", "User", "Job", "1/100/1000/", 4).await;
}

pub async fn processes_pipeline_auto_canceled_by(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, iid, sha, ref, status, source, tag, created_at, traversal_path, _siphon_replicated_at) \
         VALUES \
         (5001, 1, 1000, 1, 'a', 'main', 'canceled', 1, false, '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00'), \
         (5002, 1, 1000, 2, 'b', 'main', 'success',  1, false, '2024-01-15 10:05:00', '1/100/1000/', '2024-01-20 12:00:00')",
    )
    .await;
    ctx.execute(
        "ALTER TABLE siphon_p_ci_pipelines UPDATE auto_canceled_by_id = 5002 WHERE id = 5001 SETTINGS mutations_sync = 1",
    ).await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(
        ctx,
        "AUTO_CANCELED_BY",
        "Pipeline",
        "Pipeline",
        "1/100/1000/",
        1,
    )
    .await;
}

pub async fn processes_job_in_pipeline_and_runs_on(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_runner(ctx, 9001, 1, "shared", None).await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, iid, sha, ref, status, source, tag, created_at, traversal_path, _siphon_replicated_at) \
         VALUES (5001, 1, 1000, 1, 'a', 'main', 'success', 1, false, '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at) \
         VALUES (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, commit_id, project_id, runner_id, type, name, status, ref, allow_failure, traversal_path, _siphon_replicated_at) \
         VALUES (7001, 1, 6001, 5001, 1000, 9001, 'Ci::Build', 'compile', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .unwrap();
    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "IN_PIPELINE", "Job", "Pipeline", "1/100/1000/", 1).await;
    assert_edges_have_traversal_path(ctx, "HAS_JOB", "Pipeline", "Job", "1/100/1000/", 1).await;
    assert_edges_have_traversal_path(ctx, "RUNS_ON", "Job", "Runner", "1/100/1000/", 1).await;
    assert_node_count(ctx, "gl_runner", 1).await;
}

pub async fn processes_runs_for_group_and_project(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_runner(ctx, 9002, 2, "group-runner", Some(1)).await;
    create_runner(ctx, 9003, 3, "project-runner", Some(1)).await;
    create_runner_namespace(ctx, 1, 9002, 100, "1/100/").await;
    create_runner_project(ctx, 1, 9003, 1000, "1/100/1000/").await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .unwrap();
    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "RUNS_FOR_GROUP", "Runner", "Group", "1/100/", 1).await;
    assert_edges_have_traversal_path(
        ctx,
        "RUNS_FOR_PROJECT",
        "Runner",
        "Project",
        "1/100/1000/",
        1,
    )
    .await;
}

pub async fn processes_ci_sources_pipelines(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, iid, sha, ref, status, source, tag, created_at, traversal_path, _siphon_replicated_at) \
         VALUES \
         (5001, 1, 1000, 1, 'a', 'main', 'success', 1, false, '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00'), \
         (5002, 1, 1000, 2, 'b', 'main', 'success', 12, false, '2024-01-15 10:05:00', '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at) \
         VALUES (6001, 1, 5001, 1000, 'trigger-stage', 3, 0, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, commit_id, project_id, type, name, status, ref, allow_failure, traversal_path, _siphon_replicated_at) \
         VALUES (7001, 1, 6001, 5001, 1000, 'Ci::Bridge', 'trigger-downstream', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_ci_sources_pipelines (id, project_id, source_project_id, source_job_id, partition_id, source_partition_id, pipeline_id, source_pipeline_id, traversal_path, _siphon_replicated_at) \
         VALUES (1, 1000, 1000, 7001, 1, 1, 5002, 5001, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "CHILD_OF", "Pipeline", "Pipeline", "1/100/1000/", 1)
        .await;
    assert_edges_have_traversal_path(
        ctx,
        "TRIGGERS_PIPELINE",
        "Job",
        "Pipeline",
        "1/100/1000/",
        1,
    )
    .await;

    // Standalone edge with both sides enriched: CHILD_OF Pipeline → Pipeline.
    // Source (child pipeline 5002, status=success) and target (parent pipeline 5001, status=success).
    assert_edge_tags_by_source(
        ctx,
        "CHILD_OF",
        "Pipeline",
        "Pipeline",
        "source_tags",
        &[(5002, &["status:success"])],
    )
    .await;
    assert_edge_tags_by_target(
        ctx,
        "CHILD_OF",
        "Pipeline",
        "Pipeline",
        "target_tags",
        &[(5001, &["status:success"])],
    )
    .await;
}

pub async fn processes_job_metadata(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at) \
         VALUES (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, project_id, type, name, status, ref, allow_failure, traversal_path, _siphon_replicated_at) \
         VALUES (7001, 1, 6001, 1000, 'Ci::Build', 'compile', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;
    ctx.execute(
        "INSERT INTO siphon_p_ci_builds_metadata (id, build_id, project_id, partition_id, timeout, timeout_source, interruptible, expanded_environment_name, traversal_path, _siphon_replicated_at) \
         VALUES (9001, 7001, 1000, 1, 3600, 2, true, 'production', '1/100/1000/', '2024-01-20 12:00:00')",
    ).await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_job_metadata", 1).await;
    assert_edges_have_traversal_path(ctx, "HAS_METADATA", "Job", "JobMetadata", "1/100/1000/", 1)
        .await;
}
