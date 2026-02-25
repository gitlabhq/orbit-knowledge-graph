//! Integration subtests for CI pipeline, stage, and job processing.

use indexer::testkit::TestEnvelopeFactory;

use crate::common::{
    IndexerTestExt, TestContext, create_namespace_payload, default_test_watermark,
    get_string_column,
};

pub async fn processes_pipelines(context: &TestContext) {
    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, path, namespace_id, creator_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 'project-alpha', 100, 1, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_users (id, username, email, name, _siphon_replicated_at)
            VALUES (1, 'testuser', 'test@example.com', 'Test User', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, user_id, iid, sha, ref, status, source, tag, duration, created_at, started_at, finished_at, traversal_path, _siphon_replicated_at)
            VALUES
            (5001, 1, 1000, 1, 1, 'abc123def456', 'main', 'success', 1, false, 120, '2024-01-15 10:00:00', '2024-01-15 10:01:00', '2024-01-15 10:03:00', '1/100/1000/', '2024-01-20 12:00:00'),
            (5002, 1, 1000, 1, 2, 'def456abc789', 'feature-branch', 'failed', 1, false, 60, '2024-01-16 10:00:00', '2024-01-16 10:01:00', '2024-01-16 10:02:00', '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_pipeline ORDER BY id").await;
    assert!(!result.is_empty(), "pipeline result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 pipelines");

    let status_column = get_string_column(batch, "status");
    assert_eq!(status_column.value(0), "success");
    assert_eq!(status_column.value(1), "failed");

    context
        .assert_edges_have_traversal_path("IN_PROJECT", "Pipeline", "Project", "1/100/1000/", 2)
        .await;
    context
        .assert_edges_have_traversal_path("TRIGGERED", "User", "Pipeline", "1/100/1000/", 2)
        .await;
}

pub async fn processes_stages(context: &TestContext) {
    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, path, namespace_id, creator_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 'project-alpha', 100, 1, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, iid, sha, ref, status, source, tag, traversal_path, _siphon_replicated_at)
            VALUES (5001, 1, 1000, 1, 'abc123', 'main', 'success', 1, false, '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, created_at, updated_at, traversal_path, _siphon_replicated_at)
            VALUES
            (6001, 1, 5001, 1000, 'build', 3, 0, '2024-01-15 10:00:00', '2024-01-15 10:01:00', '1/100/1000/', '2024-01-20 12:00:00'),
            (6002, 1, 5001, 1000, 'test', 3, 1, '2024-01-15 10:01:00', '2024-01-15 10:02:00', '1/100/1000/', '2024-01-20 12:00:00'),
            (6003, 1, 5001, 1000, 'deploy', 3, 2, '2024-01-15 10:02:00', '2024-01-15 10:03:00', '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_stage ORDER BY id").await;
    assert!(!result.is_empty(), "stage result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3, "should have 3 stages");

    let name_column = get_string_column(batch, "name");
    assert_eq!(name_column.value(0), "build");
    assert_eq!(name_column.value(1), "test");
    assert_eq!(name_column.value(2), "deploy");

    context
        .assert_edges_have_traversal_path("IN_PROJECT", "Stage", "Project", "1/100/1000/", 3)
        .await;
    context
        .assert_edges_have_traversal_path("HAS_STAGE", "Pipeline", "Stage", "1/100/1000/", 3)
        .await;
}

pub async fn processes_jobs(context: &TestContext) {
    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, path, namespace_id, creator_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 'project-alpha', 100, 1, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_users (id, username, email, name, _siphon_replicated_at)
            VALUES (1, 'testuser', 'test@example.com', 'Test User', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at)
            VALUES (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, project_id, user_id, name, status, ref, tag, allow_failure, environment, `when`, retried, created_at, started_at, finished_at, queued_at, traversal_path, _siphon_replicated_at)
            VALUES
            (7001, 1, 6001, 1000, 1, 'compile', 'success', 'main', false, false, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00'),
            (7002, 1, 6001, 1000, 1, 'lint', 'success', 'main', false, true, NULL, 'on_success', false, '2024-01-15 10:00:00', '2024-01-15 10:00:30', '2024-01-15 10:01:00', '2024-01-15 10:00:00', '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_job ORDER BY id").await;
    assert!(!result.is_empty(), "job result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 jobs");

    let name_column = get_string_column(batch, "name");
    assert_eq!(name_column.value(0), "compile");
    assert_eq!(name_column.value(1), "lint");

    context
        .assert_edges_have_traversal_path("IN_PROJECT", "Job", "Project", "1/100/1000/", 2)
        .await;
    context
        .assert_edges_have_traversal_path("HAS_JOB", "Stage", "Job", "1/100/1000/", 2)
        .await;
    context
        .assert_edges_have_traversal_path("TRIGGERED", "User", "Job", "1/100/1000/", 2)
        .await;
}

pub async fn processes_ci_hierarchy(context: &TestContext) {
    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, path, namespace_id, creator_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 'project-alpha', 100, 1, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_users (id, username, email, name, _siphon_replicated_at)
            VALUES (1, 'testuser', 'test@example.com', 'Test User', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_pipelines (id, partition_id, project_id, user_id, iid, sha, ref, status, source, tag, traversal_path, _siphon_replicated_at)
            VALUES (5001, 1, 1000, 1, 1, 'abc123', 'main', 'success', 1, false, '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_stages (id, partition_id, pipeline_id, project_id, name, status, position, traversal_path, _siphon_replicated_at)
            VALUES
            (6001, 1, 5001, 1000, 'build', 3, 0, '1/100/1000/', '2024-01-20 12:00:00'),
            (6002, 1, 5001, 1000, 'test', 3, 1, '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_builds (id, partition_id, stage_id, project_id, user_id, name, status, ref, allow_failure, traversal_path, _siphon_replicated_at)
            VALUES
            (7001, 1, 6001, 1000, 1, 'compile', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00'),
            (7002, 1, 6001, 1000, 1, 'docker-build', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00'),
            (7003, 1, 6002, 1000, 1, 'unit-tests', 'success', 'main', false, '1/100/1000/', '2024-01-20 12:00:00'),
            (7004, 1, 6002, 1000, 1, 'integration-tests', 'success', 'main', true, '1/100/1000/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let pipeline_result = context.query("SELECT * FROM gl_pipeline").await;
    assert!(
        !pipeline_result.is_empty(),
        "pipeline result should not be empty"
    );
    assert_eq!(pipeline_result[0].num_rows(), 1, "should have 1 pipeline");

    let stage_result = context
        .query("SELECT * FROM gl_stage ORDER BY position")
        .await;
    assert!(!stage_result.is_empty(), "stage result should not be empty");
    assert_eq!(stage_result[0].num_rows(), 2, "should have 2 stages");

    let job_result = context.query("SELECT * FROM gl_job ORDER BY id").await;
    assert!(!job_result.is_empty(), "job result should not be empty");
    assert_eq!(job_result[0].num_rows(), 4, "should have 4 jobs");

    context
        .assert_edges_have_traversal_path("IN_PROJECT", "Pipeline", "Project", "1/100/1000/", 1)
        .await;
    context
        .assert_edges_have_traversal_path("HAS_STAGE", "Pipeline", "Stage", "1/100/1000/", 2)
        .await;
    context
        .assert_edges_have_traversal_path("HAS_JOB", "Stage", "Job", "1/100/1000/", 4)
        .await;
    context
        .assert_edges_have_traversal_path("TRIGGERED", "User", "Pipeline", "1/100/1000/", 1)
        .await;
    context
        .assert_edges_have_traversal_path("TRIGGERED", "User", "Job", "1/100/1000/", 4)
        .await;
}
