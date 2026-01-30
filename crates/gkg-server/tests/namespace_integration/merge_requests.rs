//! Integration tests for merge request processing in the namespace handler.

use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, create_namespace_payload, default_test_watermark, get_namespace_handler,
    get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_requests_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
            VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, author_id, assignee_ids, merge_user_id, milestone_id,
                 traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'Implements feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, 1, '2/3', NULL, 10, '1/100/', '2024-01-20 12:00:00'),
                (2, 102, 'Fix bug Y', 'Fixes critical bug', 'fix-y', 'main', 3, 'merged',
                 false, false, 1000, 2, '', 1, NULL, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, title, state, merge_status, draft, squash FROM gl_merge_requests ORDER BY id")
        .await;
    assert!(!result.is_empty(), "merge requests should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let titles = get_string_column(batch, "title");
    assert_eq!(titles.value(0), "Add feature X");
    assert_eq!(titles.value(1), "Fix bug Y");

    let states = get_string_column(batch, "state");
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "merged");

    let in_project_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'in_project' AND source_kind = 'MergeRequest'",
        )
        .await;
    assert_eq!(
        in_project_edges[0].num_rows(),
        2,
        "both MRs should have in_project edges"
    );

    let authored_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'authored' AND target_kind = 'MergeRequest'
             ORDER BY target_id",
        )
        .await;
    assert_eq!(
        authored_edges[0].num_rows(),
        2,
        "both MRs should have author edges"
    );

    let assigned_edges = context
        .query(
            "SELECT target_id FROM gl_edges
             WHERE relationship_kind = 'assigned' AND target_kind = 'MergeRequest'",
        )
        .await;
    assert_eq!(
        assigned_edges[0].num_rows(),
        2,
        "MR 1 has two assignees (multi-value)"
    );

    let merged_by_edges = context
        .query(
            "SELECT target_id FROM gl_edges
             WHERE relationship_kind = 'merged_by' AND target_kind = 'MergeRequest'",
        )
        .await;
    assert_eq!(merged_by_edges[0].num_rows(), 1, "only MR 2 was merged");

    let in_milestone_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'in_milestone' AND source_kind = 'MergeRequest' AND target_kind = 'Milestone'",
        )
        .await;
    assert_eq!(
        in_milestone_edges[0].num_rows(),
        1,
        "only MR 1 has a milestone"
    );
}
