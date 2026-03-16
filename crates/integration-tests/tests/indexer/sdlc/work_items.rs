use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_work_items_with_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
        VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_issues (id, title, description, author_id, project_id, state_id, work_item_type_id, _siphon_replicated_at)
        VALUES
            (1, 'Fix login bug', 'Users cannot log in', 1, 1000, 1, 1, '2024-01-20 12:00:00'),
            (2, 'Add feature Y', 'New feature request', 2, 1000, 2, 5, '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO hierarchy_work_items
            (id, iid, title, author_id, state_id, work_item_type_id, confidential,
             milestone_id, namespace_id, assignee_ids, label_ids,
             traversal_path, version, custom_status_id, system_defined_status_id)
        VALUES
            (1, 1, 'Fix login bug', 1, 1, 1, false, 10, 100, '2/3', '5/6/7', '1/100/', '2024-01-20 12:00:00', 0, 0),
            (2, 2, 'Add feature Y', 2, 2, 5, true, NULL, 100, '', '8', '1/100/', '2024-01-20 12:00:00', 0, 0)",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_work_item", 2).await;

    let result = ctx
        .query("SELECT title, state, work_item_type FROM gl_work_item FINAL ORDER BY id")
        .await;
    let batch = &result[0];

    let titles =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "title").expect("title column");
    assert_eq!(titles.value(0), "Fix login bug");
    assert_eq!(titles.value(1), "Add feature Y");

    let states =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "state").expect("state column");
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "closed");

    let work_item_types = ArrowUtils::get_column_by_name::<StringArray>(batch, "work_item_type")
        .expect("work_item_type column");
    assert_eq!(work_item_types.value(0), "issue");
    assert_eq!(work_item_types.value(1), "task");
}

pub async fn processes_work_item_single_value_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
        VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO namespace_traversal_paths (id, traversal_path, version)
        VALUES (100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_issues (id, title, description, _siphon_replicated_at)
        VALUES (1, 'Test issue', 'Test description', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO hierarchy_work_items
            (id, title, author_id, state_id, work_item_type_id, confidential,
             milestone_id, namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
        VALUES (1, 'Test issue', 1, 1, 1, false, 10, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "WorkItem", "1/100/", 1).await;
    assert_edges_have_traversal_path(ctx, "IN_MILESTONE", "WorkItem", "Milestone", "1/100/", 1)
        .await;
    assert_edges_have_traversal_path(ctx, "IN_GROUP", "WorkItem", "Group", "1/100/", 1).await;
}

pub async fn processes_work_item_multi_target_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_issues (id, title, description, _siphon_replicated_at)
        VALUES (1, 'Test issue', 'Test description', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO hierarchy_work_items
            (id, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, assignee_ids, label_ids, traversal_path, version, custom_status_id, system_defined_status_id)
        VALUES (1, 'Test issue', 1, 1, 1, false, 100, '10/20/30', '5/6', '1/100/', '2024-01-20 12:00:00', 0, 0)",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "ASSIGNED", "User", "WorkItem", "1/100/", 3).await;
    assert_edges_have_traversal_path(ctx, "HAS_LABEL", "WorkItem", "Label", "1/100/", 2).await;
}

pub async fn processes_work_item_parent_links(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO siphon_issues (id, title, project_id, author_id, state_id, work_item_type_id, _siphon_replicated_at)
        VALUES
            (1, 'Epic: Q1 Goals', 1000, 1, 1, 8, '2024-01-20 12:00:00'),
            (2, 'Task: Design review', 1000, 1, 1, 5, '2024-01-20 12:00:00'),
            (3, 'Task: Implementation', 1000, 1, 1, 5, '2024-01-20 12:00:00'),
            (4, 'Sub-task: Frontend', 1000, 1, 1, 5, '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO hierarchy_work_items
            (id, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
        VALUES
            (1, 'Epic: Q1 Goals', 1, 1, 8, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
            (2, 'Task: Design review', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
            (3, 'Task: Implementation', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
            (4, 'Sub-task: Frontend', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_work_item_parent_links
            (id, work_item_id, work_item_parent_id, namespace_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 2, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 3, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (3, 4, 3, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "CONTAINS", "WorkItem", "WorkItem", "1/100/", 3).await;
}

pub async fn processes_issue_links(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO siphon_issues (id, title, project_id, author_id, state_id, work_item_type_id, _siphon_replicated_at)
        VALUES
            (1, 'Issue A', 1000, 1, 1, 1, '2024-01-20 12:00:00'),
            (2, 'Issue B', 1000, 1, 1, 1, '2024-01-20 12:00:00'),
            (3, 'Issue C', 1000, 1, 1, 1, '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO hierarchy_work_items
            (id, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
        VALUES
            (1, 'Issue A', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
            (2, 'Issue B', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
            (3, 'Issue C', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_issue_links
            (id, source_id, target_id, link_type, namespace_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 1, 2, 0, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 2, 3, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "RELATED_TO", "WorkItem", "WorkItem", "1/100/", 2).await;
}
