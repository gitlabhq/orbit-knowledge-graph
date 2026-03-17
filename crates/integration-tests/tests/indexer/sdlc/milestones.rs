use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    create_project, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_milestones_with_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_milestones
            (id, iid, title, description, state, due_date, start_date, project_id, group_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'v1.0', 'First release', 'active', '2024-03-01', '2024-01-01', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'v2.0', 'Second release', 'closed', '2024-06-01', '2024-03-01', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
            (3, 1, 'Q1 Goals', 'Group milestone', 'active', '2024-03-31', '2024-01-01', NULL, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_milestone", 3).await;

    let result = ctx
        .query("SELECT title, state FROM gl_milestone FINAL ORDER BY id")
        .await;
    let batch = &result[0];
    let titles =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "title").expect("title column");
    assert_eq!(titles.value(0), "v1.0");
    assert_eq!(titles.value(1), "v2.0");
    assert_eq!(titles.value(2), "Q1 Goals");

    let states =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "state").expect("state column");
    assert_eq!(states.value(0), "active");
    assert_eq!(states.value(1), "closed");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Milestone", "Project", "1/100/", 2).await;
    assert_edges_have_traversal_path(ctx, "IN_GROUP", "Milestone", "Group", "1/100/", 1).await;
}
