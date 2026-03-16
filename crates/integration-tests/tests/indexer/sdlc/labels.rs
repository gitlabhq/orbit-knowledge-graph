use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    create_project, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_labels_with_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_labels
            (id, title, color, description, project_id, group_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 'bug', '#ff0000', 'Bug reports', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
            (2, 'feature', '#00ff00', 'New features', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
            (3, 'priority', '#0000ff', 'Priority items', NULL, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_label", 3).await;

    let result = ctx
        .query("SELECT title, color FROM gl_label FINAL ORDER BY id")
        .await;
    let batch = &result[0];
    let titles =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "title").expect("title column");
    assert_eq!(titles.value(0), "bug");
    assert_eq!(titles.value(1), "feature");
    assert_eq!(titles.value(2), "priority");

    let colors =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "color").expect("color column");
    assert_eq!(colors.value(0), "#ff0000");
    assert_eq!(colors.value(1), "#00ff00");
    assert_eq!(colors.value(2), "#0000ff");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Label", "Project", "1/100/", 2).await;
    assert_edges_have_traversal_path(ctx, "IN_GROUP", "Label", "Group", "1/100/", 1).await;
}
