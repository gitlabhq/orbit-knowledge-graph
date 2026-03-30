use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, assert_edge_count_for_traversal_path, assert_edges_have_traversal_path,
    assert_node_count, create_member, create_namespace, create_namespace_with_path, create_project,
    create_project_with_path, create_user, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_projects(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_project(ctx, 1001, 100, 2, 20, "1/100/1001/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_project", 2).await;

    let result = ctx
        .query("SELECT visibility_level FROM gl_project FINAL ORDER BY id")
        .await;
    let visibility = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "visibility_level")
        .expect("visibility_level column");
    assert_eq!(visibility.value(0), "private");
    assert_eq!(visibility.value(1), "public");

    assert_edge_count_for_traversal_path(ctx, "CREATOR", "User", "Project", "1/100/1000/", 1).await;
    assert_edge_count_for_traversal_path(ctx, "CREATOR", "User", "Project", "1/100/1001/", 1).await;
    assert_edge_count_for_traversal_path(ctx, "CONTAINS", "Group", "Project", "1/100/1000/", 1)
        .await;
    assert_edge_count_for_traversal_path(ctx, "CONTAINS", "Group", "Project", "1/100/1001/", 1)
        .await;
}

pub async fn computes_full_path_for_projects(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("gitlab-org")).await;
    create_namespace_with_path(ctx, 200, Some(100), 0, "1/100/200/", Some("orbit")).await;
    create_project_with_path(ctx, 1000, 100, 1, 0, "1/100/1000/", Some("gitlab")).await;
    create_project_with_path(ctx, 1001, 200, 1, 0, "1/100/200/1001/", Some("knowledge-graph"))
        .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_project", 2).await;

    let result = ctx
        .query("SELECT id, full_path FROM gl_project FINAL ORDER BY id")
        .await;
    let ids = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(&result[0], "id")
        .expect("id column");
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");

    assert_eq!(ids.value(0), 1000);
    assert_eq!(paths.value(0), "gitlab-org/gitlab");
    assert_eq!(ids.value(1), 1001);
    assert_eq!(paths.value(1), "gitlab-org/orbit/knowledge-graph");
}

pub async fn creates_member_of_edges_for_projects(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_user(ctx, 1).await;
    create_member(ctx, 1, 1000, "Project", "1/100/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "MEMBER_OF", "User", "Project", "1/100/", 1).await;
}
