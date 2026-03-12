use crate::indexer::common::{
    TestContext, assert_edge_count_for_traversal_path, assert_edges_have_traversal_path,
    assert_node_count, create_member, create_namespace, create_project, create_user,
    get_string_column, handler_context, namespace_envelope, namespace_handler,
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
    let visibility = get_string_column(&result[0], "visibility_level");
    assert_eq!(visibility.value(0), "private");
    assert_eq!(visibility.value(1), "public");

    assert_edge_count_for_traversal_path(ctx, "CREATOR", "User", "Project", "1/100/1000/", 1).await;
    assert_edge_count_for_traversal_path(ctx, "CREATOR", "User", "Project", "1/100/1001/", 1).await;
    assert_edge_count_for_traversal_path(ctx, "CONTAINS", "Group", "Project", "1/100/1000/", 1)
        .await;
    assert_edge_count_for_traversal_path(ctx, "CONTAINS", "Group", "Project", "1/100/1001/", 1)
        .await;
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
