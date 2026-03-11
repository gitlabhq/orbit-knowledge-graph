use crate::common::{
    TestContext, assert_edge_count_for_traversal_path, assert_edges_have_traversal_path,
    assert_node_count, create_member, create_namespace, create_user, get_string_column,
    handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_and_transforms_groups(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_namespace(ctx, 101, Some(100), 10, "1/100/101/").await;
    create_namespace(ctx, 102, Some(100), 20, "1/100/102/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_group", 3).await;

    let result = ctx
        .query("SELECT visibility_level FROM gl_group FINAL ORDER BY id")
        .await;
    let visibility = get_string_column(&result[0], "visibility_level");
    assert_eq!(visibility.value(0), "private");
    assert_eq!(visibility.value(1), "internal");
    assert_eq!(visibility.value(2), "public");
}

pub async fn creates_group_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_namespace(ctx, 101, Some(100), 0, "1/100/101/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edge_count_for_traversal_path(ctx, "OWNER", "User", "Group", "1/100/", 1).await;
    assert_edge_count_for_traversal_path(ctx, "OWNER", "User", "Group", "1/100/101/", 1).await;
    assert_edges_have_traversal_path(ctx, "CONTAINS", "Group", "Group", "1/100/101/", 1).await;
}

pub async fn creates_member_of_edges_for_groups(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_user(ctx, 1).await;
    create_user(ctx, 2).await;
    create_member(ctx, 1, 100, "Namespace", "1/100/").await;
    create_member(ctx, 2, 100, "Namespace", "1/100/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "MEMBER_OF", "User", "Group", "1/100/", 2).await;
}
