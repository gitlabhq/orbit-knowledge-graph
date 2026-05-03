use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edge_count_for_traversal_path, assert_edges_have_traversal_path,
    assert_node_count, create_member, create_namespace, create_namespace_with_path, create_route,
    create_user, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_and_transforms_groups(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_namespace(ctx, 101, Some(100), 10, "1/100/101/").await;
    create_namespace(ctx, 102, Some(100), 20, "1/100/102/").await;
    ctx.execute(
        "INSERT INTO siphon_namespaces \
         (id, name, path, type, visibility_level, parent_id, owner_id, traversal_ids, created_at, updated_at, _siphon_replicated_at) \
         VALUES (1000, 'project-namespace', 'project-namespace', 'Project', 0, 100, 1, [1,100,1000], \
                 '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_namespace_details (namespace_id, description) VALUES (1000, NULL)",
    )
    .await;
    ctx.execute(
        "INSERT INTO namespace_traversal_paths (id, traversal_path) VALUES (1000, '1/100/1000/')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_group", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT visibility_level FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let visibility = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "visibility_level")
        .expect("visibility_level column");
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

pub async fn computes_full_path_for_top_level_group(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("acme")).await;
    create_route(ctx, 100, 100, "Namespace", "acme", 100, "1/100/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT full_path FROM {} FINAL WHERE id = 100",
            t("gl_group")
        ))
        .await;
    let full_path = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");
    assert_eq!(full_path.value(0), "acme");
}

pub async fn computes_full_path_for_nested_subgroups(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("gitlab-org")).await;
    create_namespace_with_path(ctx, 200, Some(100), 0, "1/100/200/", Some("orbit")).await;
    create_namespace_with_path(
        ctx,
        300,
        Some(200),
        0,
        "1/100/200/300/",
        Some("knowledge-graph"),
    )
    .await;
    create_route(ctx, 100, 100, "Namespace", "gitlab-org", 100, "1/100/").await;
    create_route(
        ctx,
        200,
        200,
        "Namespace",
        "gitlab-org/orbit",
        200,
        "1/100/200/",
    )
    .await;
    create_route(
        ctx,
        300,
        300,
        "Namespace",
        "gitlab-org/orbit/knowledge-graph",
        300,
        "1/100/200/300/",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_group", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT id, full_path FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(&result[0], "id")
        .expect("id column");
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");

    assert_eq!(ids.value(0), 100);
    assert_eq!(paths.value(0), "gitlab-org");
    assert_eq!(ids.value(1), 200);
    assert_eq!(paths.value(1), "gitlab-org/orbit");
    assert_eq!(ids.value(2), 300);
    assert_eq!(paths.value(2), "gitlab-org/orbit/knowledge-graph");
}

pub async fn route_rename_updates_full_path(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("old-name")).await;
    create_route(ctx, 100, 100, "Namespace", "old-name", 100, "1/100/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT full_path FROM {} FINAL WHERE id = 100",
            t("gl_group")
        ))
        .await;
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");
    assert_eq!(paths.value(0), "old-name");

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('ns.100.Group', '2024-01-20 12:00:00.000000', 'null')",
        t("checkpoint")
    ))
    .await;
    ctx.execute(
        "INSERT INTO siphon_routes (id, source_id, source_type, path, namespace_id, traversal_path, _siphon_replicated_at) \
         VALUES (100, 100, 'Namespace', 'renamed-group', 100, '1/100/', '2024-01-20 18:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_namespaces (id, name, path, type, visibility_level, traversal_ids, _siphon_replicated_at) \
         VALUES (100, 'renamed-group', 'renamed-group', 'Group', 0, [1,100], '2024-01-20 18:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_namespace_details (namespace_id, description, _siphon_replicated_at) \
         VALUES (100, NULL, '2024-01-20 18:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT full_path FROM {} FINAL WHERE id = 100",
            t("gl_group")
        ))
        .await;
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");
    assert_eq!(paths.value(0), "renamed-group");
}

pub async fn child_route_reflects_parent_rename(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("parent")).await;
    create_namespace_with_path(ctx, 200, Some(100), 0, "1/100/200/", Some("child")).await;
    create_route(ctx, 100, 100, "Namespace", "parent", 100, "1/100/").await;
    create_route(
        ctx,
        200,
        200,
        "Namespace",
        "parent/child",
        200,
        "1/100/200/",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT id, full_path FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(&result[0], "id")
        .expect("id column");
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");
    assert_eq!(ids.value(0), 100);
    assert_eq!(paths.value(0), "parent");
    assert_eq!(ids.value(1), 200);
    assert_eq!(paths.value(1), "parent/child");

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('ns.100.Group', '2024-01-20 12:00:00.000000', 'null')",
        t("checkpoint")
    ))
    .await;
    ctx.execute(
        "INSERT INTO siphon_routes (id, source_id, source_type, path, namespace_id, traversal_path, _siphon_replicated_at) \
         VALUES (100, 100, 'Namespace', 'new-parent', 100, '1/100/', '2024-01-20 18:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_routes (id, source_id, source_type, path, namespace_id, traversal_path, _siphon_replicated_at) \
         VALUES (200, 200, 'Namespace', 'new-parent/child', 200, '1/100/200/', '2024-01-20 18:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_namespaces (id, name, path, type, visibility_level, parent_id, traversal_ids, _siphon_replicated_at) \
         VALUES (100, 'new-parent', 'new-parent', 'Group', 0, NULL, [1,100], '2024-01-20 18:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_namespaces (id, name, path, type, visibility_level, parent_id, traversal_ids, _siphon_replicated_at) \
         VALUES (200, 'child', 'child', 'Group', 0, 100, [1,100,200], '2024-01-20 18:00:00')",
    )
    .await;
    ctx.execute(
        "INSERT INTO siphon_namespace_details (namespace_id, description, _siphon_replicated_at) \
         VALUES (100, NULL, '2024-01-20 18:00:00'), (200, NULL, '2024-01-20 18:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT id, full_path FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<arrow::array::Int64Array>(&result[0], "id")
        .expect("id column");
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");
    assert_eq!(ids.value(0), 100);
    assert_eq!(paths.value(0), "new-parent");
    assert_eq!(ids.value(1), 200);
    assert_eq!(paths.value(1), "new-parent/child");
}

pub async fn no_route_falls_back_to_slug(ctx: &TestContext) {
    create_namespace_with_path(ctx, 100, None, 0, "1/100/", Some("my-group")).await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT full_path FROM {} FINAL WHERE id = 100",
            t("gl_group")
        ))
        .await;
    let paths = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "full_path")
        .expect("full_path column");
    assert_eq!(paths.value(0), "my-group");
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
