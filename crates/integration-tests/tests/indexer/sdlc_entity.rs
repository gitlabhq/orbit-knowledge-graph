use arrow::array::{BooleanArray, StringArray};
use gkg_utils::arrow::ArrowUtils;
use indexer::topic::IndexingScope;
use integration_testkit::t;

use super::common::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, assert_edge_count_for_traversal_path,
    assert_node_count, create_namespace, create_project, entity_envelope, entity_handler,
    handler_context,
};
use integration_testkit::run_subtests;

#[tokio::test]
async fn entity_global_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests!(&ctx, processes_users_via_entity_handler,);
}

#[tokio::test]
async fn entity_namespace_indexing() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    run_subtests!(&ctx, processes_projects_via_entity_handler,);
}

async fn processes_users_via_entity_handler(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_users (
            id, username, email, name, first_name, last_name, state,
            public_email, preferred_language, last_activity_on, private_profile,
            admin, auditor, external, user_type, created_at, updated_at, _siphon_replicated_at
        ) VALUES
        (1, 'alice', 'alice@test.com', 'Alice Smith', 'Alice', 'Smith', 'active',
         'alice.public@test.com', 'en', '2024-01-15', false, true, false, false, 0,
         '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
        (2, 'bob', 'bob@test.com', 'Bob Jones', 'Bob', 'Jones', 'active',
         'bob.public@test.com', 'es', '2024-01-10', true, false, false, true, 1,
         '2023-06-15', '2024-01-10', '2024-01-20 12:00:00')",
    )
    .await;

    entity_handler(ctx, "User")
        .await
        .handle(
            handler_context(ctx),
            entity_envelope("User", IndexingScope::Global),
        )
        .await
        .expect("entity handler should succeed");

    assert_node_count(ctx, "gl_user", 2).await;

    let result = ctx
        .query(&format!("SELECT * FROM {} FINAL ORDER BY id", t("gl_user")))
        .await;
    let batch = &result[0];

    let user_type = ArrowUtils::get_column_by_name::<StringArray>(batch, "user_type")
        .expect("user_type column");
    assert_eq!(user_type.value(0), "human");
    assert_eq!(user_type.value(1), "support_bot");

    let is_admin =
        ArrowUtils::get_column_by_name::<BooleanArray>(batch, "is_admin").expect("is_admin column");
    assert!(is_admin.value(0));
    assert!(!is_admin.value(1));
}

async fn processes_projects_via_entity_handler(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_project(ctx, 1001, 100, 2, 20, "1/100/1001/").await;

    entity_handler(ctx, "Project")
        .await
        .handle(
            handler_context(ctx),
            entity_envelope(
                "Project",
                IndexingScope::Namespace {
                    namespace_id: 100,
                    traversal_path: "1/100/".to_string(),
                },
            ),
        )
        .await
        .expect("entity handler should succeed");

    assert_node_count(ctx, "gl_project", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT visibility_level FROM {} FINAL ORDER BY id",
            t("gl_project")
        ))
        .await;
    let visibility = ArrowUtils::get_column_by_name::<StringArray>(&result[0], "visibility_level")
        .expect("visibility_level column");
    assert_eq!(visibility.value(0), "private");
    assert_eq!(visibility.value(1), "public");

    assert_edge_count_for_traversal_path(ctx, "CREATOR", "User", "Project", "1/100/1000/", 1).await;
    assert_edge_count_for_traversal_path(ctx, "CONTAINS", "Group", "Project", "1/100/1000/", 1)
        .await;
}
