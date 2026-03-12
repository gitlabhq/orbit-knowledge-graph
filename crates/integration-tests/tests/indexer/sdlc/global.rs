use arrow::array::{BooleanArray, StringArray, UInt64Array};

use crate::indexer::common::{
    TestContext, assert_node_count, get_string_column, global_envelope, global_handler,
    handler_context,
};

pub async fn processes_and_transforms_users(ctx: &TestContext) {
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
         '2023-06-15', '2024-01-10', '2024-01-20 12:00:00'),
        (3, 'charlie', 'charlie@test.com', 'Charlie Brown', 'Charlie', 'Brown', 'blocked',
         '', 'fr', '2024-01-05', false, false, true, false, 4,
         '2023-09-20', '2024-01-05', '2024-01-20 12:00:00')",
    )
    .await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    assert_node_count(ctx, "gl_user", 3).await;

    let result = ctx.query("SELECT * FROM gl_user FINAL ORDER BY id").await;
    let batch = &result[0];

    let user_type = batch
        .column_by_name("user_type")
        .expect("user_type column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("user_type should be StringArray");
    assert_eq!(user_type.value(0), "human");
    assert_eq!(user_type.value(1), "support_bot");
    assert_eq!(user_type.value(2), "service_user");

    let is_admin = batch
        .column_by_name("is_admin")
        .expect("is_admin column should exist")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("is_admin should be BooleanArray");
    assert!(is_admin.value(0));
    assert!(!is_admin.value(1));
    assert!(!is_admin.value(2));
}

pub async fn uses_watermark_for_incremental_processing(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO checkpoint (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-19 00:00:00.000000', 'null')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_users (
            id, username, email, name, first_name, last_name, state,
            public_email, preferred_language, last_activity_on, private_profile,
            admin, auditor, external, user_type, created_at, updated_at, _siphon_replicated_at
        ) VALUES
        (1, 'old_user', 'old@test.com', 'Old User', 'Old', 'User', 'active',
         '', 'en', '2024-01-01', false, false, false, false, 0,
         '2023-01-01', '2024-01-01', '2024-01-18 12:00:00'),
        (2, 'new_user', 'new@test.com', 'New User', 'New', 'User', 'active',
         '', 'en', '2024-01-20', false, false, false, false, 0,
         '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
    )
    .await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    let result = ctx.query("SELECT count() as cnt FROM gl_user FINAL").await;
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");
    assert_eq!(
        count.value(0),
        1,
        "should only process new_user, not old_user"
    );

    let usernames = ctx.query("SELECT username FROM gl_user FINAL").await;
    let username = get_string_column(&usernames[0], "username");
    assert_eq!(username.value(0), "new_user");
}
