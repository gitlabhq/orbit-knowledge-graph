use arrow::array::{BooleanArray, Int64Array, StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_node_count, global_envelope, global_handler, handler_context,
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
         '2023-09-20', '2024-01-05', '2024-01-20 12:00:00'),
        (4, 'service-account', 'service-account@test.com', 'Service Account', 'Service', 'Account', 'active',
         '', 'en', '2024-01-07', false, false, false, false, 13,
         '2023-09-20', '2024-01-05', '2024-01-20 12:00:00')",
    )
    .await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    assert_node_count(ctx, "gl_user", 4).await;

    let result = ctx
        .query(&format!("SELECT * FROM {} FINAL ORDER BY id", t("gl_user")))
        .await;
    let batch = &result[0];

    let user_type = ArrowUtils::get_column_by_name::<StringArray>(batch, "user_type")
        .expect("user_type column");
    assert_eq!(user_type.value(0), "human");
    assert_eq!(user_type.value(1), "support_bot");
    assert_eq!(user_type.value(2), "service_user");
    assert_eq!(user_type.value(3), "service_account");

    let is_admin =
        ArrowUtils::get_column_by_name::<BooleanArray>(batch, "is_admin").expect("is_admin column");
    assert!(is_admin.value(0));
    assert!(!is_admin.value(1));
    assert!(!is_admin.value(2));
}

pub async fn uses_watermark_for_incremental_processing(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-19 00:00:00.000000', 'null')",
        t("checkpoint")
    ))
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

    let result = ctx
        .query(&format!(
            "SELECT count() as cnt FROM {} FINAL",
            t("gl_user")
        ))
        .await;
    let count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "cnt").expect("cnt column");
    assert_eq!(
        count.value(0),
        1,
        "should only process new_user, not old_user"
    );

    let usernames = ctx
        .query(&format!("SELECT username FROM {} FINAL", t("gl_user")))
        .await;
    let username = ArrowUtils::get_column_by_name::<StringArray>(&usernames[0], "username")
        .expect("username column");
    assert_eq!(username.value(0), "new_user");
}

/// Validates keyset cursor resume: a saved cursor_values=["2"] must cause the
/// extract query's DNF (`id > '2'`) to skip rows with id ≤ 2 and process the rest.
/// This exercises the single-column cursor path that no other integration test
/// covers (all others start with cursor_values='null').
pub async fn resumes_from_saved_cursor_skipping_processed_users(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-19 00:00:00.000000', '[\"2\"]')",
        t("checkpoint")
    ))
    .await;

    ctx.execute(
        "INSERT INTO siphon_users (
            id, username, email, name, first_name, last_name, state,
            public_email, preferred_language, last_activity_on, private_profile,
            admin, auditor, external, user_type, created_at, updated_at, _siphon_replicated_at
        ) VALUES
        (1, 'alice', 'a@t', 'Alice', 'A', 'L', 'active', '', 'en', '2024-01-01',
         false, false, false, false, 0, '2023-01-01', '2024-01-01', '2024-01-20 12:00:00'),
        (2, 'bob', 'b@t', 'Bob', 'B', 'L', 'active', '', 'en', '2024-01-01',
         false, false, false, false, 0, '2023-01-01', '2024-01-01', '2024-01-20 12:00:00'),
        (3, 'charlie', 'c@t', 'Charlie', 'C', 'L', 'active', '', 'en', '2024-01-01',
         false, false, false, false, 0, '2023-01-01', '2024-01-01', '2024-01-20 12:00:00'),
        (4, 'dave', 'd@t', 'Dave', 'D', 'L', 'active', '', 'en', '2024-01-01',
         false, false, false, false, 0, '2023-01-01', '2024-01-01', '2024-01-20 12:00:00')",
    )
    .await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_user")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let processed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        processed,
        vec![3, 4],
        "saved cursor at id=2 must skip users 1-2 and process 3-4"
    );
}
