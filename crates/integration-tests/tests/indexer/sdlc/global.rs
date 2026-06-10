use arrow::array::{Int64Array, StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_user, entity_handler_with_partitions, global_envelope, global_handler,
    handler_context,
};

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
         VALUES ('global.User', '2024-01-21 00:00:00.000000', '{{\"c\":[\"2\"]}}')",
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

pub async fn incomplete_checkpoint_does_not_advance_watermark_on_resume(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-20 12:00:00.000000', '{{\"c\":[\"2\"]}}')",
        t("checkpoint")
    ))
    .await;

    for id in 1..=4 {
        create_user(ctx, id).await;
    }

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
    let processed: Vec<i64> = result
        .iter()
        .filter_map(|batch| ArrowUtils::get_column_by_name::<Int64Array>(batch, "id"))
        .flat_map(|ids| (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>())
        .collect();
    assert_eq!(
        processed,
        vec![3, 4],
        "an incomplete checkpoint must not advance last_watermark: users 3-4 \
         (replicated_at equal to the in-progress watermark, past the cursor) must still index"
    );
}

/// Seeds two users past the cursor (id > 2): one replicated before the floor and
/// one within `(floor, target]`. A resume must reprocess only its original window,
/// so the below-floor user is skipped. Before the floor was persisted, resume
/// rescanned from epoch and would have indexed both.
fn insert_user_at(id: i64, replicated_at: &str) -> String {
    format!(
        "INSERT INTO siphon_users \
         (id, email, username, name, state, organization_id, _siphon_replicated_at) \
         VALUES ({id}, 'u{id}@t', 'u{id}', 'User {id}', 'active', 1, '{replicated_at}')"
    )
}

pub async fn resume_is_bounded_by_window_floor(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-20 00:00:00.000000', \
                 '{{\"c\":[\"2\"],\"f\":\"2024-01-10T00:00:00Z\"}}')",
        t("checkpoint")
    ))
    .await;

    ctx.execute(&insert_user_at(3, "2024-01-05 00:00:00")).await;
    ctx.execute(&insert_user_at(4, "2024-01-15 00:00:00")).await;

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
        vec![4],
        "resume must stay within (floor, target]: user 3 (replicated before the \
         floor) is skipped; rescanning from epoch would have indexed it"
    );
}

pub async fn resume_is_bounded_by_window_floor_for_partitioned_entity(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-20 00:00:00.000000', \
                 '{{\"c\":[\"2\"],\"f\":\"2024-01-10T00:00:00Z\"}}')",
        t("checkpoint")
    ))
    .await;

    ctx.execute(&insert_user_at(3, "2024-01-05 00:00:00")).await;
    ctx.execute(&insert_user_at(4, "2024-01-15 00:00:00")).await;

    // A partition-configured entity with an in-progress parent checkpoint resumes
    // the single-pull path (it does not re-partition), and must honor the floor.
    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("partitioned handler should succeed");

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
        vec![4],
        "partitioned resume must also honor the floor"
    );
}
