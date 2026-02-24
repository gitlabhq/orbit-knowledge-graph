//! Integration subtests for the global handler (User entities).

use arrow::array::{BooleanArray, StringArray, UInt64Array};
use chrono::{DateTime, Utc};
use indexer::testkit::TestEnvelopeFactory;

use crate::common::{TestContext, create_user_payload, get_global_handler};

pub async fn processes_and_transforms_users(context: &TestContext) {
    context
        .execute(
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

    let global_handler = get_global_handler(context).await;

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_user_payload(watermark));
    let handler_context = context.create_handler_context();

    global_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_user ORDER BY id").await;

    assert!(!result.is_empty(), "result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let user_type_column = batch
        .column_by_name("user_type")
        .expect("user_type column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("user_type should be StringArray");

    assert_eq!(user_type_column.value(0), "human");
    assert_eq!(user_type_column.value(1), "support_bot");
    assert_eq!(user_type_column.value(2), "service_user");

    let is_admin_column = batch
        .column_by_name("is_admin")
        .expect("is_admin column should exist")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("is_admin should be BooleanArray");

    assert!(is_admin_column.value(0));
    assert!(!is_admin_column.value(1));
    assert!(!is_admin_column.value(2));
}

pub async fn uses_watermark_for_incremental_processing(context: &TestContext) {
    context
        .execute("INSERT INTO global_indexing_watermark (watermark) VALUES ('2024-01-19 00:00:00')")
        .await;

    context
        .execute(
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

    let global_handler = get_global_handler(context).await;

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_user_payload(watermark));
    let handler_context = context.create_handler_context();

    global_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT count() as cnt FROM gl_user").await;
    let count_array = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");

    assert_eq!(
        count_array.value(0),
        1,
        "should only process new_user, not old_user"
    );

    let usernames = context.query("SELECT username FROM gl_user").await;

    let username_array = usernames[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("username should be StringArray");

    assert_eq!(username_array.value(0), "new_user");
}
